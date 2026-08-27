package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// fanoutQueue is how many pending updates a node will hold for one peer before
// it starts dropping them.
//
// Dropping is not a bug to be fixed later. Replication here is best-effort by
// design, and a queue that grew without limit would turn a partitioned peer
// into an out-of-memory failure, which is a different and much less
// interesting way to be wrong.
const fanoutQueue = 256

// QuorumStore is deliberately wrong.
//
// Each of the three nodes keeps its own map. A write is applied to the local
// map and then handed to a background fan-out that posts it to the other two,
// best-effort, with no acknowledgement and no retry. A read is answered from
// the local map and never consults anybody. Conflicting updates are resolved
// last-writer-wins on a wall-clock timestamp, tie-broken by node id.
//
// The consequence is that it stays fully available under a partition and it is
// not linearizable. A write that has completed at one node is invisible at
// another until the update happens to arrive, so a read issued strictly after
// that write can legitimately return the old value. That is a real-time
// violation, and it is reachable in a few milliseconds without any fault
// injection at all.
//
// It is a fixture, not a strawman someone shipped. It is a faithful model of
// the design people reach for before they have met consensus - local writes,
// gossip the change, resolve conflicts by timestamp - and every part of it is
// individually reasonable. The point of running the checker against it is to
// see a concrete counterexample for a system that looks fine in every manual
// test, not to mock the design. Its last-writer-wins rule is genuinely
// convergent: given enough quiet time the nodes agree. What it cannot offer is
// any guarantee about what a read sees before then.
//
// # Synchronous mode
//
// With synchronous replication switched on, a write waits for every peer
// before it replies - and then replies ok whether or not any of them answered.
// That is a second, subtler wrong design, and a much more common one: teams
// reach for it precisely because they do not want a write to fail just because
// one replica is down.
//
// The reason it is worth having is that it changes when the store breaks. In
// the default asynchronous mode a stale read is available on a perfectly
// healthy network, within milliseconds, so a run that finds a violation has
// not demonstrated anything about the fault injection. In synchronous mode a
// healthy network delivers the value to every replica before the client is
// told the write finished, so reads are current and the history usually
// linearizes; break the network and the far side never gets the update, the
// write says ok regardless, and the far side then serves a stale value to a
// read that strictly follows. That is a control group.
//
// Synchronous mode does not make the store correct, and it is not meant to.
// Nothing coordinates the nodes, so two compare-and-swaps issued against
// different nodes at the same moment can both report that they swapped even on
// a healthy network. It makes the store correct often enough that a partition
// is the thing that reliably breaks it, which is all a control group has to be.
type QuorumStore struct {
	id      string
	timeout time.Duration
	log     *slog.Logger

	mu sync.Mutex
	// synchronous makes a write wait for its peers before replying. It still
	// replies ok when they did not answer; see the note above.
	synchronous bool
	data        map[string]entry
	fan         *fanout
	closed      bool
}

// entry is one register's value together with the version that produced it.
type entry struct {
	value int
	stamp int64
	node  string
}

// dominates reports whether e should replace o under last-writer-wins. The
// node id breaks a tie so that every node resolves it the same way; without
// it, two updates stamped the same nanosecond would settle differently
// depending on the order each node happened to receive them, and the store
// would never converge at all.
func (e entry) dominates(o entry) bool {
	if e.stamp != o.stamp {
		return e.stamp > o.stamp
	}
	return e.node > o.node
}

// NewQuorumStore returns a node with its own map, replicating to peers.
//
// timeout bounds each outbound replication end to end. The peer addresses are
// usually fault proxies rather than the nodes themselves, and a blackholed
// proxy accepts a connection and then says nothing for ever, so a bound on the
// whole request rather than only on the dial is what stops a fan-out worker
// being lost for the rest of the run. A nil logger discards.
func NewQuorumStore(id string, peers []string, timeout time.Duration, logger *slog.Logger) *QuorumStore {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &QuorumStore{
		id:      id,
		timeout: timeout,
		log:     logger,
		data:    map[string]entry{},
		fan:     newFanout(id, peers, timeout, logger),
	}
}

// Apply performs one operation against the local map and returns at once.
//
// It never returns an error and never waits for a peer. That is the whole
// bargain: the node answers from what it knows, which is always something,
// even when it is out of date.
func (s *QuorumStore) Apply(ctx context.Context, req Request) (Response, error) {
	switch req.Kind {
	case history.Write:
		s.mu.Lock()
		e := entry{value: req.Value, stamp: s.stampFor(req.Key), node: s.id}
		s.data[req.Key] = e
		fan, synchronous := s.fan, s.synchronous
		s.mu.Unlock()
		s.replicate(ctx, fan, synchronous, replication{Key: req.Key, Value: e.value, Stamp: e.stamp, Node: e.node})
		return WriteOK(), nil

	case history.CAS:
		s.mu.Lock()
		if s.get(req.Key).value != req.From {
			s.mu.Unlock()
			return CASOK(false), nil
		}
		e := entry{value: req.To, stamp: s.stampFor(req.Key), node: s.id}
		s.data[req.Key] = e
		fan, synchronous := s.fan, s.synchronous
		s.mu.Unlock()
		s.replicate(ctx, fan, synchronous, replication{Key: req.Key, Value: e.value, Stamp: e.stamp, Node: e.node})
		return CASOK(true), nil

	default:
		// Reads are local and uncoordinated in both modes. This is the line
		// that makes the store interesting, and switching on synchronous
		// replication does not change it.
		s.mu.Lock()
		defer s.mu.Unlock()
		return ReadOK(s.get(req.Key).value), nil
	}
}

// replicate hands an update to the peers, waiting for them only in synchronous
// mode. Either way the caller goes on to report success, which in synchronous
// mode is the deliberate bug.
func (s *QuorumStore) replicate(ctx context.Context, fan *fanout, synchronous bool, u replication) {
	if synchronous {
		fan.broadcast(ctx, u)
		return
	}
	fan.send(u)
}

// Configure replaces the peer list, stopping the previous fan-out first. An
// empty list isolates the node, which is how a harness can partition one
// without touching the network.
func (s *QuorumStore) Configure(cfg Config) error {
	// Checked before anything is applied, so a configuration this node refuses
	// leaves it exactly as it was, sync mode included. The addresses arrive
	// over an unauthenticated endpoint and decide where this process sends
	// traffic; newFanout ignores empty entries, so this does too.
	if cfg.Peers != nil {
		for _, addr := range *cfg.Peers {
			if addr == "" {
				continue
			}
			if _, err := peerBase(addr); err != nil {
				return err
			}
		}
	}
	if cfg.Sync != nil {
		s.mu.Lock()
		s.synchronous = *cfg.Sync
		s.mu.Unlock()
	}
	if cfg.Peers == nil {
		return nil
	}
	next := newFanout(s.id, *cfg.Peers, s.timeout, s.log)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		next.close()
		return fmt.Errorf("node %s is shutting down", s.id)
	}
	prev := s.fan
	s.fan = next
	s.mu.Unlock()

	// Outside the lock: closing waits for the old workers to stop, and they
	// may be mid-request to a peer that is not answering.
	prev.close()
	return nil
}

// Close stops the fan-out and waits for it. It is safe to call more than once,
// and the store keeps answering afterwards so that a request still in flight
// when the listener closes does not panic.
func (s *QuorumStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	fan := s.fan
	s.mu.Unlock()

	fan.close()
	return nil
}

// Routes adds the peer-to-peer endpoint. It is not part of the client
// protocol; only other nodes call it.
func (s *QuorumStore) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /replicate", s.handleReplicate)
}

// get reads a key, supplying the initial value for one never written. The
// caller holds the lock.
func (s *QuorumStore) get(key string) entry {
	return s.data[key]
}

// stampFor returns the timestamp for an update this node is generating. The
// caller holds the lock.
//
// It is the wall clock, except that it is never allowed to be behind or equal
// to a stamp this node already holds for the key. Without that clamp a node
// whose clock had drifted behind a peer's would have to either discard its own
// write after reporting success, or break the last-writer-wins rule; the first
// is a lie to the client and the second stops the nodes converging. Clamping
// is a small dishonesty about the clock in exchange for both.
func (s *QuorumStore) stampFor(key string) int64 {
	stamp := time.Now().UnixNano()
	if held, ok := s.data[key]; ok && stamp <= held.stamp {
		stamp = held.stamp + 1
	}
	return stamp
}

// replication is one update on its way to a peer.
type replication struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
	Stamp int64  `json:"timestamp"`
	Node  string `json:"node"`
}

func (s *QuorumStore) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var u replication
	if err := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytes)).Decode(&u); err != nil {
		writeJSON(w, http.StatusOK, Declined("malformed replication: %v", err))
		return
	}
	if u.Key == "" || u.Node == "" {
		writeJSON(w, http.StatusOK, Declined("a replication needs both a key and a node"))
		return
	}

	applied := s.applyReplication(u)
	s.log.Info("replicate", "node", s.id, "from", u.Node, "key", u.Key,
		"value", u.Value, "stamp", u.Stamp, "applied", applied)
	writeJSON(w, http.StatusOK, Response{OK: true})
}

// applyReplication takes a peer's update only if it beats what is held.
func (s *QuorumStore) applyReplication(u replication) bool {
	next := entry{value: u.Value, stamp: u.Stamp, node: u.Node}
	s.mu.Lock()
	defer s.mu.Unlock()
	if held, ok := s.data[u.Key]; ok && !next.dominates(held) {
		return false
	}
	s.data[u.Key] = next
	return true
}

// fanout delivers updates to peers in the background.
//
// One worker per peer, each with a bounded queue, so the number of goroutines
// is fixed by the configuration and cannot grow with the number of operations
// however long a peer stays unreachable.
type fanout struct {
	id     string
	log    *slog.Logger
	queues []*peerQueue
	wg     sync.WaitGroup

	// ctx is cancelled by close, which aborts any request already in flight.
	// Without it, closing would wait out the peer timeout on every worker
	// that happened to be talking to a blackholed proxy, and a node that is
	// slow to exit holds its port and poisons the next run.
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
}

type peerQueue struct {
	client *Client
	ch     chan replication
}

func newFanout(id string, peers []string, timeout time.Duration, log *slog.Logger) *fanout {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fanout{id: id, log: log, ctx: ctx, cancel: cancel}
	for _, addr := range peers {
		if addr == "" {
			continue
		}
		q := &peerQueue{client: NewClient(addr, timeout), ch: make(chan replication, fanoutQueue)}
		f.queues = append(f.queues, q)
		f.wg.Add(1)
		go f.pump(q)
	}
	return f
}

// send hands an update to every peer's queue without blocking.
//
// A full queue means a peer has been unreachable for a while; the update is
// dropped and logged. That is the honest behaviour for best-effort
// replication, and it is one of the reasons this store is not linearizable.
func (f *fanout) send(u replication) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	for _, q := range f.queues {
		select {
		case q.ch <- u:
		default:
			f.log.Warn("dropped a replication", "node", f.id, "peer", q.client.Addr(),
				"key", u.Key, "queued", len(q.ch))
		}
	}
}

// broadcast posts an update to every peer in parallel and waits for all of
// them, each bounded by the client's timeout.
//
// The caller reports success whatever this returns. That is the point: a store
// that waited for its replicas and then ignored what they said is the design
// this mode exists to model, and it is the reason a partition, rather than
// ordinary asynchrony, is what breaks the store in synchronous mode.
func (f *fanout) broadcast(ctx context.Context, u replication) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	// The slice is never mutated after construction, so it is safe to use
	// outside the lock; a reconfiguration builds a whole new fanout.
	queues := f.queues
	f.mu.Unlock()

	var wg sync.WaitGroup
	for _, q := range queues {
		wg.Add(1)
		go func(q *peerQueue) {
			defer wg.Done()
			if err := q.client.postJSON(ctx, "/replicate", u); err != nil {
				f.log.Warn("synchronous replication failed but the write will still be reported as successful",
					"node", f.id, "peer", q.client.Addr(), "key", u.Key, "error", err)
			}
		}(q)
	}
	wg.Wait()
}

func (f *fanout) pump(q *peerQueue) {
	defer f.wg.Done()
	for u := range q.ch {
		if f.ctx.Err() != nil {
			// Shutting down. Drain the rest without sending, so the worker
			// finishes promptly rather than failing one request per update.
			continue
		}
		if err := q.client.postJSON(f.ctx, "/replicate", u); err != nil {
			f.log.Warn("replication failed", "node", f.id, "peer", q.client.Addr(),
				"key", u.Key, "error", err)
		}
	}
}

// close stops every worker and waits for them. It is idempotent.
func (f *fanout) close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	f.mu.Unlock()

	f.cancel()
	for _, q := range f.queues {
		close(q.ch)
	}
	f.wg.Wait()
}

var _ Store = (*QuorumStore)(nil)
var _ Router = (*QuorumStore)(nil)
