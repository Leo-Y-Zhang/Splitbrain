package kv

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// defaultPromoteAfter is how many consecutive failed forwards it takes to
// promote a follower, when promotion is switched on at all.
const defaultPromoteAfter = 3

// ForwardStore is one node of a three-node store with a fixed leader.
//
// The leader holds the only copy of the data. A follower holds nothing at all
// and forwards every operation, so every operation in the system serialises at
// one mutex in one process and the arrangement is linearizable for the same
// reason SingleStore is. What it buys over SingleStore is a partition to
// break: cut a follower off from the leader and that follower stops being able
// to answer. It gives up availability and keeps consistency, which is the CP
// corner, and the visible symptom is errors and timeouts rather than wrong
// values.
//
// In its default configuration a follower must never answer from anything of
// its own. It has nothing of its own, and a fallback to a local map is
// precisely the bug this fixture exists to make unattractive.
//
// # Promotion
//
// With promotion switched on, that bug is put in deliberately, and it is the
// one the repository is named after. A follower caches every value it sees go
// past on its way to and from the leader. It counts consecutive failed
// forwards, and after a threshold it declares itself leader and starts serving
// clients from that cache - reads, writes and compare-and-swap alike, all
// answering ok as though nothing were wrong. It demotes again as soon as a
// forward succeeds, and the writes it accepted while promoted are simply lost.
//
// Nothing about that is subtle once written down, and yet it is what a retry
// count plus a failover routine amounts to in a great many real systems. The
// contrast is the point. On a healthy network the leader is always reachable,
// no node ever promotes, and this is the ordinary linearizable kvforward.
// Break the network and the isolated side promotes, both halves serve clients,
// and they disagree - a violation that only exists because of the fault.
//
// A real system does not prevent this with a longer timeout or a larger retry
// count. It prevents it by requiring a quorum before anyone may lead, or by
// fencing: the promoted node carries a token the storage layer checks, so the
// old leader's writes are rejected the moment a new one is anointed. A retry
// count cannot distinguish "the leader is dead" from "I cannot see the
// leader", and those need opposite responses.
type ForwardStore struct {
	timeout time.Duration
	log     *slog.Logger

	// mu guards everything below. /configure can change the role while
	// requests are in flight, because the harness assigns addresses after the
	// nodes are up, and the promotion state changes on the request path.
	mu     sync.RWMutex
	leader *Client      // nil when this node is itself the leader
	local  *SingleStore // the data, used only while leader is nil

	promote      bool
	promoteAfter int
	failures     int
	promoted     bool
	cache        map[string]int
}

// NewForwardStore returns a node that forwards to leaderAddr, or, when
// leaderAddr is empty, a node that is itself the leader and holds the data.
// timeout bounds each forwarded request end to end, which matters because the
// address it is given is a fault proxy that can stop forwarding at any moment.
// A nil logger discards.
func NewForwardStore(leaderAddr string, timeout time.Duration, logger *slog.Logger) *ForwardStore {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &ForwardStore{
		timeout:      timeout,
		log:          logger,
		local:        NewSingleStore(),
		promoteAfter: defaultPromoteAfter,
		cache:        map[string]int{},
	}
	if leaderAddr != "" {
		s.leader = NewClient(leaderAddr, timeout)
	}
	return s
}

// Apply answers locally when this node is the leader, and forwards otherwise.
func (s *ForwardStore) Apply(ctx context.Context, req Request) (Response, error) {
	s.mu.RLock()
	leader, local, promoted := s.leader, s.local, s.promoted
	s.mu.RUnlock()

	if leader == nil {
		return local.Apply(ctx, req)
	}
	if promoted {
		// Already serving on its own. It still tries the leader first, so
		// that it notices the moment the partition heals.
		if resp, err := leader.Send(ctx, req); err == nil {
			s.forwardSucceeded(req, resp)
			return resp, nil
		} else {
			s.forwardFailed(err)
		}
		return s.serveFromCache(req), nil
	}

	resp, err := leader.Send(ctx, req)
	switch {
	case err == nil:
		// The leader reached a decision, whatever it was. Pass it through
		// untouched, including a refusal: it is the leader's refusal, not the
		// follower's, and the follower has nothing to add to it.
		s.forwardSucceeded(req, resp)
		return resp, nil

	case isNeverSent(err):
		// The dial was refused, so nothing was delivered and the leader
		// cannot have applied anything. This is the only branch in which a
		// follower may state a definite failure.
		if s.forwardFailed(err) {
			return s.serveFromCache(req), nil
		}
		return Declined("leader %s refused the connection", leader.Addr()), nil

	default:
		// Everything else - a timeout, a reset, a truncated or unparseable
		// reply, a non-200 from the leader - leaves the follower genuinely
		// ignorant. The leader may have applied the operation and then failed
		// to say so. Answering ok:false here would be a lie that the checker
		// believes: it would drop the operation from the history, and a later
		// read that legitimately observes the write would be reported as a
		// violation of a store that is in fact perfectly consistent. 504
		// carries the ignorance out to the client as history.Info.
		if s.forwardFailed(err) {
			return s.serveFromCache(req), nil
		}
		return Response{}, Indeterminate(http.StatusGatewayTimeout,
			"no answer from leader %s: %v", leader.Addr(), err)
	}
}

// forwardSucceeded records what the leader said and clears the failure count.
// Caching only happens when promotion is enabled; a plain follower keeps
// nothing, so that it cannot answer from stale data even by accident.
func (s *ForwardStore) forwardSucceeded(req Request, resp Response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = 0
	if s.promoted {
		s.promoted = false
		s.log.Warn("demoted: the leader is reachable again, and anything written while promoted is lost",
			"key", req.Key)
	}
	if !s.promote || !resp.OK {
		return
	}
	switch req.Kind {
	case history.Read:
		if resp.Value != nil {
			s.cache[req.Key] = *resp.Value
		}
	case history.Write:
		s.cache[req.Key] = req.Value
	case history.CAS:
		if resp.Swapped != nil && *resp.Swapped {
			s.cache[req.Key] = req.To
		}
	}
}

// forwardFailed counts one failed forward and reports whether this node is now
// serving on its own.
func (s *ForwardStore) forwardFailed(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures++
	if !s.promote || s.promoted || s.failures < s.promoteAfter {
		return s.promoted
	}
	s.promoted = true
	s.log.Warn("promoted: serving from a local cache after consecutive failed forwards; "+
		"if the leader is still alive on the other side of the partition, both halves are now leaders",
		"failures", s.failures, "error", err)
	return true
}

// serveFromCache answers as though this node were the leader. Every reply is
// ok, which is the whole bug: the client cannot tell it is talking to a node
// that lost contact and appointed itself.
func (s *ForwardStore) serveFromCache(req Request) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	held, ok := s.cache[req.Key]
	if !ok {
		held = model.InitialValue
	}
	switch req.Kind {
	case history.Write:
		s.cache[req.Key] = req.Value
		return WriteOK()
	case history.CAS:
		if held != req.From {
			return CASOK(false)
		}
		s.cache[req.Key] = req.To
		return CASOK(true)
	default:
		return ReadOK(held)
	}
}

// Promoted reports whether this node has appointed itself leader. It exists so
// a test can assert that a healthy run never promotes, which is a stronger
// claim than the answers merely being right.
func (s *ForwardStore) Promoted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.promoted
}

// Configure switches this node between leader and follower, and sets the
// promotion behaviour. An empty leader address promotes it; any other address
// makes it forward there.
func (s *ForwardStore) Configure(cfg Config) error {
	if cfg.PromoteAfter != nil && *cfg.PromoteAfter < 1 {
		return errors.New("promote-after must be at least 1")
	}
	// Checked before anything is applied, so a configuration this node refuses
	// leaves it exactly as it was. The address arrives over an unauthenticated
	// endpoint and decides where this process sends traffic, so it is worth
	// refusing by name rather than discovering at the first forward.
	if cfg.Leader != nil && *cfg.Leader != "" {
		if _, err := peerBase(*cfg.Leader); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.Promote != nil {
		s.promote = *cfg.Promote
		if !s.promote {
			// Switching promotion off puts the node back to being an ordinary
			// follower at once, cache and all, rather than at the next
			// successful forward.
			s.promoted = false
			s.failures = 0
			clear(s.cache)
		}
	}
	if cfg.PromoteAfter != nil {
		s.promoteAfter = *cfg.PromoteAfter
	}
	if cfg.Leader != nil {
		if *cfg.Leader == "" {
			s.leader = nil
		} else {
			s.leader = NewClient(*cfg.Leader, s.timeout)
		}
		s.promoted = false
		s.failures = 0
	}
	return nil
}

// Close does nothing; forwarding is synchronous and starts no background work.
func (s *ForwardStore) Close() error { return nil }

// isNeverSent reports whether err proves the request was never delivered. It
// is the same three-way rule the client applies, one hop further in.
func isNeverSent(err error) bool {
	var te *TransportError
	return errors.As(err, &te) && te.NeverSent
}

var _ Store = (*ForwardStore)(nil)
