// Package harness drives a real cluster through a seeded fault schedule and
// records what the clients saw.
//
// It is deliberately ignorant of what it is testing. It knows how to open TCP
// connections through a fault proxy, how to generate operations, and how to
// write down exactly what came back - including the cases where nothing came
// back. Whether the answers were correct is not its business; that question
// belongs to the checker, and keeping the two apart is what stops the harness
// from quietly excusing the system under test.
package harness

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/clock"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/faultnet"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/kv"
)

// Config describes one run. The topology - which nodes exist and which hops
// are proxied - is passed separately, because the caller has to build it
// before it can tell the cluster what addresses to use for peer traffic.
type Config struct {
	// Clients is the number of concurrent client processes. They are spread
	// round-robin across the nodes, which is what makes a partition visible
	// at all: point every client at one node and you are testing a single
	// server with extra steps.
	Clients int

	// Keys is how many independent registers the clients share. More keys
	// means more operations before the checker's per-key search gets
	// expensive, but fewer operations competing over any one register, so
	// fewer chances to catch a violation on it.
	Keys int

	// Duration is how long the fault schedule and the client load run for.
	Duration time.Duration

	// MaxOps caps the total number of operations. Zero means no cap. The cap
	// exists because linearizability checking is exponential in the worst
	// case: an unbounded run produces a history nobody can check.
	MaxOps int

	// OpTimeout bounds one client operation. It must be short relative to
	// Duration, or a blackholed client spends the whole run inside one call
	// and the history has nothing in it.
	OpTimeout time.Duration

	// ThinkMax is the upper bound on the random pause between one client's
	// operations. A little jitter is what produces overlapping intervals, and
	// overlapping intervals are the only reason the checker has work to do.
	ThinkMax time.Duration

	// ValueMax is the largest value clients write. A small domain makes
	// coincidental agreement likely and violations hard to see; a large one
	// makes almost every value unique and almost every read decisive. It must
	// be at least minValueMax; a domain of one value is not a small domain but
	// an impossible one.
	ValueMax int

	// Faults names the schedule: "none", "partition", or any kind understood
	// by faultnet.Generate.
	Faults string

	// Seed drives both the fault schedule and the operation generator.
	Seed int64

	// Quiesce is how long to wait after healing before the final round of
	// reads. Those reads are the most informative in the history: with no
	// faults in force and nothing else running, every node is expected to
	// answer, and a disagreement between them has no concurrency left to
	// excuse it.
	Quiesce time.Duration

	// KeepAlive lets client connections be reused between operations. Off by
	// default, because a pooled connection can carry a request straight past
	// a partition that was applied after it was opened.
	KeepAlive bool
}

// WithDefaults fills in the values a caller can reasonably leave blank.
func (c Config) WithDefaults() Config {
	if c.Clients <= 0 {
		c.Clients = 6
	}
	if c.Keys <= 0 {
		c.Keys = 4
	}
	if c.Duration <= 0 {
		c.Duration = 8 * time.Second
	}
	if c.OpTimeout <= 0 {
		c.OpTimeout = 400 * time.Millisecond
	}
	if c.ThinkMax <= 0 {
		c.ThinkMax = 8 * time.Millisecond
	}
	if c.ValueMax <= 0 {
		c.ValueMax = 1_000_000
	}
	if c.Faults == "" {
		c.Faults = "partition"
	}
	if c.Quiesce <= 0 {
		c.Quiesce = 750 * time.Millisecond
	}
	return c
}

// Result is everything one run produced.
type Result struct {
	// History is what the clients observed, ready for the checker.
	History history.History

	// Schedule is the fault timeline that was planned; Applied is what
	// actually fired. They differ when a run is cut short.
	Schedule *faultnet.Schedule
	Applied  []faultnet.Event

	// LinkStats is per-link traffic, which is the cheapest way to notice that
	// a run did nothing: a partition schedule that dropped no bytes did not
	// happen.
	LinkStats map[string]faultnet.LinkStats

	// ProcessNode records which node each client process was talking to. The
	// history itself has no place for this - a linearizable store gives the
	// same answers whichever node you ask - but it is what makes a
	// counterexample readable.
	ProcessNode map[int]string

	// Clock describes where the timestamps came from and how precise they
	// are. It belongs in the report: every real-time constraint the checker
	// applies is only as trustworthy as this, and a coarse clock invents
	// orderings that produce violations no store committed.
	Clock string

	Elapsed time.Duration
}

// Keys returns the key names a run of this shape uses.
func Keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("k%d", i)
	}
	return out
}

// BuildSchedule produces the fault timeline for a run.
func BuildSchedule(t *Topology, cfg Config) (*faultnet.Schedule, error) {
	cfg = cfg.WithDefaults()
	switch cfg.Faults {
	case "none":
		return faultnet.Healthy(t.LinkNames(), cfg.Seed, cfg.Duration), nil
	case "partition":
		return PartitionSchedule(t, cfg.Seed, cfg.Duration, faultnet.Drop)
	case "refuse":
		// The same node-level partitions, but the nodes refuse connections
		// instead of swallowing them. Clients then get definite failures
		// rather than indeterminate ones, which is a much easier history to
		// check - and a much less realistic one.
		return PartitionSchedule(t, cfg.Seed, cfg.Duration, faultnet.Refuse)
	default:
		return faultnet.Generate(cfg.Faults, t.LinkNames(), cfg.Seed, cfg.Duration)
	}
}

// Run executes one campaign against the cluster behind t and returns what the
// clients saw.
//
// It returns an error only for setup failures. A run in which every operation
// timed out is a successful run with a boring history, not a failure of the
// harness - and telling those two apart is the caller's job, using LinkStats
// and the outcome counts.
func Run(ctx context.Context, t *Topology, cfg Config) (*Result, error) {
	cfg = cfg.WithDefaults()
	if t == nil || t.Nodes() == 0 {
		return nil, fmt.Errorf("harness: empty topology")
	}
	if cfg.ValueMax < minValueMax {
		return nil, fmt.Errorf("harness: value-max is %d; it must be at least %d, because a compare-and-swap "+
			"needs two different values to move between", cfg.ValueMax, minValueMax)
	}
	if cfg.Clients < t.Nodes() {
		// Clients are spread round-robin, so fewer clients than nodes leaves
		// some node with nobody talking to it. The partition still cuts that
		// node's peer links, the counters still show bytes dropped somewhere,
		// and the run still returns a verdict - about hops that carried
		// nothing. Measured: one client against three nodes gives five clean
		// seeds in which the two followers answered four requests each, all of
		// them after the network had healed.
		return nil, fmt.Errorf("harness: %d client(s) across %d nodes; there must be at least one per node, "+
			"or a partition cuts hops that were carrying no traffic and the clean verdict means nothing",
			cfg.Clients, t.Nodes())
	}
	if cfg.ValueMax < minValueMax {
		// Refusing rather than hanging. A compare-and-swap has to move the
		// register to a different value - a no-op CAS constrains nothing, and
		// history.Validate rejects one - and generate finds that value by
		// retrying. On a domain of one value there is nothing to retry towards,
		// so the loop never ends. It would not end at the start of the run
		// either, but the first time a client came to believe a key held the
		// only value there is, at which point the run stops making progress,
		// ignores Duration, and leaves its node processes behind.
		return nil, fmt.Errorf("harness: value-max is %d; it must be at least %d, because a compare-and-swap needs two different values to move between",
			cfg.ValueMax, minValueMax)
	}

	sched, err := BuildSchedule(t, cfg)
	if err != nil {
		return nil, fmt.Errorf("harness: %w", err)
	}
	nem := faultnet.NewNemesis(t.Links(), sched)

	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var nemDone sync.WaitGroup
	nemDone.Add(1)
	go func() {
		defer nemDone.Done()
		nem.Run(runCtx)
	}()

	// One clock for the whole run, read by every client goroutine. Its
	// resolution is measured rather than assumed, because on some platforms it
	// is coarser than the operations being timed.
	clk := clock.New()
	start := time.Now()
	rec := &recorder{}
	pn := &processNodes{m: map[int]string{}}

	// Process identifiers start at zero for the initial clients and climb from
	// there. A client that suffers an indeterminate operation must retire its
	// identifier and take a fresh one: a process with a request that may still
	// be in flight cannot claim its next operation happened after the previous
	// one, and the history's own validator enforces exactly that.
	nextProc := &atomic.Int64{}
	nextProc.Store(int64(cfg.Clients))

	var opCount atomic.Int64
	keys := Keys(cfg.Keys)

	var clients sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		nodeIdx := i % t.Nodes()
		clients.Add(1)
		go func(clientIdx, nodeIdx, proc int) {
			defer clients.Done()
			nodeName := clientLinkName(nodeIdx)
			pn.set(proc, nodeName)

			// Each client gets its own generator seeded from the run seed.
			// That is not enough to replay the workload and the comment here
			// used to say it was: the generator reads what this client last
			// observed, which is whatever another client wrote a moment
			// earlier, and it short-circuits on that value before drawing, so
			// two runs of the same seed diverge within a few dozen operations
			// and then stay diverged. Measured. The seed replays the fault
			// schedule; nothing replays the run.
			rng := rand.New(rand.NewPCG(uint64(cfg.Seed), uint64(0x9E3779B9+clientIdx)))
			c := kv.NewClientWithOptions(t.ClientAddr(nodeIdx), cfg.OpTimeout,
				kv.ClientOptions{KeepAlive: cfg.KeepAlive})
			believed := map[string]int{}

			for {
				if runCtx.Err() != nil {
					return
				}
				if cfg.MaxOps > 0 && opCount.Load() >= int64(cfg.MaxOps) {
					return
				}
				opCount.Add(1)

				op := c.Do(runCtx, clk, proc, generate(rng, keys, believed, cfg.ValueMax))
				rec.add(op)

				switch {
				case op.Outcome == history.Info:
					// This process can never issue another operation.
					delete(believed, op.Key)
					proc = int(nextProc.Add(1))
					pn.set(proc, nodeName)
				case op.Outcome == history.OK && op.Kind == history.Read:
					believed[op.Key] = op.Observed
				case op.Outcome == history.OK && op.Kind == history.Write:
					believed[op.Key] = op.Value
				case op.Outcome == history.OK && op.Kind == history.CAS && op.Swapped:
					believed[op.Key] = op.To
				}

				if cfg.ThinkMax > 0 {
					sleep(runCtx, time.Duration(rng.Int64N(int64(cfg.ThinkMax)+1)))
				}
			}
		}(i, nodeIdx, i)
	}

	clients.Wait()
	cancel()
	nemDone.Wait()
	nem.Heal()

	// The quiescent phase: network whole, no other load. A disagreement
	// between nodes here cannot be blamed on concurrency.
	if cfg.Quiesce > 0 {
		settle := context.Background()
		sleep(settle, cfg.Quiesce)
		for i := 0; i < t.Nodes(); i++ {
			nodeName := clientLinkName(i)
			proc := int(nextProc.Add(1))
			pn.set(proc, nodeName)

			// A fresh connection per operation, deliberately: the final reads
			// must not be answered over a socket that predates the heal.
			c := kv.NewClient(t.ClientAddr(i), cfg.OpTimeout)
			for _, k := range keys {
				op := c.Do(settle, clk, proc, kv.Request{Kind: history.Read, Key: k})
				rec.add(op)
				if op.Outcome == history.Info {
					proc = int(nextProc.Add(1))
					pn.set(proc, nodeName)
				}
			}
		}
	}

	h := rec.take()
	h.SortByInvoke()

	return &Result{
		History:     h,
		Schedule:    sched,
		Applied:     nem.Applied(),
		LinkStats:   t.Stats(),
		ProcessNode: pn.snapshot(),
		Clock:       clk.String(),
		Elapsed:     time.Since(start),
	}, nil
}

// minValueMax is the smallest value domain a run can use: a compare-and-swap
// has to move the register between two different values, and generate finds the
// second one by retrying, so one value is not a small domain but an impossible
// one. Run refuses anything below this.
const minValueMax = 2

// generate picks the next operation for a client.
//
// The mix matters more than it looks. Reads and writes alone catch surprisingly
// little, because almost any value a broken store returns can be excused by
// some ordering of the concurrent writes. Compare-and-swap is the operation
// that pins things down: a store that reports it swapped is asserting the
// register held one exact value at one instant, and two nodes cannot both be
// right about that. Using the client's own last observation as the expected
// value most of the time is what makes those swaps succeed often enough to
// matter, instead of failing harmlessly against a value nobody ever wrote.
func generate(rng *rand.Rand, keys []string, believed map[string]int, valueMax int) kv.Request {
	key := keys[rng.IntN(len(keys))]
	switch n := rng.IntN(100); {
	case n < 35:
		return kv.Request{Kind: history.Read, Key: key}
	case n < 70:
		return kv.Request{Kind: history.Write, Key: key, Value: 1 + rng.IntN(valueMax)}
	default:
		from, seen := believed[key]
		if !seen || rng.IntN(10) == 0 {
			from = rng.IntN(valueMax)
		}
		to := 1 + rng.IntN(valueMax)
		for to == from {
			to = 1 + rng.IntN(valueMax)
		}
		return kv.Request{Kind: history.CAS, Key: key, From: from, To: to}
	}
}

// sleep waits for d, or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// recorder collects operations from every client goroutine.
type recorder struct {
	mu  sync.Mutex
	ops history.History
}

func (r *recorder) add(op history.Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recorder) take() history.History {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.ops
	r.ops = nil
	return out
}

// processNodes records which link each process talked through.
type processNodes struct {
	mu sync.Mutex
	m  map[int]string
}

func (p *processNodes) set(proc int, node string) {
	p.mu.Lock()
	p.m[proc] = node
	p.mu.Unlock()
}

func (p *processNodes) snapshot() map[int]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[int]string, len(p.m))
	for k, v := range p.m {
		out[k] = v
	}
	return out
}

// Summary is a one-line account of a run, for the command line.
func (r *Result) Summary() string {
	ok, fail, info := r.History.Counts()
	var dropped, fwd int64
	names := make([]string, 0, len(r.LinkStats))
	for name, s := range r.LinkStats {
		dropped += s.BytesDropped
		fwd += s.BytesForward
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Sprintf(
		"%d operations in %s: %d ok, %d failed, %d indeterminate; %d keys; %d fault events; %d bytes forwarded, %d dropped; clock %s",
		len(r.History), r.Elapsed.Round(time.Millisecond), ok, fail, info,
		len(r.History.Keys()), len(r.Applied), fwd, dropped, r.Clock)
}
