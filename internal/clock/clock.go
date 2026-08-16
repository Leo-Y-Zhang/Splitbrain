// Package clock provides the timestamps a history is built from, and - more
// importantly - says how much to trust them.
//
// This looks like over-engineering until you measure the platform. Go's
// monotonic clock on Windows advances in steps of roughly half a millisecond,
// and a loopback HTTP round trip takes about a third of one. So a great many
// operations measure as taking no time at all, and two operations that really
// overlapped are frequently recorded as though one finished before the other
// started.
//
// That is not a rounding error, it is a fabricated fact. Real-time precedence
// is the entire constraint a linearizability checker works from: if the history
// says A finished before B started, the checker must order them that way, and
// if that never happened it will report a violation against a store that never
// committed one. A single-node store behind one mutex cannot be non-
// linearizable, so every such report is the measuring apparatus lying.
//
// Two things here fix it. The clock is read at the finest resolution the
// platform offers, which on Windows means the performance counter rather than
// the coarse system timer. And whatever remains, the granularity is measured
// rather than assumed, and completion times are widened by one tick - so the
// history only claims A preceded B when the latest instant A could have
// finished is still no later than the earliest instant B could have started.
// The residual error then always points at "concurrent", which can lose a
// violation but can never invent one.
package clock

import (
	"fmt"
	"time"
)

// A Clock produces the monotonic timestamps of one run.
//
// It is safe for concurrent use: reading the underlying counter is, and
// nothing here is written after construction.
type Clock struct {
	origin      int64
	granularity int64
	source      string
}

// New starts a clock and measures its resolution.
//
// Measuring costs a millisecond or two and happens once per run, which is
// nothing against the alternative of guessing.
func New() *Clock {
	g, source := measure()
	return &Clock{origin: rawNanos(), granularity: g, source: source}
}

// Nanos returns nanoseconds since the clock was created.
func (c *Clock) Nanos() int64 { return rawNanos() - c.origin }

// Granularity is the smallest step this clock was observed to take, in
// nanoseconds. It is never zero.
func (c *Clock) Granularity() int64 { return c.granularity }

// Source names where the timestamps come from, for the run report.
func (c *Clock) Source() string { return c.source }

// Completion converts a measured completion into the one to record.
//
// It adds a tick. A reading of T means the true instant lies somewhere in
// [T, T+granularity), so the latest moment an operation could have finished is
// its reading plus one tick. Recording that, and leaving invocations as read,
// makes "A completed at or before B was invoked" a claim the measurement
// actually supports. Anything less is the harness inventing an ordering.
//
// It also guarantees a strictly positive duration, which matters on its own:
// a zero-width interval makes an operation both precede and follow everything
// at the same instant, and a checker handed that contradiction has no honest
// answer.
func (c *Clock) Completion(invoke, measured int64) int64 {
	if measured < invoke {
		measured = invoke
	}
	return measured + c.granularity
}

// String describes the clock for a run report.
func (c *Clock) String() string {
	return fmt.Sprintf("%s, %v granularity", c.source, time.Duration(c.granularity))
}

// measure finds the smallest step the raw clock takes.
//
// It waits for the counter to change, several times over, and keeps the
// smallest change seen. Taking the minimum rather than the mean is deliberate:
// the mean includes however long this goroutine was descheduled, and the
// quantity wanted is the clock's own step, not the scheduler's.
func measure() (int64, string) {
	const trials = 64
	best := int64(1) << 62
	for i := 0; i < trials; i++ {
		t0 := rawNanos()
		var t1 int64
		// Bounded so a pathological clock cannot hang a run at startup.
		for spins := 0; spins < 50_000_000; spins++ {
			t1 = rawNanos()
			if t1 != t0 {
				break
			}
		}
		if d := t1 - t0; d > 0 && d < best {
			best = d
		}
	}
	if best <= 0 || best == int64(1)<<62 {
		// The clock never moved, or moved backwards. Neither should happen;
		// claim a microsecond rather than zero, because zero would silently
		// switch the widening off.
		return 1000, rawSource + " (resolution not measurable)"
	}
	return best, rawSource
}
