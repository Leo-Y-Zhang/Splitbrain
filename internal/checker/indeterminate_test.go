package checker

import (
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// The histories in this file are the ones a fault-injecting run actually
// produces, and they are the ones the search used to give up on.
//
// Measured on a captured 4008-operation kvsingle run under `-faults chaos`:
// eight keys, 483 to 528 operations each, but a maximum concurrency of only
// four or five among the operations that returned. All the width came from the
// 42 to 60 operations per key the client never heard back from, which never
// return and so stay placeable to the end of the history. Five of the eight
// keys burned the whole two-million transition budget and the run reported
// unknown.
//
// A test here therefore asserts a budget as well as a verdict. That is unusual
// and deliberate: "linearizable, but only if you let it run for a week" is the
// failure this package exists to avoid, and a verdict-only test would not see
// it come back.
//
// What these tests do NOT pin, because nothing here changed it: concurrency
// among the operations that returned is still exponential in the concurrency.
// The captured run was only four or five deep, which is why it was the
// indeterminate operations that mattered; a run pointed at one key with sixteen
// clients is a different problem and not this one.

// indeterminateHistory builds one single-key history in the shape a chaos run
// produces: several hundred operations across a handful of concurrent clients,
// a tenth of them indeterminate.
//
// clients is the concurrency, and it is small on purpose: the captured run
// spread sixteen clients over eight keys and reached a maximum of five
// overlapping operations on any one of them.
//
// lateSpan says when an indeterminate operation actually took effect, and it is
// the knob that decides everything: whether any later read observes a value
// only that operation can explain, and so whether it can be linearized at the
// end or has to go at a particular point in the middle. Both regimes occur in
// captured runs - see the constants below - so both are generated here.
func indeterminateHistory(tb testing.TB, rng *rand.Rand, total, clients, infoPct, values int, lateSpan int64) history.History {
	tb.Helper()

	// A client that never got an answer cannot issue anything else, so the
	// harness retires its process and mints a fresh one. The captured run used
	// 428 process identifiers for 16 concurrent clients and 411 indeterminate
	// operations; this does the same.
	cursors := make([]int64, clients)
	live := make([]int, clients)
	for i := range live {
		live[i] = i
	}

	ops := make(history.History, 0, total)
	for len(ops) < total {
		slot := rng.IntN(len(live))
		p := live[slot]

		inv := cursors[p] + int64(rng.IntN(8))
		comp := inv + 1 + int64(rng.IntN(30))
		op := history.Op{Process: p, Key: "x", Invoke: inv, Complete: comp, Outcome: history.OK}
		switch rng.IntN(3) {
		case 0:
			op.Kind = history.Read
		case 1:
			op.Kind = history.Write
			op.Value = rng.IntN(values)
		default:
			op.Kind = history.CAS
			op.From = rng.IntN(values)
			// history.Validate rejects a cas from a value to itself.
			op.To = (op.From + 1 + rng.IntN(values-1)) % values
		}

		if rng.IntN(100) < infoPct {
			op.Outcome = history.Info
			op.Complete = history.Pending
			cursors = append(cursors, inv)
			live[slot] = len(cursors) - 1
		} else {
			cursors[p] = comp + int64(rng.IntN(4))
		}
		ops = append(ops, op)
	}

	fillOutputsWithLateness(tb, ops, rng, lateSpan)
	return ops
}

// fillOutputsWithLateness picks a linearization point inside each operation's
// interval, runs the model in that order, and records what each client would
// have seen, so the history is linearizable by construction.
//
// It is brute_test.go's fillOutputs with one difference, and the difference is
// the point of this file: where an indeterminate operation's point comes from.
//
// Sorting by linearization point cannot contradict real time. For an operation
// that returned, its point is below its own completion, which is at most the
// invocation of anything that follows it in real time. An indeterminate
// operation never completes, so nothing is required to follow it and any point
// at or after its invocation is legal.
func fillOutputsWithLateness(tb testing.TB, ops history.History, rng *rand.Rand, lateSpan int64) {
	tb.Helper()
	if len(ops) == 0 {
		return
	}

	var maxT int64
	for _, op := range ops {
		if op.Complete != history.Pending && op.Complete > maxT {
			maxT = op.Complete
		}
	}

	pts := make([]int64, len(ops))
	for i, op := range ops {
		switch {
		case op.Outcome != history.Info:
			pts[i] = op.Invoke + int64(rng.IntN(int(op.Complete-op.Invoke)))
		case lateSpan == pendingAtEnd:
			pts[i] = maxT + 1 + int64(i)
		default:
			pts[i] = op.Invoke + rng.Int64N(lateSpan+1)
		}
	}

	order := make([]int, len(ops))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return pts[order[a]] < pts[order[b]] })

	s := model.State(model.InitialValue)
	for _, i := range order {
		op := &ops[i]
		v := s.(int)
		switch op.Kind {
		case history.Read:
			op.Observed = v
		case history.CAS:
			op.Swapped = v == op.From
		}
		next, ok := casReg.Step(s, *op)
		if !ok {
			tb.Fatalf("the generator produced an output the model rejects: %+v against state %v", *op, s)
		}
		s = next
	}
}

// The two regimes the generator is asked for, both taken from captured runs.
//
// pendingAtEnd is kvsingle under `-faults chaos`, where measurement found not
// one successful read observing a value only an indeterminate operation could
// have produced - zero, across 4008 operations and eight keys. Every one of
// them can be linearized after everything else, and the search must find that
// without help.
//
// lateSpan is kvforward, where reads do observe values only a timed-out write
// explains, so an indeterminate operation has to be placed at a particular
// point in the middle. It is the window, in timestamp units, in which such an
// operation actually took effect; operations here last up to thirty ticks, so
// three hundred is about ten operations' worth.
const (
	pendingAtEnd int64 = -1
	lateSpan     int64 = 300
)

// checkIndeterminateCorpus runs one generated corpus and returns the worst
// transition count, failing the test on any verdict other than linearizable.
func checkIndeterminateCorpus(t *testing.T, seed uint64, runs, total, clients, infoPct int, span int64, budget int) int {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 0x0f1e2d3c))
	worst := 0
	for i := 0; i < runs; i++ {
		h := indeterminateHistory(t, rng, total, clients, infoPct, genValues, span)
		if err := h.Validate(); err != nil {
			t.Fatalf("case %d: the generator produced an invalid history: %v", i, err)
		}
		_, _, info := h.Counts()
		got, err := CheckKey(h, model.CASRegister{}, Options{MaxVisits: budget, MaxCacheBytes: 512 << 20})
		if err != nil {
			t.Fatalf("case %d: CheckKey: %v", i, err)
		}
		if got.Verdict != Linearizable {
			t.Fatalf("case %d: verdict %s on a history that is linearizable by construction "+
				"(%d operations, %d indeterminate, %d transitions): %s",
				i, got.Verdict, len(h), info, got.Visits, got.Reason)
		}
		if got.Visits > worst {
			worst = got.Visits
		}
	}
	return worst
}

// TestPendingOperationsNobodyObservedAreNearlyFree is the regression test for
// the run this work started from. Nothing observes an indeterminate operation,
// so all of them belong at the end - and finding that must cost about what
// walking the history once costs, not a budget.
//
// The ceiling is deliberately tight. Before the deferred order existed the same
// corpus took the whole two-million transition budget and came back unknown, so
// a loose ceiling would pass either way and pin nothing.
func TestPendingOperationsNobodyObservedAreNearlyFree(t *testing.T) {
	const (
		runs  = 40
		total = 500
		// Eight transitions per operation. The measured cost is a shade over
		// one; this leaves room for the backtracking a denser history needs
		// without leaving room for a search that is exploring orderings.
		perOp = 8
	)
	worst := checkIndeterminateCorpus(t, 0x5a17b7a1, runs, total, 5, 10, pendingAtEnd, perOp*total)
	t.Logf("%d histories of %d operations decided in at most %d model transitions, %.2f per operation",
		runs, total, worst, float64(worst)/float64(total))
}

// TestPendingOperationsThatWereObservedStillDecide is the other regime, and the
// reason the deferred order is a probe rather than the search. Here reads do
// observe values only an indeterminate operation explains, so those operations
// have to go in the middle and deferring them is exactly the wrong instinct.
// The probe fails, the ordinary search picks it up, and the answer still
// arrives inside the command line's default budget.
func TestPendingOperationsThatWereObservedStillDecide(t *testing.T) {
	const budget = 2_000_000 // the command line's default per-key budget
	worst := checkIndeterminateCorpus(t, 0xc0ffee11, 20, 500, 5, 10, lateSpan, budget)
	t.Logf("20 histories of 500 operations decided; worst case %d model transitions of a %d budget", worst, budget)
}

// TestTheFastPathIsBoundedWhenItCannotHelp pins the price of the fast path on
// the histories it does nothing for. There are two of them, and they cost
// differently.
//
// A history with no indeterminate operation never runs the fast path at all, so
// it must cost exactly what it always cost. The number below was measured
// before the fast path existed; if it moves, something changed the search
// itself.
//
// A history that does carry indeterminate operations and still has no
// linearization pays the fast path in full and learns nothing, because failing
// to place the indeterminate operations last says nothing about the history.
// That is the worst case, and it must stay inside the fast path's own budget
// rather than growing with the search behind it.
func TestTheFastPathIsBoundedWhenItCannotHelp(t *testing.T) {
	t.Run("no_indeterminate_operations_cost_exactly_what_they_did", func(t *testing.T) {
		ops := exhaustiveWorkload(7)
		got, err := CheckKey(ops, casReg, Options{})
		if err != nil {
			t.Fatalf("CheckKey: %v", err)
		}
		if got.Verdict != NotLinearizable {
			t.Fatalf("verdict = %s, want not linearizable; this test is about the cost of "+
				"exhausting the space, so there must be nothing to find", got.Verdict)
		}
		if got.Visits != 76560 {
			t.Errorf("the exhaustive search took %d model transitions, want 76560; a history with "+
				"nothing indeterminate on it must not pay for the fast path", got.Visits)
		}
	})

	t.Run("indeterminate_operations_pay_no_more_than_the_fast_path_budget", func(t *testing.T) {
		ops := history.History{
			infoWr(0, 1, 0), infoWr(1, 2, 1), infoWr(2, 3, 2), infoCAS(3, 1, 9, 3),
			rd(4, 5, 5, 60), rd(5, 7, 6, 61), wr(6, 4, 7, 62), rd(7, 8, 8, 63),
		}
		got, err := CheckKey(ops, casReg, Options{})
		if err != nil {
			t.Fatalf("CheckKey: %v", err)
		}
		if got.Verdict != NotLinearizable {
			t.Fatalf("verdict = %s, want not linearizable", got.Verdict)
		}
		// 371 is what the search alone took, measured before the fast path
		// existed. Everything above it is the fast path, and its ceiling is
		// what indeterminateLastVisits promises.
		const searchAlone = 371
		if got.Visits < searchAlone {
			t.Fatalf("the search took %d model transitions, fewer than the %d it takes on its own; "+
				"the fast path must not be answering this", got.Visits, searchAlone)
		}
		if over := got.Visits - searchAlone; over > indeterminateLastVisits*len(ops) {
			t.Errorf("the fast path spent %d model transitions on a history it cannot help with, "+
				"over its budget of %d", over, indeterminateLastVisits*len(ops))
		}
	})
}
