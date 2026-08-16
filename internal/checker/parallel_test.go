package checker

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// Keys share nothing, so searching them at the same time is exact rather than
// an approximation - it is the same compositional argument that lets Check
// split the history by key at all. What it endangers is not the verdict but the
// report: a checker that names whichever key lost the race is worse than a slow
// one, because two runs over the same file disagree and neither is wrong.
//
// Everything in this file is about that.

// racyHistory is built so that a checker which reports the first key to finish
// reports the wrong one. The violation on "bbb" takes thousands of times longer
// to find than the ones on "yyy" and "zzz", and "bbb" is the one that must come
// back every time because it is first in sorted order.
func racyHistory() history.History {
	var h history.History
	h = append(h, onKey("aaa", 100, cleanTraffic(0, 0, 8)...)...)
	h = append(h, onKey("bbb", 200, exhaustiveWorkload(8)...)...)
	h = append(h, onKey("ccc", 300, cleanTraffic(0, 0, 8)...)...)
	h = append(h, onKey("yyy", 400, violationTail(0)...)...)
	h = append(h, onKey("zzz", 500, violationTail(0)...)...)
	return h
}

func TestReportedKeyNeverVariesUnderParallelism(t *testing.T) {
	h := racyHistory()

	first, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if first.Verdict != NotLinearizable {
		t.Fatalf("verdict = %s, want not linearizable", first.Verdict)
	}
	if first.Key != "bbb" {
		t.Fatalf("Key = %q, want %q: the first failing key in sorted order, not the first one found",
			first.Key, "bbb")
	}

	// Many runs, because a race that shows up one time in fifty is still a race.
	for i := 0; i < 200; i++ {
		again, err := Check(h, casReg, Options{})
		if err != nil {
			t.Fatalf("run %d: Check: %v", i, err)
		}
		if again.Key != first.Key || again.Verdict != first.Verdict ||
			again.Reason != first.Reason || again.Visits != first.Visits ||
			len(again.PerKey) != len(first.PerKey) {
			t.Fatalf("run %d differs:\nfirst = %s %q visits=%d %d keys %q\nagain = %s %q visits=%d %d keys %q",
				i, first.Verdict, first.Key, first.Visits, len(first.PerKey), first.Reason,
				again.Verdict, again.Key, again.Visits, len(again.PerKey), again.Reason)
		}
		for k, want := range first.PerKey {
			got, ok := again.PerKey[k]
			if !ok {
				t.Fatalf("run %d: PerKey lost key %q", i, k)
			}
			if got.Ops != want.Ops || got.Visits != want.Visits || got.Verdict != want.Verdict {
				t.Fatalf("run %d: PerKey[%q] = %+v, want %+v", i, k, got, want)
			}
		}
	}
}

// TestUnknownKeyIsAlsoTheFirstInSortedOrder covers the other half of the
// reporting rule, where no key fails outright and the answer is whichever ran
// out of budget first alphabetically.
func TestUnknownKeyIsAlsoTheFirstInSortedOrder(t *testing.T) {
	var h history.History
	// One operation, so this key decides inside the budget below and sorts
	// before every key that does not.
	h = append(h, onKey("aaa", 100, rd(0, 0, 0, 10))...)
	for i, k := range []string{"mmm", "nnn", "ooo", "ppp"} {
		h = append(h, onKey(k, 200+100*i, budgetHistory()...)...)
	}

	for i := 0; i < 100; i++ {
		got, err := Check(h, casReg, Options{MaxVisits: 3})
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if got.Verdict != Unknown {
			t.Fatalf("verdict = %s, want unknown", got.Verdict)
		}
		if got.Key != "mmm" {
			t.Fatalf("run %d: Key = %q, want %q", i, got.Key, "mmm")
		}
		if len(got.PerKey) != 5 {
			t.Fatalf("run %d: PerKey has %d entries, want 5; every key is reported, not just the failing one",
				i, len(got.PerKey))
		}
	}
}

func TestParallelismDoesNotChangeTheResult(t *testing.T) {
	h := racyHistory()

	want, err := Check(h, casReg, Options{Parallelism: 1, Minimize: true})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	for _, p := range []int{0, 2, 3, 8, 64} {
		got, err := Check(h, casReg, Options{Parallelism: p, Minimize: true})
		if err != nil {
			t.Fatalf("Parallelism=%d: Check: %v", p, err)
		}
		if got.Verdict != want.Verdict || got.Key != want.Key || got.Reason != want.Reason ||
			got.Visits != want.Visits || len(got.Ops) != len(want.Ops) {
			t.Fatalf("Parallelism=%d: %s %q visits=%d %d ops, want %s %q visits=%d %d ops",
				p, got.Verdict, got.Key, got.Visits, len(got.Ops),
				want.Verdict, want.Key, want.Visits, len(want.Ops))
		}
	}
}

func TestVisitsAreTheSumOfEveryKey(t *testing.T) {
	h := racyHistory()
	got, err := Check(h, casReg, Options{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	sum := 0
	for _, st := range got.PerKey {
		sum += st.Visits
	}
	if got.Visits != sum {
		t.Errorf("Result.Visits = %d but the per-key visits sum to %d; an accumulator that loses a "+
			"goroutine's contribution is a wrong number nobody notices", got.Visits, sum)
	}
	if got.Visits == 0 {
		t.Error("no transitions at all, so this proves nothing")
	}
}

// watchingModel records how many goroutines were inside Step at once. Check
// does not report how many workers it started, so this is the only way to see
// the bound from outside.
type watchingModel struct {
	inner model.Model
	live  atomic.Int32
	peak  atomic.Int32
}

func (w *watchingModel) Name() string                  { return w.inner.Name() }
func (w *watchingModel) Init() model.State             { return w.inner.Init() }
func (w *watchingModel) Describe(s model.State) string { return w.inner.Describe(s) }
func (w *watchingModel) Explain(s model.State, op history.Op) string {
	return w.inner.Explain(s, op)
}

func (w *watchingModel) Step(s model.State, op history.Op) (model.State, bool) {
	n := w.live.Add(1)
	for {
		p := w.peak.Load()
		if n <= p || w.peak.CompareAndSwap(p, n) {
			break
		}
	}
	out, ok := w.inner.Step(s, op)
	w.live.Add(-1)
	return out, ok
}

// slowKeys is several keys' worth of work, each expensive enough that the
// searches overlap in time rather than finishing one after another.
func slowKeys(n int) history.History {
	var h history.History
	for i := 0; i < n; i++ {
		h = append(h, onKey(fmt.Sprintf("k%02d", i), 100*i, exhaustiveWorkload(8)...)...)
	}
	return h
}

func TestParallelismIsBounded(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("one processor, so there is no bound to observe")
	}
	h := slowKeys(8)

	t.Run("an_explicit_bound_is_respected", func(t *testing.T) {
		for _, p := range []int{1, 2, 3} {
			w := &watchingModel{inner: casReg}
			if _, err := Check(h, w, Options{Parallelism: p}); err != nil {
				t.Fatalf("Check: %v", err)
			}
			if peak := int(w.peak.Load()); peak > p {
				t.Errorf("Parallelism=%d but %d goroutines were searching at once", p, peak)
			}
		}
	})

	t.Run("zero_means_the_machine", func(t *testing.T) {
		w := &watchingModel{inner: casReg}
		if _, err := Check(h, w, Options{}); err != nil {
			t.Fatalf("Check: %v", err)
		}
		peak := int(w.peak.Load())
		if peak > runtime.GOMAXPROCS(0) {
			t.Errorf("%d goroutines were searching at once, above GOMAXPROCS of %d",
				peak, runtime.GOMAXPROCS(0))
		}
		// And it really did run them together, or the bound above is vacuous.
		if peak < 2 {
			t.Errorf("peak concurrency was %d on %d expensive keys; the keys are not being "+
				"searched in parallel at all", peak, 8)
		}
	})
}

// TestTimeoutIsSharedAcrossKeys pins the one option that is not per key. Eight
// keys the search cannot finish, checked one at a time, must still stop at the
// timeout rather than eight times it.
func TestTimeoutIsSharedAcrossKeys(t *testing.T) {
	const timeout = 150 * time.Millisecond
	h := slowKeys(8)

	// Sequential, so the only thing that can save it is a shared deadline.
	start := time.Now()
	got, err := Check(h, casReg, Options{Timeout: timeout, Parallelism: 1})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Verdict == Linearizable {
		t.Fatalf("verdict = %s; these keys do not linearize", got.Verdict)
	}
	if elapsed > 4*timeout {
		t.Errorf("the whole check took %s against a %s timeout; the budget is per call, not per key",
			elapsed, timeout)
	}
	t.Logf("verdict %s after %s of a %s budget", got.Verdict, elapsed, timeout)
}
