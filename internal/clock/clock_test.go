package clock

import (
	"sync"
	"testing"
	"time"
)

func TestClockAdvances(t *testing.T) {
	c := New()
	a := c.Nanos()
	time.Sleep(5 * time.Millisecond)
	b := c.Nanos()
	if b <= a {
		t.Fatalf("the clock did not advance over 5ms: %d then %d", a, b)
	}
	if elapsed := time.Duration(b - a); elapsed < 3*time.Millisecond || elapsed > time.Second {
		t.Fatalf("5ms of sleep measured as %v, which means the conversion is wrong", elapsed)
	}
}

func TestClockStartsNearZero(t *testing.T) {
	c := New()
	// Not exactly zero - measuring the granularity happens first - but a large
	// value would mean the origin was never subtracted, and every timestamp in
	// every history would be an absolute counter reading.
	if n := c.Nanos(); n < 0 || n > int64(time.Second) {
		t.Fatalf("a fresh clock reads %v", time.Duration(n))
	}
}

func TestGranularityIsPositiveAndPlausible(t *testing.T) {
	c := New()
	g := c.Granularity()
	if g <= 0 {
		t.Fatalf("granularity is %d; zero would switch the widening off silently", g)
	}
	// A tick coarser than a tenth of a second is not a clock anyone can time
	// a network round trip with, and would mean the measurement went wrong.
	if g > int64(100*time.Millisecond) {
		t.Fatalf("granularity measured as %v, which cannot be right", time.Duration(g))
	}
	t.Logf("%s", c)
}

func TestGranularityBeatsGoOnWindows(t *testing.T) {
	// The entire reason this package exists. On Windows Go's own clock steps
	// in hundreds of microseconds, which is longer than the operations being
	// timed; if this stops being an improvement the package should be deleted
	// rather than kept as decoration.
	c := New()
	if rawSource == "Go monotonic clock" {
		t.Skipf("no platform-specific clock here; %s", c)
	}
	if c.Granularity() >= int64(50*time.Microsecond) {
		t.Fatalf("the platform clock resolves to %v, no better than the one it replaced", time.Duration(c.Granularity()))
	}
	t.Logf("%s", c)
}

func TestCompletionIsAlwaysAfterInvocation(t *testing.T) {
	c := New()
	// A zero-width interval makes an operation precede and follow everything
	// at the same instant, and a checker handed that has no honest answer.
	for _, measured := range []int64{0, 100, 99} {
		if got := c.Completion(100, measured); got <= 100 {
			t.Errorf("Completion(100, %d) = %d, which is not after the invocation", measured, got)
		}
	}
}

func TestCompletionWidensByExactlyOneTick(t *testing.T) {
	c := New()
	g := c.Granularity()
	// The widening is what makes "A finished before B started" a claim the
	// measurement supports. One tick, no more: widening further would only
	// lose violations.
	if got, want := c.Completion(1000, 5000), int64(5000)+g; got != want {
		t.Fatalf("Completion(1000, 5000) = %d, want %d", got, want)
	}
}

func TestCompletionClampsAMeasurementThatWentBackwards(t *testing.T) {
	c := New()
	if got := c.Completion(5000, 1000); got < 5000 {
		t.Fatalf("Completion(5000, 1000) = %d, which is before the invocation", got)
	}
}

func TestConcurrentReadsAreSafe(t *testing.T) {
	// The nemesis, every client goroutine and the recorder all read this clock
	// at once. CI runs this under the race detector.
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := int64(-1)
			for j := 0; j < 20_000; j++ {
				n := c.Nanos()
				if n < last {
					t.Errorf("the clock went backwards: %d after %d", n, last)
					return
				}
				last = n
			}
		}()
	}
	wg.Wait()
}

func TestClockIsFineEnoughToSeparateRealOperations(t *testing.T) {
	// The property the harness actually needs: two things that happen one
	// after the other must get different timestamps. If they do not, the
	// history records them as simultaneous and a correct store gets accused.
	c := New()
	const trials = 200
	identical := 0
	for i := 0; i < trials; i++ {
		a := c.Nanos()
		// Roughly the cost of a loopback request.
		time.Sleep(200 * time.Microsecond)
		if c.Nanos() == a {
			identical++
		}
	}
	if identical > trials/20 {
		t.Fatalf("%d of %d pairs separated by 200us measured as simultaneous; the clock cannot time these operations", identical, trials)
	}
}
