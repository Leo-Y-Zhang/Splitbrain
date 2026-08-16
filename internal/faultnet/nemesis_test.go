package faultnet

import (
	"context"
	"net"
	"testing"
	"time"
)

// runNemesis starts a nemesis and returns a channel closed when Run returns.
func runNemesis(n *Nemesis, ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.Run(ctx)
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}, budget time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(budget):
		t.Fatalf("Run did not return within %v", budget)
	}
}

func testLinks(t *testing.T, names ...string) map[string]*Link {
	t.Helper()
	srv := echoServer(t)
	links := make(map[string]*Link, len(names))
	for _, n := range names {
		l, err := NewLink(n, srv)
		if err != nil {
			t.Fatalf("NewLink %s: %v", n, err)
		}
		t.Cleanup(func() { l.Close() })
		links[n] = l
	}
	return links
}

// TestNemesisRunsAHandBuiltSchedule covers the path the harness itself uses:
// events computed elsewhere from a topology this package knows nothing about.
func TestNemesisRunsAHandBuiltSchedule(t *testing.T) {
	links := testLinks(t, "a", "b")
	const d = 600 * time.Millisecond
	want := []Event{
		{At: 50 * time.Millisecond, Link: "a", Fault: Drop},
		{At: 150 * time.Millisecond, Link: "b", Fault: Delay, Delay: 20 * time.Millisecond},
		{At: 300 * time.Millisecond, Link: allLinks, Fault: Pass},
	}
	s, err := NewSchedule([]string{"a", "b"}, d, want)
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}

	n := NewNemesis(links, s)
	start := time.Now()
	done := runNemesis(n, context.Background())

	// The faults must land while the run is going, not all at the end.
	if !waitFor(5*time.Second, func() bool { return links["a"].Fault() == Drop }) {
		t.Fatalf("link a never went into Drop (it is %v)", links["a"].Fault())
	}
	if !waitFor(5*time.Second, func() bool { return links["b"].Fault() == Delay }) {
		t.Fatalf("link b never went into Delay (it is %v)", links["b"].Fault())
	}
	if got := links["b"].DelayFor(); got != 20*time.Millisecond {
		t.Fatalf("link b delay = %v, want the 20ms the event carried", got)
	}

	waitDone(t, done, 10*time.Second)
	elapsed := time.Since(start)
	if elapsed < d {
		t.Fatalf("Run returned after %v, before the schedule's %v was up", elapsed, d)
	}

	for name, l := range links {
		if l.Fault() != Pass {
			t.Fatalf("link %s left in %v after the run", name, l.Fault())
		}
	}

	// Applied carries the times things really happened, so each entry can
	// only be at or after the time it was asked for. A small tolerance
	// covers timer granularity; the point is that nothing fired early, which
	// would mean the clock was not being consulted at all.
	const slack = 10 * time.Millisecond
	applied := n.Applied()
	if len(applied) < len(want) {
		t.Fatalf("Applied has %d entries, want at least %d: %v", len(applied), len(want), applied)
	}
	for i, w := range want {
		if applied[i].Link != w.Link || applied[i].Fault != w.Fault {
			t.Fatalf("applied[%d] = %v, want link %s fault %v", i, applied[i], w.Link, w.Fault)
		}
		if applied[i].At+slack < w.At {
			t.Fatalf("applied[%d] fired at %v, before its scheduled %v", i, applied[i].At, w.At)
		}
	}
	t.Logf("applied: %v", applied)
}

func TestNemesisAppliedRecordsWhatFired(t *testing.T) {
	links := testLinks(t, "a", "b")
	const d = 400 * time.Millisecond
	want := []Event{
		{At: 50 * time.Millisecond, Link: "a", Fault: Drop},
		{At: 150 * time.Millisecond, Link: allLinks, Fault: Reset},
		{At: 250 * time.Millisecond, Link: allLinks, Fault: Pass},
	}
	s, err := NewSchedule([]string{"a", "b"}, d, want)
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	n := NewNemesis(links, s)
	n.Run(context.Background())

	applied := n.Applied()
	if len(applied) != len(want)+1 {
		t.Fatalf("Applied has %d entries, want the %d scheduled plus the closing heal: %v", len(applied), len(want), applied)
	}
	const slack = 10 * time.Millisecond
	for i, w := range want {
		got := applied[i]
		if got.Link != w.Link || got.Fault != w.Fault {
			t.Fatalf("applied[%d] = %v, want link %s fault %v", i, got, w.Link, w.Fault)
		}
		if got.At+slack < w.At {
			t.Fatalf("applied[%d] fired at %v, before its scheduled %v", i, got.At, w.At)
		}
		if got.At > w.At+5*time.Second {
			t.Fatalf("applied[%d] fired at %v, hopelessly after its scheduled %v", i, got.At, w.At)
		}
	}
	if last := applied[len(applied)-1]; last.Link != allLinks || last.Fault != Pass {
		t.Fatalf("the last thing a run does must be to heal, got %v", last)
	}
}

// TestNemesisHealsOnCancellation is the one that matters most. A run that
// stops mid-partition leaves the system under test unable to answer the final
// reads, and a report with no final reads cannot be interpreted.
func TestNemesisHealsOnCancellation(t *testing.T) {
	links := testLinks(t, "a", "b", "c")
	s, err := NewSchedule([]string{"a", "b", "c"}, 30*time.Second, []Event{
		{At: 0, Link: "a", Fault: Drop},
		{At: 10 * time.Millisecond, Link: "b", Fault: Refuse},
		{At: 20 * time.Millisecond, Link: "c", Fault: Reset},
		{At: 25 * time.Second, Link: allLinks, Fault: Pass},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	n := NewNemesis(links, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := runNemesis(n, ctx)

	if !waitFor(5*time.Second, func() bool { return links["c"].Fault() == Reset }) {
		t.Fatal("the third fault never landed, so the cancellation would not be mid-partition")
	}
	cancel()
	waitDone(t, done, 10*time.Second)

	for name, l := range links {
		if l.Fault() != Pass {
			t.Fatalf("link %s left in %v after cancellation", name, l.Fault())
		}
	}
	// Healing must mean the network works again, not just that a field says
	// Pass: the refused link has to be listening on its original address.
	for name, l := range links {
		c, err := net.DialTimeout("tcp", l.Addr(), 5*time.Second)
		if err != nil {
			t.Fatalf("link %s unusable after the heal: %v", name, err)
		}
		got, err := roundTrip(c, "ping", 10*time.Second)
		c.Close()
		if err != nil || got != "ping" {
			t.Fatalf("link %s round trip after the heal: got %q err %v", name, got, err)
		}
	}
	for _, e := range n.Applied() {
		if e.At > 20*time.Second {
			t.Fatalf("an event scheduled past the cancellation still fired: %v", e)
		}
	}
}

func TestNemesisRunsForTheWholeScheduleWithNoEvents(t *testing.T) {
	links := testLinks(t, "a")
	const d = 200 * time.Millisecond
	n := NewNemesis(links, Healthy([]string{"a"}, 1, d))

	start := time.Now()
	n.Run(context.Background())
	if elapsed := time.Since(start); elapsed < d {
		t.Fatalf("Run returned after %v; an empty schedule still lasts %v", elapsed, d)
	}
	if links["a"].Fault() != Pass {
		t.Fatalf("link a is %v after a healthy run", links["a"].Fault())
	}
}

func TestNemesisStarAppliesToEveryLink(t *testing.T) {
	links := testLinks(t, "a", "b", "c")
	s, err := NewSchedule([]string{"a", "b", "c"}, 100*time.Millisecond, []Event{
		{At: 0, Link: allLinks, Fault: Delay, Delay: 40 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	n := NewNemesis(links, s)
	done := runNemesis(n, context.Background())

	if !waitFor(5*time.Second, func() bool {
		for _, l := range links {
			if l.Fault() != Delay || l.DelayFor() != 40*time.Millisecond {
				return false
			}
		}
		return true
	}) {
		for name, l := range links {
			t.Logf("link %s: %v %v", name, l.Fault(), l.DelayFor())
		}
		t.Fatal(`"*" did not reach every link`)
	}
	waitDone(t, done, 10*time.Second)
}

func TestNemesisIgnoresUnknownLinks(t *testing.T) {
	links := testLinks(t, "a")
	s, err := NewSchedule([]string{"a", "ghost"}, 50*time.Millisecond, []Event{
		{At: 0, Link: "ghost", Fault: Drop},
		{At: 10 * time.Millisecond, Link: "a", Fault: Drop},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	n := NewNemesis(links, s)
	n.Run(context.Background())

	for _, e := range n.Applied() {
		if e.Link == "ghost" {
			t.Fatal("an event for a link the nemesis does not hold was recorded as applied")
		}
	}
	if links["a"].Fault() != Pass {
		t.Fatalf("link a is %v after the run", links["a"].Fault())
	}
}

func TestNemesisHealIsIndependentlyUsable(t *testing.T) {
	links := testLinks(t, "a", "b")
	links["a"].Set(Drop)
	links["b"].Set(Refuse)
	n := NewNemesis(links, Healthy([]string{"a", "b"}, 1, time.Second))
	n.Heal()
	for name, l := range links {
		if l.Fault() != Pass {
			t.Fatalf("link %s is %v after Heal", name, l.Fault())
		}
	}
}

func TestNemesisCopiesItsLinkMap(t *testing.T) {
	links := testLinks(t, "a")
	n := NewNemesis(links, Healthy([]string{"a"}, 1, 10*time.Millisecond))
	kept := links["a"]
	delete(links, "a")
	n.Run(context.Background())
	if kept.Fault() != Pass {
		t.Fatalf("link a is %v; the nemesis should still hold it", kept.Fault())
	}
}
