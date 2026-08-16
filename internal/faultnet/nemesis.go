package faultnet

import (
	"context"
	"sync"
	"time"
)

// A Nemesis applies a Schedule to a set of Links on a wall clock.
type Nemesis struct {
	links map[string]*Link
	sched *Schedule

	mu      sync.Mutex
	started time.Time
	applied []Event
}

// NewNemesis pairs a set of links, keyed by the names the schedule uses, with
// the schedule to apply to them. The map is copied, so the caller may keep
// using its own.
func NewNemesis(links map[string]*Link, s *Schedule) *Nemesis {
	held := make(map[string]*Link, len(links))
	for name, l := range links {
		held[name] = l
	}
	return &Nemesis{links: held, sched: s}
}

// Run applies the schedule, blocking until it ends or ctx is done.
//
// It always heals on the way out, however it leaves. A run that stops
// mid-partition leaves the system under test unable to answer the final reads,
// and a history whose last operations all time out for a reason the harness
// caused is not interpretable: there is no way to tell a store that lost data
// from one that was simply still cut off.
func (n *Nemesis) Run(ctx context.Context) {
	defer n.Heal()
	if n.sched == nil {
		return
	}

	start := time.Now()
	n.mu.Lock()
	n.started = start
	n.mu.Unlock()

	for _, e := range n.sched.events {
		if !waitUntil(ctx, start, e.At) {
			return
		}
		n.apply(e, time.Since(start))
	}
	// The schedule lasts as long as it says it does, even after its last
	// event, so a healthy run is still a run of that length.
	waitUntil(ctx, start, n.sched.dur)
}

// Applied lists the events that actually fired, in the order they fired, with
// At rewritten to the moment each one really landed rather than the moment it
// was asked for. The closing heal is included, so the report can say when the
// network was whole again.
func (n *Nemesis) Applied() []Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Event, len(n.applied))
	copy(out, n.applied)
	return out
}

// Heal sets every link back to Pass and records that it did.
func (n *Nemesis) Heal() {
	for _, l := range n.links {
		l.Set(Pass)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	e := Event{Link: allLinks, Fault: Pass}
	if !n.started.IsZero() {
		e.At = time.Since(n.started)
	}
	n.applied = append(n.applied, e)
}

// apply puts one event into force. An event naming a link this nemesis does
// not hold is ignored rather than fatal: a schedule can outlive the links it
// was written for, and refusing to run the rest of it would turn a stale name
// into a silent healthy run, which is worse.
func (n *Nemesis) apply(e Event, at time.Duration) {
	if e.Link == allLinks {
		for _, l := range n.links {
			applyTo(l, e)
		}
	} else {
		l, ok := n.links[e.Link]
		if !ok {
			return
		}
		applyTo(l, e)
	}
	rec := e
	rec.At = at
	n.mu.Lock()
	n.applied = append(n.applied, rec)
	n.mu.Unlock()
}

func applyTo(l *Link, e Event) {
	// The delay is set first, so a link never spends an instant in Delay
	// holding bytes for whatever the previous event left behind.
	if e.Fault == Delay {
		l.SetDelay(e.Delay)
	}
	l.Set(e.Fault)
}

// waitUntil sleeps until at has elapsed since start, reporting false if ctx
// finished first.
func waitUntil(ctx context.Context, start time.Time, at time.Duration) bool {
	d := time.Until(start.Add(at))
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
