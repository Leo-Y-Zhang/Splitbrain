package faultnet

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

// allLinks is the link name that means "every link in the run".
const allLinks = "*"

// An Event is one fault applied to one link at one moment.
type Event struct {
	// At is the offset from the start of the run.
	At time.Duration

	// Link names the link the fault applies to, or "*" for every link.
	Link string

	// Fault is the condition to put the link into.
	Fault Fault

	// Delay is the hold to apply, and is only meaningful when Fault is
	// Delay.
	Delay time.Duration
}

// A Schedule decides what each link is doing over time.
//
// A generated schedule comes from a seed, so the sequence of faults replays
// exactly. That is not the same as a deterministic run, and it is worth being
// blunt about the difference: the system under test has its own clocks,
// goroutines and timeouts, so replaying a seed reproduces the weather, not the
// flight. It narrows the search, it does not close it.
type Schedule struct {
	kind   string
	seed   int64
	dur    time.Duration
	events []Event
}

// newSchedule is the single constructor everything else funnels through.
// It takes ownership of events, which must already be sorted and valid.
func newSchedule(kind string, seed int64, d time.Duration, events []Event) *Schedule {
	return &Schedule{kind: kind, seed: seed, dur: d, events: events}
}

// NewSchedule builds a schedule from events supplied by the caller. It sorts
// them by time and rejects an event naming a link that is not in links, or a
// time outside [0, d].
//
// This is the door for faults the generators here cannot express, because they
// depend on a topology this package deliberately knows nothing about. Cutting
// one node out of a cluster means cutting its client link and every peer link
// that crosses the boundary at the same instant; which links those are is the
// harness's business, so the harness works it out and hands the events over.
func NewSchedule(links []string, d time.Duration, events []Event) (*Schedule, error) {
	return NewNamedSchedule("custom", 0, links, d, events)
}

// NewNamedSchedule is NewSchedule for a caller that knows where its events came
// from. The kind and seed are carried only so a report can say what produced
// this timeline; nothing reads them back.
//
// The harness is the caller that needs it. It builds a partition out of its own
// topology, so the events arrive here already made, and without this the report
// for a seeded run says "custom seed=0" - which reads as a hand-written
// timeline nobody can replay.
func NewNamedSchedule(kind string, seed int64, links []string, d time.Duration, events []Event) (*Schedule, error) {
	if d < 0 {
		return nil, fmt.Errorf("faultnet: a schedule cannot last %v", d)
	}
	known := make(map[string]bool, len(links))
	for _, n := range links {
		known[n] = true
	}

	out := make([]Event, len(events))
	copy(out, events)
	for i, e := range out {
		if e.Link != allLinks && !known[e.Link] {
			return nil, fmt.Errorf("faultnet: event %d names link %q, which is not one of %v", i, e.Link, links)
		}
		if e.At < 0 || e.At > d {
			return nil, fmt.Errorf("faultnet: event %d is at %v, outside [0, %v]", i, e.At, d)
		}
		if e.Fault > Refuse {
			return nil, fmt.Errorf("faultnet: event %d has unknown fault %v", i, e.Fault)
		}
		if e.Delay < 0 {
			return nil, fmt.Errorf("faultnet: event %d has a negative delay of %v", i, e.Delay)
		}
	}
	// Stable, so two events at the same instant keep the order the caller
	// wrote them in. A topology cut is a group of events sharing a timestamp
	// and reading the report is easier when they stay together.
	sort.SliceStable(out, func(i, j int) bool { return out[i].At < out[j].At })

	return newSchedule(kind, seed, d, out), nil
}

// Events lists the schedule's events in time order. The slice is a copy, so a
// caller cannot rewrite history by editing it.
func (s *Schedule) Events() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// Kind is the generator that produced the schedule, or "custom" if the events
// were supplied directly.
func (s *Schedule) Kind() string { return s.kind }

// Seed is the seed the schedule was generated from. It is zero for a custom
// schedule, which has nothing to replay from.
func (s *Schedule) Seed() int64 { return s.seed }

// Duration is how long the schedule lasts.
func (s *Schedule) Duration() time.Duration { return s.dur }

// String renders the schedule as a compact timeline, one event per line under
// a header. It is meant to be read next to a history when working out which
// fault a violation sits under.
func (s *Schedule) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s seed=%d for %s, %s", s.kind, s.seed, s.dur, countEvents(len(s.events)))

	width := 1
	for _, e := range s.events {
		if len(e.Link) > width {
			width = len(e.Link)
		}
	}
	for _, e := range s.events {
		fmt.Fprintf(&b, "\n  %9s  %-*s  %s", e.At.Round(time.Millisecond), width, e.Link, e.Fault)
		if e.Fault == Delay {
			fmt.Fprintf(&b, " %s", e.Delay)
		}
	}
	return b.String()
}

func countEvents(n int) string {
	switch n {
	case 0:
		return "no events"
	case 1:
		return "1 event"
	default:
		return fmt.Sprintf("%d events", n)
	}
}

// phase is one thing a generator can decide to do next.
type phase uint8

const (
	phaseCut phase = iota
	phaseReset
	phaseRefuse
	phaseDelayed
)

// Healthy is the control case: no faults at all.
//
// It is not a formality. A violation found under Healthy is a bug the system
// under test has without any help from the network, and chasing it through a
// partition report wastes the afternoon.
func Healthy(links []string, seed int64, d time.Duration) *Schedule {
	return newSchedule("healthy", seed, d, nil)
}

// Partition alternates cut and heal phases. Each cut blackholes a random
// non-empty strict subset of the links; each heal returns everything to Pass.
//
// Never every link at once: that strands every client, so nothing completes,
// and a run in which nothing completes measures nothing. With fewer than two
// links there is no such subset and Partition produces no events at all.
func Partition(links []string, seed int64, d time.Duration) *Schedule {
	return generate("partition", links, seed, d, []phase{phaseCut})
}

// Flaky blips individual links: short resets and refusals, and stretches of
// added latency. These are the faults a client is supposed to survive, so a
// violation here is a bug in its retry logic rather than in the store.
func Flaky(links []string, seed int64, d time.Duration) *Schedule {
	return generate("flaky", links, seed, d, []phase{phaseReset, phaseRefuse, phaseDelayed})
}

// Chaos mixes partitions with the flaky blips.
func Chaos(links []string, seed int64, d time.Duration) *Schedule {
	return generate("chaos", links, seed, d, []phase{phaseCut, phaseReset, phaseRefuse, phaseDelayed})
}

// Generate builds a schedule by generator name.
func Generate(kind string, links []string, seed int64, d time.Duration) (*Schedule, error) {
	switch kind {
	case "healthy":
		return Healthy(links, seed, d), nil
	case "partition":
		return Partition(links, seed, d), nil
	case "flaky":
		return Flaky(links, seed, d), nil
	case "chaos":
		return Chaos(links, seed, d), nil
	default:
		return nil, fmt.Errorf("unknown schedule %q (have: healthy, partition, flaky, chaos)", kind)
	}
}

// generate lays phases end to end until the run is used up.
//
// Every phase is a fault followed by its heal, and no two phases overlap, so
// the timeline reads top to bottom with no bookkeeping. A phase that would run
// past the end of the run is dropped rather than truncated: a schedule whose
// last act is a cut says the network was broken when the run stopped, which is
// not what happened, because the nemesis heals on the way out.
func generate(kind string, links []string, seed int64, d time.Duration, choices []phase) *Schedule {
	if d <= 0 || len(links) == 0 || len(choices) == 0 {
		return newSchedule(kind, seed, d, nil)
	}
	// The caller's slice is never reordered; a generator that shuffled it
	// would change what a later call with the same seed produces.
	names := append([]string(nil), links...)

	// Seeded explicitly, never the global source, and mixed with the kind so
	// that the same seed does not lay every kind's phases at the same
	// instants.
	rng := rand.New(rand.NewPCG(uint64(seed), fnv1a(kind)))

	lo, hi := phaseBounds(d)
	var events []Event
	for t := time.Duration(0); ; {
		t += between(rng, lo, hi)
		if t >= d {
			break
		}
		p := choices[rng.IntN(len(choices))]

		hold := between(rng, lo, hi)
		if p != phaseCut {
			// Blips are meant to be short enough that a client's retry
			// rides straight over them.
			hold = between(rng, blip(lo), blip(hi))
		}
		end := t + hold
		if end > d {
			break
		}

		switch p {
		case phaseCut:
			cut := strictSubset(rng, names)
			if len(cut) == 0 {
				continue
			}
			for _, n := range cut {
				events = append(events, Event{At: t, Link: n, Fault: Drop})
			}
			events = append(events, Event{At: end, Link: allLinks, Fault: Pass})
		case phaseReset:
			n := names[rng.IntN(len(names))]
			events = append(events,
				Event{At: t, Link: n, Fault: Reset},
				Event{At: end, Link: n, Fault: Pass})
		case phaseRefuse:
			n := names[rng.IntN(len(names))]
			events = append(events,
				Event{At: t, Link: n, Fault: Refuse},
				Event{At: end, Link: n, Fault: Pass})
		case phaseDelayed:
			n := names[rng.IntN(len(names))]
			// Rounded because nobody reading a report cares about the
			// nanoseconds, and a delay is an order of magnitude anyway.
			hold := between(rng, 20*time.Millisecond, 250*time.Millisecond).Round(time.Millisecond)
			events = append(events,
				Event{At: t, Link: n, Fault: Delay, Delay: hold},
				Event{At: end, Link: n, Fault: Pass})
		}
		t = end
	}
	return newSchedule(kind, seed, d, events)
}

// strictSubset picks between one and all-but-one of the names. It returns
// nothing when there are fewer than two, because then no such subset exists.
func strictSubset(rng *rand.Rand, names []string) []string {
	if len(names) < 2 {
		return nil
	}
	k := 1 + rng.IntN(len(names)-1)
	perm := append([]string(nil), names...)
	rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
	cut := perm[:k]
	sort.Strings(cut)
	return cut
}

// phaseBounds gives the range a phase may last, squeezed to fit short runs so
// that a two second run still gets a few faults rather than one that does not
// fit.
func phaseBounds(d time.Duration) (lo, hi time.Duration) {
	lo, hi = 300*time.Millisecond, 1500*time.Millisecond
	if quarter := d / 4; hi > quarter {
		hi = quarter
	}
	if lo > hi {
		lo = hi
	}
	if lo <= 0 {
		lo = time.Millisecond
	}
	if hi < lo {
		hi = lo
	}
	return lo, hi
}

func blip(d time.Duration) time.Duration {
	if b := d / 3; b > time.Millisecond {
		return b
	}
	return time.Millisecond
}

func between(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rng.Int64N(int64(hi-lo)))
}

// fnv1a hashes the schedule kind into the second word of the RNG seed.
func fnv1a(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}
