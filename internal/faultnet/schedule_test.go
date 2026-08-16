package faultnet

import (
	"strings"
	"testing"
	"time"
)

var generatedKinds = []string{"partition", "flaky", "chaos"}

func sameEvents(a, b []Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHealthyHasNoEvents(t *testing.T) {
	s := Healthy([]string{"a", "b"}, 1, 5*time.Second)
	if got := s.Events(); len(got) != 0 {
		t.Fatalf("Healthy produced %d events: %v", len(got), got)
	}
	if s.Kind() != "healthy" || s.Duration() != 5*time.Second {
		t.Fatalf("kind %q dur %v", s.Kind(), s.Duration())
	}
	if !strings.Contains(s.String(), "healthy") {
		t.Fatalf("String() = %q, want it to name the kind", s.String())
	}
}

func TestGenerateDispatchesByName(t *testing.T) {
	links := []string{"a", "b", "c"}
	for _, kind := range append([]string{"healthy"}, generatedKinds...) {
		s, err := Generate(kind, links, 42, 6*time.Second)
		if err != nil {
			t.Fatalf("Generate(%q): %v", kind, err)
		}
		if s.Kind() != kind {
			t.Fatalf("Generate(%q).Kind() = %q", kind, s.Kind())
		}
		direct := map[string]*Schedule{
			"healthy":   Healthy(links, 42, 6*time.Second),
			"partition": Partition(links, 42, 6*time.Second),
			"flaky":     Flaky(links, 42, 6*time.Second),
			"chaos":     Chaos(links, 42, 6*time.Second),
		}[kind]
		if !sameEvents(s.Events(), direct.Events()) {
			t.Fatalf("Generate(%q) and the direct call disagree", kind)
		}
	}
	if _, err := Generate("hurricane", links, 1, time.Second); err == nil {
		t.Fatal("Generate accepted an unknown kind")
	}
}

// TestScheduleIsDeterministic is the property the whole idea rests on: a run
// that found a violation must be replayable from its seed.
func TestScheduleIsDeterministic(t *testing.T) {
	links := []string{"a", "b", "c", "d"}
	const d = 8 * time.Second

	for _, kind := range generatedKinds {
		for _, seed := range []int64{0, 1, 7, -3, 1 << 40} {
			first, err := Generate(kind, links, seed, d)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			second, err := Generate(kind, links, seed, d)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if !sameEvents(first.Events(), second.Events()) {
				t.Fatalf("%s seed %d replayed differently:\n%s\n---\n%s",
					kind, seed, first, second)
			}
			if len(first.Events()) == 0 {
				t.Fatalf("%s seed %d produced no events at all over %v", kind, seed, d)
			}
		}
	}
}

// TestDifferentSeedsGiveDifferentTimelines would catch a generator that
// ignored its seed, which is the failure mode that makes a whole campaign of
// runs test the same thing over and over.
func TestDifferentSeedsGiveDifferentTimelines(t *testing.T) {
	links := []string{"a", "b", "c"}
	const d = 8 * time.Second
	seeds := []int64{1, 2, 3, 4, 5, 6, 7, 8}

	for _, kind := range generatedKinds {
		seen := map[string]int64{}
		for _, seed := range seeds {
			s, err := Generate(kind, links, seed, d)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			key := s.String()
			// The header carries the seed, so compare the body only.
			if i := strings.Index(key, "\n"); i >= 0 {
				key = key[i:]
			}
			if prev, dup := seen[key]; dup {
				t.Fatalf("%s: seeds %d and %d produced identical timelines:\n%s", kind, prev, seed, s)
			}
			seen[key] = seed
		}
	}
}

// TestEventsLieWithinTheRun keeps the nemesis honest: an event past the end of
// the run would never fire, and one before the start would fire late and be
// recorded at a time that never happened.
func TestEventsLieWithinTheRun(t *testing.T) {
	links := []string{"a", "b", "c"}
	known := map[string]bool{"a": true, "b": true, "c": true, "*": true}

	for _, kind := range generatedKinds {
		for _, d := range []time.Duration{500 * time.Millisecond, 2 * time.Second, 10 * time.Second} {
			for seed := int64(0); seed < 25; seed++ {
				s, err := Generate(kind, links, seed, d)
				if err != nil {
					t.Fatalf("Generate: %v", err)
				}
				var last time.Duration
				for i, e := range s.Events() {
					if e.At < 0 || e.At > d {
						t.Fatalf("%s seed %d: event %d at %v is outside [0, %v]", kind, seed, i, e.At, d)
					}
					if e.At < last {
						t.Fatalf("%s seed %d: event %d at %v goes backwards from %v", kind, seed, i, e.At, last)
					}
					last = e.At
					if !known[e.Link] {
						t.Fatalf("%s seed %d: event %d names unknown link %q", kind, seed, i, e.Link)
					}
					if e.Fault > Refuse {
						t.Fatalf("%s seed %d: event %d has fault %v", kind, seed, i, e.Fault)
					}
					if e.Fault == Delay && e.Delay <= 0 {
						t.Fatalf("%s seed %d: event %d is a delay of %v", kind, seed, i, e.Delay)
					}
				}
			}
		}
	}
}

// TestPartitionNeverCutsEveryLink guards the rule that makes a partition run
// worth anything: if every link is blackholed at once then every client is
// stranded, no operation completes, and the history says nothing.
func TestPartitionNeverCutsEveryLink(t *testing.T) {
	for _, n := range []int{2, 3, 5} {
		links := make([]string, n)
		for i := range links {
			links[i] = string(rune('a' + i))
		}
		for seed := int64(0); seed < 200; seed++ {
			s := Partition(links, seed, 10*time.Second)
			cut := map[string]bool{}
			for i, e := range s.Events() {
				switch {
				case e.Link == "*" && e.Fault == Pass:
					cut = map[string]bool{}
				case e.Fault == Drop:
					cut[e.Link] = true
				case e.Fault == Pass:
					delete(cut, e.Link)
				default:
					t.Fatalf("partition seed %d: event %d is a %v, want only drop and pass", seed, i, e.Fault)
				}
				if len(cut) >= n {
					t.Fatalf("partition seed %d: every one of %d links cut at %v\n%s", seed, n, e.At, s)
				}
			}
		}
	}
}

// TestPartitionNeedsTwoLinks documents the degenerate case rather than
// pretending it is a partition.
func TestPartitionNeedsTwoLinks(t *testing.T) {
	for _, links := range [][]string{nil, {}, {"only"}} {
		s := Partition(links, 1, 10*time.Second)
		if got := s.Events(); len(got) != 0 {
			t.Fatalf("Partition(%v) produced %d events; there is no non-empty strict subset to cut", links, len(got))
		}
	}
}

func TestGeneratorsDoNotMutateTheirInput(t *testing.T) {
	links := []string{"c", "a", "b"}
	before := strings.Join(links, ",")
	for _, kind := range generatedKinds {
		if _, err := Generate(kind, links, 3, 5*time.Second); err != nil {
			t.Fatalf("Generate: %v", err)
		}
	}
	if after := strings.Join(links, ","); after != before {
		t.Fatalf("the caller's slice was reordered: %s -> %s", before, after)
	}
}

func TestEventsReturnsACopy(t *testing.T) {
	s := Chaos([]string{"a", "b"}, 5, 5*time.Second)
	got := s.Events()
	if len(got) == 0 {
		t.Fatal("no events to test with")
	}
	got[0].Link = "tampered"
	if s.Events()[0].Link == "tampered" {
		t.Fatal("Events() handed out the schedule's own slice")
	}
}

func TestScheduleStringListsEveryEvent(t *testing.T) {
	s := Chaos([]string{"a", "b"}, 11, 6*time.Second)
	out := s.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if want := len(s.Events()) + 1; len(lines) != want {
		t.Fatalf("String() has %d lines, want a header plus %d events:\n%s", len(lines), len(s.Events()), out)
	}
	if !strings.Contains(lines[0], "chaos") || !strings.Contains(lines[0], "11") {
		t.Fatalf("header %q should name the kind and the seed", lines[0])
	}
	for i, e := range s.Events() {
		if !strings.Contains(lines[i+1], e.Fault.String()) {
			t.Fatalf("line %q does not mention fault %v", lines[i+1], e.Fault)
		}
		if !strings.Contains(lines[i+1], e.Link) {
			t.Fatalf("line %q does not mention link %q", lines[i+1], e.Link)
		}
	}
}

func TestNewScheduleSortsAndCopies(t *testing.T) {
	links := []string{"a", "b"}
	in := []Event{
		{At: 300 * time.Millisecond, Link: "*", Fault: Pass},
		{At: 100 * time.Millisecond, Link: "a", Fault: Drop},
		{At: 200 * time.Millisecond, Link: "b", Fault: Delay, Delay: 20 * time.Millisecond},
	}
	s, err := NewSchedule(links, time.Second, in)
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	got := s.Events()
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].At < got[i-1].At {
			t.Fatalf("events are not sorted: %v then %v", got[i-1], got[i])
		}
	}
	if s.Duration() != time.Second {
		t.Fatalf("Duration() = %v", s.Duration())
	}
	if s.Kind() == "" {
		t.Fatal("a hand-built schedule still needs a kind for the report")
	}
	// The caller's slice must be untouched, and must not alias the schedule.
	if in[0].At != 300*time.Millisecond {
		t.Fatal("NewSchedule sorted the caller's slice in place")
	}
	in[1].Link = "tampered"
	if s.Events()[0].Link == "tampered" {
		t.Fatal("the schedule aliases the caller's slice")
	}
	if !strings.Contains(s.String(), "drop") {
		t.Fatalf("String() = %q", s.String())
	}
}

func TestNewScheduleRejectsBadEvents(t *testing.T) {
	links := []string{"a", "b"}
	const d = time.Second
	cases := []struct {
		name  string
		event Event
	}{
		{"unknown link", Event{At: 0, Link: "z", Fault: Drop}},
		{"empty link", Event{At: 0, Link: "", Fault: Drop}},
		{"negative time", Event{At: -time.Millisecond, Link: "a", Fault: Drop}},
		{"past the end", Event{At: d + time.Millisecond, Link: "a", Fault: Drop}},
		{"unknown fault", Event{At: 0, Link: "a", Fault: Fault(99)}},
		{"negative delay", Event{At: 0, Link: "a", Fault: Delay, Delay: -time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewSchedule(links, d, []Event{tc.event}); err == nil {
				t.Fatalf("NewSchedule accepted %+v", tc.event)
			}
		})
	}
	if _, err := NewSchedule(links, -time.Second, nil); err == nil {
		t.Fatal("NewSchedule accepted a negative duration")
	}
	if _, err := NewSchedule(links, d, []Event{{At: d, Link: "*", Fault: Pass}}); err != nil {
		t.Fatalf(`NewSchedule rejected "*" or an event exactly at the end: %v`, err)
	}
}
