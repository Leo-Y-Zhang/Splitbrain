package harness

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/faultnet"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// deadAddrs are addresses nothing is listening on. The topology only needs to
// bind its own listeners; it never dials the target until a client does.
func deadAddrs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "127.0.0.1:9"
	}
	return out
}

func TestBuildTopologyCreatesEveryHop(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	// Three client links plus one for each ordered pair.
	if got, want := len(topo.LinkNames()), 3+6; got != want {
		t.Fatalf("%d links, want %d: %v", got, want, topo.LinkNames())
	}
	if got := len(topo.Links()); got != 9 {
		t.Fatalf("Links() returned %d entries, want 9", got)
	}

	seen := map[string]bool{}
	for _, n := range topo.LinkNames() {
		if seen[n] {
			t.Fatalf("duplicate link name %q", n)
		}
		seen[n] = true
	}
	for i := 0; i < 3; i++ {
		if !seen[clientLinkName(i)] {
			t.Errorf("no client link for node %d", i)
		}
		for j := 0; j < 3; j++ {
			if i != j && !seen[peerLinkName(i, j)] {
				t.Errorf("no peer link %d->%d", i, j)
			}
		}
	}
}

func TestBuildTopologyWithoutPeers(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(1), false)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	if got := len(topo.LinkNames()); got != 1 {
		t.Fatalf("%d links, want 1", got)
	}
	// With no peer links the fallback must still give a usable address rather
	// than an empty string, or a single-node target would be configured with
	// nothing and fail in a confusing place.
	if got := topo.PeerAddr(0, 0); got != topo.ClientAddr(0) {
		t.Fatalf("PeerAddr fell back to %q, want the client link %q", got, topo.ClientAddr(0))
	}
}

func TestClientAddressesAreDistinctAndNotTheBackend(t *testing.T) {
	addrs := deadAddrs(3)
	topo, err := BuildTopology(addrs, true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		a := topo.ClientAddr(i)
		if a == addrs[i] {
			t.Fatalf("node %d's client address is the backend itself; nothing would be proxied", i)
		}
		if seen[a] {
			t.Fatalf("two links share the address %s", a)
		}
		seen[a] = true
	}
}

func TestCutEventsCutOnlyTheBoundary(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	got := map[string]bool{}
	for _, e := range topo.CutEvents(time.Second, []int{1}, faultnet.Drop) {
		if e.At != time.Second {
			t.Errorf("event at %s, want 1s", e.At)
		}
		if e.Fault != faultnet.Drop {
			t.Errorf("event carries %s, want drop", e.Fault)
		}
		got[e.Link] = true
	}

	// Isolating node 1 cuts every peer link that crosses the boundary, in both
	// directions, and nothing else. Nodes 0 and 2 are on the same side and must
	// still be able to talk. Crucially the client links stay up: both halves of
	// a split cluster carry on serving, which is the situation that makes an
	// asynchronously replicated store diverge where somebody can see it.
	want := []string{"p0-1", "p1-0", "p1-2", "p2-1"}
	unwanted := []string{"c0", "c1", "c2", "p0-2", "p2-0"}
	for _, w := range want {
		if !got[w] {
			t.Errorf("%s should have been cut", w)
		}
	}
	for _, u := range unwanted {
		if got[u] {
			t.Errorf("%s was cut, but it does not cross the boundary", u)
		}
	}
	if len(got) != len(want) {
		t.Errorf("cut %d links, want %d: %v", len(got), len(want), got)
	}
}

func TestCutEventsWithEverythingOnOneSideCutsNothing(t *testing.T) {
	// A degenerate request: if every node is on the same side there is no
	// boundary for anything to cross, so the "partition" must be a no-op rather
	// than a total blackout that makes the run measure client timeouts.
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	if got := topo.CutEvents(0, []int{0, 1, 2}, faultnet.Drop); len(got) != 0 {
		t.Fatalf("isolating every node produced %d cuts: %v", len(got), got)
	}
}

func TestCutClientEventsStrandOnlyWhatWasAsked(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	got := topo.CutClientEvents(time.Second, []int{2}, faultnet.Drop)
	if len(got) != 1 || got[0].Link != "c2" {
		t.Fatalf("CutClientEvents([2]) = %v, want exactly c2", got)
	}
	// Out-of-range indices must be ignored rather than panic: a schedule
	// generator that shrinks the cluster should not take the run down with it.
	if got := topo.CutClientEvents(0, []int{-1, 7}, faultnet.Drop); len(got) != 0 {
		t.Fatalf("out-of-range nodes produced %v", got)
	}
}

func TestHealEventsCoverEveryLink(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	events := topo.HealEvents(2 * time.Second)
	if len(events) != len(topo.LinkNames()) {
		t.Fatalf("%d heal events for %d links", len(events), len(topo.LinkNames()))
	}
	for _, e := range events {
		if e.Fault != faultnet.Pass {
			t.Errorf("heal event for %s carries %s", e.Link, e.Fault)
		}
	}
}

func TestPartitionScheduleIsDeterministic(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	a, err := PartitionSchedule(topo, 7, 8*time.Second, faultnet.Drop)
	if err != nil {
		t.Fatal(err)
	}
	b, err := PartitionSchedule(topo, 7, 8*time.Second, faultnet.Drop)
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != b.String() {
		t.Fatalf("the same seed gave two different timelines:\n%s\n---\n%s", a.String(), b.String())
	}

	// And different seeds must actually differ, or the seed is decoration.
	same := 0
	for seed := int64(0); seed < 8; seed++ {
		s, err := PartitionSchedule(topo, seed, 8*time.Second, faultnet.Drop)
		if err != nil {
			t.Fatal(err)
		}
		if s.String() == a.String() {
			same++
		}
	}
	if same > 1 {
		t.Fatalf("%d of 8 seeds produced the same timeline as seed 7", same)
	}
}

func TestPartitionScheduleStaysInsideTheRun(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	const d = 6 * time.Second
	for seed := int64(0); seed < 20; seed++ {
		s, err := PartitionSchedule(topo, seed, d, faultnet.Drop)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		events := s.Events()
		if len(events) == 0 {
			t.Fatalf("seed %d produced no events; a partition schedule that partitions nothing makes every verdict meaningless", seed)
		}
		for _, e := range events {
			if e.At < 0 || e.At > d {
				t.Fatalf("seed %d: event at %s is outside [0,%s]", seed, e.At, d)
			}
		}
		// The run has to end healed, or the final reads measure the network
		// rather than the store.
		last := events[len(events)-1]
		if last.Fault != faultnet.Pass {
			t.Fatalf("seed %d ends with %s on %s rather than healed", seed, last.Fault, last.Link)
		}
	}
}

func TestPartitionScheduleNeverIsolatesEveryNodeAtOnce(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	for seed := int64(0); seed < 40; seed++ {
		s, err := PartitionSchedule(topo, seed, 8*time.Second, faultnet.Drop)
		if err != nil {
			t.Fatal(err)
		}
		// Replay the timeline. At no instant may every client link be down -
		// every client would be stranded and the run would be measuring the
		// client timeout - and at no instant may every peer link be down in
		// both directions, which is a total blackout rather than a partition.
		down := map[string]bool{}
		for _, e := range s.Events() {
			down[e.Link] = e.Fault != faultnet.Pass
			if down["c0"] && down["c1"] && down["c2"] {
				t.Fatalf("seed %d strands every client at %s", seed, e.At)
			}
			if down["p0-1"] && down["p1-0"] && down["p0-2"] && down["p2-0"] && down["p1-2"] && down["p2-1"] {
				t.Fatalf("seed %d cuts every peer link at %s, which is a blackout and not a partition", seed, e.At)
			}
		}
	}
}

func TestPartitionScheduleMixesPeerCutsWithStrandedClients(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	seedsWithPeerCut, seedsWithStrandedClient := 0, 0
	const seeds = 30
	for seed := int64(0); seed < seeds; seed++ {
		s, err := PartitionSchedule(topo, seed, 8*time.Second, faultnet.Drop)
		if err != nil {
			t.Fatal(err)
		}
		peer, client := false, false
		for _, e := range s.Events() {
			if e.Fault == faultnet.Pass {
				continue
			}
			if e.Link[0] == 'c' {
				client = true
			} else {
				peer = true
			}
		}
		if peer {
			seedsWithPeerCut++
		}
		if client {
			seedsWithStrandedClient++
		}
	}

	// Every seed must cut peer links: that is what makes replicas diverge, and
	// a schedule that never does it hands out clean verdicts for free.
	if seedsWithPeerCut != seeds {
		t.Errorf("only %d of %d seeds cut a peer link", seedsWithPeerCut, seeds)
	}
	// Some, but not all, should also strand clients. Never doing it means no
	// run has an indeterminate operation; always doing it means nobody is left
	// watching the divergence.
	if seedsWithStrandedClient == 0 {
		t.Error("no seed ever stranded a client, so no run will produce an indeterminate operation")
	}
	if seedsWithStrandedClient == seeds {
		t.Error("every seed stranded a client; the divergence would go unobserved")
	}
	t.Logf("%d/%d seeds cut peers, %d/%d also stranded a client", seedsWithPeerCut, seeds, seedsWithStrandedClient, seeds)
}

func TestSingleNodeScheduleStillProducesFaults(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(1), false)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	s, err := PartitionSchedule(topo, 3, 6*time.Second, faultnet.Drop)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Events()) < 2 {
		t.Fatalf("a single-node run produced %d events; it should still meet timeouts", len(s.Events()))
	}
}

// TestSingleNodeScheduleStaysInsideTheRun is the multi-node test above asked of
// the other half of PartitionSchedule, which is where it was never asked.
//
// The single-node path blips one client link and then heals it, and the heal
// was placed at now+cutFor with nothing to stop it landing after the run ends.
// A cut may begin at any instant before d-d/8 and last up to 700ms, so any run
// shorter than about 5.6s can generate a heal past its own duration - at which
// point NewNamedSchedule refuses the whole schedule and `splitbrain run
// -target kvsingle -duration 3s` dies before it has started a single process,
// on some seeds and not others.
//
// Three seconds is not an arbitrary choice: it is what the campaign tests run
// kvsingle for, so this was a seed away from being a flake in the suite that
// proves the correct store is never accused.
func TestSingleNodeScheduleStaysInsideTheRun(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(1), false)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	for _, d := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 6 * time.Second} {
		for seed := int64(0); seed < 20; seed++ {
			s, err := PartitionSchedule(topo, seed, d, faultnet.Drop)
			if err != nil {
				t.Fatalf("duration %s, seed %d: %v", d, seed, err)
			}
			events := s.Events()
			if len(events) == 0 {
				t.Fatalf("duration %s, seed %d produced no events; a run that meets no timeouts proves nothing", d, seed)
			}
			for _, e := range events {
				if e.At < 0 || e.At > d {
					t.Fatalf("duration %s, seed %d: event at %s is outside [0,%s]", d, seed, e.At, d)
				}
			}
			// The run has to end healed, or the final reads measure the
			// network rather than the store.
			last := events[len(events)-1]
			if last.Fault != faultnet.Pass {
				t.Fatalf("duration %s, seed %d ends with %s on %s rather than healed", d, seed, last.Fault, last.Link)
			}
		}
	}
}

func TestRandomProperSubsetRefusesTinyClusters(t *testing.T) {
	// The rejection loop would spin for ever looking for a subset that does
	// not exist.
	rng := rand.New(rand.NewPCG(1, 2))
	for n := 0; n < 2; n++ {
		if got := randomProperSubset(rng, n); got != nil {
			t.Fatalf("randomProperSubset(%d) = %v, want nil", n, got)
		}
	}
	for n := 2; n < 6; n++ {
		for i := 0; i < 200; i++ {
			got := randomProperSubset(rng, n)
			if len(got) == 0 || len(got) >= n {
				t.Fatalf("randomProperSubset(%d) = %v, which is not a non-empty proper subset", n, got)
			}
		}
	}
}

func TestBuildScheduleKinds(t *testing.T) {
	topo, err := BuildTopology(deadAddrs(3), true)
	if err != nil {
		t.Fatal(err)
	}
	defer topo.Close()

	healthy, err := BuildSchedule(topo, Config{Faults: "none", Seed: 1, Duration: 4 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(healthy.Events()) != 0 {
		t.Fatalf("the healthy schedule has %d events; it is the control and must do nothing", len(healthy.Events()))
	}

	for _, kind := range []string{"partition", "refuse"} {
		s, err := BuildSchedule(topo, Config{Faults: kind, Seed: 1, Duration: 6 * time.Second})
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if len(s.Events()) == 0 {
			t.Fatalf("%s produced no events", kind)
		}
	}

	if _, err := BuildSchedule(topo, Config{Faults: "nonsense", Seed: 1, Duration: time.Second}); err == nil {
		t.Fatal("an unknown fault kind was accepted; a typo would silently run with no faults at all")
	}
}

func TestGenerateProducesAUsefulMix(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 22))
	keys := Keys(4)
	believed := map[string]int{}
	counts := map[history.Kind]int{}
	usedBelief := 0

	for i := 0; i < 5000; i++ {
		req := generate(rng, keys, believed, 1000)
		counts[req.Kind]++

		if !contains(keys, req.Key) {
			t.Fatalf("generated an operation on key %q, which is not in the run's key set", req.Key)
		}
		if req.Kind == history.CAS {
			// A no-op compare-and-swap constrains nothing and the history
			// validator rejects it outright.
			if req.From == req.To {
				t.Fatal("generated a cas from a value to itself")
			}
			if v, ok := believed[req.Key]; ok && req.From == v {
				usedBelief++
			}
		}
		// Pretend the operation succeeded so the belief map evolves the way it
		// does in a real run.
		switch req.Kind {
		case history.Write:
			believed[req.Key] = req.Value
		case history.CAS:
			if believed[req.Key] == req.From {
				believed[req.Key] = req.To
			}
		}
	}

	for _, k := range []history.Kind{history.Read, history.Write, history.CAS} {
		if counts[k] < 500 {
			t.Errorf("only %d %s operations in 5000; the mix is lopsided", counts[k], k)
		}
	}
	// The whole reason compare-and-swap catches split brain is that it usually
	// swaps. A generator that always guesses a value nobody wrote produces
	// "did not swap" every time, which constrains almost nothing.
	if ratio := float64(usedBelief) / float64(counts[history.CAS]); ratio < 0.5 {
		t.Errorf("only %.0f%% of compare-and-swaps used the client's own last observation; they will almost never swap", ratio*100)
	}
}

func TestKeysAreStable(t *testing.T) {
	got := Keys(3)
	want := []string{"k0", "k1", "k2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Keys(3) = %v, want %v", got, want)
	}
}

func TestConfigDefaultsAreSane(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.OpTimeout >= c.Duration {
		t.Fatal("the default operation timeout is not shorter than the default run; a blackholed client would spend the whole run in one call")
	}
	if c.Clients < 2 {
		t.Fatal("fewer than two clients cannot produce concurrency")
	}
	if c.Quiesce <= 0 {
		t.Fatal("without a quiescent phase the run has no uncontended final reads")
	}
}

func TestResultSummaryReportsWhatHappened(t *testing.T) {
	r := &Result{
		History: history.History{
			{Process: 0, Key: "k0", Kind: history.Read, Outcome: history.OK, Invoke: 1, Complete: 2},
			{Process: 1, Key: "k0", Kind: history.Write, Outcome: history.Info, Invoke: 3, Complete: history.Pending},
		},
		LinkStats: map[string]faultnet.LinkStats{"c0": {BytesForward: 100, BytesDropped: 50}},
		Elapsed:   time.Second,
	}
	got := r.Summary()
	for _, want := range []string{"2 operations", "1 ok", "1 indeterminate", "100 bytes forwarded", "50 dropped"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q is missing %q", got, want)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
