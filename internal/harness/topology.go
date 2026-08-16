package harness

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/faultnet"
)

// A Topology is the set of fault-injecting proxies standing in front of every
// hop in the cluster: one in front of each node for the clients, and one for
// each ordered pair of nodes for their traffic to each other.
//
// Proxying the peer hops as well as the client hops is what makes a partition
// real. Cut only the client links and the nodes carry on gossiping happily
// behind your back, the replicas never diverge, and the run proves nothing.
type Topology struct {
	client []*faultnet.Link
	peer   map[[2]int]*faultnet.Link
	names  []string
}

// clientLinkName and peerLinkName are the names used in schedules and reports.
func clientLinkName(node int) string   { return fmt.Sprintf("c%d", node) }
func peerLinkName(from, to int) string { return fmt.Sprintf("p%d-%d", from, to) }

// BuildTopology starts a proxy in front of every hop. addrs are the nodes' own
// addresses, in node order. When peers is false only the client links are
// created, which is right for a single-node target.
func BuildTopology(addrs []string, peers bool) (t *Topology, err error) {
	t = &Topology{peer: map[[2]int]*faultnet.Link{}}
	defer func() {
		if err != nil {
			_ = t.Close()
		}
	}()

	for i, addr := range addrs {
		l, lerr := faultnet.NewLink(clientLinkName(i), addr)
		if lerr != nil {
			return nil, fmt.Errorf("topology: client link for node %d: %w", i, lerr)
		}
		t.client = append(t.client, l)
		t.names = append(t.names, l.Name())
	}
	if peers {
		for i := range addrs {
			for j := range addrs {
				if i == j {
					continue
				}
				l, lerr := faultnet.NewLink(peerLinkName(i, j), addrs[j])
				if lerr != nil {
					return nil, fmt.Errorf("topology: peer link %d->%d: %w", i, j, lerr)
				}
				t.peer[[2]int{i, j}] = l
				t.names = append(t.names, l.Name())
			}
		}
	}
	sort.Strings(t.names)
	return t, nil
}

// Nodes is how many nodes the topology fronts.
func (t *Topology) Nodes() int { return len(t.client) }

// ClientAddr is the address a client should dial to reach node i.
func (t *Topology) ClientAddr(i int) string { return t.client[i].Addr() }

// ClientLink returns the proxy in front of node i.
func (t *Topology) ClientLink(i int) *faultnet.Link { return t.client[i] }

// PeerAddr is the address node `from` should dial to reach node `to`. When the
// topology has no peer links it falls back to the client link, so a caller
// does not have to special-case the single-node target.
func (t *Topology) PeerAddr(from, to int) string {
	if l, ok := t.peer[[2]int{from, to}]; ok {
		return l.Addr()
	}
	return t.client[to].Addr()
}

// LinkNames lists every link, sorted, for schedule generation.
func (t *Topology) LinkNames() []string {
	out := make([]string, len(t.names))
	copy(out, t.names)
	return out
}

// Links maps every link by name, for a Nemesis.
func (t *Topology) Links() map[string]*faultnet.Link {
	out := make(map[string]*faultnet.Link, len(t.names))
	for i, l := range t.client {
		out[clientLinkName(i)] = l
	}
	for k, l := range t.peer {
		out[peerLinkName(k[0], k[1])] = l
	}
	return out
}

// Stats snapshots every link.
func (t *Topology) Stats() map[string]faultnet.LinkStats {
	out := map[string]faultnet.LinkStats{}
	for name, l := range t.Links() {
		out[name] = l.Stats()
	}
	return out
}

// Close shuts every proxy down.
func (t *Topology) Close() error {
	var first error
	for _, l := range t.client {
		if err := l.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, l := range t.peer {
		if err := l.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// CutEvents returns the events that partition the network at time `at`, with
// the given nodes on one side of the break and the rest on the other.
//
// Only the peer links that cross the boundary go down. Client links stay up,
// and that is the whole point. In a real deployment a client sits on the same
// side of a break as the node it is talking to, so **both halves of a split
// cluster carry on serving** - which is exactly the situation in which an
// asynchronously replicated store starts telling two clients different things.
//
// Cutting the client links as well would strand the isolated side's clients.
// Nobody would ask those nodes anything, they could not diverge in a way anyone
// observed, and the run would report a clean history for a store that is
// plainly wrong. That is a much easier test to pass and a much worse one.
func (t *Topology) CutEvents(at time.Duration, isolated []int, f faultnet.Fault) []faultnet.Event {
	in := map[int]bool{}
	for _, n := range isolated {
		in[n] = true
	}
	var out []faultnet.Event
	for k := range t.peer {
		if in[k[0]] != in[k[1]] {
			out = append(out, faultnet.Event{At: at, Link: peerLinkName(k[0], k[1]), Fault: f})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Link < out[b].Link })
	return out
}

// CutClientEvents additionally strands the clients of the given nodes.
//
// This is the other half of the picture: a client that can reach nothing gets
// no answer, and an operation with no answer is indeterminate rather than
// failed. Without some of this a run never exercises the hardest case the
// checker has to handle, so partition schedules mix it in - but sparingly,
// because every stranded client is one fewer observer of the divergence.
func (t *Topology) CutClientEvents(at time.Duration, isolated []int, f faultnet.Fault) []faultnet.Event {
	out := make([]faultnet.Event, 0, len(isolated))
	for _, n := range isolated {
		if n >= 0 && n < len(t.client) {
			out = append(out, faultnet.Event{At: at, Link: clientLinkName(n), Fault: f})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Link < out[b].Link })
	return out
}

// HealEvents returns the events that restore every link at time `at`.
func (t *Topology) HealEvents(at time.Duration) []faultnet.Event {
	names := t.LinkNames()
	out := make([]faultnet.Event, 0, len(names))
	for _, n := range names {
		out = append(out, faultnet.Event{At: at, Link: n, Fault: faultnet.Pass})
	}
	return out
}

// PartitionSchedule builds a node-level partition timeline from a seed.
//
// It alternates cut and heal phases for the whole run, choosing a fresh
// non-empty strict subset of nodes to isolate each time. Isolating every node
// is excluded deliberately: nothing would be reachable, every operation would
// time out, and the run would measure the client timeout rather than the store.
//
// The last phase is always a heal, so the harness's quiescent reads happen on a
// whole network. A run that ends mid-partition produces a final round of
// timeouts and a history that cannot distinguish a broken store from a broken
// network.
func PartitionSchedule(t *Topology, seed int64, d time.Duration, f faultnet.Fault) (*faultnet.Schedule, error) {
	names := t.LinkNames()
	if t.Nodes() < 2 {
		// One node has no boundary to cut. Blip its client link instead, so a
		// single-node target still meets timeouts and indeterminate results.
		return singleNodeSchedule(names, seed, d, f)
	}

	rng := rand.New(rand.NewPCG(uint64(seed), 0x5EED))
	var events []faultnet.Event

	// Leave the first slice of the run clean so the clients establish a
	// baseline, and reserve the last for the heal.
	warmup := d / 8
	tail := d / 8
	now := warmup
	for now < d-tail {
		isolated := randomProperSubset(rng, t.Nodes())
		cutFor := jitter(rng, 300*time.Millisecond, 1200*time.Millisecond)
		if now+cutFor > d-tail {
			cutFor = d - tail - now
		}
		if cutFor <= 0 {
			break
		}
		events = append(events, t.CutEvents(now, isolated, f)...)

		// Occasionally strand one node's clients as well, so that some
		// operations come back indeterminate - the hardest case the checker
		// has to handle, and one a pure peer-level partition never produces.
		//
		// Only ever when the isolated side has a node to spare. Stranding the
		// clients of a lone isolated node would leave that side of the split
		// with nobody asking it anything, and a divergence nobody observes is
		// a divergence the history cannot record.
		if len(isolated) >= 2 && rng.IntN(3) == 0 {
			events = append(events, t.CutClientEvents(now, isolated[:1], f)...)
		}

		events = append(events, t.HealEvents(now+cutFor)...)
		now += cutFor + jitter(rng, 200*time.Millisecond, 900*time.Millisecond)
	}
	events = append(events, t.HealEvents(d-tail)...)
	return faultnet.NewNamedSchedule("partition", seed, names, d, events)
}

// singleNodeSchedule blips the one client link of a single-node target.
func singleNodeSchedule(names []string, seed int64, d time.Duration, f faultnet.Fault) (*faultnet.Schedule, error) {
	rng := rand.New(rand.NewPCG(uint64(seed), 0x5EED))
	var events []faultnet.Event
	tail := d - (d / 8)
	now := d / 8
	for now < tail {
		cutFor := jitter(rng, 200*time.Millisecond, 700*time.Millisecond)
		// Clamped the way the multi-node path clamps it. A cut beginning just
		// before the tail and lasting the full 700ms heals after the run has
		// ended, and NewNamedSchedule then refuses the entire schedule - so a
		// short single-node run failed to start at all, on 21 of 200 seeds at
		// three seconds and on none at eight.
		if now+cutFor > tail {
			cutFor = tail - now
		}
		events = append(events,
			faultnet.Event{At: now, Link: names[0], Fault: f},
			faultnet.Event{At: now + cutFor, Link: names[0], Fault: faultnet.Pass},
		)
		now += cutFor + jitter(rng, 200*time.Millisecond, 900*time.Millisecond)
	}
	events = append(events, faultnet.Event{At: d - (d / 8), Link: names[0], Fault: faultnet.Pass})
	return faultnet.NewNamedSchedule("partition", seed, names, d, events)
}

// randomProperSubset picks a non-empty subset of [0,n) that is not all of it.
//
// Fewer than two nodes has no such subset, and the rejection loop below would
// spin for ever looking for one. Callers are supposed to have taken the
// single-node path already; this refuses rather than hangs if one has not.
func randomProperSubset(rng *rand.Rand, n int) []int {
	if n < 2 {
		return nil
	}
	for {
		var chosen []int
		for i := 0; i < n; i++ {
			if rng.IntN(2) == 1 {
				chosen = append(chosen, i)
			}
		}
		if len(chosen) > 0 && len(chosen) < n {
			return chosen
		}
	}
}

// jitter returns a duration drawn uniformly from [lo, hi].
func jitter(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rng.Int64N(int64(hi-lo)+1))
}
