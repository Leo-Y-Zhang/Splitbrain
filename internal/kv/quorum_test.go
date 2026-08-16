package kv

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// nowhere is an address nothing will ever answer on. Using it rather than
// waiting and hoping is what keeps the tests below deterministic: replication
// to a refused port cannot succeed later, so an assertion that two nodes
// disagree is not a race against a background goroutine.
func nowhere(t *testing.T) string { return closedPort(t) }

// quorumPair returns two nodes that replicate to each other.
func quorumPair(t *testing.T) (a, b *Client) {
	t.Helper()
	sa, sb := NewQuorumStore("a", nil, testTimeout, nil), NewQuorumStore("b", nil, testTimeout, nil)
	a, b = serve(t, "a", sa), serve(t, "b", sb)
	if err := sa.Configure(Config{Peers: &[]string{b.Addr()}}); err != nil {
		t.Fatalf("configure a: %v", err)
	}
	if err := sb.Configure(Config{Peers: &[]string{a.Addr()}}); err != nil {
		t.Fatalf("configure b: %v", err)
	}
	return a, b
}

func read(t *testing.T, c *Client, key string) history.Op {
	t.Helper()
	return c.Do(context.Background(), testClock, 0, Request{Kind: history.Read, Key: key})
}

func write(t *testing.T, c *Client, key string, v int) history.Op {
	t.Helper()
	return c.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: key, Value: v})
}

// waitFor polls until c reads want, or gives up. Replication is asynchronous,
// so the positive direction is the only one that may be waited for.
func waitFor(t *testing.T, c *Client, key string, want int) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if op := read(t, c, key); op.Outcome == history.OK && op.Observed == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// The fixture has to actually replicate, or its wrongness would be trivial and
// would prove nothing about asynchronous replication.
func TestQuorumReplicatesEventually(t *testing.T) {
	a, b := quorumPair(t)
	if op := write(t, a, "x", 7); op.Outcome != history.OK {
		t.Fatalf("write to a = %s (%s)", op.Outcome, op.Err)
	}
	if !waitFor(t, b, "x", 7) {
		t.Error("b never saw the write; the fan-out is not working at all")
	}
	if op := write(t, b, "y", 9); op.Outcome != history.OK {
		t.Fatalf("write to b = %s (%s)", op.Outcome, op.Err)
	}
	if !waitFor(t, a, "y", 9) {
		t.Error("a never saw b's write; replication is one-directional")
	}
}

// The demonstration the whole tool exists for. Two nodes that have not
// exchanged replication give different answers to the same question, and the
// disagreement is not merely stale - it is a linearizability violation that
// can be pointed at without running the checker.
func TestQuorumStaleReadIsALinearizabilityViolation(t *testing.T) {
	dead := nowhere(t)
	a := serve(t, "a", NewQuorumStore("a", []string{dead}, testTimeout, nil))
	b := serve(t, "b", NewQuorumStore("b", []string{dead}, testTimeout, nil))

	origin := testClock
	ctx := context.Background()

	w := a.Do(ctx, origin, 0, Request{Kind: history.Write, Key: "x", Value: 7})
	if w.Outcome != history.OK {
		t.Fatalf("write to a = %s (%s)", w.Outcome, w.Err)
	}

	// Let the platform clock advance past its own granularity, which on
	// Windows is about half a millisecond. This is not waiting for
	// replication: a's only peer is a closed port, so the update can never
	// arrive at b however long anyone waits.
	time.Sleep(10 * time.Millisecond)

	r := b.Do(ctx, origin, 1, Request{Kind: history.Read, Key: "x"})
	if r.Outcome != history.OK {
		t.Fatalf("read from b = %s (%s); the point of this store is that it stays available", r.Outcome, r.Err)
	}

	if r.Observed == 7 {
		t.Fatal("b returned the value written to a; the nodes did not disagree, so the fixture " +
			"is not demonstrating anything and every downstream test of the checker is vacuous")
	}
	if r.Observed != 0 {
		t.Errorf("b returned %d, want the initial value 0", r.Observed)
	}

	// Spell out why this is a violation rather than merely surprising. The
	// write completed before the read was invoked, so in any sequential order
	// consistent with real time the write comes first and the read must
	// return 7.
	if !(w.Complete <= r.Invoke) {
		t.Fatalf("the write did not finish before the read began (%d..%d then %d..%d); "+
			"without that the reader is free to be ordered first and nothing is proved",
			w.Invoke, w.Complete, r.Invoke, r.Complete)
	}
	if err := (history.History{w, r}).Validate(); err != nil {
		t.Errorf("the two operations do not form a valid history: %v", err)
	}
}

// A node must answer from its own map without waiting on anyone. A peer that
// accepts connections and then says nothing is the worst case, because a dial
// to it succeeds.
func TestQuorumDoesNotBlockOnAnUnreachablePeer(t *testing.T) {
	blackhole := blackholeAddr(t)
	// The peer timeout is generous so that, if the handler did wait for the
	// fan-out, it would wait far longer than the assertion below allows.
	a := serve(t, "a", NewQuorumStore("a", []string{blackhole}, 10*time.Second, nil))

	started := time.Now()
	for i := 0; i < 5; i++ {
		if op := write(t, a, "x", i); op.Outcome != history.OK {
			t.Fatalf("write %d = %s (%s)", i, op.Outcome, op.Err)
		}
		if op := read(t, a, "x"); op.Outcome != history.OK || op.Observed != i {
			t.Fatalf("read %d = %s/%d", i, op.Outcome, op.Observed)
		}
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Errorf("ten local operations took %v; the handler is waiting on the peer", elapsed)
	}
}

// Replicated updates are ordered by wall-clock timestamp and tie-broken by
// node id. An older update arriving late must not overwrite a newer one.
func TestQuorumLastWriterWins(t *testing.T) {
	s := NewQuorumStore("z", nil, testTimeout, nil)
	c := serve(t, "z", s)

	replicate := func(key string, value int, stamp int64, node string) {
		t.Helper()
		body := fmt.Sprintf(`{"key":%q,"value":%d,"timestamp":%d,"node":%q}`, key, value, stamp, node)
		if status, reply := post(t, c, "/replicate", body); status != 200 || !strings.Contains(reply, `"ok":true`) {
			t.Fatalf("replicate: status %d, %s", status, reply)
		}
	}

	replicate("k", 10, 1000, "a")
	if op := read(t, c, "k"); op.Observed != 10 {
		t.Fatalf("after the first update, k = %d, want 10", op.Observed)
	}
	replicate("k", 20, 2000, "a")
	if op := read(t, c, "k"); op.Observed != 20 {
		t.Errorf("a newer update did not win: k = %d, want 20", op.Observed)
	}
	replicate("k", 30, 1500, "a")
	if op := read(t, c, "k"); op.Observed != 20 {
		t.Errorf("an older update arriving late overwrote a newer one: k = %d, want 20", op.Observed)
	}
	// Same instant, higher node id wins, so that all nodes resolve the tie the
	// same way and do not diverge for ever.
	replicate("k", 40, 2000, "b")
	if op := read(t, c, "k"); op.Observed != 40 {
		t.Errorf("the tie was not broken by node id: k = %d, want 40", op.Observed)
	}
	replicate("k", 50, 2000, "a")
	if op := read(t, c, "k"); op.Observed != 40 {
		t.Errorf("a lower node id won a tie: k = %d, want 40", op.Observed)
	}
}

func TestQuorumReplicateRejectsMalformedInput(t *testing.T) {
	c := serve(t, "z", NewQuorumStore("z", nil, testTimeout, nil))
	for _, body := range []string{``, `{`, `{"key":"","value":1,"timestamp":1,"node":"a"}`, `{"key":"k","value":1,"timestamp":1}`} {
		status, reply := post(t, c, "/replicate", body)
		if status != 200 {
			t.Errorf("body %q: status = %d, want 200", body, status)
		}
		if !strings.Contains(reply, `"ok":false`) {
			t.Errorf("body %q: reply = %s, want a refusal", body, reply)
		}
	}
	// Still usable afterwards.
	if op := read(t, c, "k"); op.Outcome != history.OK {
		t.Errorf("the node stopped working after a bad replication: %s", op.Err)
	}
}

// A locally generated write must beat anything the node already holds, or the
// node would have to report success for a write it discarded.
func TestQuorumLocalWriteBeatsAHeldUpdate(t *testing.T) {
	s := NewQuorumStore("a", nil, testTimeout, nil)
	c := serve(t, "a", s)

	// A replicated update stamped far in the future, which a naive
	// last-writer-wins comparison would let win for ever.
	future := time.Now().Add(time.Hour).UnixNano()
	body := fmt.Sprintf(`{"key":"k","value":99,"timestamp":%d,"node":"z"}`, future)
	if status, _ := post(t, c, "/replicate", body); status != 200 {
		t.Fatalf("replicate: status %d", status)
	}
	if op := read(t, c, "k"); op.Observed != 99 {
		t.Fatalf("setup: k = %d, want 99", op.Observed)
	}
	if op := write(t, c, "k", 1); op.Outcome != history.OK {
		t.Fatalf("write = %s (%s)", op.Outcome, op.Err)
	}
	if op := read(t, c, "k"); op.Observed != 1 {
		t.Errorf("k = %d after a local write reported success, want 1; the node reported a "+
			"write it then discarded", op.Observed)
	}
}

// CAS is a local read-modify-write. Under the local mutex it is atomic, which
// is what makes the store look correct until two nodes are involved.
func TestQuorumCASIsLocal(t *testing.T) {
	dead := nowhere(t)
	a := serve(t, "a", NewQuorumStore("a", []string{dead}, testTimeout, nil))
	b := serve(t, "b", NewQuorumStore("b", []string{dead}, testTimeout, nil))
	ctx := context.Background()

	if op := write(t, a, "x", 3); op.Outcome != history.OK {
		t.Fatalf("setup write: %s", op.Err)
	}
	op := a.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 3, To: 4})
	if op.Outcome != history.OK || !op.Swapped {
		t.Errorf("cas on a = %s/swapped=%t, want ok/true", op.Outcome, op.Swapped)
	}
	// The same CAS on the node that never heard about the write succeeds from
	// a different starting value, which is the divergence in its clearest
	// form: both nodes believe they swapped from 3.
	if op := write(t, b, "x", 3); op.Outcome != history.OK {
		t.Fatalf("setup write on b: %s", op.Err)
	}
	op = b.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 3, To: 5})
	if op.Outcome != history.OK || !op.Swapped {
		t.Errorf("cas on b = %s/swapped=%t, want ok/true", op.Outcome, op.Swapped)
	}
	if ra, rb := read(t, a, "x"), read(t, b, "x"); ra.Observed == rb.Observed {
		t.Errorf("both nodes hold %d; the nodes were supposed to have diverged", ra.Observed)
	}
}

func TestQuorumConfigureReplacesPeers(t *testing.T) {
	sa := NewQuorumStore("a", []string{nowhere(t)}, testTimeout, nil)
	a := serve(t, "a", sa)
	b := serve(t, "b", NewQuorumStore("b", nil, testTimeout, nil))

	if op := write(t, a, "x", 1); op.Outcome != history.OK {
		t.Fatalf("write: %s", op.Err)
	}
	if op := read(t, b, "x"); op.Observed != 0 {
		t.Fatalf("b already holds %d before being configured as a peer", op.Observed)
	}

	if err := sa.Configure(Config{Peers: &[]string{b.Addr()}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if op := write(t, a, "x", 2); op.Outcome != history.OK {
		t.Fatalf("write after configure: %s", op.Err)
	}
	if !waitFor(t, b, "x", 2) {
		t.Error("b never saw the write after being configured as a's peer")
	}

	// Replacing the peer list with an empty one isolates the node again.
	if err := sa.Configure(Config{Peers: &[]string{}}); err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if op := write(t, a, "x", 3); op.Outcome != history.OK {
		t.Fatalf("write after isolation: %s", op.Err)
	}
	time.Sleep(50 * time.Millisecond)
	if op := read(t, b, "x"); op.Observed == 3 {
		t.Error("b saw a write made after it was removed from the peer list")
	}
}

// Close must return promptly even with a peer that never answers, and must be
// safe to call twice, because the server calls it on shutdown and tests call
// it in cleanup.
func TestQuorumCloseIsPromptAndIdempotent(t *testing.T) {
	s := NewQuorumStore("a", []string{blackholeAddr(t)}, 10*time.Second, nil)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, err := s.Apply(ctx, Request{Kind: history.Write, Key: "k", Value: i}); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return; the fan-out is waiting on an unreachable peer")
	}
	if err := s.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
	// A store that has been closed must still answer rather than panic; the
	// server shuts down the listener first, but a request can be in flight.
	if _, err := s.Apply(ctx, Request{Kind: history.Write, Key: "k", Value: 1}); err != nil {
		t.Errorf("apply after close: %v", err)
	}
}

// boolptr is for building a Config that switches synchronous mode explicitly
// on or off, as opposed to leaving it alone.
func boolptr(b bool) *bool { return &b }

// In synchronous mode a healthy network has delivered the value to every peer
// by the time the client is told the write finished. There is no sleep in this
// test on purpose: if the write returned early the read below would race it.
func TestQuorumSyncWriteReachesEveryPeerBeforeReturning(t *testing.T) {
	sa := NewQuorumStore("a", nil, 5*time.Second, nil)
	sb := NewQuorumStore("b", nil, 5*time.Second, nil)
	sc := NewQuorumStore("c", nil, 5*time.Second, nil)
	a, b, c := serve(t, "a", sa), serve(t, "b", sb), serve(t, "c", sc)

	if err := sa.Configure(Config{Peers: &[]string{b.Addr(), c.Addr()}, Sync: boolptr(true)}); err != nil {
		t.Fatalf("configure a: %v", err)
	}

	if op := write(t, a, "x", 7); op.Outcome != history.OK {
		t.Fatalf("write to a = %s (%s)", op.Outcome, op.Err)
	}
	for name, peer := range map[string]*Client{"b": b, "c": c} {
		if op := read(t, peer, "x"); op.Observed != 7 {
			t.Errorf("%s holds %d immediately after the write returned, want 7; the write did not "+
				"wait for its peers", name, op.Observed)
		}
	}

	// The write half of a CAS replicates the same way.
	if op := a.Do(context.Background(), testClock, 0, Request{Kind: history.CAS, Key: "x", From: 7, To: 8}); !op.Swapped {
		t.Fatalf("cas did not swap")
	}
	if op := read(t, b, "x"); op.Observed != 8 {
		t.Errorf("b holds %d after a synchronous cas, want 8", op.Observed)
	}
}

// The deliberate bug in synchronous mode, pinned so nobody "fixes" it.
//
// A write whose peer never answers still reports success. That is the design
// this fixture models - teams choose it precisely so a write does not fail
// when a replica is down - and it is what makes a partition, rather than
// ordinary asynchrony, the thing that breaks the store.
func TestQuorumSyncWriteReportsSuccessEvenWhenAPeerNeverAnswers(t *testing.T) {
	const peerTimeout = 200 * time.Millisecond
	s := NewQuorumStore("a", nil, peerTimeout, nil)
	node := serve(t, "a", s)
	// The client must outlast the node's own peer timeout, or this would
	// measure the client giving up rather than what the node decided.
	c := patient(node)
	if err := s.Configure(Config{Peers: &[]string{blackholeAddr(t)}, Sync: boolptr(true)}); err != nil {
		t.Fatalf("configure: %v", err)
	}

	started := time.Now()
	op := c.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 7})
	elapsed := time.Since(started)

	if op.Outcome != history.OK {
		t.Fatalf("write = %s (%s); a synchronous write is supposed to report success regardless",
			op.Outcome, op.Err)
	}
	if elapsed < peerTimeout {
		t.Errorf("the write returned in %v, sooner than the %v peer timeout; it did not actually "+
			"wait for the peer", elapsed, peerTimeout)
	}
	if elapsed > 5*time.Second {
		t.Errorf("the write took %v; the peer request is not bounded by the timeout", elapsed)
	}
	if op := read(t, c, "x"); op.Observed != 7 {
		t.Errorf("the local value is %d, want 7; the local apply must happen either way", op.Observed)
	}
	t.Logf("write reported %s after %v with a peer timeout of %v", op.Outcome, elapsed.Round(time.Millisecond), peerTimeout)
}

// The contrast that makes synchronous mode worth having, measured against the
// same unreachable peer: asynchronous returns at once, synchronous waits.
func TestQuorumAsyncDoesNotWaitButSyncDoes(t *testing.T) {
	const peerTimeout = 300 * time.Millisecond
	blackhole := blackholeAddr(t)

	s := NewQuorumStore("a", nil, peerTimeout, nil)
	node := serve(t, "a", s)
	c := patient(node)
	if err := s.Configure(Config{Peers: &[]string{blackhole}}); err != nil {
		t.Fatalf("configure: %v", err)
	}

	started := time.Now()
	if op := c.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1}); op.Outcome != history.OK {
		t.Fatalf("asynchronous write = %s", op.Outcome)
	}
	async := time.Since(started)
	if async >= peerTimeout {
		t.Errorf("an asynchronous write took %v, at least as long as the %v peer timeout; it is "+
			"waiting for the fan-out", async, peerTimeout)
	}

	if err := s.Configure(Config{Sync: boolptr(true)}); err != nil {
		t.Fatalf("switch to sync: %v", err)
	}
	started = time.Now()
	if op := c.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 2}); op.Outcome != history.OK {
		t.Fatalf("synchronous write = %s", op.Outcome)
	}
	sync := time.Since(started)
	if sync < peerTimeout {
		t.Errorf("a synchronous write took %v, less than the %v peer timeout; it is not waiting",
			sync, peerTimeout)
	}
	t.Logf("same unreachable peer: asynchronous %v, synchronous %v, peer timeout %v",
		async.Round(time.Millisecond), sync.Round(time.Millisecond), peerTimeout)
}

// Switching synchronous mode off again restores the asynchronous behaviour,
// so the harness can reuse a node between experiments.
func TestQuorumSyncCanBeSwitchedOff(t *testing.T) {
	const peerTimeout = 300 * time.Millisecond
	s := NewQuorumStore("a", nil, peerTimeout, nil)
	c := patient(serve(t, "a", s))
	if err := s.Configure(Config{Peers: &[]string{blackholeAddr(t)}, Sync: boolptr(true)}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if err := s.Configure(Config{Sync: boolptr(false)}); err != nil {
		t.Fatalf("switch off: %v", err)
	}
	started := time.Now()
	if op := c.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1}); op.Outcome != history.OK {
		t.Fatalf("write = %s", op.Outcome)
	}
	if elapsed := time.Since(started); elapsed >= peerTimeout {
		t.Errorf("the write took %v after synchronous mode was switched off", elapsed)
	}
}

// A node that already holds an update stamped later than its own clock must
// still be able to write, and its write must be accepted by the other nodes.
//
// Without the clamp in stampFor the local apply still succeeds, so the node
// looks fine on its own; what breaks is that the replication it sends out
// carries a losing stamp, every peer rejects it, and the cluster diverges for
// good. That is only visible with a real peer, which is why this test exists
// separately from the local one.
func TestQuorumLocalWriteReplicatesOverAHeldFutureStamp(t *testing.T) {
	sa := NewQuorumStore("a", nil, 5*time.Second, nil)
	sb := NewQuorumStore("b", nil, 5*time.Second, nil)
	a, b := serve(t, "a", sa), serve(t, "b", sb)
	if err := sa.Configure(Config{Peers: &[]string{b.Addr()}, Sync: boolptr(true)}); err != nil {
		t.Fatalf("configure a: %v", err)
	}

	// Both nodes learn of a value stamped an hour from now, from a third node
	// whose clock is wrong.
	future := time.Now().Add(time.Hour).UnixNano()
	body := fmt.Sprintf(`{"key":"k","value":99,"timestamp":%d,"node":"z"}`, future)
	for _, c := range []*Client{a, b} {
		if status, reply := post(t, c, "/replicate", body); status != 200 || !strings.Contains(reply, `"ok":true`) {
			t.Fatalf("seeding: status %d, %s", status, reply)
		}
	}

	if op := write(t, a, "k", 1); op.Outcome != history.OK {
		t.Fatalf("write to a = %s (%s)", op.Outcome, op.Err)
	}
	if op := read(t, a, "k"); op.Observed != 1 {
		t.Errorf("a holds %d after its own write, want 1", op.Observed)
	}
	if op := read(t, b, "k"); op.Observed != 1 {
		t.Errorf("b still holds %d after a's write was replicated to it, want 1; a generated a "+
			"stamp that lost to what the cluster already held, so the nodes have diverged "+
			"permanently", op.Observed)
	}
}

// blackholeAddr returns the address of a listener that accepts connections and
// then never answers. A dial to it succeeds, so it exercises the case a closed
// port cannot: a peer that is reachable but silent, which is what a blackholed
// fault proxy looks like.
func blackholeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var held []net.Conn
		defer func() {
			for _, c := range held {
				c.Close()
			}
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
	})
	return ln.Addr().String()
}
