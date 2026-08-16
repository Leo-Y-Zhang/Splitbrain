package kv

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// patient returns a client for the same node that will wait far longer than
// the node's own forwarding timeout.
//
// Without it, a test of what a follower says about an unreachable leader races
// its own client: both sides use the same deadline, and if the client gives up
// first the test observes the client's timeout instead of the follower's
// answer and passes no matter what the follower would have said. That is
// exactly how this file's contract test passed against a deliberately unsound
// implementation the first time it was run.
func patient(c *Client) *Client { return NewClient(c.Addr(), 5*time.Second) }

// forwardPair stands up a leader and a follower pointed at it, and returns a
// client for each.
func forwardPair(t *testing.T) (leader, follower *Client) {
	t.Helper()
	leader = serve(t, "leader", NewForwardStore("", testTimeout, nil))
	follower = serve(t, "follower", NewForwardStore(leader.Addr(), testTimeout, nil))
	return leader, follower
}

// A follower holds no data. Everything it answers came from the leader, which
// is what makes the arrangement linearizable.
func TestForwardFollowerReturnsTheLeadersAnswer(t *testing.T) {
	leader, follower := forwardPair(t)
	ctx := context.Background()

	if op := follower.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 4}); op.Outcome != history.OK {
		t.Fatalf("write through the follower = %s (%s)", op.Outcome, op.Err)
	}
	// Written through the follower, visible at the leader: it really was
	// forwarded rather than stored locally.
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 4 {
		t.Errorf("leader read = %s/%d, want ok/4", op.Outcome, op.Observed)
	}
	if op := follower.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 4 {
		t.Errorf("follower read = %s/%d, want ok/4", op.Outcome, op.Observed)
	}
	if op := follower.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 4, To: 8}); op.Outcome != history.OK || !op.Swapped {
		t.Errorf("follower cas = %s/swapped=%t, want ok/true", op.Outcome, op.Swapped)
	}
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Observed != 8 {
		t.Errorf("value at the leader after a forwarded cas = %d, want 8", op.Observed)
	}
}

// A leader that declines has reached a decision, and the follower must pass
// that through rather than dressing it up as its own.
func TestForwardFollowerPassesThroughALeadersRefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, Declined("the leader is read-only today"))
	}))
	t.Cleanup(srv.Close)

	follower := serve(t, "follower", NewForwardStore(strings.TrimPrefix(srv.URL, "http://"), testTimeout, nil))
	op := follower.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Fail)
	if !strings.Contains(op.Err, "read-only today") {
		t.Errorf("Err = %q, want the leader's own reason", op.Err)
	}
}

// The leader's port is closed, so the follower's dial is refused and nothing
// was delivered. That is the one case where a definite failure is honest.
func TestForwardRefusedLeaderIsFail(t *testing.T) {
	follower := serve(t, "follower", NewForwardStore(closedPort(t), testTimeout, nil))
	op := follower.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Fail)
	if op.Err == "" {
		t.Error("Err is empty; a refusal should say what refused")
	}
}

// The contract test. A leader that stops answering may already have applied
// the write, so the follower must not claim it failed. If this regresses, a
// later read that legitimately observes the write is reported as a violation
// and the blame lands on the checker instead of here.
func TestForwardUnresponsiveLeaderIsInfoNeverFail(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		srv.CloseClientConnections()
		srv.Close()
	})

	follower := patient(serve(t, "follower", NewForwardStore(strings.TrimPrefix(srv.URL, "http://"), testTimeout, nil)))
	op := follower.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1})

	if op.Outcome == history.Fail {
		t.Fatal("a timed-out forward was recorded as a definite failure; the leader may have applied it")
	}
	checkOutcome(t, op, history.Info)
}

// Every other way a forward can go wrong lands in the same place. None of
// these tell the follower whether the leader applied the operation.
func TestForwardBrokenLeaderRepliesAreInfo(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    http.HandlerFunc
	}{
		{"leader returns 500", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}},
		{"leader returns garbage", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("<html/>"))
		}},
		{"leader says ok without a value", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"ok":true}`))
		}},
		{"leader drops the connection mid-body", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "64")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"ok":tr`))
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.h)
			t.Cleanup(srv.Close)
			follower := serve(t, "follower", NewForwardStore(strings.TrimPrefix(srv.URL, "http://"), testTimeout, nil))
			op := follower.Do(context.Background(), testClock, 0, Request{Kind: history.Read, Key: "x"})
			if op.Outcome == history.Fail {
				t.Fatalf("recorded a definite failure; the leader may have applied it (err %q)", op.Err)
			}
			checkOutcome(t, op, history.Info)
		})
	}
}

// A follower must never answer from data of its own, because it has none. The
// failure this guards against is a store that quietly falls back to a local
// map when the leader is away, which produces plausible wrong values instead
// of honest errors.
func TestForwardFollowerNeverFabricates(t *testing.T) {
	leader, follower := forwardPair(t)
	ctx := context.Background()

	if op := follower.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 42}); op.Outcome != history.OK {
		t.Fatalf("setup write: %s", op.Err)
	}
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Observed != 42 {
		t.Fatalf("setup read: got %d", op.Observed)
	}

	// Now cut the follower off by pointing it at a dead address, and check it
	// has kept nothing.
	if err := follower.Configure(ctx, Config{Leader: strptr(closedPort(t))}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	for i := 0; i < 3; i++ {
		op := follower.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
		if op.Outcome == history.OK {
			t.Fatalf("a cut-off follower answered a read with %d; it holds no data and must not invent one", op.Observed)
		}
	}
}

// /configure is the path the harness uses, because the leader's port is not
// known until it has started.
func TestForwardConfigureSetsTheLeader(t *testing.T) {
	ctx := context.Background()
	leader := serve(t, "leader", NewForwardStore("", testTimeout, nil))
	// Started knowing nothing: with no leader address it is its own leader,
	// so it is configured into a follower afterwards.
	follower := serve(t, "follower", NewForwardStore("", testTimeout, nil))

	if err := follower.Configure(ctx, Config{Leader: strptr(leader.Addr())}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	if op := follower.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 3}); op.Outcome != history.OK {
		t.Fatalf("write after configure = %s (%s)", op.Outcome, op.Err)
	}
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Observed != 3 {
		t.Errorf("leader holds %d, want 3; the follower did not forward", op.Observed)
	}

	// Configuring back to the empty string makes it a leader again.
	if err := follower.Configure(ctx, Config{Leader: strptr("")}); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if op := follower.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "y", Value: 1}); op.Outcome != history.OK {
		t.Fatalf("write after being promoted = %s (%s)", op.Outcome, op.Err)
	}
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "y"}); op.Observed != 0 {
		t.Errorf("the old leader saw %d; a promoted node should stop forwarding", op.Observed)
	}
}

func strptr(s string) *string { return &s }
func intptr(i int) *int       { return &i }

// promotionSetup stands up a leader whose reachability the test controls, and
// a follower with promotion switched on.
//
// The gate is a proxy rather than a stopped process, because a partition is
// not a crash: the leader carries on serving the other half, which is what
// makes a promoted follower a genuine second leader rather than a failover.
func promotionSetup(t *testing.T, promoteAfter int) (leaderClient, followerClient *Client, follower *ForwardStore, cut func(bool)) {
	t.Helper()
	leaderStore := NewForwardStore("", testTimeout, nil)
	leaderClient = serve(t, "leader", leaderStore)

	var mu sync.Mutex
	blocked := false
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		down := blocked
		mu.Unlock()
		if down {
			// A partition, not a refusal: the follower gets no answer at all
			// and has to decide what that means.
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
					return
				}
			}
			return
		}
		proxy(t, w, r, leaderClient.base)
	}))
	t.Cleanup(func() {
		gate.CloseClientConnections()
		gate.Close()
	})

	follower = NewForwardStore(strings.TrimPrefix(gate.URL, "http://"), testTimeout, nil)
	if err := follower.Configure(Config{Promote: boolptr(true), PromoteAfter: intptr(promoteAfter)}); err != nil {
		t.Fatalf("configure follower: %v", err)
	}
	followerClient = patient(serve(t, "follower", follower))

	return leaderClient, followerClient, follower, func(down bool) {
		mu.Lock()
		blocked = down
		mu.Unlock()
	}
}

// proxy forwards one request to base and copies the reply back.
func proxy(t *testing.T, w http.ResponseWriter, r *http.Request, base string) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	resp, err := http.Post(base+r.URL.Path, "application/json", bytes.NewReader(body))
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(out)
}

// On a healthy network promotion never fires, so the store is the ordinary
// linearizable kvforward. Asserting the flag matters as much as asserting the
// answers: a node that promoted and happened to hold the right values would
// give identical replies while being a completely different system.
func TestForwardPromotionDoesNotFireOnAHealthyNetwork(t *testing.T) {
	leader, followerClient, follower, _ := promotionSetup(t, 3)
	ctx := context.Background()

	for i := 1; i <= 12; i++ {
		if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: i}); op.Outcome != history.OK {
			t.Fatalf("write %d = %s (%s)", i, op.Outcome, op.Err)
		}
		if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != i {
			t.Fatalf("read %d = %s/%d, want ok/%d", i, op.Outcome, op.Observed, i)
		}
	}
	if follower.Promoted() {
		t.Error("the follower promoted itself on a healthy network")
	}
	// The leader really is where the data lives.
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Observed != 12 {
		t.Errorf("leader holds %d, want 12", op.Observed)
	}
}

// The split brain itself. After exactly promoteAfter failed forwards the
// follower appoints itself and answers from the last value it saw pass by,
// while the leader carries on serving the other half with a different value.
func TestForwardPromotesAfterExactlyTheThresholdAndServesStaleValues(t *testing.T) {
	const threshold = 3
	leader, followerClient, follower, cut := promotionSetup(t, threshold)
	ctx := context.Background()

	// Prime the cache through the leader.
	if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 42}); op.Outcome != history.OK {
		t.Fatalf("priming write: %s (%s)", op.Outcome, op.Err)
	}
	if follower.Promoted() {
		t.Fatal("promoted before anything went wrong")
	}

	cut(true)
	for i := 1; i < threshold; i++ {
		op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
		if op.Outcome == history.OK {
			t.Fatalf("failure %d answered ok; promotion happened early", i)
		}
		if follower.Promoted() {
			t.Fatalf("promoted after %d failures, want %d", i, threshold)
		}
	}

	// The threshold-th failure is the one that promotes it, and that same
	// request is answered from the cache rather than failing.
	op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	if !follower.Promoted() {
		t.Fatalf("still not promoted after %d failures", threshold)
	}
	if op.Outcome != history.OK {
		t.Fatalf("the promoting request = %s (%s), want ok from the cache", op.Outcome, op.Err)
	}
	if op.Observed != 42 {
		t.Errorf("promoted read returned %d, want the cached 42", op.Observed)
	}
	t.Logf("promoted on failure %d of %d, then answered ok with the cached %d", threshold, threshold, op.Observed)

	// Meanwhile the leader is still serving the other half of the partition,
	// and the two halves now disagree.
	if op := leader.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 99}); op.Outcome != history.OK {
		t.Fatalf("write to the leader: %s", op.Err)
	}
	stale := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	if stale.Outcome != history.OK || stale.Observed != 42 {
		t.Errorf("promoted follower read = %s/%d, want ok/42", stale.Outcome, stale.Observed)
	}
	fresh := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	if fresh.Observed != 99 {
		t.Errorf("leader read = %d, want 99", fresh.Observed)
	}
	if stale.Observed == fresh.Observed {
		t.Error("the two halves agree; there is no split brain to find")
	}

	// A promoted node accepts writes and compare-and-swap too, all reported
	// as successes, and all of them about to be thrown away.
	if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 42, To: 7}); op.Outcome != history.OK || !op.Swapped {
		t.Errorf("promoted cas = %s/swapped=%t, want ok/true", op.Outcome, op.Swapped)
	}
}

// Healing the partition demotes the follower again, and everything it accepted
// while promoted is silently gone.
func TestForwardDemotesWhenTheLeaderReturns(t *testing.T) {
	leader, followerClient, follower, cut := promotionSetup(t, 2)
	ctx := context.Background()

	if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1}); op.Outcome != history.OK {
		t.Fatalf("priming write: %s", op.Err)
	}
	cut(true)
	for i := 0; i < 3; i++ {
		followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	}
	if !follower.Promoted() {
		t.Fatal("did not promote while cut off")
	}
	if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 555}); op.Outcome != history.OK {
		t.Fatalf("write while promoted: %s", op.Err)
	}

	cut(false)
	op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	if op.Outcome != history.OK {
		t.Fatalf("read after healing = %s (%s)", op.Outcome, op.Err)
	}
	if follower.Promoted() {
		t.Error("still promoted after a successful forward")
	}
	if op.Observed != 1 {
		t.Errorf("read after healing = %d, want the leader's 1; the follower is still answering "+
			"from its own cache", op.Observed)
	}
	t.Logf("demoted on the first successful forward; the 555 written while promoted is gone, the read returned %d", op.Observed)
	if lop := leader.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); lop.Observed != 1 {
		t.Errorf("leader holds %d, want 1; the promoted write should have been lost, not merged", lop.Observed)
	}
}

// The regression guard. Promotion is opt-in, and with it off a follower cut
// off from its leader must still never invent a value - that is the rule the
// whole tool's soundness rests on, and adding promotion must not have weakened
// it by accident.
func TestForwardWithoutPromotionStillNeverFabricates(t *testing.T) {
	_, followerClient, follower, cut := promotionSetup(t, 2)
	if err := follower.Configure(Config{Promote: boolptr(false)}); err != nil {
		t.Fatalf("switch promotion off: %v", err)
	}
	ctx := context.Background()

	if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 42}); op.Outcome != history.OK {
		t.Fatalf("priming write: %s", op.Err)
	}
	cut(true)
	for i := 0; i < 8; i++ {
		op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
		if op.Outcome == history.OK {
			t.Fatalf("attempt %d answered ok with %d; a follower without promotion holds nothing "+
				"and must never invent a value", i, op.Observed)
		}
		checkOutcome(t, op, history.Info)
	}
	if follower.Promoted() {
		t.Error("promoted with promotion switched off")
	}
}

// Switching promotion off while a node is promoted returns it to being an
// ordinary follower at once, so the harness can reuse a node between
// experiments without carrying a stale cache into the next one.
func TestForwardPromotionCanBeSwitchedOffWhilePromoted(t *testing.T) {
	_, followerClient, follower, cut := promotionSetup(t, 1)
	ctx := context.Background()

	if op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 5}); op.Outcome != history.OK {
		t.Fatalf("priming write: %s", op.Err)
	}
	cut(true)
	followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	if !follower.Promoted() {
		t.Fatal("did not promote")
	}

	if err := follower.Configure(Config{Promote: boolptr(false)}); err != nil {
		t.Fatalf("switch off: %v", err)
	}
	if follower.Promoted() {
		t.Fatal("still promoted after promotion was switched off")
	}
	op := followerClient.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"})
	if op.Outcome == history.OK {
		t.Errorf("answered ok with %d after promotion was switched off", op.Observed)
	}
}

func TestForwardConfigureRejectsAnImpossibleThreshold(t *testing.T) {
	s := NewForwardStore("", testTimeout, nil)
	if err := s.Configure(Config{PromoteAfter: intptr(0)}); err == nil {
		t.Error("a threshold of 0 was accepted; it would promote before any forward was attempted")
	}
}
