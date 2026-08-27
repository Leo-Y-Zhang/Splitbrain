package kv

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/clock"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// testTimeout is short on purpose: several tests here deliberately provoke a
// timeout, and the package's whole suite is meant to stay well inside a minute.
const testTimeout = 150 * time.Millisecond

// handlerServer starts an httptest server for h and returns a client pointed
// at it. The client uses the package default of one connection per operation.
func handlerServer(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		// Close alone blocks until every handler returns, and two tests here
		// hold a handler open on purpose. Dropping the connections first
		// cancels their request contexts so the suite does not pay for it.
		srv.CloseClientConnections()
		srv.Close()
	})
	return NewClient(strings.TrimPrefix(srv.URL, "http://"), testTimeout)
}

// replyWith serves one fixed body with status 200 to every request.
func replyWith(t *testing.T, body string) *Client {
	t.Helper()
	return handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	})
}

// blockingServer starts a server whose handler never answers, which is how a
// client-side timeout is provoked.
//
// The handler is released by a channel rather than by watching the request
// context: net/http only starts detecting a disconnected client once the
// handler has returned or has read the body, so a handler that does neither
// sits there until its own deadline and httptest.Server.Close waits for it.
// The cleanup below is registered after the server's, and cleanups run last
// registered first, so the handler is let go before Close is called.
func blockingServer(t *testing.T) *Client {
	t.Helper()
	release := make(chan struct{})
	c := handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
	})
	t.Cleanup(func() { close(release) })
	return c
}

func do(t *testing.T, c *Client, req Request) history.Op {
	t.Helper()
	return c.Do(context.Background(), testClock, 0, req)
}

// testClock is shared by every test in the package. Constructing a clock
// measures the platform's resolution, which there is no point repeating, and
// one origin means timestamps taken in different helpers stay comparable.
var testClock = clock.New()

// checkOutcome asserts the outcome and the invariants history.Validate cares
// about, since getting the outcome right but the timestamps wrong produces a
// history the checker will reject anyway.
func checkOutcome(t *testing.T, op history.Op, want history.Outcome) {
	t.Helper()
	if op.Outcome != want {
		t.Errorf("outcome = %s, want %s (err %q)", op.Outcome, want, op.Err)
	}
	switch want {
	case history.Info:
		if op.Complete != history.Pending {
			t.Errorf("indeterminate op has completion time %d, want Pending", op.Complete)
		}
		if op.Err == "" {
			t.Error("indeterminate op carries no error text; a human reading the history learns nothing")
		}
	default:
		if op.Complete == history.Pending {
			t.Errorf("%s op has no completion time", want)
		}
		if op.Complete <= op.Invoke {
			t.Errorf("completes at %d, not strictly after its invocation at %d; a zero-width interval "+
				"lets two operations each precede the other", op.Complete, op.Invoke)
		}
	}
	if err := (history.History{op}).Validate(); err != nil {
		t.Errorf("op does not validate: %v", err)
	}
}

func TestDoReadOK(t *testing.T) {
	c := replyWith(t, `{"ok":true,"value":7}`)
	op := do(t, c, Request{Kind: history.Read, Key: "x"})
	checkOutcome(t, op, history.OK)
	if op.Observed != 7 {
		t.Errorf("Observed = %d, want 7", op.Observed)
	}
}

// A read of a key nobody has written returns zero, and zero has to survive the
// round trip as an answer rather than as an absent field.
func TestDoReadOKZeroIsAnAnswer(t *testing.T) {
	c := replyWith(t, `{"ok":true,"value":0}`)
	op := do(t, c, Request{Kind: history.Read, Key: "x"})
	checkOutcome(t, op, history.OK)
	if op.Observed != 0 {
		t.Errorf("Observed = %d, want 0", op.Observed)
	}
}

func TestDoWriteOK(t *testing.T) {
	c := replyWith(t, `{"ok":true}`)
	op := do(t, c, Request{Kind: history.Write, Key: "x", Value: 9})
	checkOutcome(t, op, history.OK)
	if op.Value != 9 {
		t.Errorf("Value = %d, want 9", op.Value)
	}
}

func TestDoCASOKSwapped(t *testing.T) {
	c := replyWith(t, `{"ok":true,"swapped":true}`)
	op := do(t, c, Request{Kind: history.CAS, Key: "x", From: 3, To: 7})
	checkOutcome(t, op, history.OK)
	if !op.Swapped {
		t.Error("Swapped = false, want true")
	}
}

// A CAS that found the wrong value still succeeded. It must not be confused
// with a refusal: the server compared and correctly did nothing, and that is a
// claim about the register's value at that instant.
func TestDoCASOKNotSwapped(t *testing.T) {
	c := replyWith(t, `{"ok":true,"swapped":false}`)
	op := do(t, c, Request{Kind: history.CAS, Key: "x", From: 3, To: 7})
	checkOutcome(t, op, history.OK)
	if op.Swapped {
		t.Error("Swapped = true, want false")
	}
}

// The server reached a decision and declined, so the operation definitely did
// not take effect.
func TestDoServerDeclinedIsFail(t *testing.T) {
	c := replyWith(t, `{"ok":false,"error":"not leader"}`)
	op := do(t, c, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Fail)
	if !strings.Contains(op.Err, "not leader") {
		t.Errorf("Err = %q, want it to carry the server's reason", op.Err)
	}
}

// A refused connection is the one transport failure that proves nothing
// happened: the kernel on the far side answered with RST because no process
// was listening, so no byte was ever handed to a server.
func TestDoRefusedConnectionIsFail(t *testing.T) {
	addr := closedPort(t)
	c := NewClient(addr, testTimeout)
	op := do(t, c, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Fail)
	if op.Err == "" {
		t.Error("Err is empty; the reason a refusal was recorded should be legible")
	}
}

func TestDoTimeoutIsInfo(t *testing.T) {
	c := blockingServer(t)
	op := do(t, c, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Info)
}

// A context cancelled by the caller is indistinguishable, from the client's
// side, from the server being slow. It cannot be a Fail.
func TestDoCancelledContextIsInfo(t *testing.T) {
	c := blockingServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	defer cancel()
	op := c.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Info)
}

// The server promised more body than it sent and then dropped the connection.
// The operation may well have been applied before the wire broke.
func TestDoTruncatedResponseIsInfo(t *testing.T) {
	c := handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "64")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"ok":tr`)
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		conn.Close()
	})
	op := do(t, c, Request{Kind: history.Read, Key: "x"})
	checkOutcome(t, op, history.Info)
}

// A server that answers 500 has told us nothing about whether it applied the
// operation first. Only a 200 with a well-formed body is a decision.
func TestDoNon200IsInfo(t *testing.T) {
	c := handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	op := do(t, c, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Info)
}

// 502 is what a Splitbrain server sends when it does not know either. The
// status has to be read before the body, or a body that happens to say
// "ok":false would be mistaken for a decision.
func TestDoGatewayErrorIsInfo(t *testing.T) {
	c := handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, `{"ok":false,"error":"leader did not answer"}`)
	})
	op := do(t, c, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Info)
}

func TestDoGarbageBodyIsInfo(t *testing.T) {
	c := replyWith(t, `<html>proxy says no</html>`)
	op := do(t, c, Request{Kind: history.Read, Key: "x"})
	checkOutcome(t, op, history.Info)
}

// "ok":true with no value is not an answer to a read. Filling in zero would
// put a fact into the history that no server ever stated.
func TestDoOKWithoutValueIsInfo(t *testing.T) {
	c := replyWith(t, `{"ok":true}`)
	op := do(t, c, Request{Kind: history.Read, Key: "x"})
	checkOutcome(t, op, history.Info)
}

func TestDoOKWithoutSwappedIsInfo(t *testing.T) {
	c := replyWith(t, `{"ok":true}`)
	op := do(t, c, Request{Kind: history.CAS, Key: "x", From: 3, To: 7})
	checkOutcome(t, op, history.Info)
}

// The request the client puts on the wire has to be the one the brief
// describes, because a human is expected to be able to replay it with curl.
func TestRequestWireForm(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  Request
		want string
	}{
		{"read", Request{Kind: history.Read, Key: "x"}, `{"op":"read","key":"x"}`},
		{"write", Request{Kind: history.Write, Key: "x", Value: 7}, `{"op":"write","key":"x","value":7}`},
		{"write zero", Request{Kind: history.Write, Key: "x", Value: 0}, `{"op":"write","key":"x","value":0}`},
		{"cas", Request{Kind: history.CAS, Key: "x", From: 3, To: 7}, `{"op":"cas","key":"x","from":3,"to":7}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan string, 1)
			c := handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				got <- string(b)
				io.WriteString(w, `{"ok":true,"value":0,"swapped":false}`)
			})
			do(t, c, tc.req)
			if body := <-got; body != tc.want {
				t.Errorf("wire form = %s, want %s", body, tc.want)
			}
		})
	}
}

// Every completed operation must occupy a non-empty interval. A zero-width
// one both precedes and follows everything at the same instant, and a checker
// handed that contradiction has no honest answer.
func TestFastOperationStillHasWidth(t *testing.T) {
	c := replyWith(t, `{"ok":true,"value":1}`)
	for i := 0; i < 50; i++ {
		op := do(t, c, Request{Kind: history.Read, Key: "x"})
		if op.Complete <= op.Invoke {
			t.Fatalf("op %d: Invoke %d, Complete %d", i, op.Invoke, op.Complete)
		}
	}
}

// Operations that genuinely overlapped must be recorded as concurrent.
//
// This is the anti-false-violation property this package is responsible for.
// Real-time precedence is "A completed at or before B was invoked", so if two
// operations that really were in flight together come back with one preceding
// the other, the checker is obliged to order them and will report a violation
// against a store that never committed one.
//
// The overlap is forced rather than hoped for: the handler holds both requests
// until both have arrived, so each was demonstrably in flight while the other
// was.
func TestGenuinelyOverlappingOperationsAreRecordedAsConcurrent(t *testing.T) {
	var once sync.Once
	arrived := make(chan struct{}, 2)
	both := make(chan struct{})
	c := handlerServer(t, func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		if len(arrived) == 2 {
			once.Do(func() { close(both) })
		}
		select {
		case <-both:
		case <-time.After(5 * time.Second):
		}
		io.WriteString(w, `{"ok":true,"value":1}`)
	})

	ops := make([]history.Op, 2)
	var wg sync.WaitGroup
	for i := range ops {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ops[i] = c.Do(context.Background(), testClock, i, Request{Kind: history.Read, Key: "x"})
		}(i)
	}
	wg.Wait()

	a, b := ops[0], ops[1]
	if a.Outcome != history.OK || b.Outcome != history.OK {
		t.Fatalf("outcomes = %s and %s, want both ok", a.Outcome, b.Outcome)
	}
	if !a.Concurrent(b) {
		t.Errorf("two operations that were provably in flight together were recorded as ordered: "+
			"%d..%d and %d..%d; a checker handed that must order them and can report a violation "+
			"that never happened", a.Invoke, a.Complete, b.Invoke, b.Complete)
	}
}

// A recorded history has to satisfy the structural rules the checker relies
// on, and the one that is easiest to break by accident is the per-process
// rule: one client cannot have two requests in flight, so each operation must
// be invoked no earlier than the previous one completed.
//
// Widening completions is what makes that non-trivial. A completion is pushed
// forward by one clock tick, while the next invocation is recorded as read, so
// if the gap between one operation returning and the next starting is shorter
// than a tick the history claims the process overlapped itself. This test
// drives that gap as small as it goes.
func TestBackToBackOperationsFromOneProcessValidate(t *testing.T) {
	c := replyWith(t, `{"ok":true,"value":1}`)
	const n = 400

	h := make(history.History, 0, n)
	for i := 0; i < n; i++ {
		h = append(h, c.Do(context.Background(), testClock, 0, Request{Kind: history.Read, Key: "x"}))
	}
	if err := h.Validate(); err != nil {
		t.Errorf("a history of %d back-to-back operations does not validate: %v\n"+
			"clock: %s", n, err, testClock)
	}

	var flush, inverted int
	for i := 1; i < n; i++ {
		switch prev, next := h[i-1], h[i]; {
		case next.Invoke < prev.Complete:
			inverted++
		case next.Invoke == prev.Complete:
			flush++
		}
	}
	t.Logf("clock: %s; %d operations, %d flush boundaries, %d inverted", testClock, n, flush, inverted)
}

// The refusal check decides whether an operation may be deleted from a
// history, so each of its branches is pinned separately. A real refused dial
// is covered end to end by TestDoRefusedConnectionIsFail; this covers the
// cases that are awkward to provoke over a real socket, above all a dial that
// failed for some reason other than a refusal.
func TestIsConnRefused(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: connRefused}}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"a refused dial", refused, true},
		{"a refused dial wrapped by net/http", &url.Error{Op: "Post", URL: "http://x/kv", Err: refused}, true},
		{"the portable errno", &net.OpError{Op: "dial", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}, true},
		// A dial that timed out never proved anything. The connect may have
		// completed at the far end and the reply been lost.
		{"a dial that timed out", &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}, false},
		{"a dial cancelled by the caller", &net.OpError{Op: "dial", Err: context.Canceled}, false},
		// A refusal reported on a read is not a refused dial; on some
		// platforms it can follow a connection that was already established.
		{"a refusal on a read", &net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: connRefused}}, false},
		{"a reset connection", &net.OpError{Op: "read", Err: syscall.ECONNRESET}, false},
		{"an unexpected EOF", io.ErrUnexpectedEOF, false},
		{"a plain error", errors.New("something went wrong"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isConnRefused(tc.err); got != tc.want {
				t.Errorf("isConnRefused(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// A peer address decides where this process sends traffic and it arrives over
// an unauthenticated endpoint, so what counts as one is worth pinning exactly.
func TestPeerBase(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
		// want is the prefix every request to the peer is built from, or the
		// empty string when the address has to be refused outright.
		want string
	}{
		{"a host and port", "127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"an explicit scheme", "http://127.0.0.1:8080", "http://127.0.0.1:8080"},
		{"https", "https://example.test:443", "https://example.test:443"},
		{"a trailing slash", "http://127.0.0.1:8080/", "http://127.0.0.1:8080"},
		{"a name with no port", "example.test", "http://example.test"},
		{"IPv6", "[::1]:8080", "http://[::1]:8080"},
		// Everything below was accepted before, and glued to the endpoint path.
		// That is what let a configured address name any URL on a host rather
		// than only the host.
		{"a path", "127.0.0.1:8080/admin/wipe", ""},
		{"a path behind a scheme", "http://127.0.0.1:8080/admin/wipe", ""},
		{"a query", "http://127.0.0.1:8080/?ok=", ""},
		{"a fragment", "http://127.0.0.1:8080/#x", ""},
		{"credentials", "http://user:pass@127.0.0.1:8080", ""},
		{"nothing at all", "", ""},
		{"a path and no host", "/admin/wipe", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := peerBase(tc.addr)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("peerBase(%q) = %q, want a refusal", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("peerBase(%q): %v", tc.addr, err)
			}
			if got != tc.want {
				t.Errorf("peerBase(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}

// A Client built on an address it cannot point at must send nothing at all.
// The address is the entire destination, so falling back to some prefix of it
// would be worse than refusing: it would put a request somewhere nobody asked
// for, which is the whole failure being prevented.
func TestClientOnAnUnusableAddressNeverSends(t *testing.T) {
	var seen atomic.Int32
	victim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(victim.Close)

	// A host with a path attached, which is what a hostile POST /configure
	// would supply. This used to reach the victim as POST /admin/wipe/kv.
	c := NewClient(strings.TrimPrefix(victim.URL, "http://")+"/admin/wipe", testTimeout)

	_, err := c.Send(context.Background(), Request{Kind: history.Write, Key: "x", Value: 1})
	var te *TransportError
	if !errors.As(err, &te) {
		t.Fatalf("Send = %v, want a *TransportError", err)
	}
	if !te.NeverSent {
		t.Error("NeverSent is false, but nothing was dialled")
	}
	if err := c.postJSON(context.Background(), "/replicate", replication{Key: "x"}); err == nil {
		t.Error("postJSON to an unusable address reported success")
	}
	if _, err := c.Health(context.Background()); err == nil {
		t.Error("Health of an unusable address reported success")
	}
	if n := seen.Load(); n != 0 {
		t.Errorf("the victim received %d requests, want none", n)
	}
}

// closedPort binds a port and immediately releases it, so a dial to it is
// refused rather than dropped. Nothing else in the suite can claim it in the
// window between, and if the platform ever reused it the test would fail
// loudly rather than pass for the wrong reason.
func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}
