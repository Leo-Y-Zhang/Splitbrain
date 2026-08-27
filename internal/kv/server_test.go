package kv

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// serve puts store behind the real HTTP surface and returns a client for it.
func serve(t *testing.T, id string, store Store) *Client {
	t.Helper()
	srv := httptest.NewServer(NewServer(id, store, nil).Handler())
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		store.Close()
	})
	return NewClient(strings.TrimPrefix(srv.URL, "http://"), testTimeout)
}

// post sends a raw body to a path, bypassing the client, so that malformed
// input can be tested without the client refusing to produce it.
func post(t *testing.T, c *Client, path, body string) (int, string) {
	t.Helper()
	url := c.base + path
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, strings.TrimSpace(string(raw))
}

// Every request and reply shape has to survive the real handler, not a mock.
func TestHandlerRoundTrip(t *testing.T) {
	c := serve(t, "n1", NewSingleStore())
	ctx := context.Background()

	if op := c.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 0 {
		t.Errorf("read of an untouched key = %s/%d, want ok/0", op.Outcome, op.Observed)
	}
	if op := c.Do(ctx, testClock, 0, Request{Kind: history.Write, Key: "x", Value: 7}); op.Outcome != history.OK {
		t.Errorf("write = %s (%s), want ok", op.Outcome, op.Err)
	}
	if op := c.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 7 {
		t.Errorf("read after write = %s/%d, want ok/7", op.Outcome, op.Observed)
	}
	if op := c.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 7, To: 9}); op.Outcome != history.OK || !op.Swapped {
		t.Errorf("matching cas = %s/swapped=%t, want ok/true", op.Outcome, op.Swapped)
	}
	if op := c.Do(ctx, testClock, 0, Request{Kind: history.CAS, Key: "x", From: 7, To: 9}); op.Outcome != history.OK || op.Swapped {
		t.Errorf("stale cas = %s/swapped=%t, want ok/false", op.Outcome, op.Swapped)
	}
	if op := c.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "x"}); op.Outcome != history.OK || op.Observed != 9 {
		t.Errorf("read after cas = %s/%d, want ok/9", op.Outcome, op.Observed)
	}
	// Registers are independent; the checker relies on being able to split a
	// history by key.
	if op := c.Do(ctx, testClock, 0, Request{Kind: history.Read, Key: "y"}); op.Outcome != history.OK || op.Observed != 0 {
		t.Errorf("read of a different key = %s/%d, want ok/0", op.Outcome, op.Observed)
	}
}

// Malformed input must be declined, not crash the fixture, and the reply must
// be shaped so the client records a definite failure.
func TestHandlerRejectsMalformedInput(t *testing.T) {
	c := serve(t, "n1", NewSingleStore())
	for _, tc := range []struct {
		name, body, wantErr string
	}{
		{"empty", ``, "malformed"},
		{"not json", `<xml/>`, "malformed"},
		{"truncated", `{"op":"read","key":`, "malformed"},
		{"unknown op", `{"op":"increment","key":"x"}`, "unknown operation kind"},
		{"no key", `{"op":"read"}`, "missing key"},
		{"write without value", `{"op":"write","key":"x"}`, "write needs a value"},
		{"cas without to", `{"op":"cas","key":"x","from":1}`, "cas needs both"},
		{"two operations in one body", `{"op":"read","key":"x"}{"op":"read","key":"x"}`, "trailing content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := post(t, c, "/kv", tc.body)
			if status != http.StatusOK {
				t.Errorf("status = %d, want 200; a decision is always 200", status)
			}
			if !strings.Contains(body, `"ok":false`) {
				t.Errorf("body = %s, want a definite refusal", body)
			}
			if !strings.Contains(body, tc.wantErr) {
				t.Errorf("body = %s, want it to mention %q", body, tc.wantErr)
			}
		})
	}
}

func TestHandlerHealth(t *testing.T) {
	c := serve(t, "node-b", NewSingleStore())
	id, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if id != "node-b" {
		t.Errorf("id = %q, want %q", id, "node-b")
	}
}

func TestHandlerRejectsWrongMethodAndPath(t *testing.T) {
	c := serve(t, "n1", NewSingleStore())
	base := c.base

	resp, err := http.Get(base + "/kv")
	if err != nil {
		t.Fatalf("get /kv: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Error("GET /kv returned 200; only POST is a decision")
	}

	resp, err = http.Get(base + "/nope")
	if err != nil {
		t.Fatalf("get /nope: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET /nope status = %d, want 404", resp.StatusCode)
	}
}

// indecisiveStore does not know what happened. It stands in for a follower
// whose leader stopped answering mid-request.
type indecisiveStore struct{}

func (indecisiveStore) Apply(ctx context.Context, req Request) (Response, error) {
	return Response{}, errors.New("the leader stopped answering")
}
func (indecisiveStore) Configure(Config) error { return nil }
func (indecisiveStore) Close() error           { return nil }

// A store that does not know must not be turned into a definite failure by the
// HTTP layer. This is the seam where an unsound harness is easiest to build.
func TestHandlerStoreUncertaintyBecomesIndeterminate(t *testing.T) {
	c := serve(t, "n1", indecisiveStore{})
	op := c.Do(context.Background(), testClock, 0, Request{Kind: history.Write, Key: "x", Value: 1})
	checkOutcome(t, op, history.Info)
	if !strings.Contains(op.Err, "stopped answering") {
		t.Errorf("Err = %q, want the store's reason", op.Err)
	}

	status, body := post(t, c, "/kv", `{"op":"read","key":"x"}`)
	if status != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", status)
	}
	// The body must not look like a decision to a careless reader.
	if strings.Contains(body, `"ok"`) {
		t.Errorf("body = %s, want no ok field on an indeterminate reply", body)
	}
}

func TestConfigureAcceptsAndReplaces(t *testing.T) {
	c := serve(t, "n1", NewSingleStore())
	ctx := context.Background()

	// A single node has nothing to configure but must still accept the call,
	// so the harness can wire all three stores identically.
	if err := c.Configure(ctx, Config{}); err != nil {
		t.Errorf("empty configure: %v", err)
	}
	peers := []string{"127.0.0.1:1", "127.0.0.1:2"}
	if err := c.Configure(ctx, Config{Peers: &peers}); err != nil {
		t.Errorf("configure with peers: %v", err)
	}
	if err := c.Configure(ctx, Config{Peers: &peers}); err != nil {
		t.Errorf("second configure: %v", err)
	}
}

// runBriefly binds addr, lets Run announce itself, stops it, and returns
// everything it wrote to the two standard streams.
//
// It swaps os.Stdout and os.Stderr for pipes because those are what Run writes
// to directly, and what a person starting a node actually sees. Nothing in this
// package runs in parallel, so the swap cannot be observed by another test.
func runBriefly(t *testing.T, addr string) (stdout, stderr string) {
	t.Helper()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	realOut, realErr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = outW, errW
	defer func() { os.Stdout, os.Stderr = realOut, realErr }()

	// Both streams are read while Run is still going. Collecting them
	// afterwards would deadlock the moment either pipe filled.
	var wg sync.WaitGroup
	var out, errs []byte
	wg.Add(2)
	go func() { defer wg.Done(); out, _ = io.ReadAll(outR) }()
	go func() { defer wg.Done(); errs, _ = io.ReadAll(errR) }()

	// Cancelled before it starts: Run still binds, warns and announces, then
	// shuts down without serving anything, which is all this needs.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, addr, "n0", NewSingleStore(), nil); err != nil {
		t.Errorf("Run(%s): %v", addr, err)
	}
	outW.Close()
	errW.Close()
	wg.Wait()
	outR.Close()
	errR.Close()
	return string(out), string(errs)
}

// TestRunWarnsWhenItBindsOffLoopback covers the one thing a person can act on.
// These stores have no authentication by design, and /configure turns any node
// that can be reached into something that will issue requests wherever it is
// told, so a node bound to every interface is a different proposition from one
// on loopback and the operator has to be told which they have started.
func TestRunWarnsWhenItBindsOffLoopback(t *testing.T) {
	// The warning names an address, so the test has to bind a real one; some
	// sandboxes only allow loopback, and there is nothing to check there.
	probe, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Skipf("this machine will not bind an unspecified address, so there is nothing to warn about: %v", err)
	}
	probe.Close()

	stdout, stderr := runBriefly(t, "0.0.0.0:0")
	addr, ok := strings.CutPrefix(strings.TrimSpace(stdout), "listening ")
	if !ok {
		t.Fatalf("stdout = %q, want a single \"listening <addr>\" line", stdout)
	}
	warning := strings.TrimRight(stderr, "\n")
	if warning == "" {
		t.Fatal("a node bound to every interface said nothing about it")
	}
	if strings.Contains(warning, "\n") {
		t.Errorf("the warning is %d lines; one is what gets read:\n%s", strings.Count(warning, "\n")+1, warning)
	}
	for _, want := range []string{addr, "no authentication", "/configure"} {
		if !strings.Contains(warning, want) {
			t.Errorf("the warning does not mention %q:\n%s", want, warning)
		}
	}
}

// TestRunIsSilentOnLoopback keeps the warning from becoming noise. Every node
// the harness starts is on loopback, and a warning printed dozens of times per
// run is one nobody reads when it matters.
func TestRunIsSilentOnLoopback(t *testing.T) {
	stdout, stderr := runBriefly(t, "127.0.0.1:0")
	if stderr != "" {
		t.Errorf("a loopback node wrote to stderr:\n%s", stderr)
	}
	// The harness parses stdout for the address, so a warning that went there
	// instead would be worse than no warning at all.
	if !strings.HasPrefix(stdout, "listening 127.0.0.1:") || strings.Count(stdout, "\n") != 1 {
		t.Errorf("stdout = %q, want exactly one \"listening 127.0.0.1:<port>\" line", stdout)
	}
}

func TestConfigureRejectsMalformedBody(t *testing.T) {
	c := serve(t, "n1", NewSingleStore())
	status, body := post(t, c, "/configure", `{"peers":`)
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200", status)
	}
	if !strings.Contains(body, `"ok":false`) {
		t.Errorf("body = %s, want a refusal", body)
	}
	if err := c.Configure(context.Background(), Config{}); err != nil {
		t.Errorf("the node should still be usable after a bad configure: %v", err)
	}
}

// TestConfigureRefusesAPeerAddressThatIsMoreThanAHost covers the endpoint these
// fixtures are warned about. It is unauthenticated on purpose, and the address
// it carries decides where the node sends traffic: a host and a port names a
// peer, whereas a host with a path attached is an instruction to use this
// process as a way of reaching some other URL. That is refused by name, before
// any of the configuration is applied.
func TestConfigureRefusesAPeerAddressThatIsMoreThanAHost(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store Store
		body  string
	}{
		{"a quorum peer", NewQuorumStore("n1", nil, testTimeout, nil), `{"peers":["127.0.0.1:9/admin/wipe"]}`},
		{"a forward leader", NewForwardStore("", testTimeout, nil), `{"leader":"http://127.0.0.1:9/admin/wipe"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := serve(t, "n1", tc.store)
			status, body := post(t, c, "/configure", tc.body)
			if status != http.StatusOK {
				t.Errorf("status = %d, want 200", status)
			}
			if !strings.Contains(body, `"ok":false`) {
				t.Errorf("body = %s, want a refusal", body)
			}
			if err := c.Configure(context.Background(), Config{}); err != nil {
				t.Errorf("the node should still be usable after a refused configure: %v", err)
			}
		})
	}
}

// TestAHostileKeyCannotForgeALogLine pins the property that makes it safe to
// log a key exactly as the client sent it. The text handler quotes anything
// holding a control character in every position it writes - message, key,
// value and group name alike - so a key carrying a newline and a plausible
// prefix becomes one escaped field of one line rather than a second line a
// reader would take for this node's own.
//
// It is worth a test rather than a comment because the property belongs to the
// handler and not to this package: no call site here can be read to find out
// whether it holds. Pointing NewLogger at a handler that wrote values raw
// would reintroduce log injection at every call site at once, and nothing else
// in the suite would notice.
func TestAHostileKeyCannotForgeALogLine(t *testing.T) {
	var buf bytes.Buffer
	srv := httptest.NewServer(NewServer("n1", NewSingleStore(), NewLogger(&buf)).Handler())
	t.Cleanup(srv.Close)
	c := NewClient(strings.TrimPrefix(srv.URL, "http://"), testTimeout)

	// A real newline in the key, followed by something shaped exactly like a
	// line this node emits.
	const forged = `level=ERROR msg=forged node=somebody-else`
	if status, _ := post(t, c, "/kv", `{"op":"write","key":"x\n`+forged+`","value":1}`); status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}

	logged := strings.TrimRight(buf.String(), "\n")
	if logged == "" {
		t.Fatal("the operation was not logged at all")
	}
	if lines := strings.Count(logged, "\n") + 1; lines != 1 {
		t.Errorf("one operation wrote %d log lines:\n%s", lines, logged)
	}
	// Escaped, not dropped: the key still has to be legible to whoever is
	// reading the log to work out what happened.
	if !strings.Contains(logged, `\n`+forged) {
		t.Errorf("the key did not survive escaped:\n%s", logged)
	}
}
