package kv

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
