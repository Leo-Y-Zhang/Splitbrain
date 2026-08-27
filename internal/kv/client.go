package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/clock"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// A TransportError is a failure that stopped the client short of reading a
// well-formed reply.
//
// NeverSent is the whole point of the type. It is the only evidence that lets
// a client record history.Fail, so it is set in exactly one situation and the
// zero value is the safe one.
type TransportError struct {
	// NeverSent reports that the request provably never reached the server:
	// the connection was refused, so no byte of it was ever written to an
	// accepted socket. Anything weaker - a timeout, a reset, a truncated
	// reply - leaves NeverSent false, because the server may well have
	// applied the operation before the wire broke.
	NeverSent bool
	// Err is the underlying failure.
	Err error
}

// Error renders the failure.
func (e *TransportError) Error() string { return e.Err.Error() }

// Unwrap exposes the underlying failure to errors.Is and errors.As.
func (e *TransportError) Unwrap() error { return e.Err }

// ClientOptions tunes a Client. The zero value is what a harness wants.
type ClientOptions struct {
	// KeepAlive lets the transport pool TCP connections between operations.
	//
	// It defaults to off, and the default is load-bearing rather than
	// cautious. A pooled connection that was established before a partition
	// was applied keeps working through it for as long as the proxy in the
	// middle leaves the existing socket alone, so the first operation after
	// the fault silently succeeds and the fault appears to have done
	// nothing. Dialling afresh every time means the partition is felt on the
	// very next operation, which is what the run is trying to observe. Turn
	// it on only when measuring throughput, where the extra handshake
	// dominates and no faults are being injected.
	KeepAlive bool
}

// A Client speaks the Splitbrain key-value protocol to one server address.
//
// It is safe for concurrent use, but a harness should give each process its
// own Client: a process is meant to be sequential, and sharing one Client
// makes that easy to break by accident.
type Client struct {
	addr string
	base string
	// refuse is set when addr was not a usable peer address, and is returned
	// by every request method instead of dialling. A Client that cannot say
	// where it is pointed must not guess: see peerBase.
	refuse  error
	timeout time.Duration
	http    *http.Client
}

// NewClient returns a Client for addr, which may be given as "host:port" or as
// a full "http://host:port" URL. Every request dials a fresh connection; see
// ClientOptions.KeepAlive for why. Timeout bounds one whole operation, dial to
// last byte of the body; a timeout of zero or less means no bound, which is
// only sensible in a test.
func NewClient(addr string, timeout time.Duration) *Client {
	return NewClientWithOptions(addr, timeout, ClientOptions{})
}

// NewClientWithOptions is NewClient with the transport behaviour spelled out.
//
// An address that is not a bare host and port yields a Client that refuses
// every request rather than one pointed somewhere unintended; see peerBase.
func NewClientWithOptions(addr string, timeout time.Duration, opts ClientOptions) *Client {
	base, refuse := peerBase(addr)
	return &Client{
		addr:    addr,
		base:    base,
		refuse:  refuse,
		timeout: timeout,
		http: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: !opts.KeepAlive,
			},
		},
	}
}

// Addr returns the address the Client was built for, as given.
func (c *Client) Addr() string { return c.addr }

// peerBase turns a peer address into the prefix every request to that peer is
// built from, and refuses anything that is more than a host and a port.
//
// The refusal is the security-relevant half. POST /configure is
// unauthenticated by design - see exposureWarning - so the addresses arriving
// in it are the least trustworthy input this package handles, and the old rule
// simply glued whatever it was given to the endpoint path. A leader of
// "10.0.0.5/admin/wipe?ok=" made this node issue POSTs to
// 10.0.0.5/admin/wipe?ok=/kv, so whoever could reach the port could aim the
// process at any URL on any host it could see, and use its network position
// rather than their own. Rebuilding the prefix out of nothing but the scheme
// and the host leaves the endpoint paths this package asks for the only ones
// the process can ever request. Which hosts it will talk to is still whatever
// it was told, because a fixture whose peers are assigned at runtime cannot
// know them in advance; that residue is what the warning is for.
func peerBase(addr string) (string, error) {
	raw := addr
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		// Only the reason, not the *url.Error wrapping it: that restates the
		// address a second time, with the scheme this function just prepended
		// glued on. Both copies are quoted, so this is legibility rather than
		// safety, and one copy is enough for a message that is logged and
		// handed back to whoever sent the address.
		var perr *url.Error
		if errors.As(err, &perr) {
			err = perr.Err
		}
		return "", fmt.Errorf("peer address %q is not a URL: %w", addr, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("peer address %q names no host", addr)
	}
	// A bare "/" is what a trailing slash parses to, which is the same
	// destination and was always accepted; anything beyond it is not.
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("peer address %q must be a host and port and nothing else, "+
			"with no user, path, query or fragment", addr)
	}
	return u.Scheme + "://" + u.Host, nil
}

// Do issues one operation and returns it as a fully populated history.Op.
//
// clk is the run's clock. Every timestamp in a history comes from one of
// these, so all of them are comparable, and it is the clock rather than this
// package that knows how far a completion has to be widened for the recorded
// ordering to be something the measurement actually supports. See
// internal/clock for why that is not a detail.
//
// Do never returns an error. A failure is not an exception here, it is the
// result: the outcome recorded on the Op is the thing the checker reasons
// about.
func (c *Client) Do(ctx context.Context, clk *clock.Clock, process int, req Request) history.Op {
	op := history.Op{
		Process: process,
		Key:     req.Key,
		Kind:    req.Kind,
		Value:   req.Value,
		From:    req.From,
		To:      req.To,
	}
	op.Invoke = clk.Nanos()
	resp, err := c.Send(ctx, req)
	done := clk.Completion(op.Invoke, clk.Nanos())

	switch {
	case err == nil && resp.OK:
		// A well-formed reply that says it worked. This is the only case in
		// which the client may state what the server returned.
		op.Outcome = history.OK
		op.Complete = done
		if resp.Value != nil {
			op.Observed = *resp.Value
		}
		if resp.Swapped != nil {
			op.Swapped = *resp.Swapped
		}

	case err == nil:
		// A well-formed reply that says it did not work. The server reached a
		// decision and declined, so the operation definitely did not take
		// effect and the checker is free to drop it entirely.
		op.Outcome = history.Fail
		op.Complete = done
		op.Err = resp.Err

	default:
		// Everything else is indeterminate unless the client can prove the
		// request never left. This is the rule naive harnesses get wrong, and
		// getting it wrong makes the whole tool unsound in one direction or
		// the other. A timeout, a reset, a truncated reply, an unparseable
		// body and a non-200 status all have the same shape: the server may
		// have applied the operation and then failed to tell us. Recording
		// that as a failure would let the checker delete an operation that
		// really happened, and a later read that legitimately observes it
		// then looks like a violation of a history nobody performed. The one
		// exception is a refused connection, where the far kernel answered
		// with RST because nothing was listening.
		var te *TransportError
		if errors.As(err, &te) && te.NeverSent {
			op.Outcome = history.Fail
			op.Complete = done
		} else {
			op.Outcome = history.Info
			op.Complete = history.Pending
		}
		op.Err = err.Error()
	}
	return op
}

// Send issues one operation and returns the server's reply.
//
// It is the transport underneath Do, exported because a ForwardStore follower
// is itself a client of its leader and has to distinguish the same cases. A
// nil error means a well-formed reply came back with status 200; the reply may
// still be a refusal. Every non-nil error is a *TransportError.
func (c *Client) Send(ctx context.Context, req Request) (Response, error) {
	if c.refuse != nil {
		// NeverSent is exactly right here and it matters: nothing was dialled,
		// so the operation definitely did not take effect and the caller may
		// record a definite failure rather than an indeterminate one.
		return Response{}, &TransportError{NeverSent: true, Err: c.refuse}
	}
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	body, err := json.Marshal(req.toWire())
	if err != nil {
		return Response{}, &TransportError{NeverSent: true, Err: err}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/kv", bytes.NewReader(body))
	if err != nil {
		return Response{}, &TransportError{NeverSent: true, Err: err}
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return Response{}, &TransportError{NeverSent: isConnRefused(err), Err: err}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRequestBytes))
	if err != nil {
		return Response{}, &TransportError{Err: fmt.Errorf("reading reply: %w", err)}
	}
	if resp.StatusCode != http.StatusOK {
		return Response{}, &TransportError{Err: fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))}
	}

	var out Response
	if err := json.Unmarshal(raw, &out); err != nil {
		return Response{}, &TransportError{Err: fmt.Errorf("unparseable reply %q: %w", truncate(raw), err)}
	}
	if err := checkShape(req, out); err != nil {
		return Response{}, &TransportError{Err: err}
	}
	return out, nil
}

// Configure delivers late configuration to a node that was started before its
// peers' addresses were known. It is how the harness wires three nodes
// together after the kernel has chosen their ports.
func (c *Client) Configure(ctx context.Context, cfg Config) error {
	return c.postJSON(ctx, "/configure", cfg)
}

// postJSON sends v to one of the server's administrative endpoints and
// insists on an ok:true reply.
//
// Unlike Send it collapses every failure into one error, because none of these
// endpoints changes the stored data: retrying or giving up is the caller's
// business, and there is no history entry to classify. The whole exchange is
// bounded by the client's timeout, which matters for peer traffic because the
// address may be a fault proxy that has stopped forwarding.
func (c *Client) postJSON(ctx context.Context, path string, v any) error {
	if c.refuse != nil {
		return c.refuse
	}
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRequestBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", path, resp.StatusCode)
	}
	var out Response
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("%s: unparseable reply %q: %w", path, truncate(raw), err)
	}
	if !out.OK {
		return fmt.Errorf("%s: %s", path, out.Err)
	}
	return nil
}

// Health asks the server who it is. It is how a harness waits for a freshly
// started node to be ready.
func (c *Client) Health(ctx context.Context) (string, error) {
	if c.refuse != nil {
		return "", c.refuse
	}
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/health", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRequestBytes))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("health: status %d", resp.StatusCode)
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("health: unparseable reply %q: %w", truncate(raw), err)
	}
	return out.ID, nil
}

// checkShape rejects a successful reply that is missing the field the client
// needs. A read that says "ok" without a value is not an answer, and guessing
// zero would put a fact into the history that no server ever stated.
func checkShape(req Request, resp Response) error {
	if !resp.OK {
		return nil
	}
	switch req.Kind {
	case history.Read:
		if resp.Value == nil {
			return errors.New("read succeeded but the reply carried no value")
		}
	case history.CAS:
		if resp.Swapped == nil {
			return errors.New("cas succeeded but the reply did not say whether it swapped")
		}
	}
	return nil
}

func truncate(b []byte) string {
	const limit = 120
	if len(b) <= limit {
		return string(b)
	}
	return string(b[:limit]) + "..."
}
