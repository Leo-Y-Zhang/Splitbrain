// Package kv holds the three key-value stores Splitbrain tests against, and
// the client that records what it saw when it talked to them.
//
// The stores are fixtures, not products. Each one occupies a different corner
// of the availability/consistency trade-off, so that a run against all three
// tells you something about the checker as well as about the store:
//
//   - SingleStore is one map behind one mutex. It is linearizable by
//     construction, so a violation reported against it is a bug in the harness
//     or the checker, never in the store. It is the control case.
//   - ForwardStore is a fixed leader with followers that forward. Everything
//     serialises at the leader, so it is also linearizable, and it loses
//     availability the moment a follower cannot reach the leader.
//   - QuorumStore replicates asynchronously and reads locally. It stays
//     available under a partition and it is not linearizable. It is the corner
//     the checker exists to catch.
//
// The wire protocol is deliberately dull: HTTP/1.1, JSON, one endpoint. That
// keeps the fault proxy underneath it protocol-agnostic - it only has to move
// bytes and decide when to stop - and it means a human can reproduce any
// operation in the history with curl.
package kv

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
)

// maxRequestBytes bounds the body a server will read. The requests are a few
// dozen bytes; anything larger is a confused client or an attack, and reading
// it costs a server that is meant to be under test for something else.
const maxRequestBytes = 1 << 16

// A Request is one key-value operation to send to a server.
//
// Which fields matter depends on Kind: Value for a Write, From and To for a
// CAS, neither for a Read. The unused fields are ignored rather than checked,
// because the generator that fills this in is the thing being trusted, and a
// second opinion here would only hide its bugs.
type Request struct {
	// Kind is the operation: history.Read, history.Write or history.CAS.
	Kind history.Kind
	// Key names the register to act on.
	Key string
	// Value is the value to store, for a Write.
	Value int
	// From and To are the expected and the new value, for a CAS.
	From, To int
}

// String renders a request the way it appears on the wire, for log lines.
func (r Request) String() string {
	switch r.Kind {
	case history.Write:
		return fmt.Sprintf("write %s=%d", r.Key, r.Value)
	case history.CAS:
		return fmt.Sprintf("cas %s %d->%d", r.Key, r.From, r.To)
	default:
		return fmt.Sprintf("read %s", r.Key)
	}
}

// A Response is a server's reply to one Request.
//
// Value and Swapped are pointers because absent and zero are different things
// here. A read of a key that has never been written returns zero, and the
// client has to be able to tell that apart from a server that forgot to
// include the field at all - the first is an answer, the second is a
// malformed reply that must be recorded as indeterminate.
type Response struct {
	// OK reports whether the operation took effect. False means the server
	// reached a decision and declined; it is not a transport failure.
	OK bool `json:"ok"`
	// Value is the value read, present only on a successful read.
	Value *int `json:"value,omitempty"`
	// Swapped reports whether a CAS exchanged the value, present only on a
	// successful CAS.
	Swapped *bool `json:"swapped,omitempty"`
	// Err is the server's reason for declining, for humans.
	Err string `json:"error,omitempty"`
}

// ReadOK builds the reply to a successful read.
func ReadOK(value int) Response { return Response{OK: true, Value: &value} }

// WriteOK builds the reply to a successful write.
func WriteOK() Response { return Response{OK: true} }

// CASOK builds the reply to a compare-and-swap that the server carried out.
// A CAS that did not exchange the value still succeeded: the server compared,
// found a mismatch and correctly did nothing.
func CASOK(swapped bool) Response { return Response{OK: true, Swapped: &swapped} }

// Declined builds a definite server-side refusal. Use it only when the
// operation certainly did not take effect, because a client records it as
// history.Fail and the checker then removes the operation from the history
// altogether. When the server does not know, return an error from
// Store.Apply instead.
func Declined(format string, args ...any) Response {
	return Response{OK: false, Err: fmt.Sprintf(format, args...)}
}

// wireRequest is the JSON form of a Request. The pointers make an omitted
// field distinguishable from a zero one, so a write that forgot its value is
// rejected rather than silently treated as a write of 0.
type wireRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value *int   `json:"value,omitempty"`
	From  *int   `json:"from,omitempty"`
	To    *int   `json:"to,omitempty"`
}

func (r Request) toWire() wireRequest {
	w := wireRequest{Op: r.Kind.String(), Key: r.Key}
	switch r.Kind {
	case history.Write:
		v := r.Value
		w.Value = &v
	case history.CAS:
		from, to := r.From, r.To
		w.From, w.To = &from, &to
	}
	return w
}

func (w wireRequest) toRequest() (Request, error) {
	kind, err := history.ParseKind(w.Op)
	if err != nil {
		return Request{}, err
	}
	if w.Key == "" {
		return Request{}, fmt.Errorf("missing key")
	}
	r := Request{Kind: kind, Key: w.Key}
	switch kind {
	case history.Write:
		if w.Value == nil {
			return Request{}, fmt.Errorf("write needs a value")
		}
		r.Value = *w.Value
	case history.CAS:
		if w.From == nil || w.To == nil {
			return Request{}, fmt.Errorf("cas needs both from and to")
		}
		r.From, r.To = *w.From, *w.To
	}
	return r, nil
}

// decodeRequest reads one request from a body. It rejects trailing content so
// that a client which pipelines two requests into one body gets an error
// rather than having the second silently dropped.
func decodeRequest(r io.Reader) (Request, error) {
	dec := json.NewDecoder(io.LimitReader(r, maxRequestBytes))
	var w wireRequest
	if err := dec.Decode(&w); err != nil {
		return Request{}, fmt.Errorf("malformed request: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return Request{}, fmt.Errorf("malformed request: trailing content after the operation")
	}
	return w.toRequest()
}
