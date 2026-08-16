// Package history is the contract every other package in Splitbrain agrees on:
// what a recorded client operation is, and what is and is not known about it.
//
// The whole tool rests on one distinction that naive test harnesses get wrong.
// When a client sends a request over a network that is being deliberately
// broken and no reply comes back, the operation did not "fail". It is
// *indeterminate*: it may have been applied by the server, and a later read may
// legitimately observe it. Dropping those operations from the history makes a
// checker unsound - it will pass histories that are genuinely wrong. Keeping
// them but treating them as successful makes it unsound in the other
// direction. They get their own outcome here, and the checker is required to
// treat them as "may or may not have happened".
package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
)

// Kind is the operation a client asked for.
type Kind uint8

const (
	// Read returns the current value of a key.
	Read Kind = iota
	// Write unconditionally sets a key.
	Write
	// CAS sets a key to To only if it currently holds From.
	CAS
)

// String renders a Kind for JSON and for humans.
func (k Kind) String() string {
	switch k {
	case Read:
		return "read"
	case Write:
		return "write"
	case CAS:
		return "cas"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// ParseKind is the inverse of Kind.String.
func ParseKind(s string) (Kind, error) {
	switch s {
	case "read":
		return Read, nil
	case "write":
		return Write, nil
	case "cas":
		return CAS, nil
	default:
		return 0, fmt.Errorf("unknown operation kind %q", s)
	}
}

// Outcome records what the client learned about its own request.
type Outcome uint8

const (
	// OK means a well-formed response came back. The operation took effect
	// at some single instant inside [Invoke, Complete].
	OK Outcome = iota

	// Fail means the operation definitely did not take effect. This is only
	// recorded when the client can prove the request never reached the
	// server: in practice a refused connection, where no bytes were ever
	// written to an accepted socket. Anything weaker is Info.
	Fail

	// Info means indeterminate: the request may or may not have been applied.
	// A timeout, a connection reset mid-flight, a truncated reply and an EOF
	// all land here. The checker must allow such an operation to be placed
	// anywhere after its invocation, including after every other operation,
	// and it must not assume any particular result was returned.
	Info
)

// String renders an Outcome for JSON and for humans.
func (o Outcome) String() string {
	switch o {
	case OK:
		return "ok"
	case Fail:
		return "fail"
	case Info:
		return "info"
	default:
		return fmt.Sprintf("outcome(%d)", uint8(o))
	}
}

// ParseOutcome is the inverse of Outcome.String.
func ParseOutcome(s string) (Outcome, error) {
	switch s {
	case "ok":
		return OK, nil
	case "fail":
		return Fail, nil
	case "info":
		return Info, nil
	default:
		return 0, fmt.Errorf("unknown outcome %q", s)
	}
}

// Pending is the completion time of an operation that never completed. It is
// deliberately larger than any timestamp a run can produce, so code that sorts
// by completion time puts indeterminate operations last without special cases.
const Pending int64 = math.MaxInt64

// An Op is one client operation with everything the checker is allowed to know
// about it. Times are nanoseconds measured from the start of the run on one
// machine's monotonic clock; they are only ever compared with each other.
type Op struct {
	// Process identifies the client that issued this operation. A process is
	// sequential: its operations never overlap in time. Validate enforces it,
	// because a harness bug there silently weakens every real-time constraint
	// the checker relies on.
	Process int

	// Key names the register this operation acts on. Registers are
	// independent, which is what lets the checker split the history by key.
	Key string

	Kind Kind

	// Value is the value written, for Write.
	Value int
	// From and To are the expected and new values, for CAS.
	From, To int

	// Observed is the value returned, for a Read with Outcome OK.
	Observed int
	// Swapped reports whether a CAS with Outcome OK actually swapped.
	Swapped bool

	Outcome Outcome

	// Invoke is when the client began the request; Complete is when it
	// learned the answer. For Outcome Info, Complete is Pending.
	Invoke, Complete int64

	// Err carries the transport error for a Fail or Info, for humans only.
	// The checker never reads it.
	Err string
}

// Concurrent reports whether two operations overlap in real time, and so may
// be ordered either way by a linearization.
func (o Op) Concurrent(p Op) bool {
	return o.Invoke < p.Complete && p.Invoke < o.Complete
}

// A History is a set of operations recorded during one run. Order in the slice
// carries no meaning; the timestamps do.
type History []Op

// wireOp is the on-disk form. The field names are spelled out so a history
// file can be read by a person, and by something that is not this program.
type wireOp struct {
	Process  int    `json:"process"`
	Key      string `json:"key"`
	Kind     string `json:"kind"`
	Value    *int   `json:"value,omitempty"`
	From     *int   `json:"from,omitempty"`
	To       *int   `json:"to,omitempty"`
	Observed *int   `json:"observed,omitempty"`
	Swapped  *bool  `json:"swapped,omitempty"`
	Outcome  string `json:"outcome"`
	Invoke   int64  `json:"invoke_ns"`
	Complete *int64 `json:"complete_ns,omitempty"`
	Err      string `json:"error,omitempty"`
}

func intp(v int) *int    { return &v }
func boolp(v bool) *bool { return &v }

func (o Op) toWire() wireOp {
	w := wireOp{
		Process: o.Process,
		Key:     o.Key,
		Kind:    o.Kind.String(),
		Outcome: o.Outcome.String(),
		Invoke:  o.Invoke,
		Err:     o.Err,
	}
	switch o.Kind {
	case Write:
		w.Value = intp(o.Value)
	case CAS:
		w.From, w.To = intp(o.From), intp(o.To)
	}
	if o.Outcome == OK {
		switch o.Kind {
		case Read:
			w.Observed = intp(o.Observed)
		case CAS:
			w.Swapped = boolp(o.Swapped)
		}
	}
	if o.Outcome != Info {
		c := o.Complete
		w.Complete = &c
	}
	return w
}

func (w wireOp) toOp() (Op, error) {
	k, err := ParseKind(w.Kind)
	if err != nil {
		return Op{}, err
	}
	oc, err := ParseOutcome(w.Outcome)
	if err != nil {
		return Op{}, err
	}
	o := Op{
		Process:  w.Process,
		Key:      w.Key,
		Kind:     k,
		Outcome:  oc,
		Invoke:   w.Invoke,
		Complete: Pending,
		Err:      w.Err,
	}
	if w.Value != nil {
		o.Value = *w.Value
	}
	if w.From != nil {
		o.From = *w.From
	}
	if w.To != nil {
		o.To = *w.To
	}
	if w.Observed != nil {
		o.Observed = *w.Observed
	}
	if w.Swapped != nil {
		o.Swapped = *w.Swapped
	}
	if w.Complete != nil {
		o.Complete = *w.Complete
	} else if oc != Info {
		return Op{}, fmt.Errorf("outcome %q needs a complete_ns", w.Outcome)
	}
	return o, nil
}

// Save emits the history as JSON Lines, one operation per line, sorted by
// invocation time so the file reads in the order the run happened.
//
// It is Save rather than Write because Write is already the name of an
// operation kind in this package.
func (h History) Save(w io.Writer) error {
	sorted := make(History, len(h))
	copy(sorted, h)
	sorted.SortByInvoke()

	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	for _, op := range sorted {
		if err := enc.Encode(op.toWire()); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// Load parses a JSON Lines history. A malformed line is an error rather than a
// skip: a checker that silently drops operations it could not parse would
// report a verdict about a history nobody wrote.
func Load(r io.Reader) (History, error) {
	var h History
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var w wireOp
		if err := json.Unmarshal(b, &w); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		op, err := w.toOp()
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		h = append(h, op)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return h, nil
}

// SortByInvoke orders operations by invocation time, breaking ties by process
// so the order is total and stable across runs.
func (h History) SortByInvoke() {
	sort.SliceStable(h, func(i, j int) bool {
		if h[i].Invoke != h[j].Invoke {
			return h[i].Invoke < h[j].Invoke
		}
		return h[i].Process < h[j].Process
	})
}

// Keys lists the keys the history touches, in sorted order.
func (h History) Keys() []string {
	seen := map[string]bool{}
	var out []string
	for _, op := range h {
		if !seen[op.Key] {
			seen[op.Key] = true
			out = append(out, op.Key)
		}
	}
	sort.Strings(out)
	return out
}

// ByKey splits the history into one sub-history per key.
//
// This split is the reason the checker is usable at all. Linearizability is
// compositional (Herlihy and Wing 1990, Theorem 1): a history over a set of
// independent objects is linearizable exactly when each object's sub-history
// is. Searching one register's operations is exponential in the worst case, so
// searching eight registers separately is enormously cheaper than searching
// their interleaving - and it is not an approximation, it is the same answer.
func (h History) ByKey() map[string]History {
	out := map[string]History{}
	for _, op := range h {
		out[op.Key] = append(out[op.Key], op)
	}
	return out
}

// DropFailed removes operations that definitely never took effect.
//
// Only Fail is dropped. Info stays, because it might have happened.
func (h History) DropFailed() History {
	out := make(History, 0, len(h))
	for _, op := range h {
		if op.Outcome != Fail {
			out = append(out, op)
		}
	}
	return out
}

// Counts summarises a history by outcome.
func (h History) Counts() (ok, fail, info int) {
	for _, op := range h {
		switch op.Outcome {
		case OK:
			ok++
		case Fail:
			fail++
		case Info:
			info++
		}
	}
	return
}

// Validate checks the structural invariants a recorded history must satisfy.
// Every one of these has bitten a real harness: a completion before its own
// invocation, or one process with two operations in flight at once, quietly
// removes a real-time constraint and turns a checker into a rubber stamp.
func (h History) Validate() error {
	byProc := map[int]History{}
	for i, op := range h {
		if op.Invoke < 0 {
			return fmt.Errorf("op %d: negative invoke time %d", i, op.Invoke)
		}
		if op.Key == "" {
			return fmt.Errorf("op %d: empty key", i)
		}
		switch op.Outcome {
		case OK, Fail:
			if op.Complete == Pending {
				return fmt.Errorf("op %d: outcome %s but no completion time", i, op.Outcome)
			}
			if op.Complete < op.Invoke {
				return fmt.Errorf("op %d: completes at %d before it is invoked at %d", i, op.Complete, op.Invoke)
			}
		case Info:
			if op.Complete != Pending {
				return fmt.Errorf("op %d: indeterminate operations must not carry a completion time", i)
			}
		default:
			return fmt.Errorf("op %d: unknown outcome %d", i, op.Outcome)
		}
		if op.Kind == CAS && op.From == op.To {
			// Not wrong, but a no-op CAS constrains nothing and is almost
			// always a generator bug rather than an interesting operation.
			return fmt.Errorf("op %d: cas from %d to the same value", i, op.From)
		}
		byProc[op.Process] = append(byProc[op.Process], op)
	}

	for proc, ops := range byProc {
		ops.SortByInvoke()
		for i := 1; i < len(ops); i++ {
			prev := ops[i-1]
			// An indeterminate operation blocks its process forever in the
			// model, so it must be that process's last recorded operation.
			if prev.Outcome == Info {
				return fmt.Errorf("process %d: operation after an indeterminate one; a client cannot know it is safe to continue", proc)
			}
			if ops[i].Invoke < prev.Complete {
				return fmt.Errorf("process %d: operations overlap (%d..%d then %d..); one client cannot have two requests in flight",
					proc, prev.Invoke, prev.Complete, ops[i].Invoke)
			}
		}
	}
	return nil
}
