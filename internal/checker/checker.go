// Package checker decides whether a recorded history is linearizable: whether
// some total order of the client operations both obeys a sequential model and
// respects real time, so that an operation which completed before another was
// invoked comes first.
//
// The search is Wing and Gong's (1993) with Lowe's optimisations (Lowe,
// Testing for linearizability, CCPE 2017): a doubly-linked list of call and
// return entries, depth-first placement of whichever call fits the model next,
// O(1) removal and restoration of a placed operation, and a cache of
// (operations placed, model state) pairs that have already been explored.
//
// Two rules run through everything here. Indeterminate operations are kept,
// because an operation the client never got an answer for may still have been
// applied and a later read may legitimately observe it. And an exhausted search
// is never reported as a pass: the verdict is Unknown, which is a different
// thing from Linearizable and must stay different all the way to the exit code.
package checker

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sort"
	"time"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// Verdict is the answer to "does a valid linearization exist".
type Verdict uint8

const (
	// Linearizable means a valid total order was found.
	Linearizable Verdict = iota
	// NotLinearizable means the search was exhaustive and found none.
	NotLinearizable
	// Unknown means the search ran out of budget. It is never a pass. Callers
	// that collapse it into Linearizable turn this into a rubber stamp.
	Unknown
)

// String renders a Verdict for reports and for the command line.
func (v Verdict) String() string {
	switch v {
	case Linearizable:
		return "linearizable"
	case NotLinearizable:
		return "not linearizable"
	case Unknown:
		return "unknown"
	default:
		return fmt.Sprintf("verdict(%d)", uint8(v))
	}
}

// Options tunes one Check call.
type Options struct {
	// MaxVisits caps the model transitions spent on each key. Zero is
	// unlimited. Exceeding it yields Unknown for that key, never a pass.
	MaxVisits int

	// Timeout caps the whole Check call, shared across every key. Zero is no
	// limit. Exceeding it yields Unknown, never a pass.
	Timeout time.Duration

	// Minimize asks for the smallest failing truncation of the history when a
	// violation is found. It costs a handful of extra searches.
	Minimize bool
}

// KeyStat is what happened on one key.
type KeyStat struct {
	// Ops is the number of operations on this key, after definitely-failed
	// ones were dropped. Visits is the model transitions the search spent.
	Ops, Visits int
	Verdict     Verdict
	Elapsed     time.Duration
}

// Result is everything a Check call learned.
type Result struct {
	Verdict Verdict

	// Key is the key that failed, or that ran out of budget. It is empty when
	// the whole history linearizes.
	Key string

	// Ops is the minimal failing truncation, set only when Verdict is
	// NotLinearizable and Options.Minimize was on. It is sorted by invocation
	// time. Because the truncation is by time it is always a prefix of the
	// key's operations, so a long clean run before the violation is kept.
	Ops history.History

	// Culprit is the last-invoked operation of that truncation when one was
	// computed. Otherwise it is the operation the search got furthest with and
	// still could not place, which is the one worth reading first.
	Culprit *history.Op

	// Reason is one human-readable line.
	Reason string

	// Visits is the model transitions attempted across every key.
	Visits  int
	Elapsed time.Duration

	// PerKey reports every key, including the ones that passed.
	PerKey map[string]KeyStat
}

// Check decides whether the whole history is linearizable against m.
//
// It rejects a malformed history rather than guessing at it: a checker that
// tolerates a completion before its own invocation, or one client with two
// requests in flight, is quietly answering a question about a history nobody
// recorded.
//
// Every key is checked, not just up to the first failure, so PerKey is a
// complete picture and the reported key is the first failure in sorted key
// order regardless of how the operations were laid out in the slice.
func Check(h history.History, m model.Model, opt Options) (Result, error) {
	start := time.Now()
	if m == nil {
		return Result{Verdict: Unknown}, errors.New("checker: no model given")
	}
	if err := h.Validate(); err != nil {
		return Result{Verdict: Unknown}, fmt.Errorf("checker: malformed history: %w", err)
	}

	live := h.DropFailed()
	byKey := live.ByKey()
	keys := live.Keys()

	var deadline time.Time
	if opt.Timeout > 0 {
		deadline = start.Add(opt.Timeout)
	}

	perKey := make(map[string]KeyStat, len(keys))
	results := make(map[string]keyResult, len(keys))
	total := 0
	for _, k := range keys {
		ops := byKey[k]
		// Sorting makes the search independent of the order operations happen
		// to sit in the input slice, which history says carries no meaning.
		ops.SortByInvoke()
		kr := searchKey(ops, m, opt, deadline)
		results[k] = kr
		perKey[k] = KeyStat{Ops: len(ops), Visits: kr.visits, Verdict: kr.verdict, Elapsed: kr.elapsed}
		total += kr.visits
	}

	res := Result{Verdict: Linearizable, Visits: total, PerKey: perKey}
	res.Reason = summarise(len(live), len(keys))

	reported := ""
	for _, want := range []Verdict{NotLinearizable, Unknown} {
		for _, k := range keys {
			if results[k].verdict == want {
				reported, res.Verdict = k, want
				break
			}
		}
		if reported != "" {
			break
		}
	}

	if reported != "" {
		kr := results[reported]
		res.Key = reported
		res.Reason = fmt.Sprintf("key %q: %s", reported, kr.reason)
		res.attachDetail(byKey[reported], kr, m, opt, deadline)
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

// CheckKey decides whether one key's operations are linearizable.
//
// It refuses a history spanning several keys. Feeding two registers' operations
// to one register model would produce a confident verdict about an object that
// never existed, and that is worse than an error.
func CheckKey(ops history.History, m model.Model, opt Options) (Result, error) {
	start := time.Now()
	if m == nil {
		return Result{Verdict: Unknown}, errors.New("checker: no model given")
	}
	if err := ops.Validate(); err != nil {
		return Result{Verdict: Unknown}, fmt.Errorf("checker: malformed history: %w", err)
	}
	keys := ops.Keys()
	if len(keys) > 1 {
		return Result{Verdict: Unknown}, fmt.Errorf("checker: CheckKey was given %d keys (%v); use Check", len(keys), keys)
	}

	live := ops.DropFailed()
	live.SortByInvoke()

	var deadline time.Time
	if opt.Timeout > 0 {
		deadline = start.Add(opt.Timeout)
	}
	kr := searchKey(live, m, opt, deadline)

	res := Result{
		Verdict: kr.verdict,
		Visits:  kr.visits,
		Reason:  summarise(len(live), len(keys)),
		PerKey:  map[string]KeyStat{},
	}
	key := ""
	if len(keys) == 1 {
		key = keys[0]
		res.PerKey[key] = KeyStat{Ops: len(live), Visits: kr.visits, Verdict: kr.verdict, Elapsed: kr.elapsed}
	}
	if kr.verdict != Linearizable {
		res.Key = key
		res.Reason = fmt.Sprintf("key %q: %s", key, kr.reason)
		res.attachDetail(live, kr, m, opt, deadline)
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

func summarise(ops, keys int) string {
	if ops == 0 {
		return "the history is empty"
	}
	return fmt.Sprintf("all %s across %s linearize", plural(ops, "operation"), plural(keys, "key"))
}

func plural(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// attachDetail fills in the counterexample fields once a failing key is known.
func (r *Result) attachDetail(ops history.History, kr keyResult, m model.Model, opt Options, deadline time.Time) {
	if kr.hasCulprit {
		c := kr.culprit
		r.Culprit = &c
	}
	if r.Verdict != NotLinearizable || !opt.Minimize {
		return
	}
	trunc, ok := minimise(ops, m, opt, deadline)
	if !ok {
		// Minimisation ran out of budget. Report the violation without a
		// truncation rather than a truncation we cannot stand behind.
		r.Reason += " (minimisation ran out of budget)"
		return
	}
	trunc.SortByInvoke()
	r.Ops = trunc
	if n := len(trunc); n > 0 {
		last := trunc[n-1]
		r.Culprit = &last
	}
}

// keyResult is the per-key outcome, before it is dressed up as a Result.
type keyResult struct {
	verdict Verdict
	visits  int
	elapsed time.Duration
	reason  string

	// culprit is the operation the search got deepest with and still could not
	// place. It is a good pointer at the problem rather than a proof: the
	// search may have failed for several reasons at that depth.
	culprit    history.Op
	hasCulprit bool
}

// An entry is one endpoint of an operation on the timeline: the moment the
// client called, or the moment it heard back.
type entry struct {
	op       *history.Op
	id       int
	isReturn bool

	// match is the other endpoint of the same operation. Placing an operation
	// removes both, which is what makes the remaining list exactly the set of
	// operations still to be placed.
	match      *entry
	prev, next *entry
}

// position gives the sort key of an entry as a time and a phase within that
// time.
//
// Returns come before calls at an identical timestamp, because history.Op's
// Concurrent test uses a strict <: an operation completing at exactly t and one
// invoked at exactly t are not concurrent, and the entry list has to agree or
// the search will happily reorder operations real time separates.
//
// The middle phase exists for the one case that rule cannot express. An
// operation whose invocation and completion coincide would otherwise sort its
// own return before its own call, which is nonsense and breaks the walk. Such
// an operation is instead placed as a call/return pair between the returns and
// the calls at that instant, which is exactly where an instantaneous operation
// belongs. Two zero-duration operations sharing a timestamp are a degenerate
// input - each strictly precedes the other - and get an arbitrary but
// deterministic order.
func (e *entry) position() (t int64, phase int8) {
	op := e.op
	instant := op.Complete == op.Invoke
	if e.isReturn {
		if instant {
			return op.Complete, 1
		}
		return op.Complete, 0
	}
	if instant {
		return op.Invoke, 1
	}
	return op.Invoke, 2
}

func entryLess(a, b *entry) bool {
	at, ap := a.position()
	bt, bp := b.position()
	if at != bt {
		return at < bt
	}
	if ap != bp {
		return ap < bp
	}
	if a.id != b.id {
		return a.id < b.id
	}
	return !a.isReturn && b.isReturn
}

// buildEntries lays the operations out as a doubly-linked list behind a
// sentinel head, and returns that sentinel.
func buildEntries(ops history.History) *entry {
	nodes := make([]entry, 2*len(ops))
	order := make([]*entry, 0, 2*len(ops))
	for i := range ops {
		call, ret := &nodes[2*i], &nodes[2*i+1]
		*call = entry{op: &ops[i], id: i, match: ret}
		*ret = entry{op: &ops[i], id: i, isReturn: true, match: call}
		order = append(order, call, ret)
	}
	sort.Slice(order, func(a, b int) bool { return entryLess(order[a], order[b]) })

	head := &entry{id: -1}
	prev := head
	for _, e := range order {
		prev.next = e
		e.prev = prev
		prev = e
	}
	return head
}

// lift removes a call and its matching return from the list in O(1). The two
// removed entries keep their own prev and next pointers, which is what lets
// unlift put them back without searching for the place.
//
// The call must be spliced out first. When the two entries are adjacent the
// return's prev is the call itself, so removing the return first writes
// through it and clobbers the call's own next pointer - and that pointer is
// exactly what unlift needs to restore the pair. Doing it in the other order
// leaves the list looking plausible and the verdicts wrong.
func (e *entry) lift() {
	e.prev.next = e.next
	e.next.prev = e.prev // a call always has its own return after it

	m := e.match
	m.prev.next = m.next
	if m.next != nil {
		m.next.prev = m.prev
	}
}

// unlift restores what lift removed, in the reverse order for symmetry. Unlike
// lift the order is not load-bearing here: every write lands on a neighbour's
// pointer rather than on the two entries' own, so both orders settle on the
// same list.
func (e *entry) unlift() {
	m := e.match
	m.prev.next = m
	if m.next != nil {
		m.next.prev = m
	}
	e.prev.next = e
	e.next.prev = e
}

// cacheEntry is one explored configuration: the set of operations already
// placed, and the model state that placing them reached.
type cacheEntry struct {
	lin   bitset
	state model.State
}

// cache remembers explored configurations. Reaching the same set of placed
// operations in the same state by a different route can only lead to the same
// answer, so the second route is abandoned. This is the optimisation that makes
// the difference between minutes and milliseconds on real histories.
type cache struct {
	seed    maphash.Seed
	buckets map[uint64][]cacheEntry
}

func newCache() *cache {
	return &cache{seed: maphash.MakeSeed(), buckets: make(map[uint64][]cacheEntry)}
}

// hash mixes the placed-set with the state. model.State is required to be
// comparable, which is also what lets the bucket be scanned with ==.
func (c *cache) hash(lin bitset, s model.State) uint64 {
	return lin.hash() ^ maphash.Comparable(c.seed, s)
}

func (c *cache) contains(lin bitset, s model.State) bool {
	for _, e := range c.buckets[c.hash(lin, s)] {
		if e.state == s && e.lin.equal(lin) {
			return true
		}
	}
	return false
}

func (c *cache) add(lin bitset, s model.State) {
	h := c.hash(lin, s)
	c.buckets[h] = append(c.buckets[h], cacheEntry{lin: lin.clone(), state: s})
}

// deadlineCheckEvery is how many loop iterations pass between clock reads. The
// clock is far more expensive than an iteration, and a few thousand iterations
// either side of a timeout does not matter.
const deadlineCheckEvery = 4096

// searchKey runs the Wing-Gong search over one key's operations, which must
// already be sorted by invocation time.
func searchKey(ops history.History, m model.Model, opt Options, deadline time.Time) keyResult {
	start := time.Now()
	kr := keyResult{verdict: Linearizable}
	kr.reason = fmt.Sprintf("all %s on this key linearize", plural(len(ops), "operation"))
	if len(ops) == 0 {
		kr.elapsed = time.Since(start)
		return kr
	}

	head := buildEntries(ops)
	lin := newBitset(len(ops))
	seen := newCache()
	state := m.Init()

	// calls is the stack of operations placed so far, each with the state the
	// search was in before it was placed, so a backtrack can restore it.
	type frame struct {
		call  *entry
		state model.State
	}
	calls := make([]frame, 0, len(ops))

	// The deepest point reached is what the report is written from. It is the
	// most informative thing the search knows when it gives up.
	bestDepth := -1
	var bestState model.State
	var bestOp *history.Op

	visits, iters := 0, 0
	e := head.next
	for head.next != nil {
		if opt.MaxVisits > 0 && visits >= opt.MaxVisits {
			kr.verdict = Unknown
			kr.reason = fmt.Sprintf("undecided after %d model transitions, the visit budget", visits)
			break
		}
		if !deadline.IsZero() && iters%deadlineCheckEvery == 0 && time.Now().After(deadline) {
			kr.verdict = Unknown
			kr.reason = fmt.Sprintf("undecided after %s, the time budget", opt.Timeout)
			break
		}
		iters++

		if e == nil {
			// Unreachable: the last entry in the list is always a return, and a
			// return either backtracks or ends the search. Failing closed here
			// rather than falling out of the loop means a bug in this file can
			// never be read as a pass.
			kr.verdict = Unknown
			kr.reason = "the search walked off the end of the entry list, which is a bug in the checker"
			break
		}

		if !e.isReturn {
			next, ok := m.Step(state, *e.op)
			visits++
			if ok {
				lin.set(e.id)
				if !seen.contains(lin, next) {
					seen.add(lin, next)
					calls = append(calls, frame{call: e, state: state})
					state = next
					e.lift()
					e = head.next
					continue
				}
				lin.clear(e.id)
			} else if len(calls) > bestDepth {
				bestDepth = len(calls)
				bestState = state
				bestOp = e.op
			}
			e = e.next
			continue
		}

		// A return whose call is still in the list: everything invoked before
		// this operation returned has been tried and none of it fits, so the
		// last choice was wrong.
		if len(calls) == 0 {
			kr.verdict = NotLinearizable
			kr.reason = "no ordering of these operations satisfies the model"
			if bestOp != nil {
				kr.reason = m.Explain(bestState, *bestOp)
			}
			break
		}
		top := calls[len(calls)-1]
		calls = calls[:len(calls)-1]
		state = top.state
		lin.clear(top.call.id)
		top.call.unlift()
		e = top.call.next
	}

	if bestOp != nil && kr.verdict != Linearizable {
		kr.culprit, kr.hasCulprit = *bestOp, true
	}
	kr.visits = visits
	kr.elapsed = time.Since(start)
	return kr
}

// eventTimes lists, in order, the distinct times at which anything observable
// happened, plus one sentinel past the end that means "keep everything".
func eventTimes(ops history.History) []int64 {
	seen := make(map[int64]bool, 2*len(ops))
	times := make([]int64, 0, 2*len(ops)+1)
	add := func(t int64) {
		if !seen[t] {
			seen[t] = true
			times = append(times, t)
		}
	}
	var last int64
	for _, op := range ops {
		add(op.Invoke)
		if op.Invoke > last {
			last = op.Invoke
		}
		if op.Complete != history.Pending {
			add(op.Complete)
			if op.Complete > last {
				last = op.Complete
			}
		}
	}
	add(last + 1)
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return times
}

// truncateAt cuts the history off at time t: everything invoked before t is
// kept, and anything still in flight at t is downgraded to indeterminate,
// because at t the client had not yet learned anything about it.
func truncateAt(ops history.History, t int64) history.History {
	out := make(history.History, 0, len(ops))
	for _, op := range ops {
		if op.Invoke >= t {
			continue
		}
		if op.Complete >= t {
			op.Outcome = history.Info
			op.Complete = history.Pending
			// Observed and Swapped are left as they were; Model.Step ignores
			// them for an indeterminate operation.
		}
		out = append(out, op)
	}
	return out
}

// minimise finds the earliest moment at which the violation became unavoidable.
//
// The only sound shrink here is truncation in time. Deleting operations is not:
// removing a write can turn a linearizable history into an unlinearizable one,
// so a subset that fails is not evidence that the original failed for the same
// reason. Truncation avoids that because it never removes anything that another
// kept operation could have depended on - every dropped operation was invoked
// after every kept operation with a constrained result had already completed.
//
// That is also why a binary search is valid. If the truncation at t' fails to
// linearize for no earlier t, then every later truncation fails too: restrict a
// linearization of the later one to the earlier one's operations and the state
// sequence is unchanged, while the in-flight operations lose their output
// constraints entirely. The predicate is monotone, so the smallest failing time
// can be found in a logarithmic number of searches.
func minimise(ops history.History, m model.Model, opt Options, deadline time.Time) (history.History, bool) {
	probe := opt
	probe.Minimize = false

	times := eventTimes(ops)
	// The sentinel keeps everything, and the caller only gets here because
	// keeping everything fails, so the predicate is true at the top.
	lo, hi := 0, len(times)-1
	for lo < hi {
		mid := (lo + hi) / 2
		kr := searchKey(sortedTruncation(ops, times[mid]), m, probe, deadline)
		switch kr.verdict {
		case NotLinearizable:
			hi = mid
		case Linearizable:
			lo = mid + 1
		default:
			// An undecided probe breaks monotonicity, so there is nothing
			// honest to return.
			return nil, false
		}
	}
	return sortedTruncation(ops, times[lo]), true
}

func sortedTruncation(ops history.History, t int64) history.History {
	out := truncateAt(ops, t)
	out.SortByInvoke()
	return out
}
