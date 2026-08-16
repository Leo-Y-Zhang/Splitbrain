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
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
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

	// MaxCacheBytes caps how much the search may remember per key, in bytes.
	// Zero is no limit, which is a foot-gun rather than a default: the command
	// line sets a real one.
	//
	// MaxVisits alone does not bound memory. It counts model transitions, and
	// every transition can add a cache entry whose width grows with the number
	// of operations on the key, so a large history reaches the visit budget
	// having already allocated gigabytes. Measured before this existed: a
	// twelve-megabyte history file took 4.96 GB of resident memory in 8.4
	// seconds under the default flags, and then reported Unknown anyway. A
	// history file is the artefact people pass around, so it is the input that
	// has to be survivable.
	MaxCacheBytes int

	// Minimize asks for the smallest failing truncation of the history when a
	// violation is found. It costs a handful of extra searches.
	Minimize bool

	// Parallelism is how many keys Check may search at once. Zero means
	// runtime.GOMAXPROCS(0); one is sequential. It never exceeds the number of
	// keys, so a two-key history does not start eight goroutines to sit idle.
	//
	// It cannot change a verdict. Keys are independent objects and
	// linearizability is compositional over them, which is the same argument
	// that lets Check split the history by key in the first place; searching
	// two of them at the same time is the same question asked twice, not an
	// approximation. What it does change is that Model.Step is called from
	// several goroutines at once, so a Model must be safe for concurrent use.
	// Every model in this repository is a stateless empty struct.
	Parallelism int
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

	// One slot per key, in sorted key order, filled by whichever goroutine took
	// that key. Nothing is shared for writing, so there is nothing to lock and
	// nothing for the race detector to find - and, more to the point, nothing
	// about the answer depends on which key finished first. A shared map behind
	// a mutex would be safe and still wrong: it would hand "which key failed"
	// to the scheduler, and two runs over the same file would disagree.
	results := make([]keyResult, len(keys))
	searchAll(keys, byKey, results, m, opt, deadline)

	perKey := make(map[string]KeyStat, len(keys))
	total := 0
	for i, k := range keys {
		kr := results[i]
		perKey[k] = KeyStat{Ops: len(byKey[k]), Visits: kr.visits, Verdict: kr.verdict, Elapsed: kr.elapsed}
		total += kr.visits
	}

	res := Result{Verdict: Linearizable, Visits: total, PerKey: perKey}
	res.Reason = summarise(len(live), len(keys))

	reported := -1
	for _, want := range []Verdict{NotLinearizable, Unknown} {
		for i := range keys {
			if results[i].verdict == want {
				reported, res.Verdict = i, want
				break
			}
		}
		if reported >= 0 {
			break
		}
	}

	if reported >= 0 {
		k := keys[reported]
		kr := results[reported]
		res.Key = k
		res.Reason = fmt.Sprintf("key %q: %s", k, kr.reason)
		res.attachDetail(byKey[k], kr, m, opt, deadline)
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

// searchAll searches every key, writing each key's outcome into its own slot of
// results, and returns once all of them are done.
//
// Keys are independent objects, so this is exact rather than an approximation:
// it is the compositional argument that lets Check split the history by key at
// all, applied to when the searches run rather than to whether they are
// separate. Each goroutine touches one key's operations and one slot; byKey and
// keys are only read.
//
// Options.Timeout is deliberately not divided up. It bounds the call, so every
// key races the same deadline; MaxVisits and MaxCacheBytes are per key and stay
// per key, which means the memory ceiling is now per key in flight rather than
// per key in turn.
func searchAll(keys []string, byKey map[string]history.History, results []keyResult,
	m model.Model, opt Options, deadline time.Time) {

	workers := opt.Parallelism
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > len(keys) {
		workers = len(keys)
	}
	if workers <= 1 {
		for i, k := range keys {
			results[i] = searchOne(byKey[k], m, opt, deadline)
		}
		return
	}

	// Keys cost wildly different amounts - on a captured run one key took 515
	// model transitions and another took two million - so the work is handed
	// out as it is asked for rather than dealt out in advance.
	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(keys) {
					return
				}
				results[i] = searchOne(byKey[keys[i]], m, opt, deadline)
			}
		}()
	}
	wg.Wait()
}

// searchOne sorts one key's operations and searches them.
//
// The sort belongs here, with the search, because it makes the answer
// independent of the order the operations happen to sit in the input slice,
// which history says carries no meaning.
func searchOne(ops history.History, m model.Model, opt Options, deadline time.Time) keyResult {
	ops.SortByInvoke()
	return searchKey(ops, m, opt, deadline)
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

	// finalState is the model state the linearization ends in. It is only
	// meaningful when verdict is Linearizable, and only indeterminateLast reads it.
	finalState model.State
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
	// bytes is a running estimate of what this cache is holding. It is an
	// estimate because the map's own overhead is not observable from here; it
	// tracks the part that grows without bound, which is the cloned bitsets.
	bytes int
	// entryBytes is what one entry costs: the bitset's words, plus a flat
	// allowance for the entry struct and its share of the bucket slice.
	entryBytes int
}

// perEntryOverhead is the flat part of a cache entry's cost: the cacheEntry
// struct, the interface holding the state, and a share of the slice header and
// map bucket it lives in. Approximate on purpose - the point is a ceiling that
// holds, not an exact figure.
const perEntryOverhead = 64

func newCache(words int) *cache {
	return &cache{
		seed:       maphash.MakeSeed(),
		buckets:    make(map[uint64][]cacheEntry),
		entryBytes: words*8 + perEntryOverhead,
	}
}

// Bytes is the running estimate of what the cache holds.
func (c *cache) Bytes() int { return c.bytes }

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
	c.bytes += c.entryBytes
}

// deadlineCheckEvery is how many loop iterations pass between clock reads. The
// clock is far more expensive than an iteration, and a few thousand iterations
// either side of a timeout does not matter.
const deadlineCheckEvery = 4096

// indeterminateLastVisits is the transition budget the fast path below is allowed,
// per operation on the key. It is small on purpose: the fast path either
// succeeds almost immediately or is the wrong shape for this history, and every
// transition it spends comes off the budget for the real search, so the ceiling
// the caller asked for is the ceiling they get.
//
// Measured: where the fast path works it takes a shade over one transition per
// operation - 515 for a 505-operation key. Thirty-two is a wide margin over
// that and still under one per cent of the command line's default budget for a
// key that size.
const indeterminateLastVisits = 32

// searchKey decides one key's operations, which must already be sorted by
// invocation time.
//
// It tries a cheap special case first, then the general search. See indeterminateLast
// for the special case and searchFull for the search.
func searchKey(ops history.History, m model.Model, opt Options, deadline time.Time) keyResult {
	start := time.Now()
	if len(ops) == 0 {
		return keyResult{
			verdict: Linearizable,
			reason:  fmt.Sprintf("all %s on this key linearize", plural(0, "operation")),
			elapsed: time.Since(start),
		}
	}

	found, spent := indeterminateLast(ops, m, opt, deadline)
	if found {
		return keyResult{
			verdict: Linearizable,
			reason:  fmt.Sprintf("all %s on this key linearize", plural(len(ops), "operation")),
			visits:  spent,
			elapsed: time.Since(start),
		}
	}
	rest := opt
	if opt.MaxVisits > 0 {
		if spent >= opt.MaxVisits {
			// The fast path used the lot. Handing the real search a remaining
			// budget of zero would read as "unlimited", which is the one thing
			// it must never mean.
			return keyResult{
				verdict: Unknown,
				reason:  fmt.Sprintf("undecided after %d model transitions, the visit budget", spent),
				visits:  spent,
				elapsed: time.Since(start),
			}
		}
		rest.MaxVisits = opt.MaxVisits - spent
	}
	kr := searchFull(ops, m, rest, deadline)
	kr.visits += spent
	kr.elapsed = time.Since(start)
	return kr
}

// indeterminateLast looks for one particular shape of linearization: every operation
// the client never heard back from placed after every operation that returned.
// It reports whether it found one, and what it spent finding out.
//
// It can only ever answer yes. Failing to find this shape says nothing at all
// about the history - a linearization may exist with a timed-out write in the
// middle - so a failure falls through to the full search and never becomes a
// verdict. That one-sidedness is what makes it safe to give up on early.
//
// When it answers yes it has a witness, so the answer is exact. The ordering is
// legal in real time by construction: an operation that never returned has no
// completion time, so nothing is required to follow it, and putting all of them
// at the end cannot violate a constraint that does not exist. The operations
// that did return keep the order the search found for them among themselves.
//
// This is worth the code because it is what a real run produces. On a captured
// 4008-operation kvsingle run under `-faults chaos`, not one successful read
// observed a value only an indeterminate operation could have explained: zero,
// across eight keys carrying 42 to 60 such operations each. Those operations
// therefore all belong at the end, and finding that costs a walk. Searching for
// it in the general way instead spent the whole two-million transition budget
// on five of the eight keys and reported unknown, because an operation that
// never returns is a candidate at every node and always fits a register model,
// so the search tries each of them at every step.
//
// The indeterminate operations are appended in invocation order, which is the
// order the requests were sent in and so the order a server that applied them
// late would have applied them. If the model refuses one of them in that order
// the fast path gives up rather than searching their permutations.
func indeterminateLast(ops history.History, m model.Model, opt Options, deadline time.Time) (bool, int) {
	// Classify by outcome rather than by completion time. They agree on any
	// history Validate accepted, but Outcome is the thing being reasoned about.
	returned := make(history.History, 0, len(ops))
	var indeterminate history.History
	for _, op := range ops {
		if op.Outcome == history.Info {
			indeterminate = append(indeterminate, op)
		} else {
			returned = append(returned, op)
		}
	}
	if len(indeterminate) == 0 {
		// Nothing to move to the end, so this is just the full search again.
		return false, 0
	}

	probe := opt
	probe.MaxVisits = indeterminateLastVisits * len(ops)
	if opt.MaxVisits > 0 && opt.MaxVisits < probe.MaxVisits {
		probe.MaxVisits = opt.MaxVisits
	}
	kr := searchFull(returned, m, probe, deadline)
	if kr.verdict != Linearizable {
		return false, kr.visits
	}

	state := kr.finalState
	visits := kr.visits
	for _, op := range indeterminate {
		next, ok := m.Step(state, op)
		visits++
		if !ok {
			return false, visits
		}
		state = next
	}
	return true, visits
}

// searchFull runs the Wing-Gong search over one key's operations, which must
// already be sorted by invocation time.
func searchFull(ops history.History, m model.Model, opt Options, deadline time.Time) keyResult {
	start := time.Now()
	kr := keyResult{verdict: Linearizable}
	kr.reason = fmt.Sprintf("all %s on this key linearize", plural(len(ops), "operation"))
	if len(ops) == 0 {
		kr.finalState = m.Init()
		kr.elapsed = time.Since(start)
		return kr
	}

	head := buildEntries(ops)
	lin := newBitset(len(ops))
	seen := newCache(len(lin))
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
		if opt.MaxCacheBytes > 0 && seen.Bytes() >= opt.MaxCacheBytes {
			// Giving up here costs nothing in soundness: Unknown is already
			// the honest verdict of a search that has not finished, and it is
			// the one the visit budget would have produced a few seconds later
			// after taking several more gigabytes to get there.
			kr.verdict = Unknown
			kr.reason = fmt.Sprintf("undecided after %s of remembered states, the memory budget",
				humanBytes(seen.Bytes()))
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
	if kr.verdict == Linearizable {
		// Every operation is placed, so this is the state the linearization
		// ends in. indeterminateLast continues from here.
		kr.finalState = state
	}
	kr.visits = visits
	kr.elapsed = time.Since(start)
	return kr
}

// humanBytes renders a size the way a person reading a verdict wants it.
func humanBytes(n int) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.0f KB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d bytes", n)
	}
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
