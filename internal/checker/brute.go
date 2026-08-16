package checker

import (
	"errors"
	"fmt"

	"github.com/Leo-Y-Zhang/Splitbrain/internal/history"
	"github.com/Leo-Y-Zhang/Splitbrain/internal/model"
)

// BruteForce is the reference implementation: correct by inspection,
// exponential, and only usable for small histories. It exists so the fast
// checker can be differentially tested against something nobody has to trust.
//
// It returns an error, and never Linearizable, when the history is larger than
// maxOps. Operations the client proved never happened are dropped; operations
// the client never got an answer for are not, and impose no output constraint.
func BruteForce(ops history.History, m model.Model, maxOps int) (Verdict, error) {
	if m == nil {
		return Unknown, errors.New("checker: no model given")
	}
	if len(ops) > maxOps {
		return Unknown, fmt.Errorf("checker: brute force refuses %d operations, the limit is %d", len(ops), maxOps)
	}

	live := ops.DropFailed()
	placed := make([]bool, len(live))
	if bruteSearch(live, m, m.Init(), placed, len(live)) {
		return Linearizable, nil
	}
	return NotLinearizable, nil
}

// bruteSearch tries every operation that could legally come next, in every
// order, with no cache and no cleverness.
func bruteSearch(ops history.History, m model.Model, s model.State, placed []bool, remaining int) bool {
	if remaining == 0 {
		return true
	}
	for i := range ops {
		if placed[i] || !bruteMinimal(ops, placed, i) {
			continue
		}
		next, ok := m.Step(s, ops[i])
		if !ok {
			continue
		}
		placed[i] = true
		found := bruteSearch(ops, m, next, placed, remaining-1)
		placed[i] = false
		if found {
			return true
		}
	}
	return false
}

// bruteMinimal reports whether ops[i] can legally be next: whether no operation
// still to be placed finished before ops[i] started.
//
// This is the "earliest completion time" test written out longhand. The usual
// shortcut - compare against the minimum completion time over everything
// remaining, including ops[i] itself - gets an operation whose invocation and
// completion coincide wrong, and refuses to place it at all.
func bruteMinimal(ops history.History, placed []bool, i int) bool {
	for j := range ops {
		if j == i || placed[j] {
			continue
		}
		if ops[j].Complete <= ops[i].Invoke {
			return false
		}
	}
	return true
}
