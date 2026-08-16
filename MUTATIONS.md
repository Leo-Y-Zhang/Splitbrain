# Mutation evidence

Every rule below was switched off, the tests were run, and a named test died.
A rule with no test to defend it is a rule the suite does not actually check,
so a surviving mutation fails `tools/mutations.py` and therefore fails CI.

This file is generated. Run `python3 tools/mutations.py` to regenerate it;
CI regenerates it on a machine that is not the author's and fails if the
result differs from what is committed.

| id | file | rule the test defends | killed by |
| --- | --- | --- | --- |
| H01 | `internal/history/history.go` | Indeterminate operations are kept. Dropping them lets a checker pass a history in which a timed-out write really did take effect. | `TestDropFailedKeepsInfo` |
| H02 | `internal/history/history.go` | Operations that merely abut are not concurrent. Loosening this to <= invents a real-time ordering freedom the run never had. | `TestConcurrentUsesStrictInequality` |
| H03 | `internal/history/history.go` | One client cannot have two requests in flight. Without this check a harness bug silently removes real-time constraints and the checker becomes a rubber stamp. | `TestValidateRejects` |
| H04 | `internal/history/history.go` | A process retires after an indeterminate operation. Letting it continue asserts an ordering the client had no way to know. | `TestValidateExplainsWhyAProcessMustRetire` |
| H05 | `internal/history/history.go` | Two zero-width operations on one key at one instant are refused. Each completes at or before the other is invoked, so no order respects real time - and the entry list cannot express that, so the search picks one and reports a pass. | `TestAdversarialDifferentialAgainstBruteForce` |
| M01 | `internal/model/model.go` | A successful read must return the value the register holds. | `TestCASRegisterRead` |
| M02 | `internal/model/model.go` | A compare-and-swap that reports success is a claim about the exact value the register held. It is the operation that catches split brain. | `TestCASRegisterCatchesLyingCAS` |
| M03 | `internal/model/model.go` | An indeterminate operation reports nothing, so nothing it appears to have returned may be believed. | `TestIndeterminateOperationsConstrainNothingButStillApply` |
| M04 | `internal/model/model.go` | A model must refuse operations it does not describe rather than quietly accept them. | `TestRegisterRefusesCAS` |
| V01 | `internal/harness/harness.go` | A zero field means "choose for me"; a negative one means the caller asked for something impossible. Widening this back to <= silently replaces -5s with the default while the banner still echoes -5s. | `TestWithDefaultsLeavesAnImpossibleValueAlone` |
| V02 | `internal/harness/harness.go` | An explicitly requested zero pause is honoured, not defaulted. Without the marker there is no way to say "I mean zero", because a zero pause and an unspecified one are the same bits. | `TestAnExplicitZeroPauseSurvivesDefaults` |
| K01 | `internal/clock/clock.go` | A completion is widened by one clock tick. Without it the history claims real-time orderings the clock was too coarse to observe, and a single-node store gets accused of a violation it cannot commit. | `TestCompletionIsAlwaysAfterInvocation` |
| C01 | `internal/checker/checker.go` | An exhausted search is never a pass. Collapsing the budget verdict into Linearizable turns a default-deny tool into a rubber stamp. | `TestBudgetExhaustionIsUnknownAndNeverLinearizable` |
| C02 | `internal/checker/checker.go` | A return sorts before a call at the same instant, matching the strict inequality in Op.Concurrent. Reversing it invents an ordering freedom real time did not allow. | `TestAdversarialDifferentialAgainstBruteForce` |
| C03 | `internal/checker/checker.go` | A cached configuration is the placed set AND the model state. Keying on the placed set alone abandons orderings the search never explored, and the verdict stops being exhaustive. | `TestAdversarialDifferentialAgainstBruteForce` |
| C04 | `internal/checker/checker.go` | The call is spliced out before its return. When the two are adjacent the return's prev is the call, so the other order clobbers the pointer unlift needs and leaves the list plausible but wrong. | `TestAdversarialDifferentialAgainstBruteForce` |
| C08 | `internal/checker/checker.go` | The search stops when it has remembered too much. The visit budget counts transitions, not bytes, so without this a large history reaches it having already taken gigabytes - measured at 4.96 GB from a twelve-megabyte file. | `TestCacheBudgetYieldsUnknownRatherThanMemory` |
| C05 | `internal/checker/checker.go` | An operation still in flight at the truncation point had told the client nothing yet, so its result must be discarded. Keeping it makes the counterexample claim knowledge nobody had. | `TestMinimiseReturnsAPrefixWhenTheViolationIsLast` |
| C06 | `internal/checker/checker.go` | Minimisation returns the earliest failing truncation, not the whole history. Returning everything is not wrong, only useless, and nothing would have noticed. | `TestMinimiseShrinksToTheViolation` |
| C07 | `internal/checker/brute.go` | The reference oracle enforces real time with the same strictness as the history's own concurrency test. A weaker oracle would agree with a broken checker. | `TestAdversarialDifferentialAgainstBruteForce` |

