# Splitbrain

[![CI](https://github.com/Leo-Y-Zhang/Splitbrain/actions/workflows/ci.yml/badge.svg)](https://github.com/Leo-Y-Zhang/Splitbrain/actions/workflows/ci.yml)
[![Go 1.24+](https://img.shields.io/badge/go-1.24%2B-00ADD8.svg)](https://go.dev/dl/)

A distributed-systems test harness: it drives a real key-value cluster through
seeded network partitions over real TCP, records what the clients saw, and then
**machine-checks whether what they saw was possible at all**.

The question it answers is linearizability — is there some single order of the
client operations that both obeys the store's specification and respects real
time, so that an operation which finished before another started comes first? A
store that fails it has, at some moment, told two clients things that cannot
both have been true.

The nodes are separate operating-system processes. The partitions are a
userspace TCP proxy in front of every hop, including the hops the nodes use to
reach each other. Nothing is simulated except the decision of when to break the
network, and that comes from a seed.

```
$ splitbrain run -target kvsplit -faults partition -seed 2
cluster: 3 kvsplit process(es) on 127.0.0.1:22219, 127.0.0.1:22221, 127.0.0.1:22223
network: 9 links, 61 planned fault events
run: 4012 operations in 5.946s: 4002 ok, 0 failed, 10 indeterminate; 4 keys;
     54 fault events; 1257561 bytes forwarded, 6136 dropped;
     clock QueryPerformanceCounter, 100ns granularity
  k0       964 ops  not linearizable       1400 states  1ms
  k1       985 ops  not linearizable       1112 states  0s
  k2      1047 ops  not linearizable       1876 states  1ms
  k3      1016 ops  not linearizable        544 states  0s
verdict: NOT LINEARIZABLE (4932 states in 5ms)
  key "k0": read returned 933616, but the register held 43765 at that point
```

The same store on a healthy network, same workload, same checker:
`LINEARIZABLE`. That comparison is the point, and it is asserted on every push.

## The result

Five seeds per store, six seconds each, six concurrent clients over four
registers, on a three-node cluster: 12,520 to 12,560 operations per store. These
are the numbers from `tools/demo`, which CI runs on Linux on every push, and
they match what the same command produces on the Windows machine this was
written on.

CI *asserts* the first three rows. The last two are measured rather than
asserted: how often an already-broken store gets caught without any help
depends on the machine, and pretending otherwise would be the kind of claim
this repository exists to disallow.

| store | what it is | under partition | healthy network |
| --- | --- | ---: | ---: |
| `kvsingle` | one node, one mutex | **5/5 linearizable** | — |
| `kvforward` | three nodes, all forwarding to a fixed leader | **5/5 linearizable** | — |
| `kvsplit` | the same, but a follower that loses the leader promotes itself | **0/5** — caught every time | **5/5 linearizable** |
| `kvsync` | replicates synchronously, replies `ok` even when a replica was unreachable | **0/5** — caught | 0/5 — caught anyway |
| `kvquorum` | local reads, asynchronous best-effort replication | **0/5** — caught | 0/5 — caught anyway |

Three things worth reading off that table.

**The two correct stores are never accused.** They ran under the same
partitions that catch the others, and their histories contain operations that
timed out with no answer — the hardest case a checker has to handle. A single
mutex cannot be non-linearizable, so any violation reported against `kvsingle`
is a defect in this tool. The demonstration fails on one.

**`kvsplit` is the controlled experiment.** Clean on a healthy network, caught
on every seed under partition. Same binary, same workload, same checker; the
only difference is whether the network was broken. That is what makes the fault
injection load-bearing rather than decorative.

**The other two are wrong with or without a network to blame**, and saying so is
more useful than implying the partitions found them. `kvquorum` is caught within
the first forty operations of a healthy run. `kvsync` — which waits for every
replica before replying, and then replies `ok` regardless — is caught too,
because nothing coordinates its compare-and-swaps and last-writer-wins on a
coarse clock can let an earlier write beat a later one that had already
returned. Synchronous replication is not consensus.

## Why this exists

Distributed stores fail in a specific and awkward way: they keep answering. A
partitioned replica does not throw an error, it returns a stale value, and the
bug is not in any one response but in the *set* of responses across clients and
across time. You cannot assert your way to it. You have to record everything and
then decide, after the fact, whether the whole recording was possible.

Deciding that is the interesting part. Linearizability checking is NP-complete
in general, and the naive approach — try every ordering — dies at about a dozen
operations. This implements the search that makes it practical, and then spends
most of its effort on the question that matters more than speed: **how do you
know the checker is right?**

## How the network is broken

`internal/faultnet` is a TCP proxy that operates on the raw byte stream, so it
works against any protocol without understanding it. Each link can be told, at
any instant — including while a request is in flight — to:

| fault | what the peer sees | why it matters |
| --- | --- | --- |
| `pass` | nothing | the control |
| `delay` | added latency | widens the window for reordering |
| `drop` | a hang, then a timeout | the socket stays open, so the client learns *nothing*: this is what produces indeterminate operations |
| `reset` | a broken connection | mid-flight failure |
| `refuse` | connection refused | the one case where the client can prove its request was never delivered |

A **partition** is expressed at the level of nodes, not links: isolating a node
cuts every peer link that crosses the boundary, while links inside each side
stay up, and **client links stay up on both sides**.

That last clause is the one worth stating, because getting it wrong is easy and
quiet. Cut the isolated side's clients too and nobody is left asking those nodes
anything; they cannot diverge in a way anyone observes, and the run reports a
clean history for a store that is plainly broken. A real deployment has clients
on both sides of a break. So does this. Client links are only cut occasionally
and deliberately, to produce operations with no answer.

## What the client is allowed to know

The whole tool rests on a three-way distinction that most hand-rolled harnesses
collapse into two:

- **ok** — a well-formed response came back. The operation took effect at some
  single instant inside its interval.
- **failed** — the operation definitely did not take effect. Recorded only when
  the client can *prove* the request was never delivered: a refused connection,
  or a server that reached a decision and declined.
- **indeterminate** — a timeout, a reset, an EOF, a truncated reply, a non-200
  status. The request **may** have been applied, and a later read may
  legitimately observe it.

Drop the indeterminate operations and the checker becomes unsound in the
generous direction: it will pass a history in which a timed-out write really did
land. Treat them as successful and it becomes unsound the other way. They are
kept, and the search may place them anywhere — including after everything else,
which is the same as saying they never happened.

The same rule applies one hop further in. When a follower forwards an operation
to a leader and the attempt times out, the leader may already have applied it,
so the follower must not reply "failed". It returns something the client records
as indeterminate. Getting this wrong makes a *correct* store look broken, and
the report then blames the checker.

A process that suffers an indeterminate operation retires: it cannot issue
another, because a client with a request that may still be in flight has no
basis for claiming its next operation came afterwards. The history validator
enforces that, along with a completion never preceding its own invocation and
one client never having two requests outstanding. Each of those, quietly
violated, removes a real-time constraint and turns the checker into a rubber
stamp.

## The clock is part of the apparatus

This looked like over-engineering until it was measured. Go's monotonic clock on
Windows advances in steps of **523 microseconds** — two million back-to-back
readings, all but eight identical — and a loopback HTTP round trip is shorter
than that. So most operations recorded as taking no time at all, and pairs that
genuinely overlapped were written down as though one had finished before the
other began.

That is not a rounding error, it is a fabricated fact, and it is exactly the
fact a linearizability checker is built out of. The first full run of the
demonstration duly reported `kvsingle` — one node, one mutex — as NOT
LINEARIZABLE.

Two things fix it, and both are in `internal/clock`. The counter is read at the
finest resolution the platform offers, which on Windows means
`QueryPerformanceCounter`: **100 nanoseconds measured, a five-thousandfold
improvement**. And whatever granularity remains is measured rather than assumed,
with every completion widened by one tick — so the history claims A finished
before B started only when the latest instant A could have finished is still no
later than the earliest instant B could have started. The residual error then
always points at "concurrent", which can lose a violation but can never invent
one.

## Running it

```
go run ./cmd/splitbrain run   -target kvsplit -faults partition -seed 3 \
                              -out history.jsonl -report report.html
go run ./cmd/splitbrain check testdata/split-brain.jsonl
go run ./cmd/splitbrain sweep -target kvforward -seeds 0-19 -expect linearizable
go run ./cmd/splitbrain schedule -nodes 3 -faults partition -seed 3
go run ./tools/demo           # the whole table above, from scratch
```

`run` starts the cluster itself, builds the proxies, wires the nodes to each
other *through* those proxies, drives the load, and checks the history. Exit
status is the verdict against `-expect`, so it works as a gate and not only as a
report: `0` when the verdict matched, `1` when it did not, `2` when no verdict
could be reached.

`-report` writes one self-contained HTML page — no scripts fetched from
anywhere, no external assets, opens offline — with the operations drawn on a
time axis, the network cuts shaded behind them, and the operation the search
could not place outlined in red. A counterexample printed as text is correct and
almost unreadable; the reason a history is impossible lives in the way intervals
overlap.

Two real histories are committed under `testdata/`, so the verdicts can be
reproduced without running anything:

```
$ splitbrain check testdata/split-brain.jsonl
splitbrain check: split-brain.jsonl, 795 operations across 1 key
                  (786 ok, 0 failed, 9 indeterminate), model=cas-register
verdict: NOT LINEARIZABLE (231806 states in 21ms)
  key "k0": read returned 258314, but the register held 690537 at that point
```

## Verdicts

Three, not two:

- **linearizable** — a valid order exists, and the search found it.
- **not linearizable** — the search was exhaustive and there is none.
- **unknown** — the search ran out of budget.

`unknown` is never collapsed into a pass, all the way out to the exit code: an
undecided search exits **2**, whatever `-expect` says, because "any" means either
verdict and an exhausted search is the absence of one. (It did not, at first.
`check` defaults to `-expect any`, and `any` absorbed `unknown` into a zero exit
— the claim was true in the text of the verdict and false in the number a
pipeline reads.)

There are three budgets, and all three end in `unknown` rather than a guess:
`-max-visits` on model transitions, `-timeout` on the clock, and
`-max-cache-mb` on what the search may remember. The last one exists because the
first does not bound memory: transitions are counted, but each one can add a
cache entry as wide as the key has operations, so a twelve-megabyte history file
took **4.96 GB** of resident memory in 8.4 seconds under the old defaults and
then reported `unknown` anyway. A history file is the artefact people pass
around, so it is the input that has to be survivable.

## The checker

Wing and Gong's search (1993) with Lowe's optimisations (*Testing for
linearizability*, CCPE 2017): a doubly-linked list of call and return entries,
depth-first placement of whichever operation fits the model next, O(1) removal
and restoration, and a cache of (operations placed, model state) pairs already
explored. Histories are split by key first — linearizability is compositional
over independent objects, so that is exact rather than an approximation, and it
is what keeps an exponential search tractable.

When a violation is found, the counterexample is the earliest moment it became
unavoidable, found by binary search over a **truncation in time**: keep every
operation invoked before some instant, and downgrade to indeterminate anything
still in flight at it. Deleting operations to shrink a counterexample is not
sound — removing a write can turn a linearizable history into an unlinearizable
one — and truncation is what avoids that. It also means the counterexample is
itself a valid history, so it can be re-checked on its own. That is tested, not
asserted.

## How the checker is checked

The search is easy to get subtly wrong and hard to notice, so it is pinned four
ways.

**A reference implementation.** `BruteForce` tries every ordering with no cache
and no cleverness — correct by inspection, useless above a handful of
operations, which is exactly what an oracle should be. **20,000 randomly
generated small histories** go through both against two models, and they agree
on every one. The generator is asserted to produce both verdicts in quantity
(**11,773 linearizable, 8,227 not**), because a differential test where
everything passes proves nothing.

**A corpus with known verdicts**, including the cases that look wrong and are
fine — a stale read *concurrent* with the write that supersedes it is legal —
and the cases that look fine and are wrong, which is the same read a
microsecond later.

**Mutation evidence.** Every rule that matters is switched off in turn, the
tests are run, and a named test has to die. **18 mutations, all killed**; see
[MUTATIONS.md](MUTATIONS.md), which CI regenerates on a machine that is not mine
and compares against what is committed. Two survived the first run and both were
real holes: one rule had no test at all, and one *mutation* was too weak to
observe, which is its own kind of false green.

**The two correct stores**, under the same partitions, on every push.

And then someone tried to break all of it. An adversarial generator — time
squeezed onto a handful of instants, zero-width intervals, a two-value domain,
mostly one process, mostly indeterminate — found **one** class the search and the
oracle disagree on, in the generous direction: two operations on one key whose
invocation and completion are the same instant each complete at or before the
other is invoked, so no order respects real time, and the entry list picked one
anyway. `Validate` now refuses that shape. With it removed, a further 100,000
histories across two models produced no other disagreement, which is a much
better result than the original generator could establish — it drew durations of
one to six nanoseconds and could not reach the shape at all.

## Limits, stated plainly

- **The fault schedule replays from its seed. The run does not.** The system
  under test has its own clocks, goroutines and timeouts, so re-running a seed
  reproduces the network, not the execution. That is a weaker guarantee than a
  deterministic simulator gives, and it is the price of testing real processes
  over real sockets. If a run finds a violation, the history is the artifact,
  not the seed — which is why `run` can write both the history and the minimal
  counterexample to disk.
- **The model is a register.** Read, write and compare-and-swap over an integer.
  Enough to catch what this class of bug does, and not a general-purpose
  specification language.
- **The stores are fixtures.** They exist to be tested, not deployed. The broken
  ones are deliberately broken, and each is a design people actually write.
- **A clean verdict is evidence, not proof.** It says no violation appeared in
  the operations that were run under the faults that were injected. Sweeping
  seeds raises the confidence; it does not change the kind of claim.
- **"Never accused" is a claim about the parameter space that was run.** The
  demonstration's five seeds are one region of it. Under `-faults chaos` with
  sixteen clients over eight keys, `kvsingle` comes back `unknown` on most
  seeds — the checker failing closed rather than deciding, which is correct, but
  it is not the same evidence as `5/5 linearizable`. Forty-five configurations
  were attacked looking for a false accusation and none was found; that is a
  stronger statement than any single table.
- **There must be at least one client per node.** Clients are spread
  round-robin, so fewer leaves a node nobody talks to; a partition then cuts
  hops that were carrying nothing and the clean verdict means nothing. The
  harness refuses the configuration rather than producing that evidence — it was
  producing it, and a reviewer caught it.
- **A node bound off loopback is reachable and unauthenticated.** `-addr` will
  take anything, and `POST /configure` will point a node's leader at any URL. The
  stores are fixtures and are meant to stay open; the binaries warn on stderr
  when they are not on loopback.
- **The race detector only runs in CI**, on Linux. It needs cgo, and the machine
  this was written on has no C toolchain.

## Layout

```
internal/history    what an operation is, and what is known about it
internal/model      sequential specifications (register, cas-register)
internal/checker    the linearizability search, plus the brute-force oracle
internal/clock      the timestamps, and how far they can be trusted
internal/faultnet   the TCP proxy, the fault vocabulary and the schedules
internal/kv         the client, and the servers' shared plumbing
internal/cluster    starting the stores as separate processes
internal/harness    topologies, partition schedules, load generation
internal/campaign   one complete experiment, shared by the CLI and the demo
internal/report     the self-contained HTML page
cmd/splitbrain      the command-line tool
cmd/kv{single,forward,quorum}   the stores
tools/demo          the table above, executable; CI runs it on every push
tools/mutations.py  the mutation evidence
```

7,500 lines of Go and 7,600 lines of tests — 233 test functions, 372 cases
including subtests. **No dependencies**: standard library only, and CI fails if
`go.sum` ever stops being empty.

## Licence

MIT. Copyright (c) 2026 Leo Y. Zhang.
