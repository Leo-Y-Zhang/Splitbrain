#!/usr/bin/env python3
"""Break the code on purpose and check that a test notices.

A green test suite says nothing about which rules are actually pinned. This
script takes each rule that matters, edits the source so the rule is wrong,
runs the tests, and records which test died. A mutation that survives is a
hole in the suite and fails this script, which is the point: the evidence is
the list of deaths, not the list of tests.

Two rules of the harness itself, both learned the hard way:

  * the file is restored from a copy held in memory, never with git. A restore
    that goes through the working tree will happily revert the very change
    under test if the timing is unlucky, and then every mutation "dies" for
    the wrong reason;
  * the anchor text is asserted to appear exactly once before anything is
    replaced. A replacement that silently matches nothing produces a mutant
    identical to the original, which of course "survives", and the report
    then blames the tests for a bug in the report.

Usage:
    python3 tools/mutations.py            # run every mutation, rewrite MUTATIONS.md
    python3 tools/mutations.py --list     # just list them
    python3 tools/mutations.py --only ID  # run one, for debugging
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / "MUTATIONS.md"


@dataclasses.dataclass(frozen=True)
class Mutation:
    """One deliberate defect."""

    ident: str
    path: str
    old: str
    new: str
    rule: str
    packages: tuple[str, ...]

    @property
    def file(self) -> Path:
        return ROOT / self.path


# The mutations. Each one turns a real rule off. The `rule` field is what the
# reader should learn from the row, so it is written as the claim the test
# defends rather than as a description of the edit.
MUTATIONS: list[Mutation] = [
    Mutation(
        ident="H01",
        path="internal/history/history.go",
        old="if op.Outcome != Fail {",
        new="if op.Outcome != Fail && op.Outcome != Info {",
        rule="Indeterminate operations are kept. Dropping them lets a checker pass a history in which a timed-out write really did take effect.",
        packages=("./internal/history/...", "./internal/checker/..."),
    ),
    Mutation(
        ident="H02",
        path="internal/history/history.go",
        old="return o.Invoke < p.Complete && p.Invoke < o.Complete",
        new="return o.Invoke <= p.Complete && p.Invoke <= o.Complete",
        rule="Operations that merely abut are not concurrent. Loosening this to <= invents a real-time ordering freedom the run never had.",
        packages=("./internal/history/...",),
    ),
    Mutation(
        ident="H03",
        path="internal/history/history.go",
        old="if ops[i].Invoke < prev.Complete {",
        new="if false {",
        rule="One client cannot have two requests in flight. Without this check a harness bug silently removes real-time constraints and the checker becomes a rubber stamp.",
        packages=("./internal/history/...",),
    ),
    Mutation(
        ident="H04",
        path="internal/history/history.go",
        old="if prev.Outcome == Info {",
        new="if false {",
        rule="A process retires after an indeterminate operation. Letting it continue asserts an ordering the client had no way to know.",
        packages=("./internal/history/...",),
    ),
    Mutation(
        ident="M01",
        path="internal/model/model.go",
        old="if known && op.Observed != v {",
        new="if false {",
        rule="A successful read must return the value the register holds.",
        packages=("./internal/model/...", "./internal/checker/..."),
    ),
    Mutation(
        ident="M02",
        path="internal/model/model.go",
        old="if known && op.Swapped != swapped {",
        new="if false {",
        rule="A compare-and-swap that reports success is a claim about the exact value the register held. It is the operation that catches split brain.",
        packages=("./internal/model/...", "./internal/checker/..."),
    ),
    Mutation(
        ident="M03",
        path="internal/model/model.go",
        old="known := op.Outcome == history.OK",
        new="known := true",
        rule="An indeterminate operation reports nothing, so nothing it appears to have returned may be believed.",
        packages=("./internal/model/...", "./internal/checker/..."),
    ),
    Mutation(
        ident="M04",
        path="internal/model/model.go",
        old="\tif op.Kind == history.CAS {\n\t\treturn s, false\n\t}\n\treturn CASRegister{}.Step(s, op)",
        new="\treturn CASRegister{}.Step(s, op)",
        rule="A model must refuse operations it does not describe rather than quietly accept them.",
        packages=("./internal/model/...",),
    ),
    Mutation(
        ident="K01",
        path="internal/clock/clock.go",
        old="	return measured + c.granularity",
        new="	return measured",
        rule="A completion is widened by one clock tick. Without it the history claims real-time orderings the clock was too coarse to observe, and a single-node store gets accused of a violation it cannot commit.",
        packages=("./internal/clock/...",),
    ),
    Mutation(
        ident="C01",
        path="internal/checker/checker.go",
        old='\t\t\tkr.verdict = Unknown\n\t\t\tkr.reason = fmt.Sprintf("undecided after %d model transitions, the visit budget", visits)',
        new='\t\t\tkr.verdict = Linearizable\n\t\t\tkr.reason = fmt.Sprintf("undecided after %d model transitions, the visit budget", visits)',
        rule="An exhausted search is never a pass. Collapsing the budget verdict into Linearizable turns a default-deny tool into a rubber stamp.",
        packages=("./internal/checker/...",),
    ),
    Mutation(
        ident="C02",
        path="internal/checker/checker.go",
        old="\t\treturn op.Complete, 0",
        new="\t\treturn op.Complete, 3",
        rule="A return sorts before a call at the same instant, matching the strict inequality in Op.Concurrent. Reversing it invents an ordering freedom real time did not allow.",
        packages=("./internal/checker/...",),
    ),
    Mutation(
        ident="C03",
        path="internal/checker/checker.go",
        # Dropping only the `e.state == s` comparison is not enough to see: the
        # hash still mixes the state in, so two configurations differing only in
        # state land in different buckets and the comparison almost never runs.
        # The rule is that a configuration IS the pair, so the mutant has to key
        # on the placed set alone, buckets and all.
        old=(
            "\tfor _, e := range c.buckets[c.hash(lin, s)] {\n"
            "\t\tif e.state == s && e.lin.equal(lin) {\n"
            "\t\t\treturn true\n"
            "\t\t}\n"
            "\t}\n"
        ),
        new=(
            "\tfor _, entries := range c.buckets {\n"
            "\t\tfor _, e := range entries {\n"
            "\t\t\tif e.lin.equal(lin) {\n"
            "\t\t\t\treturn true\n"
            "\t\t\t}\n"
            "\t\t}\n"
            "\t}\n"
        ),
        rule="A cached configuration is the placed set AND the model state. Keying on the placed set alone abandons orderings the search never explored, and the verdict stops being exhaustive.",
        packages=("./internal/checker/...",),
    ),
    Mutation(
        ident="C04",
        path="internal/checker/checker.go",
        old=(
            "\te.prev.next = e.next\n"
            "\te.next.prev = e.prev // a call always has its own return after it\n"
            "\n"
            "\tm := e.match\n"
            "\tm.prev.next = m.next\n"
            "\tif m.next != nil {\n"
            "\t\tm.next.prev = m.prev\n"
            "\t}\n"
        ),
        new=(
            "\tm := e.match\n"
            "\tm.prev.next = m.next\n"
            "\tif m.next != nil {\n"
            "\t\tm.next.prev = m.prev\n"
            "\t}\n"
            "\n"
            "\te.prev.next = e.next\n"
            "\te.next.prev = e.prev // a call always has its own return after it\n"
        ),
        rule="The call is spliced out before its return. When the two are adjacent the return's prev is the call, so the other order clobbers the pointer unlift needs and leaves the list plausible but wrong.",
        packages=("./internal/checker/...",),
    ),
    Mutation(
        ident="C05",
        path="internal/checker/checker.go",
        old="\t\tif op.Complete >= t {\n\t\t\top.Outcome = history.Info",
        new="\t\tif false {\n\t\t\top.Outcome = history.Info",
        rule="An operation still in flight at the truncation point had told the client nothing yet, so its result must be discarded. Keeping it makes the counterexample claim knowledge nobody had.",
        packages=("./internal/checker/...",),
    ),
    Mutation(
        ident="C06",
        path="internal/checker/checker.go",
        old="\treturn sortedTruncation(ops, times[lo]), true",
        new="\treturn ops, true",
        rule="Minimisation returns the earliest failing truncation, not the whole history. Returning everything is not wrong, only useless, and nothing would have noticed.",
        packages=("./internal/checker/...",),
    ),
    Mutation(
        ident="C07",
        path="internal/checker/brute.go",
        old="\t\tif ops[j].Complete <= ops[i].Invoke {",
        new="\t\tif ops[j].Complete < ops[i].Invoke {",
        rule="The reference oracle enforces real time with the same strictness as the history's own concurrency test. A weaker oracle would agree with a broken checker.",
        packages=("./internal/checker/...",),
    ),
]


def run_tests(packages: tuple[str, ...]) -> tuple[bool, str]:
    """Run go test over the given packages. Returns (passed, output)."""
    cmd = ["go", "test", "-count=1", *packages]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return proc.returncode == 0, proc.stdout + proc.stderr


FAIL_LINE = re.compile(r"^--- FAIL: (\S+)", re.MULTILINE)


def failing_tests(output: str) -> list[str]:
    """The named tests that failed, in order, deduplicated."""
    seen: list[str] = []
    for name in FAIL_LINE.findall(output):
        if name not in seen:
            seen.append(name)
    return seen


def read_source(path: Path) -> str:
    """Read a source file without translating its line endings.

    Python's default text mode converts CRLF to LF on the way in and back on
    the way out, so on Windows a restore would rewrite every line of the file
    it was supposed to leave alone. gofmt then reports the whole tree as
    unformatted, and the mutation harness has quietly become a source of diffs.
    """
    return path.read_text(encoding="utf-8", newline="")


def write_source(path: Path, text: str) -> None:
    """Write a source file back exactly as given. See read_source."""
    path.write_text(text, encoding="utf-8", newline="")


def apply_mutation(m: Mutation) -> str:
    """Write the mutant and return the original text, held in memory."""
    original = read_source(m.file)
    count = original.count(m.old)
    if count != 1:
        raise SystemExit(
            f"{m.ident}: anchor appears {count} times in {m.path}, expected exactly 1.\n"
            f"  anchor: {m.old!r}\n"
            f"A replacement that matches nothing produces a mutant identical to the "
            f"original, and the report would then blame the tests."
        )
    write_source(m.file, original.replace(m.old, m.new))
    return original


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--list", action="store_true", help="list the mutations and exit")
    ap.add_argument("--only", help="run a single mutation by identifier")
    ap.add_argument("--no-write", action="store_true", help="do not rewrite MUTATIONS.md")
    args = ap.parse_args()

    if args.list:
        for m in MUTATIONS:
            print(f"{m.ident}  {m.path}  {m.rule}")
        return 0

    if shutil.which("go") is None:
        print("mutations: go is not on PATH", file=sys.stderr)
        return 2

    chosen = [m for m in MUTATIONS if args.only is None or m.ident == args.only]
    if not chosen:
        print(f"mutations: no mutation named {args.only!r}", file=sys.stderr)
        return 2

    # The suite has to be green before any of this means anything.
    print("baseline: go test ./... ", end="", flush=True)
    started = time.time()
    ok, output = run_tests(("./...",))
    print(f"({time.time() - started:.1f}s)")
    if not ok:
        print("mutations: the suite is not green to begin with, so no mutation would be "
              "evidence of anything.\n" + output, file=sys.stderr)
        return 1

    rows = []
    survivors = []
    for m in chosen:
        print(f"{m.ident}: ", end="", flush=True)
        original = apply_mutation(m)
        try:
            started = time.time()
            passed, output = run_tests(m.packages)
            elapsed = time.time() - started
        finally:
            write_source(m.file, original)

        if passed:
            print(f"SURVIVED ({elapsed:.1f}s)")
            survivors.append(m)
            rows.append((m, ["(none)"]))
            continue

        killers = failing_tests(output)
        if not killers:
            # A build failure is not a test death. If the mutant does not
            # compile, the mutation is testing the compiler.
            if "build failed" in output or "cannot use" in output or "undefined:" in output:
                print(f"DID NOT COMPILE ({elapsed:.1f}s)")
                survivors.append(m)
                rows.append((m, ["(did not compile - not evidence)"]))
                continue
            killers = ["(the package failed without a named test)"]
        print(f"killed by {killers[0]}" + (f" and {len(killers) - 1} more" if len(killers) > 1 else "")
              + f" ({elapsed:.1f}s)")
        rows.append((m, killers))

    if not args.no_write and args.only is None:
        write_evidence(rows)

    if survivors:
        print(f"\n{len(survivors)} mutation(s) survived; the suite does not pin those rules:", file=sys.stderr)
        for m in survivors:
            print(f"  {m.ident}  {m.rule}", file=sys.stderr)
        return 1

    print(f"\nall {len(rows)} mutations were killed.")
    return 0


def write_evidence(rows) -> None:
    lines = [
        "# Mutation evidence",
        "",
        "Every rule below was switched off, the tests were run, and a named test died.",
        "A rule with no test to defend it is a rule the suite does not actually check,",
        "so a surviving mutation fails `tools/mutations.py` and therefore fails CI.",
        "",
        "This file is generated. Run `python3 tools/mutations.py` to regenerate it;",
        "CI regenerates it on a machine that is not the author's and fails if the",
        "result differs from what is committed.",
        "",
        "| id | file | rule the test defends | killed by |",
        "| --- | --- | --- | --- |",
    ]
    for m, killers in rows:
        shown = killers[0]
        if len(killers) > 1:
            shown += f" (+{len(killers) - 1} more)"
        lines.append(f"| {m.ident} | `{m.path}` | {m.rule} | `{shown}` |")
    lines.append("")
    EVIDENCE.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {EVIDENCE.relative_to(ROOT)}")


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
