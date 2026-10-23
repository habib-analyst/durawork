# durawork: a small durable workflow engine with a boundary-exhaustive crash proof

## Position

Durable-execution platforms (Temporal, Cadence, Restate, DBOS, Inngest,
Conductor) are large systems whose crash semantics are, in practice, trusted
from documentation and reputation. durawork explores the opposite trade: a
durable workflow engine small enough to audit completely (~880 lines of
engine core; ~2,050 lines total including the demo, CLI tools, baseline, and
the test harness), whose central claim is not asserted but *measured* — a
harness kills the worker with SIGKILL at every step boundary of a non-trivial
workflow and an independent oracle verifies exactly-once effects and final
state. The full prior-art analysis is in `KILLGATE.md`.

## Design

### History format

One append-only JSONL file per workflow. Event types:

`WorkflowStarted`, `ActivityScheduled`, `ActivityAttemptStarted`,
`ActivityAttemptFailed`, `ActivityCompleted`, `ActivityFailed`,
`TimerStarted`, `TimerFired`, `SignalWaitStarted`, `SignalClaimed`,
`SignalTimedOut`, `WorkflowCompleted`, `WorkflowFailed`, `WorkflowParked`.

Each event carries a sequence number, the index of the command it belongs
to, and the command's payload (activity input/result, timer duration and
fire time, signal payload, idempotency key). Appends are a single line write
followed by fsync (and a directory fsync on file creation). A reader
tolerates exactly one torn final line — the only corruption a kill -9 can
produce in this scheme — and treats corruption anywhere else as an error.

### Replay model

Workflow code is a straight-line Go function over three primitives. The
executor loads history, derives the recorded command sequence, and re-runs
the function. Each primitive call consumes the next command index:

- If the command is recorded **complete**, its recorded result is returned;
  nothing re-executes.
- If it is recorded **incomplete** (scheduled but not completed, timer
  started but not fired, wait started but unresolved), it is resumed: an
  activity re-executes *with the same idempotency key*, a timer fires if
  overdue, a signal wait claims from the durable inbox or keeps waiting.
- If it is **new**, it executes live and its events are appended.

Blocking unwinds the pass via a recovered panic (the same technique
Temporal's Go SDK uses internally), keeping workflow code straight-line. The
driver loop then waits — in-process for timers and signal deadlines,
polling the signal inbox so approvals resume promptly — or returns a
`waiting` status to the caller.

### The determinism contract

Replay must issue the same sequence of commands — kind, name, and (for
timers/signals) duration — as the history records. A divergence raises a
typed `NonDeterminismError`; the engine appends `WorkflowParked` and stops.
Parking rather than continuing is the only safe response: every recorded
result after the divergence point belongs to a different command than the
code now thinks it does. A workflow that *returns early* while history has
commands outstanding is treated as the same class of divergence.

### Exactly-once, stated precisely

The engine guarantees exactly-once for **effects performed through the
effects journal under the command's idempotency key**. The key
(`<workflow>/<command-index>/<activity>`) is stable across crashes,
retries, and replays because the command index is. An activity re-executed
after a crash presents the same key; the journal returns the original record
instead of applying the effect twice. The engine cannot extend this
guarantee to arbitrary external calls an activity might make outside the
journal — that is a property of the downstream system, not of any workflow
engine, and this paper does not pretend otherwise.

## Method

**Demo workflow.** "3-day onboarding": create account → send welcome email →
wait for an approval signal raced against a durable timeout → provision
workspace → sleep (day 2) → send check-in → sleep (day 3) → send check-in →
complete. Eight commands, five of which produce externally visible effects.
Durations are parameters; the harness compresses a "day" to 300 ms and the
approval timeout to 6 s.

**Boundaries.** Instrumented hook points at every step boundary:
before/inside/after each activity (inside = after the activity function
returns — effect journaled — but before the completion event is appended),
before/after each signal wait resolves, before/after each timer fires, at
workflow start, and before the completion event: **23 boundaries**.

**Procedure per boundary.** Fresh data directory → start the real runner
binary with `-kill-at <boundary>` → the process blocks at the boundary → the
harness sends a real `SIGKILL` → restart the binary without the flag → drive
to completion (delivering the approval signal when the workflow waits for
it) → the oracle judges.

**Oracle.** Independent code in the harness (no shared code with the
engine). It reads only the artefacts: the history file must contain
`WorkflowCompleted` with the expected output (`status=approved`,
`account_id=acct-u1`), and the effects journal must contain each of the five
expected effect keys **exactly once** and nothing else.

**Baseline.** The identical procedure against `durawork-naive`: the same
flow as a plain script — in-memory state, a checkpoint file rewritten
(non-atomically, no fsync) after each step, no idempotency keys, timers as
in-process sleeps, approval by polling a file. The naive binary exposes the
same 23 boundaries plus 8 more inside its checkpoint writes (31 total), and
the oracle judges it by kind/template counts.

**Determinism scenario.** Start the versioned workflow under code V1
(`step_a → step_b`), kill after both are recorded, restart under code V2
(`step_a → step_inserted → step_b` — a step inserted at command 1, the
classic in-flight deploy).

## Results

All numbers from `results.json`, produced by the harness run of 2026-10-23
on this machine (2 cores, Go 1.23.4); regenerate with
`go run ./cmd/durawork-harness -root .`.

| | boundaries | passed | failed boundaries |
|---|---|---|---|
| durawork | 23 | **23** | — |
| naive baseline | 31 | 18 | all 5 mid-activity; all 8 checkpoint-write |

**Mid-activity kills** (effect journaled, completion record lost — the
hardest point): durawork **5/5**. On recovery each activity re-executed, the
journal suppressed the duplicate by idempotency key, and the oracle found
every effect exactly once. Naive **0/5**: each produced a duplicate effect
(e.g. the welcome email sent twice), because nothing connects the retried
step to its previous partial execution.

**Checkpoint-write kills** (naive only): **0/8**. A kill between the two
chunks of the checkpoint rewrite leaves unparseable JSON; the baseline's
forgiving loader starts from an empty state and re-runs everything, so
*every* prior effect duplicates (final journals show 10 effects where 5
belong). durawork has no equivalent failure mode: its history appends are
single-line writes with fsync, and a torn final line is detected and
ignored by construction.

**Functional scenarios.** Approval-reject path: workflow completes with
`status=rejected` and no post-approval effects occur. Approval-timeout path:
with no signal delivered, the durable deadline wins the race and the
workflow completes with `status=approval_timed_out`.

**Determinism.** The V2 replay was parked at the first divergent command
with the recorded reason:

```
non-determinism at command 1: history recorded activity "step_b",
replay issued activity "step_inserted"
```

The runner exited with the parked status (exit code 4), and the effects
journal still contained exactly the two V1 effects — the inserted step never
executed, and no result was misaligned.

**Unit tests.** 13 tests, all passing: replay returns recorded results
without re-executing; retry/backoff accounting (attempts recorded, success
on attempt 3; exhaustion fails the workflow deterministically); timer
survival across a restart (overdue timers fire on recovery); signal delivery
and timeout; non-determinism detection (command substitution and early
return); effects-journal dedup; torn-final-line tolerance.

## Honest limits

- **Single node.** One worker process per workflow; the store is local
  files. Nothing here speaks to distribution, failover, or consensus.
- **The exactly-once boundary is the journal.** Effects an activity performs
  outside the idempotency-key contract are at-least-once, as with every
  engine in this class.
- **The oracle verifies this demo's effects**, not arbitrary user effects;
  the harness generalises by enumeration of boundaries, not by fuzzing
  workflow shapes. Randomised kill timing (as opposed to boundary-exact
  kills) is future work.
- **History grows unboundedly.** No compaction, snapshotting, or
  continue-as-new.
- **Wall-clock timers** on a single node's clock; no clock-skew story.
- The compressed clock (300 ms "days") exercises ordering and durability,
  not long-duration behaviour such as inbox growth over real days.

## Reproduce

```sh
go test ./...                            # 13 tests
go run ./cmd/durawork-harness -root .    # rewrites results.json; ~1 minute
```
