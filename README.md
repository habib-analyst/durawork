# durawork

A durable workflow engine written from scratch in Go — workflows-as-code with
history-based replay, retries, durable timers, and human-approval signals —
whose durability claim is **proven, not documented**: a crash harness kills
the worker process with `kill -9` at *every* step boundary of the demo
workflow and an independent oracle verifies that every externally visible
effect happened exactly once.

Standard library only. The engine core is ~880 lines; the entire system —
engine, demo workflows, runner, signal CLI, baseline, and the crash harness
itself — is ~2,050 lines of non-test Go. You can read the whole durability
argument in an afternoon. That is the point.

## The measured claim

Demo workflow (3-day onboarding, clock compressed): create account → welcome
email → human approval (signal raced against a durable timeout) → provision
workspace → day-2 check-in → day-3 check-in.

| | boundaries tested | boundaries passed |
|---|---|---|
| **durawork** | 23 | **23** |
| naive script (in-memory state + checkpoint file) | 31 | 18 |

Boundaries include the hardest point in durable execution — killed *inside*
an activity, after its effect was journaled but before its completion was
recorded (mid-activity): **durawork 5/5, naive 0/5** (duplicate welcome
emails, duplicate accounts). All 8 of the naive script's torn-checkpoint
boundaries also fail: a kill mid-checkpoint-write corrupts the checkpoint,
the script "helpfully" starts over, and every effect duplicates.

Also proven: the approval-reject path, the approval-timeout path, and the
determinism contract — start a workflow under code v1, kill it, replay under
v2 (a step inserted mid-workflow, the classic bad deploy): the engine parks
it with a typed error instead of corrupting state:

```
non-determinism at command 1: history recorded activity "step_b",
replay issued activity "step_inserted"
```

Full numbers: `results.json` (regenerate any time with the harness).
Method and honest limits: `PAPER.md`. Prior-art kill-gate: `KILLGATE.md`.

## How it works

**History.** Each workflow has an append-only event log
(`<dir>/<id>/history.jsonl`): `WorkflowStarted`, `ActivityScheduled /
AttemptStarted / AttemptFailed / Completed / Failed`, `TimerStarted /
TimerFired`, `SignalWaitStarted / SignalClaimed / SignalTimedOut`,
`WorkflowCompleted / Failed / Parked`. Every append is one line + fsync;
readers tolerate a single torn final line (the signature of a kill mid-append).

**Replay.** Workflow code is an ordinary Go function calling three
primitives: `ExecuteActivity`, `Sleep`, `WaitSignal`. On recovery the engine
re-runs the function from the top; primitives for commands already in
history return their recorded results instead of re-executing. Past the
frontier, execution is live. Blocking (timer not due, signal not arrived)
unwinds the pass; timers fire on recovery if they came due while the process
was down, and signals wait in a durable inbox (`signals.jsonl`) that any
process can write to.

**Exactly-once effects.** Each activity command gets a stable idempotency
key (`<workflow>/<cmd>/<activity>`), identical across crashes, retries, and
replays. Activities perform effects through a journal keyed by it
(`effects.jsonl`, modelling an idempotent downstream): a re-executed
activity finds its key already present and returns the recorded effect
instead of duplicating it. This is the whole trick, and the harness proves
it holds at every boundary — including the mid-activity kill, where the
effect exists but the completion record does not.

**Retries.** Per-activity policy (max attempts, exponential backoff), every
attempt recorded in history. Exhaustion is recorded too, so a replayed
workflow sees the same failure deterministically.

**Determinism contract.** Replay must issue the same command sequence
(kind + name + duration) as history records. Divergence produces a
`NonDeterminismError` and the workflow is *parked* — loudly, in its history —
rather than allowed to continue with misaligned results.

## Run it

```sh
go build ./...

# drive the demo workflow (it waits for approval)
./durawork-runner -dir data -id wf1 -day-ms 2000 -approval-timeout-ms 60000

# from another terminal: approve it
./durawork-signal -dir data -id wf1 -name approval -payload '{"decision":"approve"}'

# inspect the artefacts
cat data/wf1/history.jsonl
cat data/effects.jsonl
```

## Prove it

```sh
go test ./...                              # 13 unit tests
go run ./cmd/durawork-harness -root .      # the boundary-exhaustive crash proof
cat results.json
```

The harness builds the real binaries, runs each scenario as real OS
processes, delivers real `SIGKILL`s, and judges only from the artefacts a
real operator would have. It exits non-zero if the engine fails its own
proof.

## Honest limits

- **Single node, one worker process per workflow.** No distribution, no
  failover, no claims about either.
- **Go-only workflow code**, compiled into the worker.
- **Exactly-once applies to effects that honour the idempotency contract.**
  The engine cannot make an arbitrary non-idempotent external call
  exactly-once — no engine can; the boundary of the guarantee is stated,
  and the journal is where effects must go to be inside it.
- History grows unboundedly; no compaction/continue-as-new (yet).
- Timers and deadlines use the wall clock of the single node.

## Layout

```
engine/                 the engine: history store, replay executor, effects journal
workflows/              the onboarding demo + versioned workflows for the determinism demo
cmd/durawork-runner/    drives one workflow instance
cmd/durawork-signal/    delivers a signal to a workflow's inbox
cmd/durawork-naive/     the baseline script the harness measures against
cmd/durawork-harness/   the crash proof (independent oracle inside)
```
