# durawork — Kill-Gate Assessment

**Verdict: PROCEED.** No available tool combines (a) a small, fully auditable
history-replay workflow engine, (b) a boundary-exhaustive kill -9 crash proof
with an independent oracle, and (c) demonstrated non-determinism detection.
The incumbents are large platforms whose crash semantics you trust from
documentation; durawork's claim is narrower and *proven in the repo*.

## What exists

| System | Shape | Why it is not this |
|---|---|---|
| **Temporal** (ex-Cadence, Uber) | Server cluster (frontend/history/matching services) + worker SDKs; Cassandra/MySQL/Postgres persistence. Hundreds of thousands of lines across server + SDKs. | The reference implementation of history replay, but an infrastructure commitment, not an auditable artifact. Its durability is established by reputation and internal testing, not by a shipped, runnable, boundary-exhaustive crash proof a reader can re-run in minutes. |
| **Cadence** (Uber) | Same lineage as Temporal, server-based. | Same as Temporal. |
| **DBOS** | Library (TS/Python/Go) backed by Postgres; checkpoints step outputs in tables transactionally with app writes. | Library-small, but Postgres-dependent, checkpoint-based rather than full history replay of workflows-as-code, and ships no exhaustive crash-boundary proof harness with an independent oracle. |
| **Restate** | Server (embedded RocksDB journal) + SDKs; journaled invocations and virtual objects. | Server system; journaling is internal machinery, not a small readable reference engine; no public boundary-exhaustive kill proof. |
| **Inngest** | Hosted/self-hosted server; step memoization over an event bus. | Step functions + memoization, not history-replay workflows-as-code; durability claims are platform claims. |
| **Netflix Conductor** | Server; workflows defined as JSON task graphs executed by task workers. | Task orchestration, not code-first deterministic replay; large server system. |
| **Azure Durable Functions** | Extension over Azure Functions + storage providers; replay-based orchestrator functions. | Closest replay model, but bound to Azure Functions hosting and a large framework; not a standalone auditable engine with a crash proof. |
| **Absurd** (earendil-works) | Durable tasks implemented as Postgres stored procedures + thin SDK. | Small and auditable in spirit, but task/checkpoint model on Postgres — not history-replay workflows-as-code, not Go-stdlib, and no boundary-exhaustive SIGKILL proof harness. |
| **Vercel `durable-harness` / Workflow SDK** | TS packages for crash-proof agent sessions on Vercel/Postgres worlds. | Agent-session scope, hosted-ecosystem ties, recovery claims self-reported with no published proof harness. |
| **Due** (Django) | Crash-test tooling for Django/Celery background jobs. | Tests *your jobs'* crash behaviour; it is not an engine and does no history replay. |

## The precise differentiation

1. **Size as a feature.** durawork's engine is a few thousand lines of
   stdlib-only Go. A single engineer can read the entire durability argument
   in an afternoon. Temporal's cannot be read in a month.
2. **Proof, not promises.** The repo ships a crash harness that enumerates
   *every* step boundary of the demo workflow (before/after each activity,
   inside an activity after its effect but before its completion record,
   around timer start/fire, around signal wait/claim), kills the worker
   process with a real external `SIGKILL` at each one, restarts it, drives
   the workflow to completion, and has an **independent oracle** (code that
   shares nothing with the engine) verify that every externally visible
   effect happened exactly once and the final state is correct.
3. **Determinism as an enforced contract, demonstrated.** Replay divergence
   is a typed, loud failure (`NonDeterminismError`) that parks the workflow
   rather than corrupting state — demonstrated by changing workflow code
   mid-flight (the way real incidents happen: a deploy during a long-running
   execution), not just asserted in prose.
4. **An honest baseline.** The same harness runs against the way people
   usually write this (in-memory state + a checkpoint file) and counts how
   many boundaries produce duplicate or lost effects, so the engine's value
   is a measured difference, not a vibe.

## What durawork deliberately is not

Single node, single worker process per workflow, Go-only workflow code, file
-backed history. It makes no distributed claims. Exactly-once is claimed for
*effects performed through the engine's idempotency-key contract* — the
engine cannot make an arbitrary non-idempotent external call exactly-once,
and says so plainly (see PAPER.md).

## Kill condition

If a small, auditable, history-replay engine with a shipped
boundary-exhaustive crash proof already existed as an available tool, this
build would stop. The survey above found none. **PROCEED.**
