---
name: 03-async-concurrency
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 03 — Async Concurrency & Worker Safety

## Purpose

Audits the async concurrency model of a standalone async CLI tool and library:
how outbound API calls are bounded by a Semaphore, how tasks flow through an
asyncio.Queue to a pool of worker coroutines, how worker cancellation is
handled on shutdown. There is no database, no ORM, no
transactions, and no cross-process serialisation — all concurrency is within a
single process and a single event loop per invocation.

This file names what to examine and under which angle; the executing auditor
discovers the concrete artifacts. Owned here: Semaphore-governed API
concurrency, Queue backpressure, the worker pool lifecycle, cancellation and
shutdown discipline. Owned elsewhere: the
retry/backoff contract and the blocking-work bridge (09); import-time side effects (01); performance ceilings
(13); data pipeline task generation and worker loop semantics (04); code-quality
discipline around concurrency primitives (10).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed behaviour, a reproduced
behaviour, or a static proof that a stated property does not hold; the method
is the auditor's. A passing check is methodology, never a finding.

### 1. Concurrency ceiling and its interaction with the pool

Establish the effective bound on concurrent API calls: the Semaphore limit and
the worker pool size, and how they interact. Check whether the Semaphore is
shared across all API call types or scoped per call, whether the pool size and
the queue depth are independently bounded, and whether any path can exceed the
ceiling by bypassing the Semaphore or calling the sync API directly.

Evidence: the Semaphore value; the pool size; one call path that exceeds or
bypasses the ceiling.

### 2. Worker cancellation and shutdown discipline

Establish the shutdown sequence: what happens when the task queue is drained
and the worker coroutines are cancelled. Check whether cancellation is awaited
— a bare `cancel()` without `await` or `gather` can abandon in-flight work,
leave API tasks running, or drop results. Check whether pending tasks on the
Queue are handled before cancellation, whether a worker stuck in a non-terminal
poll loop blocks shutdown, and whether partial output survives or is lost.

Evidence: the shutdown ordering; one in-flight task abandoned on cancel; the
fate of queued-but-unprocessed items.

### 3. Backpressure through the asyncio.Queue

Establish how `Queue(maxsize=100)` bounds the task producer (the task-generator
coroutine). Check whether a full queue blocks the producer, whether the
producer is cancelled when workers are cancelled, and whether a producer that
exceeds the queue limit stalls silently or surfaces the bound. Check whether
the queue's depth is observable in logs or metrics.

Evidence: the queue maxsize and the producer's put behaviour; one induced
backpressure stall; whether the depth is observable.

### 4. Error isolation within the worker fan-out

Establish what happens when one worker raises an unhandled exception inside the
pool. Check whether one failed task aborts the entire run, whether other
workers continue processing, whether the exception is recorded with enough
context to identify the failing task, and whether a worker that crashes without
calling `task_done` deadlocks the queue join.

Evidence: per worker error — whether other workers continue; one unhandled
exception and its effect on the queue; the recorded context for a failed task.

### 5. Retry isolation inside the concurrency boundary

Establish how the retry wrapper (Semaphore-bounded API call with backoff)
interacts with the worker that holds it. Check whether a retrying call holds
the Semaphore for the full retry duration (blocking other workers), whether
jitter is applied to backoff, and whether a call that exhausts retries
surfaces a distinct error distinguishable from a permanent failure.

Evidence: the Semaphore hold duration during a retry; the backoff formula; one
exhausted-retry outcome and its error classification.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. An empty band is a valid outcome —
do not populate it with a hypothetical, and do not promote an item for sounding
alarming.

- **CRITICAL** — a worker cancellation that abandons in-flight API tasks, leaving
  the remote side computing results no one retrieves; a queue `task_done` that
  is never called so `join()` deadlocks forever, hanging every invocation.
- **HIGH** — a Semaphore held across a full retry backoff, throttling the pool
  far below its ceiling; a worker exception that aborts the whole run instead of
  isolating one task; backpressure that stalls the producer silently with no
  observable signal.
- **MEDIUM** — a cancellation path that is not awaited, risking partial state
  across Python versions; a retry that does not apply jitter, causing
  thundering-herd; a queue depth that is unbounded or non-observable.
- **LOW** — hygiene and observability drift with no runtime consequence today:
  a missing `task_done` on a path that never raises; a comment describing the
  pool size differently from the code; a cancellation log that is absent.

## Report Output

- Findings path: `.ai/audit/03-async-concurrency/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, distribution, cross-finding analysis, roadmap, rollout safety,
  appendices
- Finding-ID prefix: `ASY-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding needs
  runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
