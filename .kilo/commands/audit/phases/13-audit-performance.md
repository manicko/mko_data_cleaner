---
name: 13-performance
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 13 — Performance & Scalability

## Purpose

What the async data-fetch pipeline costs per report task, per worker, and per
report run; whether those costs are bounded; and whether the numbers the system
believes about itself are measured or asserted. There is no database, no query
plan, no cache, and no web request — the cost centres are: concurrent API calls
bounded by a Semaphore, the worker pool and task queue, the per-task API call
sequence (build → send → poll → get-result → result-to-table), the CSV write,
and the retry backoff that multiplies all of the above under API degradation.
This file names what to examine and under which angle — the executing auditor
discovers the concrete artifacts.

**Scope boundaries** — this phase owns the measurable consequence of latency,
throughput, and capacity, and nothing else. Owned elsewhere: the Semaphore
acquisition and cancellation semantics (03); the retry and backoff contract
(09); period resolution and task generation (04); the blocking-work bridge
(09); CI/CD and observability (12); code-quality discipline (10); test-suite
adequacy (11).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others,
and each carries its own evidence. Evidence is a class, not a check list:
observed behaviour, a reproduced output, or a static proof that a stated
property does not hold. Two questions here cannot be settled in the
environment at hand — a latency measurement against production-volume data, and
any measurement under real API load — establish each from what is observable
(the declared constant, the configured worker count, the call sequence and
counts, the backoff formula) and state plainly what could not be verified and
why. A passing check is methodology, never a finding, and any sound evidence is
admissible: nothing here gates a finding on this phase's own list.

### 1. Concurrency ceilings: what each tier imposes

Establish, per concurrency boundary, what bounds concurrent work. The Semaphore
limits concurrent API calls; the worker pool limits concurrent tasks; the
Queue limits pending tasks. Check whether the Semaphore is shared across all
call types or scoped per call, whether the worker count is hard-coded or
configurable, and whether any boundary is unbounded. Establish the CPU/I-O
split of a representative task — API polling is I/O-bound, the DataFrame
preparation is CPU-bound — and whether more workers would help or merely
amplify contention.

Evidence: per boundary — the limit, its source (constant or config), and the
worker/pool/queue counts; the CPU/I-O split of a representative task; one
boundary that is unbounded.

### 2. API call cost per task

Establish how many outbound API calls each report task requires: build-task,
send-task, the status-polling loop (with its sleep interval), get-result, and
result-to-table. Check whether the polling cost scales with the task's remote
processing time, whether the poll interval is configurable, and whether the
cost scales with the number of intervals × target audiences. A polling loop
with a fixed 2-second sleep that is not bounded by a max-wait can spin
indefinitely on a stuck task.

Evidence: the per-task call sequence and count; the poll interval; one task
whose polling cost exceeds the data-transfer cost; the max-wait bound, or its
absence.

### 3. Retry backoff cost

Establish the exponential backoff formula and its jitter range, and whether the
total retry duration is bounded by a caller-controllable input. Check whether
retry sleep dominates total runtime under API degradation, whether a retried
call holds the Semaphore for the full backoff (blocking other workers), and
whether retry cost is observable in logs or metrics.

Evidence: the backoff formula and jitter range; the worst-case total retry
duration; one retry that holds the Semaphore across the backoff.

### 4. Worker saturation and queue backpressure

Establish whether workers are saturated: Queue depth near its max, all workers
busy in API polling. Check whether a slow or retrying API call starves the
worker pool, whether backpressure stalls the task generator, whether the
ceiling is I/O-bound or CPU-bound, and whether the saturation point is
observable. A pool that spends its time sleeping in poll loops is I/O-bound and
will not benefit from more workers; a pool blocked on DataFrame preparation
will.

Evidence: the Queue depth and worker-busy state under load; one slow call's
effect on the queue; the bottleneck stage (I-O vs CPU) and its observation.

### 5. Data accumulation and memory pressure

Establish whether DataFrames or result sets accumulate in memory across tasks
before the CSV write drains them. Check whether a single oversized result can
starve the worker pool or trigger an out-of-memory condition, whether the
Queue's maxsize is the only backpressure on data volume, and whether an error
path (extracting a failed task's debug output) holds a large response in
memory.

Evidence: the in-memory data held per worker before write; one oversized result
and its effect; the backpressure valve on data volume.

### 6. Observable latency and its source

Establish whether the system measures or merely asserts latency. Check whether
a task's wall time (build → send → poll → extract → write) is logged, whether
the components of that time are attributable (API wait vs CPU vs I/O), and
whether a slow task is distinguishable from a stuck one. Where no measurement
exists, a slow dependency is indistinguishable from a working one.

Evidence: the latency measurement surface; the components attributed or not;
one slow task with no observable breakdown.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. The bands are effect classes, not
a closed list of mechanisms, so a defect whose mechanism is not named here still
lands by the consequence it is producing.

- **CRITICAL** — a control believed to be active that is not, on the path that
  decides whether a performance risk is caught before it ships: nothing
  distinguishes a checked subject from an unchecked one, and a reviewer cannot
  tell the difference. An empty band is a valid outcome — do not populate it
  with a hypothetical, and do not promote an item for sounding alarming.
- **HIGH** — cost that is real now and scales with a quantity the caller or the
  data controls, on a path that is on: a polling loop with no max-wait bound,
  so a stuck task spins forever; a retry that holds the Semaphore across full
  backoff, throttling the pool; a single oversized result that can starve the
  worker pool or exhaust memory.
- **MEDIUM** — a cost demonstrated but conditional on volume or configuration
  not yet in effect, where the design is unbounded rather than the present
  value being large: a queue depth with no observable signal; a backoff
  formula with no cap; a latency measurement that is asserted but not emitted.
- **LOW** — a stated objective with nothing that measures it; a comment or
  runbook line that no longer describes the behaviour beside it; a discipline
  whose declared scope is wider than its coverage.

## Report Output

- Findings path: `.ai/audit/13-performance/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `PERF-` — already in use in shipped source, tests,
  workflows, and reports from a prior cycle, with at least one identifier
  already resolving to two different findings; check for a collision before
  minting an ID and report it rather than creating a second namespace
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence
- Empty state, exactly: `No problems found in this phase.`
