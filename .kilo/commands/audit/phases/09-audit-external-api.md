---
name: 09-external-api
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 09 — External Integrations & API

## Purpose

Audits every boundary between this system and the Mediascope API: what an
outbound call puts on the wire, what bounds it, what happens when the far side
misbehaves, and what the system still believes afterwards. A dependency that is
unavailable must degrade a single report task, not the whole run. The Mediascope
API is reached through a synchronous client (`mediascope-api-lib`) wrapped in
`asyncio.to_thread`, with retries bounded by a max-attempts limit and exponential
backoff plus jitter. Credentials enter via a JSON file. This file names what to
examine; the executing auditor finds the artifacts.

**Scope Boundaries** — other phases own: entry/bootstrap and import-time
behaviour (01); settings values and secrets (02); Semaphore/Queue/worker-pool
semantics and the cancellation mechanism (03); period resolution, task
generation, and error serialisation (04); template validation (05); enum and
boundary-model discipline (10); test adequacy (11); CI/CD and observability
(12); concurrency ceilings and retry cost (13). This phase owns only outbound
integration mechanics.

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed behaviour, a static proof that
a stated property does not hold, or a reproduced divergence between two paths;
the method is the auditor's. A passing check is methodology, never a finding,
and any sound evidence is admissible — nothing here gates a finding on this
phase's own checks.

### 1. The credential seam: where a secret enters a request

Derive the outbound call set from the Mediascope API client, then per call
establish where the credential enters — path, query, header, or body —
treating absent, empty and malformed as three distinct states. Establish which
surface holds the credential and what each does with it. Confirm the wire form
cannot reach a log, an exception string, or a referrer. Without a reachable
far side, establish the property from a constructed request, and record the
residual unverified.

Evidence: per call — where the credential enters, the three states, the
holding surface; one path by which the wire form could be observed.

### 2. Retry contract: what is replayed, what bounds the total, and whether a replay is safe

Establish per call whether the bound is on each attempt or on the whole
operation — a wait the far side mandates is a bound inherited, not set.
Establish which failure classes are replay-eligible, since a guard matching one
exception class silently drops its siblings, and whether the replayed operation
is safe to repeat when the response was lost after the write. Then establish
what the caller is told when the bound is exhausted: a failure reported as
handled is a silent loss.

Evidence: per call — the per-attempt bound, the total bound, the
replay-eligible failure classes, the caller-visible outcome at exhaustion.

### 3. Backoff cost and jitter

Establish the exponential backoff formula and its jitter range. Check whether
the worst-case total retry duration is bounded by a caller-controllable input,
whether jitter is applied to prevent thundering-herd, and whether retry sleep
is observable in logs or metrics. A backoff that grows without cap can make a
single degraded task stall the worker pool indefinitely.

Evidence: the formula and jitter range; the worst-case total duration; one
retry that grows the wait exponentially without cap.

### 4. The blocking-work bridge

Establish how synchronous Mediascope API calls are reached from the async
worker: what wraps the call, on which thread it runs via `asyncio.to_thread`,
and whether the connection or session is released on the exception path as well
as the normal one. Establish what happens when the worker is cancelled while a
`to_thread` call is in flight — whether the thread is awaited, abandoned, or
left running.

Evidence: the forward wrapper and its release path; one cancelled call in
flight and the thread's fate; the worker-pool size against the thread pool.

### 5. Degradation: what each dependency's failure takes down

Derive the dependency set yourself — the Mediascope API endpoints, the
credentials file, the export folder — not only named third parties. Per
dependency establish what its unavailability takes down, whether the blast
radius is one task or the whole run, and whether a guard built to bound abuse
fails open, fails closed, or raises: a raising guard can turn a targeted
dependency outage into a total one. Then ask whether one failure reaches the
same outcome through more than one layer.

Evidence: the derived dependency list with a per-dependency blast radius; one
outage probe and what survived it.

### 6. A degraded result the consumer cannot tell from a real one

Establish, for every fallback and default, whether what it returns can be told
apart from a real result by whatever stores it — the CSV file written, the
task marked done, the error file. A fallback that returns a value where the
caller needed a status writes a plausible answer into a file thereafter read
as authoritative, and nothing alerts because the operation succeeded. Include
the case where a breaker or limiter is open and nothing counts or logs it.

Evidence: per fallback — the substituted value, its consumer, and one stored
state a reader cannot distinguish from success.

### 7. What the integration surface writes into logs

Establish what an outbound API client writes about the values it sent and the
outcomes it received, through which sanitiser and whether that sanitiser masks
the credential or task key. Establish whether a dependency's failure state is
measurable at all — a retried call, an exhausted retry, a dropped request — or
whether it exists only as an unlogged exception. Where no counter exists, a
quiet dependency is indistinguishable from a working one.

Evidence: per direction, the fields logged and the sanitiser's masking verdict;
the list of failure states with a counter, a log, or neither.

### 8. Declared dependency and declared mechanism against the code

Establish every statement the repository makes about its outside world — a
capability it claims to have, a credential or endpoint it says it fetches, a
runbook instruction — and check each against the code. Treat a comment, a
docstring, and a specification line as claims to be tested rather than as the
invariant: for every guard, which predicate does the executing path actually
evaluate, and does the documentation above it match? A comment that contradicts
the code it documents is itself a finding.

Evidence: the statement inventory with a true/false verdict each; one documented
mechanism the code contradicts; one capability claimed and absent.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. An empty band is a valid outcome —
do not populate it with a hypothetical, and do not promote an item for sounding
alarming.

- **CRITICAL** — a credential reachable without any gate, or a credential made
  usable by a path the system does not control, where the exposure cannot be
  withdrawn without rotating it; a retryable write that is not idempotent, so a
  lost response triggers a duplicate side effect at the far side.
- **HIGH** — a silent correctness failure with no self-healing path: an
  operation reports success while the far side never received it; a substituted
  result stored where a real one was required and nothing can tell them apart;
  a per-process bound that is unbounded once a single invocation multiplies
  its own retries; one dependency's failure taking down a whole report rather
  than a single task.
- **MEDIUM** — a bounded resource or operability gap: a replay bounded per
  attempt but not across the operation; a fallback with a log but no counter; a
  guard applied on one path and absent from a sibling reaching the same
  dependency; a retry-exact backoff without jitter.
- **LOW** — documentation and observability drift with no runtime consequence
  today: a declared dependency the code does not implement; a comment or
  docstring naming a predicate the executing path does not evaluate; a log line
  reporting an attempt as a confirmation; a boundary accepting a rejected field.

## Report Output

- Findings path: `.ai/audit/09-external-api/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `EXT-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- Finding IDs use the declared bare prefix; a shipped test, configuration
  comment, or runbook may already carry an ID from a prior cycle, so check for
  a collision before minting a new one
- A shipped test asserting current behaviour is a remediation blocker, not a
  gate
