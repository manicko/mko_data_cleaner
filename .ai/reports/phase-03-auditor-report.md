---
phase: "03"
phase_name: "Async Concurrency & Worker Safety"
phase_file: ".kilo/commands/audit/phases/03-audit-async-concurrency.md"
report_status: "draft"
scope: >-
  deletion analysis for the async-concurrency audit phase against a
  synchronous, single-shot, local-only CLI + library with no event loop,
  no network, and no credentials.
recommendation: DELETE
---

# Audit Report — Phase 03 Async Concurrency & Worker Safety

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **local, synchronous, single-shot** CLI tool and Python
library that post-processes Mediascope exports received as files on disk.
There is **no asyncio, no event loop, no Semaphore, no Queue, no worker pool,
no cancellation, and no retry/backoff** anywhere in the current architecture.

Baseline, code-verified against the working tree (`src/mko_data_cleaner/`,
`docs/00-overview/`):

- **Zero async primitives.** A repo-wide grep for `asyncio`, `async def`,
  `asyncio.run`, `Semaphore`, and `Queue` across `src/mko_data_cleaner` returns
  **zero matches** ([Phase 01 auditor report §1](../.ai/report/phase-01-auditor-report.md),
  [Phase 02 auditor report §1](../.ai/reports/phase-02-auditor-report.md)).
  `app.py:130-131` runs a plain Typer app; the library API
  `process_data` is synchronous. `grep` for `Queue` and `Semaphore` in `src/`
  is empty.
- **No network / no credentials.** `architecture.md:37-40` states
  "External systems: none." Exports arrive manually; nothing is fetched.
- **No concurrency layer at all.** The runtime is a straight synchronous call
  chain `app → AppService.run_report → CSVWorker → DBWorker/MappingDict`, one
  OS process per invocation. The only durable resource is a disposable SQLite
  DB owned by a `DBWorker` context manager (checkpoint WAL + delete on exit).
- **Phase 01 and Phase 02 have already been re-based** to the synchronous model:
  Phase 01's Purpose reads "no network, no credentials, no async" and its
  Process-Topology block now describes the synchronous single-shot shape (no
  `asyncio.run`, no tasks, no cancellation surface). Phase 02 likewise stripped
  its credential/async premise and even notes "Block 2 was deleted — no
  credentials exist." Phase 03 is the lone holdout still describing an async
  pipeline.

Phase 03's own Purpose (lines 13–18) assumes "a standalone async CLI tool and
library," "Semaphore-bounded API calls," "asyncio.Queue to a pool of worker
coroutines," and "worker cancellation on shutdown." None of this exists.

## 2. Per-Block Assessment (1–5)

Each block is assessed for **Fit** (does the concept apply to the real
architecture), **Outdated** (is the premise invalid), and whether any useful
concept survives a re-basing to the synchronous model.

### Block 1 — Concurrency ceiling and its interaction with the pool
- **Fit: None.** No Semaphore, no worker pool, no pool size, no queue depth.
  The synchronous chain has no concurrency boundary to bound.
- **Outdated: Entire premise.** "Semaphore value; pool size; a call path that
  exceeds or bypasses the ceiling" — there is no ceiling and no bypass surface
  ([`src/` grep: 0 matches for Semaphore/Queue/async def]).
- **Concept survives? No.** The underlying concern — "is work generation
  bounded" — has no analog here; the pipeline emits no concurrent tasks. The
  nearest analog (bounded memory during Polars chunked I/O) is owned by
  Phase 13 ("Data accumulation and memory pressure"), not this phase.

### Block 2 — Worker cancellation and shutdown discipline
- **Fit: None.** No workers, no `cancel()`, no `gather`, no event loop.
- **Outdated: Entire premise.** "bare `cancel()` without `await`... abandons
  in-flight work, leaves API tasks running, drops results" — no tasks exist.
- **Concept survives? No — and redundant.** The clean-teardown / no-abandoned-
  resource concern is real, but it is **already owned by Phase 01 Block 6**
  ("Process Topology and Shutdown"), which records the `DBWorker` context
  manager (`checkpoint WAL + delete on __exit__`, `app_service.py:133`,
  `db_service.py __exit__`). Re-basing Block 2 here would duplicate Phase 01.

### Block 3 — Backpressure through the asyncio.Queue
- **Fit: None.** No `Queue(maxsize=100)`, no producer coroutine, no `put`
  blocking.
- **Outdated: Entire premise.** "full queue blocks the producer" — there is no
  queue and no producer coroutine.
- **Concept survives? No — redundant.** Bounded work generation survives as a
  *memory/streaming* property (does the pipeline stream or buffer everything),
  but that is **Phase 13 Block 5** ("Data accumulation and memory pressure"),
  not a concurrency-backpressure property. The Queue framing has no analog.

### Block 4 — Error isolation within the worker fan-out
- **Fit: None.** No workers, no fan-out, no `task_done`/`join` deadlock risk.
- **Outdated: Entire premise.** "one failed task aborts the entire run" /
  "worker that crashes without calling `task_done` deadlocks" — no fan-out
  exists; a single unhandled exception already aborts the synchronous run by
  design.
- **Concept survives? No.** Error isolation within a fan-out only matters when
  a fan-out exists. The synchronous model has none. Per-task error capture and
  re-run-ability is **Phase 04's** domain ("Error serialisation and
  task-to-yaml"). No new ground here.

### Block 5 — Retry isolation inside the concurrency boundary
- **Fit: None.** No retry, no backoff, no jitter, no Semaphore to hold.
- **Outdated: Entire premise.** "retrying call holds the Semaphore for the
  full retry duration" — no retries (no network, no transient failures).
- **Concept survives? No.** Retry isolation requires a retry mechanism. The
  retry/backoff contract was delegated to Phase 09, which itself is entirely
  async-framed (`asyncio.to_thread`, "blocking-work bridge") and has no subject
  in the current codebase. There is nothing to re-base.

### Assessment summary
All five blocks are **Fit: None**, **Outdated: entirely**. The only surviving
*concepts* (clean teardown, bounded memory, error capture, fail-fast) are either
already owned by Phase 01's rewritten topology block or by Phase 04/Phase 13 —
none map to async concurrency specifically. Re-basing Phase 03 to the
synchronous model would produce a phase with no distinct responsibility.

## 3. Cross-Phase Impact — Handling the (03) References After Deletion

Phase 03 is cited as out-of-scope by six downstream phases. Every citation must
be removed because the cited subject (Semaphore/Queue/worker-pool/cancellation)
does not exist. The citations, found via repo-wide grep over
`.kilo/commands/audit/phases/`:

| Phase | Line | Current text | Disposition |
|---|---|---|---|
| 04 | 27 | "Semaphore and Queue concurrency (03)" | **remove** (03 deleted) |
| 09 | 23–24 | "Semaphore/Queue/worker-pool semantics and the cancellation mechanism (03)" | **remove** (03 deleted) |
| 10 | 17 | "transaction, lock, and pooler semantics (03)" | **remove** (03 deleted; SQLite transaction/lock semantics belong to the pipeline/DB layer, owned by 04) |
| 11 | 18 | "transaction, lock, and pooler semantics (03)" | **remove** (same re-targeting as 10) |
| 12 | 23 | "Semaphore/Queue/worker-pool concurrency (03)" | **remove** (03 deleted) |
| 13 | 25 | "Semaphore acquisition and cancellation semantics (03)" | **remove** (03 deleted) |

**Note on the task brief's enumeration.** The brief lists Phase 05 among phases
that reference (03); this is **not borne out by the text.** Phase 05's scope
cites only (02), (04), (10), (01) — it never cites (03). Phase 05's real
problem is that it inherits the Mediascope-API/report-template assumptions
(see its Purpose: `ReportSettings → PeriodModel → LastTimeModel` against a
"defaults-merge" chain that does not exist, per Phase 02 auditor report §2
Block 3); that is a re-basing concern distinct from a (03) citation. Phases
09 and 11 *do* cite (03) but were omitted from the brief's list — they are
included above.

**Downstream phases that are themselves async-framed and require separate
re-basing (out of scope for this deletion, but blocking on it):**

- **Phase 04** — Purpose (lines 13–22) and Block 3 assume an `asyncio.Queue`,
  a worker pool, and Mediascope API calls (`send → wait → extract → export`).
  Already flagged in Phase 01 auditor report §2 Block 6 as needing a sync
  re-basing. The (03) citation removal is mechanical; the Purpose/blocks need
  their own rewrite (mirroring how Phase 01/02 were re-based to "files on
  disk, no network").
- **Phase 09** — entirely built on `asyncio.to_thread`, retries with backoff,
  and a JSON credential file in `connections/`. None of this exists. The (03)
  citation is one symptom; Phase 09 itself has no subject and is a deletion
  candidate by the same logic (the Phase 02 auditor report §2 Block 2 already
  deleted the parallel "credential seam" from Phase 02).
- **Phase 12** — Purpose says "standalone async CLI tool" (line 15); must be
  re-based to sync (as Phase 01/02 were). The (03) citation is removed; the
  async framing in the Purpose needs rewriting.
- **Phase 13** — Purpose says "async data-fetch pipeline" (line 13) and every
  block (Semaphores, worker pool, Queue, API polling, retry backoff) is
  async-framed. The (03) citation is removed; the whole phase needs re-basing
  to the synchronous cost centres (Polars chunked I/O, SQLite bulk writes via
  ADBC, CSV export) — which, like Phase 04/09, is out of scope for this
  deletion and should be tracked as follow-on work.
- **Phase 15** — not yet read for this phase, but grep shows its Purpose
  (line 14) assumes "a standalone async CLI tool that fetches data from" the
  Mediascope API. Same re-basing class as 04/09/13.

**Orchestration files:** `audit-multi-phase.md` enumerates phases by listing
the `phases/` directory, so deleting `.kilo/commands/audit/phases/03-*.md`
removes it from the selectable set automatically — no hard-coded sequence to
edits. `audit-orphaned.md`, `audit-phase-gaps.md`, `audit-refine.md`, and
`audit-phase-refine.md` contain no hard-coded reference to phase 03 (grep for
`03` over `.kilo/commands/audit/` returned matches only in the phase files
themselves and `audit/tests/audit-tests-full.md`'s HTTP-status `403`, which is
unrelated).

**Auditor-report precedent:** The Phase 01 and Phase 02 auditor reports
both explicitly recommend dropping Phase 03's async/cancellation premise and its
scope citations — Phase 01 §2 Block 6 ("Semaphore/Queue to phase 03, which is
itself moot here") and Phase 02 §3 ("the async pipeline (03, 04)" exclusion is
vestigial and was removed). Deletion honours that already-recorded guidance.

## 4. Severity Taxonomy Assessment

Phase 03's taxonomy (lines 95–114) opens with the **sound principle** ("grade
by effect and blast radius, not by mechanism name; rate what is true now, not
the worst consequence if triggered; an empty band is a valid outcome"). That
principle should be retained project-wide.

However, **every illustrative example is async/concurrency-framed and therefore
moot:**

- CRITICAL: "worker cancellation that abandons in-flight API tasks"; "queue
  `task_done` never called so `join()` deadlocks forever." → no workers, no
  queue join.
- HIGH: "Semaphore held across a full retry backoff"; "worker exception that
  aborts the whole run." → no Semaphore, no retried calls.
- MEDIUM: "cancellation path not awaited"; "retry without jitter"; "queue
  depth unbounded/non-observable." → no cancellation, no retries.
- LOW: "missing `task_done` on a path that never raises"; "comment describing
  the pool size differently from code"; "absent cancellation log." → no pool,
  no `task_done`.

**Decision: the taxonomy travels with the phase.** Because the entire phase is
deleted, there is no surviving taxonomy to re-base. The generic concerns the
examples tried to name (clean teardown, bounded resource use, observable failure)
are **already covered** by Phase 01's rewritten synchronous taxonomy (CRITICAL =
fail-fast gate regression; HIGH/MEDIUM/LOW on import-time stdout mutation,
`settings_dir` asymmetry, dead code) and Phase 13's performance taxonomy.
Re-basing this taxonomy in isolation would duplicate those and add a phase whose
severity bands could never be populated (empty state, by design).

## 5. Conclusion — Should Phase 03 Be Deleted Entirely?

**Yes — delete Phase 03 entirely. No re-basing.**

Rationale:

1. **No concurrency model exists.** The current architecture is synchronous and
   single-shot; a repo-wide grep for `asyncio`, `async def`, `asyncio.run`,
   `Semaphore`, and `Queue` returns zero matches in `src/`. Phase 03's central
   subject — Semaphore-bounded API calls, asyncio.Queue backpressure, a worker
   coroutine pool, cancellation, and retry/backoff — is entirely fictional for
   this codebase.

2. **All five blocks are Fit: None.** Every block's premise (Semaphore ceiling,
   worker cancellation, queue backpressure, fan-out error isolation, retry
   isolation) has zero analog. The generic engineering concerns behind them
   (clean teardown, bounded memory, error capture) are already owned by Phase
   01's rewritten "Process Topology and Shutdown" block, by Phase 04's
   error-serialisation block, and by Phase 13's memory-pressure block.
   Re-basing would create redundancy, not new coverage.

3. **No surviving concept is unique to this phase.** Unlike Phase 01 Block 2
   (transformed "Singleton → Lazy Service Factory") or Phase 02 Block 2
   (deleted "Credential Seam"), Phase 03 has no synchronous subject to
   re-base onto. The synchronous analog of "worker lifecycle and clean
   shutdown" is already Phase 01 Block 6.

4. **The downstream phases are async-framed themselves.** Phases 04, 09, 12,
   13 (and 15) carry the same async/Mediascope-API fiction and need the same
   re-basing treatment Phase 01/02 received. Deleting Phase 03 removes one
   citation target; the remaining (03) citations in those phases must be
   deleted mechanically (§3 table), with the broader async re-basing of 04/09/
   12/13/15 tracked as follow-on work — not as a re-base of *this* phase.

5. **Auditor-report precedent.** Both the Phase 01 and Phase 02 auditor reports
   explicitly recommend that the async/concurrency premise and its scope
   citations be removed. Deleting Phase 03 is the strongest realisation of
   that guidance.

**Recommended action:** Delete
`.kilo/commands/audit/phases/03-audit-async-concurrency.md` and drop the (03)
out-of-scope citations listed in the §3 table across Phases 04, 09, 10, 11, 12,
and 13. The findings output path `.ai/audit/03-async-concurrency/findings.md`
and Finding-ID prefix `ASY-` are correspondingly void. Treat the broader async
re-basing of Phases 04/09/12/13/15 as a separate, sequenced task (mirror the
Phase 01/02 pattern: strip the async/Mediascope-API fiction from each Purpose,
delete credential/async-specific blocks, and re-anchor the surviving concepts
on the synchronous single-shot surface).

**Effort to delete: trivial** (one file removal + ≤6 citation line-edits).
**Priority: recommended** — not mandatory, since Phase 03 has `status: draft`
and has produced no findings yet (`.ai/audit/03-async-concurrency/` does not
exist), but it is safe to remove now and prevents auditors from chasing a
phantom model.
