---
phase: "13"
phase_name: "Performance & Scalability"
phase_file: ".kilo/commands/audit/phases/13-audit-performance.md"
report_status: "draft"
scope: >-
  re-basing assessment of Phase 13 against a synchronous, single-shot,
  local-only CLI tool + library with no event loop, no API, no network, no
  credentials, no retry/backoff, and no concurrency primitives.
code_ref: "384044a"
recommendation: REBASE
---

# Audit Report — Phase 13 Performance & Scalability (Rework Assessment)

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **local, synchronous, single-shot** CLI tool and Python
library that post-processes Mediascope TV advertising exports received as
**files on disk**. There is no outbound API, no network, no credentials, no
event loop, no `asyncio`, no retry/backoff, and no concurrency primitives
anywhere in the architecture.

Code-verified facts (`src/mko_data_cleaner/`, `docs/00-overview/`, `pyproject.toml`,
`tests/`):

- **Zero async primitives.** A repo-wide grep across `src/mko_data_cleaner/` for
  `asyncio | async def | asyncio.run | Semaphore | Queue | requests | httpx |
  aiohttp | token | credential | api_key | backoff | polling | poll`
  returns **zero code matches**. There is no concurrency layer at all — no
  thread, no coroutine, no worker pool.
- **No network / no credentials.** `architecture.md:37-40` states "External
  systems: none." Exports arrive as files; nothing is fetched. `pyproject.toml`
  declares no HTTP/async client. No `.env*` files exist.
- **One real import-time side effect.** `sys.stdout.reconfigure(line_buffering=
  True)` sits at module level in `app.py:23-24` (guarded by `hasattr` since a
  recent fix); `core/*` modules do not touch stdout.
- **Strictly synchronous call chain.** `app.py → AppService.run_report →
  CSVWorker → DBWorker / MappingDict`, one OS process per invocation
  ([architecture.md:78](docs/00-overview/architecture.md)).
- **Lazy service factory.** `get_app_service()` (`app_service.py:325-338`)
  constructs `AppService` only on call, after the `_require_settings` gate
  passes; `logging.config.dictConfig` runs inside the factory, never at import.
- **Disposable SQLite DB.** `DBWorker` is a context manager: opens two
  connections on `__init__` (`db_service.py:54-55`), and on `__exit__` runs
  `wal_checkpoint(FULL)`, closes both connections, and deletes the DB file
  and its `-wal`/`-shm` sidecars (`db_service.py:769-788`). Nothing persists
  between runs.
- **Three cost centres.** `run_report` segments work into three timed stages
  — **import** (CSV → SQLite), **mapping** (SQLite rule application),
  **export** (SQLite → CSV) — via a `timings: dict[str, float]` measured with
  `perf_counter()` (`app_service.py:124, 177, 241, 269`).

**Conclusion for Phase 13:** The six performance blocks are async-framed
(Semaphore, worker pool, Queue, API polling, retry backoff). The synchronous
analogs exist (Polars chunked I/O, SQLite PRAGMAs, `timings` dict) but the
phase file never mentions them. Like Phase 01 Block 6 (async → sync) and
Phase 05 (re-themed to real Pydantic models), Phase 13 should be **REBASE: reframe
the blocks to synchronous cost centres, not deleted** — the underlying concerns
(latency measurement, memory bounding, cost attribution) are valid and alive.

---

## 2. Per-Block Assessment (1–6)

Each block is assessed for **Fit** (does the concept apply to the real
architecture), **Outdated** (async/API/credential references that need
replacement), and **Replacement** (what synchronous analog takes its place).

| Block | Current title | Fit | Outdated (async/API references) | Replacement (sync analog) |
|---|---|---|---|---|
| 1 | Concurrency ceilings | **None** | Semaphore, worker pool, Queue, CPU/I-O split for async concurrency | Polars chunked streaming, single-threaded pipeline, `DBWorker` context-manager lifecycle |
| 2 | API call cost per task | **None** | build → send → poll → get-result → result-to-table, polling loop, status polling | Per-stage pipeline cost via `timings` dict; local file/CSV/SQLite I/O |
| 3 | Retry backoff cost | **None** | exponential backoff, jitter, retry holding Semaphore, retry budget | Fail-fast error propagation; `DBWorker.__exit__` cleanup on any exception |
| 4 | Worker saturation & queue backpressure | **None** | Queue depth, worker-busy state, backpressure on task generator, pool saturation | Chunk-based memory bounding (`CHUNK_SIZE`), `temp_store=MEMORY`, sequential execution |
| 5 | Data accumulation & memory pressure | **Partial** | Queue maxsize as backpressure, worker-pool starvation, remote response in error path | Polars chunked streaming, SQLite temp tables as bounded accumulators, `cache_size`/`temp_store` PRAGMAs |
| 6 | Observable latency | **Partial** | build → send → poll → extract → write, polling-bound latency, stuck vs slow via polling | `timings` dict + `ReportResult.duration`; but coarse-grained, per-stage timings not in CLI |

### Block 1 — Concurrency ceilings: what each tier imposes

- **Fit: None.** The block assumes a Semaphore bounds concurrent API calls, a
  worker pool bounds concurrent tasks, and a Queue bounds pending tasks. **None
  of these exist.** A repo-wide grep for `Semaphore|Queue|worker_pool|
  asyncio|async def` across `src/mko_data_cleaner/` returns zero matches. The
  runtime is single-threaded; there is no concurrency tier to tune.
- **Outdated: Entire premise.** "The Semaphore limits concurrent API calls" —
  there is no API and no Semaphore. "The worker pool limits concurrent tasks" —
  there is no pool; the pipeline runs on the main thread. "The Queue limits
  pending tasks" — there is no Queue. "Whether more workers would help"
  assumes a pool that can scale — it cannot, because there is one worker and
  no I/O to overlap.
- **Replacement:** The synchronous analog of "concurrency ceiling" is
  **chunked streaming + bounded temp-storage**:
  - **Polars chunk size (`CSVWorker.CHUNK_SIZE = 10000`)** (`csv_service.py:28`)
    — bounds in-memory DataFrame size during CSV read via `pl.read_csv_batched(
    batch_size=self.CHUNK_SIZE)` (`csv_service.py:170-175`) and during export via
    `max_rows = params.pop("chunk_size", 10000)` (`csv_service.py:312`).
  - **Single-threaded sequential execution** — there is no parallelism to exploit;
    the pipeline is a straight synchronous call chain
    `run_report → CSVWorker → DBWorker/MappingDict` (`app_service.py:110-290`).
  - **`DBWorker` context manager** (`db_service.py:783-788`) — the lifecycle
    boundary: opens two SQLite connections on construction, checkpoints WAL +
    deletes DB on `__exit__`. No resource leaks across runs.
  - **SQLite temp tables as bounded accumulators** — `non_mapped`,
    `joined_matches_table`, `full_matches_table`, `data_table_tags`,
    `mapping_table`, `temp_tbl`, `fts_temp_tbl` are all `TEMP` tables or
    drop-and-recreate tables (`architecture.md:144-148`), bounded by disk, not
    memory.
  - The CPU/I-O split is: CSV import (I/O-bound: file read + ADBC write) vs.
    SQL matching (CPU-bound: SQLite query execution) — but because execution
    is sequential, there is no I/O-to-overlap with.

### Block 2 — API call cost per task

- **Fit: None.** The block assumes each report task requires outbound API calls:
  build-task → send-task → status-polling → get-result → result-to-table. **No
  API exists.** A repo-wide grep for `requests|httpx|aiohttp|urllib` in `src/
  mko_data_cleaner/*.py` returns zero matches. The only external inputs are
  local files on disk.
- **Outdated: Entire premise.** "The per-task call sequence" — there is no
  call sequence; data is read from local files. "The polling cost scales with
  the task's remote processing time" — there is no remote processing. "A
  polling loop with a fixed 2-second sleep" — there is no sleep, no polling.
- **Replacement:** The synchronous analog of "API call cost per task" is the
  **per-stage pipeline cost**, measured by the `timings` dict:
  - `timings["import"]` = CSV → SQLite import via Polars ADBC
    `chunk.write_database()` (`app_service.py:175-177, 96-101`)
  - `timings["mapping"]` = SQLite rule application (DELETE → REPLACE → ADD via
    `generate_rules_blocks` loop, `app_service.py:180-241`)
  - `timings["export"]` = SQLite → CSV export via `export_sql_to_csv` with
    `COUNT(*)` + `fetchmany(max_rows)` pagination (`app_service.py:256-269`,
    `csv_service.py:269-371`)
  - Total wall-clock: `ReportResult.duration` = `perf_counter() - t_start`
    (`app_service.py:286`), bracketed by `started_at`/`finished_at` datetimes
    (`app_service.py:111, 271`).
  - No polling, no sleep, no remote status to check — local I/O is synchronous
    and blocking by nature.

### Block 3 — Retry backoff cost

- **Fit: None.** The block assumes an exponential backoff formula, jitter range,
  a bounded retry budget, and retry cost observable in logs/metrics. **No retry
  mechanism exists.** Local file/SQLite failures are fatal — `FileNotFoundError`,
  `sqlite3.Error`, `WrongDataSettings`, `ConfigError` are raised and propagate
  up the stack, not retried.
- **Outdated: Entire premise.** "Exponential backoff + jitter" — no backoff.
  "Retry sleep dominates total runtime" — no retry sleep. "A retried call holds
  the Semaphore for the full backoff" — no Semaphore, no retry. "Retry cost in
  logs/metrics" — no retry to observe.
- **Replacement:** The synchronous analog of "retry backoff" is **fail-fast
  error propagation**:
  - Local failures are non-transient and fatal — there is no transient-error
    surface to retry (no network, no rate-limiting, no service degradation).
  - `DBWorker.__exit__` (`db_service.py:786-788`) guarantees resource cleanup
    (WAL checkpoint → close connections → delete DB) even when a fatal error
    propagates through the `with DBWorker(...)` block (`app_service.py:146-269`).
  - There is no backoff budget to bound, and no retry cost to observe. The
    synchronous design trades retry elasticity for deterministic fail-fast
    behavior — acceptable for local-only single-shot processing.

### Block 4 — Worker saturation and queue backpressure

- **Fit: None.** The block assumes workers can be saturated (Queue depth near
  max, all workers busy in API polling), a slow/retrying API call starves the
  worker pool, backpressure stalls the task generator, and the bottleneck is
  I/O-bound vs CPU-bound. **No Queue, no workers, no saturation.**
- **Outdated: Entire premise.** "Queue depth near its max" — no Queue. "All
  workers busy in API polling" — no workers, no polling. "Backpressure stalls
  the task generator" — no task generator. "More workers won't help because the
  pool sleeps in poll loops" — no poll loops.
- **Replacement:** The synchronous analog of "saturation and backpressure" is
  **chunk-based memory bounding + sequential stall**:
  - `CHUNK_SIZE = 10000` bounds the Polars DataFrame in memory per chunk during
    both reading (`csv_service.py:28, 170-175`) and export (`csv_service.py:312`).
  - SQLite `temp_store=MEMORY` PRAGMA (`db_service.py:120`) keeps temp tables in
    RAM; `cache_size=-100000` PRAGMA (`db_service.py:121`) bounds the page cache.
  - The pipeline is sequential — a single slow SQLite operation (e.g., an FTS5
    MATCH join or a `LIKE '%term%'` scan) blocks everything; there is no queue
    to drain and no parallelism to exploit, but also no saturation signal to
    monitor.
  - **Key gap:** there is no observable signal for "the pipeline is stuck in a
    slow SQL operation" — see Block 6.

### Block 5 — Data accumulation and memory pressure

- **Fit: Partial.** The underlying concern — large in-memory datasets accumulating
  before a drain — is valid for a synchronous pipeline. But the mechanisms named
  (Queue maxsize as the only backpressure valve, worker-pool starvation, remote
  response held in memory on error) do not exist.
- **Outdated:** "Queue's maxsize is the only backpressure on data volume" — no
  Queue. "Worker pool starved by an oversized result" — no pool. "Error path
  holds a large response in memory" — no remote responses.
- **Replacement:** The synchronous analog of "data accumulation" is **chunked
  streaming + SQLite temp tables + PRAGMA memory controls**:
  - **Polars chunked streaming** bounds per-chunk DataFrames: CSV read in 10k-row
    batches (`csv_service.py:170-175`); export in `chunk_size` batches
    (`csv_service.py:312`). A single chunk is the unit of memory pressure.
  - **SQLite temp tables** are the persistence-bounded accumulator — matches are
    stored in `full_matches_table` and `joined_matches_table` as TEMP tables
    (`architecture.md:144-146`), not held in Python memory. Rule blocks are
    written to `mapping_table`/`temp_tbl`/`fts_temp_tbl` per-iteration and
    dropped after each `apply_mapping` call (`app_service.py:217-239`).
  - **`DBWorker.close()` deletes the DB file on exit** (`db_service.py:781`) —
    no data persists in memory or on disk after the run.
  - **Gap:** the import path uses `chunk.write_database(...)` (`app_service.py:96`)
    which materializes the entire 10k-row chunk as a Polars DataFrame before
    the ADBC bulk write — a single oversized chunk (>10k rows, if `CHUNK_SIZE`
    were raised) could spike memory. The export path uses `fetchmany(max_rows)`
    (`csv_service.py:338, 366`) which is truly streaming — the read path is not
    symmetric.

### Block 6 — Observable latency and its source

- **Fit: Partial.** The concern — does the system *measure* latency or merely
  *assert* it? — is valid. Latency IS measured via `perf_counter()` into the
  `timings` dict. But the "build → send → poll → extract → write" sequence is
  fictional.
- **Outdated:** "Status-polling loop" — no polling. "Slow task indistinguishable
  from stuck" via polling — no polling. "API wait vs CPU vs I/O" attribution at
  the polling level — no API.
- **Replacement:** The synchronous analog is the **`timings` dict + `ReportResult`**:
  - `timings` has three keys: `"import"`, `"mapping"`, `"export"` — measured with
    `perf_counter()` (`app_service.py:124, 175-177, 180-241, 256-269`).
  - `ReportResult.duration` = total wall-clock seconds (`app_service.py:286`).
  - `started_at`/`finished_at` datetimes (`app_service.py:111, 271`).
  - Returned to the caller: `process_data` returns `ReportResult` (`app.py:145`,
    `app_service.py:278-290`); the CLI displays `result.duration` at `app.py:84`.
  - Tests assert the structure: `test_app.py:472` asserts
    `set(result.timings.keys()) == {"import", "mapping", "export"}`;
    `test_app.py:471` asserts `result.duration > 0.0`.
  - **Gap — doc staleness:** `current-behavior.md:272-275` and
    `architecture.md:218` assert "`process_data` currently returns `None`";
    `data-flow.md:165` records "Currently `None`." This is **stale** — the code
    returns `ReportResult` (verified by `app.py:145` and `test_app.py:461`).
    The docs understate the observability surface. See candidate finding F1.
  - **Gap — coarse granularity:** only 3 stage timers exist; within "mapping,"
    the `generate_rules_blocks` loop iterates DELETE → REPLACE → ADD sequentially
    under a single `t_mapping` timer (`app_service.py:180-241`) — there is no
    per-action-type attribution. A slow mapping stage cannot be attributed to
    which action type is the bottleneck.
  - **Gap — CLI doesn't show per-stage timings:** `app.py:81-85` prints
    `result.duration` but NOT `result.timings`; only programmatic callers
    (`process_data`) see the stage breakdown.

---

## 3. Phase Purpose & Scope Assessment

### Purpose text (lines 11–21)

**Assessment: rewrite.** The Purpose is entirely async-framed:

> "What the async data-fetch pipeline costs per report task, per worker, and per
> report run... concurrent API calls bounded by a Semaphore, the worker pool and
> task queue, the per-task API call sequence (build → send → poll → get-result →
> result-to-table), the CSV write, and the retry backoff that multiplies all of
> the above under API degradation."

**Reality:** There is no async data-fetch pipeline, no worker pool, no task
queue, no API, no polling, and no retry. The actual cost centres are:

| Phase 13 text | Reality (sync) |
|---|---|
| "async data-fetch pipeline" | synchronous local pipeline: CSV → Polars → SQLite → CSV |
| "concurrent API calls bounded by a Semaphore" | no API, no Semaphore; single-threaded Polars chunked streaming (`CHUNK_SIZE=10000`) |
| "worker pool and task queue" | no pool, no queue; `run_report` is a straight synchronous call chain |
| "per-task API call sequence (build → send → poll → get-result → result-to-table)" | per-stage pipeline cost measured by `timings` dict: import, mapping, export |
| "retry backoff that multiplies all of the above" | no retry; local failures are fatal, fail-fast |

**Rewritten Purpose (recommended):**

> "What the synchronous local I/O pipeline costs per report run: the cost of
> each pipeline stage (CSV import into SQLite, SQL rule application, SQLite-to-
> CSV export), whether those costs are measured or merely asserted, and whether
> memory and data-volume bounds are explicit. There is no database query plan
> optimization, no cache tier, and no web request — the cost centres are: the
> Polars chunked streaming boundary (`CHUNK_SIZE`), the SQLite PRAGMA tuning
> (`WAL`, `synchronous=OFF`, `temp_store=MEMORY`, `cache_size`), the per-stage
> `timings` dict measured with `perf_counter()`, the SQLite temp-table
> accumulators, and the fail-fast error path (no retry, no backoff)."

### Scope boundaries (lines 23–28)

Current text:
> **Scope boundaries** — this phase owns the measurable consequence of latency,
> throughput, and capacity, and nothing else. Owned elsewhere: the Semaphore
> acquisition and cancellation semantics (03); the retry and backoff contract
> (09); period resolution and task generation (04); the blocking-work bridge
> (09); CI/CD and observability (12); code-quality discipline (10); test-suite
> adequacy (11).

| Citation | Phase exists? | Status of cited phase | Action | Rationale |
|---|---|---|---|---|
| (03) | No | Deleted (Phase 03 auditor report §5) | **REMOVE** | "Semaphore acquisition and cancellation semantics" was async-framed (Semaphore/Queue/worker-pool). Phase 03 deleted; no async primitives exist to defer. |
| (09) ×2 | No | Deleted (Phase 09 file removed; auditor report §7) | **REMOVE** | "Retry and backoff contract" and "blocking-work bridge" were async-framed (`asyncio.to_thread`, JSON credential file). Phase 09 deleted; no retry/backoff/bridge exists. |
| (04) | Yes | Re-based to Data Pipeline (sync) | **KEEP, reword** | "Period resolution and task generation" — Phase 04 exists and re-based. But Phase 13 does not own "period resolution" (not applicable — no scheduled tasks). Reword to defer only the pipeline stage ordering that Phase 04 owns, and clarify that Phase 13 *measures* those stages' cost. |
| (12) | Yes | Async-framed, to be re-based | **KEEP (reword)** | "CI/CD and observability" — Phase 12 exists. Observability of per-stage latency is genuinely owned by Phase 12's observability lens; Phase 13 owns the *measurement instrumentation* (timings dict), Phase 12 owns the *emission/observation* (logs, metrics). Clarify the boundary. |
| (10) | Yes | Re-based to Code Quality (sync) | **KEEP** | Code-quality discipline is a real concern for any codebase. No async framing. |
| (11) | Yes | Exists (sync) | **KEEP** | Test-suite adequacy is a real concern. No async framing. |

**Proposed reworked scope boundary:**

> **Scope boundaries** — this phase owns the measurable consequence of latency,
> throughput, and capacity, and nothing else. Owned elsewhere: period resolution
> and pipeline stage ordering (04); CI/CD gates and observability emission
> (12); code-quality convention (10); test-suite adequacy (11).

Remove the `(03)` Semaphore citation and both `(09)` retry/bridge citations —
both phases are deleted and their premises never existed. Reword `(04)` to
reflect that Phase 13 owns the *cost measurement* of the stages Phase 04 owns
the *ordering* of. Clarify `(12)` as observability *emission* (Phase 12) vs.
Phase 13's *instrumentation*.

Also: Phase 12's own scope boundary (line 26) cites "(13) concurrency ceilings
and retry cost" — this is async-framed and will need rewording when Phase 12 is
re-based. Phase 10's scope boundary (line 17, 131) cites "(13) measurable
performance consequences" — that citation is **valid** and should be retained
as-is when Phase 10 is re-based (Phase 10 auditor report §7.1 already keeps (13)
with a reword note).

---

## 4. Severity Taxonomy Assessment

**Verdict: Keep the principle; re-ground the examples.**

Phase 13's taxonomy (lines 123–146) opens with the **sound principle** ("Grade by
effect and blast radius, not by mechanism name; rate what is true now, not the
worst consequence if triggered"). This principle should be **retained verbatim**.

However, **every illustrative example is async-framed** and moot for the
synchronous codebase:

- **CRITICAL example** mentions "a control believed to be active that is not,
  on the path that decides whether a performance risk is caught before it
  ships." This is **framework-agnostic and valid** — it describes an observability
  gate (e.g., no per-stage timing emitted → a slow stage is invisible). Keep.
- **HIGH example** mentions "a polling loop with no max-wait bound" — no polling.
  "A retry that holds the Semaphore across full backoff" — no Semaphore, no
  retry. "A single oversized result that can starve the worker pool or exhaust
  memory" — no worker pool, but the "exhaust memory" sub-concern IS valid for
  the sync case (a single oversized chunk can spike memory).
- **MEDIUM example** mentions "a queue depth with no observable signal" — no
  queue. "A backoff formula with no cap" — no backoff. "A latency measurement
  that is asserted but not emitted" — **partially valid** (timings exist in
  code but not emitted in CLI output).
- **LOW example** mentions "a stated objective with nothing that measures it" —
  valid (e.g., docs claim `process_data` returns None, contradicting the
  `timings` dict). "A comment or runbook line that no longer describes the
  behaviour" — valid.

**Recommended re-grounding of examples:**

| Band | Current (async) example | Reworded (sync) example |
|---|---|---|
| CRITICAL | "control believed active that is not" | Keep as-is — applies to missing timing instrumentation on a cost-critical path |
| HIGH | "polling loop with no max-wait bound" | "a chunk/streaming boundary that is unbounded, so a single oversized input can exhaust memory" |
| HIGH | "retry holds Semaphore across backoff" | *remove* — no retry, no Semaphore |
| HIGH | "oversized result starves worker pool" | "oversized single chunk spikes memory: `CHUNK_SIZE=10000` is the only bound; no per-chunk memory guard" |
| MEDIUM | "queue depth with no observable signal" | "pipeline stage with no observable progress: a slow SQLite query blocks with no timeout or progress callback" |
| MEDIUM | "backoff formula with no cap" | *remove* — no backoff |
| MEDIUM | "latency asserted but not emitted" | Keep as-is — `timings` exists in code but per-stage timing not in CLI output |
| LOW | "stated objective with nothing that measures it" | Keep — docs claim `process_data` returns None, contradicting the `timings` dict |
| LOW | "comment/runbook line no longer describes behaviour" | Keep |

---

## 5. Report Output Assessment

| Item | Current (Phase 13 §Report Output) | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/13-performance/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`). Directory does not yet exist — no prior findings to reconcile. |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `severity_taxonomy` and `code_ref` fields. |
| ID prefix | `PERF-` — "already in use … with at least one identifier resolving to two different findings" | **Retain `PERF-`, DISPROVE the collision** | See §5.1 below. |
| problems-only | `true` (front matter + §159) | **Keep** | All candidate findings are code-verified defects, not passing checks. |
| per-pass lines | ≤100, incremental append (§158) | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` (§161) | **Keep** | Verbatim. |
| Shipped-test-asserts-current-behaviour | §122: "a shipped test asserting current behaviour is a remediation blocker" | **Keep** | Applies to F1 (docs-vs-code: `test_app.py:472` asserts the `timings` keys exist), F3 (coarse granularity), F5 (chunk size default). |

### 5.1 The `PERF-` prefix collision warning — DISPROVEN

Phase 13 line 154 states:
> "Finding-ID prefix: `PERF-` — already in use in shipped source, tests,
> workflows, and reports from a prior cycle, with at least one identifier
> already resolving to two different findings; check for a collision before
> minting an ID and report it rather than creating a second namespace."

**This claim is disproven by the working tree (code_ref `384044a`):**

- A repo-wide grep for `PERF-` (excluding `.venv*`, `.git*`) returns **2 matches**,
  both in the Phase 13 phase file itself (line 154) — one in
  `.kilo/commands/audit/phases/13-audit-performance.md` and one in the scratch
  copy at `.ai/tmp/scratch/.kilo/commands/audit/phases/13-audit-performance.md`.
  These are the **warning text itself**, not finding IDs in any findings document.
- A repo-wide grep for `PERF-` across `src/`, `tests/`, `.ai/`, `.kilo/`,
  `pyproject.toml`, and all workflow/YAML/JSON files finds **no `PERF-NNN`
  finding identifiers** in any findings directory, test file, shipped source, or
  report.
- No findings directory `.ai/audit/13-performance/` exists
  (`Test-Path .ai\audit\13-performance → False`). No findings were ever produced.
- The `.ai/audit/` tree contains only `templates/` and `problems/` (the latter
  has one prior finding: `01-apply-delete-double-execution.md`, prefixed `01-`
  not `PERF-`).

**Conclusion:** There is no "prior cycle" `PERF-` usage in shipped source, tests,
workflows, or reports. The Phase 13 file's warning is **phantom evidence** — it
appears to refer to an audit cycle that has since been cleared, or conflates the
phase file's own warning text with actual finding IDs. The same defect pattern
was identified and resolved for the `QLT-` prefix in Phase 10 (§6.1: "disproven
by the working tree"; "stale phantom evidence").

**Recommendation:** Retain the `PERF-` prefix (it matches the template
convention and is unique among real findings — zero collisions found). **Remove
or correct the "already in use" warning** on Phase 13 line 154. The corrected
text should state that `PERF-` is the Phase 13 prefix per the template
convention and no collision was found in the current tree.

---

## 6. Candidate Findings (with code refs)

These are code-verified starting points the re-based Phase 13 blocks should
develop into findings (with runtime evidence). Severity bands follow Phase 13's
own taxonomy, re-grounded to synchronous reality. Grouped by the block that
catches them; ownership is attributed where a root cause belongs to another phase.

### Block 1 / Block 4 findings — streaming and memory bounds

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F1. Docs assert `process_data` returns `None`; code returns `ReportResult`.** `current-behavior.md:272-275` and `architecture.md:218` both state "process_data currently returns `None`"; `data-flow.md:165` records "Currently `None`" as the intended target. The code returns `ReportResult` (`app.py:145: return get_app_service(settings_dir).run_report(path)`; `app_service.py:278-290: return ReportResult(...)`). The stale doc understates the observability surface — callers do receive timings, they are not "target work." | `app.py:145`; `app_service.py:278-290`; `current-behavior.md:272-275`; `data-flow.md:165`; `architecture.md:218` | LOW (doc-vs-code drift) | Phase 13 Block 6 (observability surface); Phase 04 Block 4 (stale doc vs code) |
| **F2. `PERF-` ID-prefix collision warning is phantom.** Phase 13 line 154 claims `PERF-` is "already in use in shipped source, tests, workflows, and reports from a prior cycle, with at least one identifier already resolving to two different findings." Grep across the entire repo (excluding deps) finds `PERF-` only in the Phase 13 phase file's own warning text — zero finding IDs in any findings document, test, or source file. Same phantom-evidence pattern as Phase 10's `QLT-` warning (disproven §5.1). | `.kilo/commands/audit/phases/13-audit-performance.md:154` | LOW | Phase 13 Report Output section |

### Block 2 / Block 6 findings — per-stage cost measurement

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F3. CLI displays `duration` but not per-stage `timings`.** `app.py:81-85` prints `result.duration` and `result.rows_loaded`/`result.rows_exported` — but never `result.timings` (the import/mapping/export breakdown). The 3-stage timing data exists (`app_service.py:124, 177, 241, 269`) and is tested (`test_app.py:472`), but only programmatic callers see it. An operator watching CLI output cannot tell whether a 30-second run spent 28s in import or 28s in mapping. | `app.py:81-85` (CLI output); `app_service.py:177, 241, 269` (timings data) | MEDIUM | Phase 13 (cost attribution); Phase 12 (observability emission) |
| **F4. Coarse-grained `timings` dict — no per-action attribution within "mapping".** The `timings` dict has exactly 3 keys: `"import"`, `"mapping"`, `"export"` (`app_service.py:124, 177, 241, 269`). Within "mapping," the `generate_rules_blocks` loop iterates DELETE → REPLACE → ADD under a single `t_mapping` timer (`app_service.py:180-241`) — no per-action-type or per-block timing. A slow mapping stage cannot be attributed to DELETE, REPLACE, or ADD. | `app_service.py:180-241` | MEDIUM | Phase 13 Block 2/6 |

### Block 4 / Block 5 findings — memory and streaming bounds

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F5. Import-vs-export chunk size mismatch (10,000 vs 100,000).** CSV reading buffers in chunks of `CHUNK_SIZE = 10000` (hardcoded, `csv_service.py:28, 170-175`). CSV export writes chunks of `chunk_size` = `100000` (from `app_config.yaml:42`), not the model default of `10000` (`models.py:89`) or the code fallback of `10000` (`csv_service.py:312`). The 10× mismatch between read and write chunk sizes is undocumented, unconfigurable as a single value, and asymmetric: the read path materializes the full chunk DataFrame before ADBC write (`app_service.py:96`), while the export path uses true `fetchmany` streaming (`csv_service.py:338, 366`). | `csv_service.py:28`; `app_config.yaml:42`; `csv_service.py:312`; `models.py:89`; `app_service.py:96` vs `csv_service.py:338` | LOW | Phase 13 (chunked-streaming boundary); Phase 02 (config value consistency — model default ≠ YAML value) |

### Block 1 / Block 3 / Block 5 findings — dead code and cleanup (cross-filed)

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F6. Dead code: `DATA_TO_SQL_PARAMS` + `data_chunk_to_sql` (pandas import path).** `db_service.py:22-27` defines `DATA_TO_SQL_PARAMS = {"if_exists": "append", "index": False, "chunksize": 2000}`; `db_service.py:505-520` defines `data_chunk_to_sql()` which defaults to it. Neither is called — the live import path uses Polars ADBC `write_database` (`app_service.py:96-101`). Grep for `data_chunk_to_sql` in `src/` and `tests/` returns zero call sites. The dead `chunksize=2000` contradicts the live `CHUNK_SIZE=10000`. `pandas` is a declared dependency used only by this dead path. | `db_service.py:22-27, 505-520`; `app_service.py:96-101` | LOW (dead code / stale config) | Phase 02 (dead code sweep); Phase 13 records the performance-config discrepancy |
| **F7. `_apply_delete` executes the identical DELETE twice.** `db_service.py:640` and `db_service.py:643` pass the same `sql` string to `perform_query()`. The second pass matches nothing (rows already deleted) but re-evaluates the `rowid IN (subquery)` predicate — wasted work on a cost-critical path. Documented in `architecture.md:252` as "harmless (second pass matches nothing) but wasteful." | `db_service.py:630-643` (lines 640, 643) | LOW | Phase 10 (one rule, one home); Phase 13 records the redundant cost |

---

## 7. Cross-Phase Citation Inventory

### 7.1 Phases that Phase 13 CITES (scope boundary, lines 23–28)

Phase 13's scope boundary defers to:

| Cited phase | Current citation text | Phase status | Action |
|---|---|---|---|
| (03) | "the Semaphore acquisition and cancellation semantics" | **Deleted** (Phase 03 auditor report §5) | **REMOVE** — no Semaphore exists |
| (09) | "the retry and backoff contract" | **Deleted** (Phase 09 file removed) | **REMOVE** — no retry/backoff |
| (04) | "period resolution and task generation" | Re-based (sync) | **KEEP, reword** — Phase 04 owns stage ordering; Phase 13 owns cost measurement of those stages |
| (09) | "the blocking-work bridge" | **Deleted** | **REMOVE** — no bridge exists (cited twice) |
| (12) | "CI/CD and observability" | Async-framed, to be re-based | **KEEP, reword** — Phase 12 owns observability *emission*; Phase 13 owns *instrumentation* |
| (10) | "code-quality discipline" | Re-based (sync) | **KEEP** |
| (11) | "test-suite adequacy" | Exists (sync) | **KEEP** |

### 7.2 Phases that CITE (13) — who references Phase 13

| Phase | Line(s) | Citation text | Phase 13 status | Action on 13 citation |
|---|---|---|---|---|
| 04 | 27–28 | "SQLite/Polars/FTS performance cost centres (13)" | Async-framed, to be re-based | **KEEP** — valid ownership; Phase 04 auditor report §6 already recommends keeping (13) with a reword |
| 10 | 17, 131 | "measurable performance consequences (13)" | Async-framed, to be re-based | **KEEP** — Phase 10 auditor report §7.1 already keeps (13) with reword |
| 12 | 26 | "concurrency ceilings and retry cost (13)" | Async-framed, to be re-based | **REWORD when 12 is re-based** — "concurrency ceilings and retry cost" is async-framed; reword to "per-stage pipeline cost measurement (13)" |

**Orchestration files:** `audit-multi-phase.md` enumerates phases by listing the
`phases/` directory, so the phase file itself is always discoverable. No
hard-coded sequence needs editing when Phase 13 is re-based.

**Precedent:** Phase 10 (REBASE) and Phase 15 (DELETE) both established the
re-basing pattern for async-framed phases against a synchronous codebase. Phase 13
falls on the Phase 10 side — the underlying concerns (latency, memory, cost
attribution) are valid and have synchronous analogs.

---

## 8. Proposed New Block Set (Synchronous Performance Angles)

The 6 async-framed blocks should be renumbered and reframed to the synchronous
cost centres that actually exist. Each new block maps 1:1 to an original block
and preserves its evidence-seeking intent.

| # | Original block | New title (sync reframing) | Core question |
|---|---|---|---|
| 1 | Concurrency ceilings | **Chunked-streaming boundaries: what bounds work and memory per stage** | What limits the data processed per chunk? Is the chunk size configurable as one value, or does read (10k) diverge from write (100k)? |
| 2 | API call cost per task | **Per-stage pipeline cost: the price of each phase of a report run** | What does each pipeline stage (import, mapping, export) cost in wall-clock time? Are the stages attributed or lumped? |
| 3 | Retry backoff cost | **No retry: fail-fast error propagation** | When a local failure occurs (file, SQLite, config), is it retried or fatal? Where does the cleanup boundary run? |
| 4 | Worker saturation & backpressure | **Sequential stall: what blocks the single worker** | With one worker, what stage can block the entire pipeline? Is there a timeout? Any progress signal during a long SQLite operation? |
| 5 | Data accumulation & memory | **Bounded accumulation: chunk + temp-table + PRAGMA memory controls** | What bounds in-memory data? Polars chunk size, SQLite temp tables, `cache_size`/`temp_store` PRAGMAs — and where is their interaction untested or unbounded? |
| 6 | Observable latency | **Latency measurement vs. assertion** | Is latency measured (yes, via `perf_counter` + `timings` dict) or asserted (no, docs claim `None`)? Is per-stage attribution available in CLI output (no, only `duration`)? |

---

## 9. Conclusion — Verdict: REBASE

**REBASE (reframe, don't delete).**

Phase 13's underlying concerns — latency measurement, memory bounding, cost
attribution, fail-fast error handling — are **valid and alive** in the synchronous,
single-shot, local-only codebase. The six blocks are framed in async primitives
(Semaphore, worker pool, Queue, API polling, retry backoff) that **do not exist**
(a repo-wide grep returns zero matches). Each block has a direct synchronous
analog:

- Semaphore/worker-pool/Queue → Polars chunked streaming (`CHUNK_SIZE=10000`)
  + SQLite temp tables + `DBWorker` context-manager lifecycle
- API polling loop → `timings` dict ("import"/"mapping"/"export") measured with
  `perf_counter()`, returned via `ReportResult`
- Retry backoff → fail-fast error propagation; `DBWorker.__exit__` cleanup
- Queue backpressure → `CHUNK_SIZE` + `cache_size`/`temp_store` PRAGMAs

The required rework is **moderate** — strip the async fiction from the Purpose,
clean up the Scope-boundary citations to deleted phases (03, 09 ×2), re-ground
the severity-taxonomy examples to synchronous reality, and correct the phantom
`PERF-` collision warning. The six blocks need reframing (not deletion) to their
synchronous analogs. No source files are modified by this report.

**Candidate findings: 7** (F1–F7) are documented in §6 with full code references,
severity bands, and ownership attribution. They span all six re-based blocks and
the Report Output section. Key findings:

- F1 (LOW): stale docs claim `process_data` returns `None` while code returns `ReportResult` — directly contradicts Block 6's "measures vs asserts" question
- F3 (MEDIUM): CLI shows `duration` but not per-stage `timings` — the measurement exists but isn't observable to operators
- F4 (MEDIUM): coarse 3-stage timing, no per-action-type attribution within "mapping"
- F2 (LOW): `PERF-` collision warning is phantom (disproven by grep) — same pattern as Phase 10's `QLT-` warning
- F5 (LOW): read chunk (10k) vs export chunk (100k) mismatch, undocumented
- F6 (LOW): dead `DATA_TO_SQL_PARAMS`/`data_chunk_to_sql` pandas path contradicts live `CHUNK_SIZE=10000`
- F7 (LOW): `_apply_delete` executes identical DELETE twice (redundant cost on a hot path)

These do not gate on changes to `docs/00-overview/*` or `src/` — the audit is a
documentation-of-the-audit and the findings are the work the phase surfaces when
executed against the synchronous codebase.

---

## Appendix A — Evidence pedigree

- **Architecture source files read:** `src/mko_data_cleaner/app.py` (full, 165
  lines); `src/mko_data_cleaner/core/app_service.py` (full, 342 lines);
  `src/mko_data_cleaner/core/csv_service.py` (full, 375 lines);
  `src/mko_data_cleaner/core/db_service.py` (full, 788 lines);
  `src/mko_data_cleaner/core/models.py` (full, 195 lines);
  `src/mko_data_cleaner/core/utils.py` (full, 173 lines);
  `src/mko_data_cleaner/core/dict_service.py` (full, 279 lines).
- **Documentation read:** `docs/00-overview/architecture.md` (full, 259 lines);
  `docs/00-overview/current-behavior.md` (full, 230 lines);
  `docs/00-overview/data-flow.md` (line 165 for `process_data` return value).
- **Phase files read:** `.kilo/commands/audit/phases/13-audit-performance.md`
  (full, 161 lines); `10-audit-code-quality.md` (scope boundary, §6 Report
  Output); `12-audit-production-ops.md` (scope boundary, lines 22-28);
  `04-audit-data-pipeline.md` (scope boundary, lines 24-28).
- **Prior auditor reports read:** `.ai/reports/phase-10-auditor-report.md`
  (REBASE report format, §6.1 `QLT-` phantom-collision precedent);
  `.ai/reports/phase-15-auditor-report.md` (DELETE report format);
  `.ai/reports/phase-03-auditor-report.md` (Phase 13 mentioned at lines 65, 84,
  157-162 — already flagged as async-framed, "should be tracked as follow-on
  work"); `.ai/reports/phase-04-auditor-report.md` (§6 cites (13) "performance
  cost centres," recommends KEEP+reword; §7 candidate findings reference
  Phase 13-owned concerns).
- **Test files read:** `tests/test_app.py` (lines 257-280, 459-473 — asserts
  `ReportResult` construction and `timings` keys `{"import", "mapping",
  "export"}`, `duration > 0.0`); `tests/test_csv_service.py` (line 45 sets
  `chunk_size: 10000` in test fixture, confirming the fallback path).
- **Template read:** `.ai/audit/templates/audit-findings.md` (full, 108 lines).
- **Primitive grep:** `src/mko_data_cleaner/**/*.py` for
  `asyncio | async def | asyncio.run | Semaphore | Queue | requests | httpx |
  aiohttp | token | credential | api_key | backoff | polling | poll` →
  **zero code matches**. `data_chunk_to_sql` grep in `src/` and `tests/` →
  **zero call sites** (definition only at `db_service.py:505`).
- **Prefix grep:** repo-wide (excluding `.venv*`, `.git*`) for `PERF-` →
  **2 matches**, both in the Phase 13 phase file's own "already in use" warning
  text (lines 154 of both `.kilo/commands/audit/phases/13-audit-performance.md`
  and `.ai/tmp/scratch/.kilo/commands/audit/phases/13-audit-performance.md`).
  **Zero** `PERF-NNN` finding identifiers in any findings document, test file,
  or shipped source. The collision warning is phantom.
- **Findings directory:** `Test-Path .ai\audit\13-performance → False` (absent;
  only `templates/` and `problems/` exist under `.ai/audit/`).
- **Docker runtime:** Not started — this audit is a static architecture-vs-doc
  comparison; no server, API endpoint, or containerised service to probe. The
  grep and code-reading evidence above is conclusive for all six blocks.
