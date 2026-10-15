---
name: 13-performance
status: draft
validated: no
executor: auditor
problems-only: true
code_ref: "384044a"
severity_taxonomy: "13-audit-performance.md#severity-taxonomy"
---

# Phase 13 — Performance & Scalability

## Purpose

Measures the wall-clock cost of the synchronous, single-shot local pipeline —
file → Polars chunks (`CHUNK_SIZE=10000`) → SQLite (ADBC bulk write) → mapping
rules (DELETE→REPLACE→ADD via FTS5/LIKE) → CSV export (`chunk_size=100000`) — and
whether the numbers the system believes about itself are measured or asserted. No
network, no database query outside SQLite, no cache, no web request. Cost centres:
Polars chunked I/O (read `CHUNK_SIZE=10000` vs export `chunk_size=100000`),
the FTS5/LIKE mapping rule pass, SQLite PRAGMAs (`WAL`, `synchronous=OFF`,
`temp_store=MEMORY`, `cache_size`), the per-stage `timings` dict (`perf_counter()`),
SQLite temp-table accumulators, and fail-fast error propagation. **No concurrency,
retries, or backoff** — single-threaded (`AppService.run_report`,
`app_service.py:110-290`; `DBWorker.__exit__` cleanup, `db_service.py:786-788`).

**Scope boundaries** — this phase owns the measurable consequence of latency,
throughput, and capacity, and nothing else. Owned elsewhere: pipeline stage
ordering (04); observability emission (12); code-quality discipline (10); test-
suite adequacy (11).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. One question cannot be settled here — a latency measurement against
production-volume data — so establish each from what is observable and state what
could not be verified and why.

### 1. Chunked-streaming boundaries: what bounds work and memory per stage

What limits data per chunk? Is the chunk size one configurable value, or does
read diverge from write? `CSVWorker.CHUNK_SIZE = 10000` (`csv_service.py:28`)
is the hardcoded read boundary; the import loop consumes it via
`pl.read_csv_batched(batch_size=self.CHUNK_SIZE, ...)` (`csv_service.py:170-172`)
inside `_import_data` (`app_service.py:95-96`), where each chunk is fully
materialized before the ADBC bulk write (`chunk.write_database(...,
engine="adbc")`, `app_service.py:96-101`). Export uses a *different* size:
`max_rows = params.pop("chunk_size", 10000)` (`csv_service.py:312`) — the fallback
is 10 000 but the template sets `chunk_size: 100000` (`app_config.yaml:42`), so
the effective export chunk is 100 k (`csv_service.py:338, 366`, true
`fetchmany` streaming). A third definition lives in the model:
`PolarsWriteCSV.chunk_size: NonNegativeInt = 10000` (`models.py:89`), dead while
the template is present; `test_csv_service.py:45` pins `chunk_size: 10000`.

### 2. Per-stage pipeline cost: the price of each phase of a report run

What does each pipeline stage cost in wall-clock time? Are stages attributed or
lumped? `run_report` (`app_service.py:110-290`) initializes `t_start =
perf_counter()` (`:112`) and `timings: dict[str, float] = {}` (`:124`), then
brackets three stages: **import** (`:175-177`), **mapping** (`:180-241`;
`timings["mapping"]` at `:241`; `generate_rules_blocks` loop at `:217-239`
spans DELETE→REPLACE→ADD), **export** (`:256-269`; `timings["export"]` at
`:269`). The dict is returned on `ReportResult` (`app_service.py:278-290`);
total wall-clock is `duration = perf_counter() - t_start` (`app_service.py:286`);
`app.py:145` returns it from `process_data`. Tests lock the shape
(`test_app.py:268, 274, 471-473`). The CLI does **not** surface the breakdown
(`app.py:81-85` prints only `duration`, `rows_loaded`, `rows_exported`,
`export_dir`).

### 3. No retry: fail-fast error propagation

When a local failure occurs, is it retried or fatal? Where does cleanup run? The
pipeline has **no retry, no backoff**. A repo-wide grep for
`backoff|retry|asyncio.sleep|time.sleep` in `src/mko_data_cleaner/**/*.py` returns
zero matches (auditor §Appendix A). Local failures — `FileNotFoundError`,
`sqlite3.Error`, `WrongDataSettings`, `ConfigError` — propagate up the synchronous
call chain unwrapped. Cleanup is the `DBWorker` context manager: `__exit__`
(`db_service.py:786-788`) always calls `self.close()`, which checkpoints WAL
(`PRAGMA wal_checkpoint(FULL)`, `db_service.py:774`), closes both connections
(`db_con`, `db_adb_con`, `db_service.py:778-779`), then deletes the DB file +
`-wal`/`-shm` sidecars (`_delete_base_files`, `db_service.py:762-767`). It runs
on **any** exception — the `with DBWorker(...)` block spans `app_service.py:146-269`
(`.__exit__` has no `exc_type` guard). No transient-error class exists (no network,
no rate-limiting).

### 4. Sequential stall: what blocks the single worker

With one worker, what stage can block the entire pipeline? Is there a timeout? A
progress signal during a long SQLite operation? The entire pipeline is one
synchronous call chain — `run_report` (`app_service.py:110-290`) → `CSVWorker` /
`DBWorker` / `MappingDict` — with no threads, coroutines, or pool. The only
progress signal is `progress_bar` (`utils.py:18`), which fires only at stage
boundaries: "Reading data" (`csv_service.py:195`), "Applying rules"
(`app_service.py:219-223`), "Exporting" (`csv_service.py:350-354`). It does **not**
fire inside a single SQLite query. A long-running FTS5 MATCH or `LIKE '%term%'`
scan (`link_search_table` at `db_service.py:431`; `insert_matches` at
`db_service.py:557-586`) blocks the whole pipeline with no per-query timeout, no
`busy_timeout`, and no `progress` callback — distinguishable from a slow query
only by wall-clock elapsed. *Gap:* no signal distinguishes a slow SQLite query from
a stuck one (design observation, not a code defect).

### 5. Bounded accumulation: chunk + temp-table + PRAGMA memory controls

What bounds in-memory data? Three bounding layers exist:

1. **Polars chunk size** — `CHUNK_SIZE = 10000` (`csv_service.py:28`) bounds the
   DataFrame per read iteration (`app_service.py:96`); export buffers at most one
   `fetchmany` chunk at a time (`csv_service.py:338, 366`).
2. **SQLite temp tables** — `non_mapped`, `full_matches_table`,
   `joined_matches_table`, `data_table_tags`, `mapping_table`, `temp_tbl`,
   `fts_temp_tbl` (`architecture.md:144-148`) live on disk, not Python memory;
   `mapping_table`/`temp_tbl` are drop-and-recreate per `apply_mapping`
   iteration (`app_service.py:217-239`).
3. **PRAGMA memory controls** — `_init_base` (`db_service.py:116`) sets
   `journal_mode=WAL` (`:118`), `synchronous=OFF` (`:119`), `temp_store=MEMORY`
   (`:120`), `cache_size=-100000` (`:121`, ~10 MB). `close()` checkpoints WAL
   (`db_service.py:774`); DB deleted on exit (`db_service.py:781`). *Gap:*
   `synchronous=OFF` (`db_service.py:119`) trades durability for speed on a
   disposable DB (`architecture.md:136-137`) — an explicit documented trade-off.

### 6. Latency measurement vs. assertion

Is latency measured (yes, via `perf_counter` + `timings` dict) or asserted (no, docs
claim `None`)? Is per-stage attribution in CLI output (no, only `duration`)? Latency
is **measured, not asserted**: `run_report` records `t_start`/`end_time` datetimes
(`app_service.py:111, 271`), three `perf_counter` deltas in `timings`
(`app_service.py:175-177, 241, 269`), and `ReportResult.duration = perf_counter() -
t_start` (`app_service.py:286`) — all returned via `ReportResult`
(`app_service.py:278-290`; `app.py:145`). Tests validate the contract
(`test_app.py:268, 274, 471-473`). Yet docs are **stale**: `current-behavior.md:272-275`
and `architecture.md:218` and `data-flow.md:165` all claim `None`. The CLI prints
`duration` but never the per-stage `timings` (`app.py:81-85`).

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true now.
An empty band is a valid outcome — do not populate it with a hypothetical.

- **CRITICAL** — a control believed to be active that is not, on the path that
  decides whether a performance risk is caught before it ships. *None found.*
- **HIGH** — cost that is real now and scales with a quantity the caller or the
  data controls: an unbounded chunk/streaming boundary lets a single oversized
  input exhaust memory; `CHUNK_SIZE=10000` is the only bound
  (`csv_service.py:28`; no per-chunk memory guard). *None found.*
- **MEDIUM** — a cost demonstrated but conditional on volume or configuration
  not yet in effect, or a measurement asserted but not emitted:
  - **PERF-001** (`app.py:81-85`) — CLI displays `duration` but not per-stage
    `timings`; the data is measured and tested (`test_app.py:472`) but never
    emitted to the operator.
- **LOW** — drift and reproducibility gaps, no runtime consequence:
  - **PERF-002** (`current-behavior.md:272-275`; `app.py:145`; `app_service.py:278-290`)
    — docs assert `process_data` returns `None`; code returns `ReportResult`.
  - **PERF-003** (`13-audit-performance.md:154`) — the `PERF-` ID-prefix collision
    warning is phantom (zero `PERF-NNN` finding identifiers in the tree at
    `384044a`).
  - **PERF-004** (`csv_service.py:28`; `app_config.yaml:42`) — import-vs-export
    chunk size mismatch: read 10 000 vs. export 100 000.
  - **PERF-005** (`db_service.py:22-27, 505-520`) — dead code: `DATA_TO_SQL_PARAMS`
    + `data_chunk_to_sql` (pandas path, `chunksize=2000`) contradicts live
    `CHUNK_SIZE=10000`.
  - **PERF-006** (`db_service.py:630-643`) — `_apply_delete` executes the
    identical DELETE twice (lines 640, 643); second pass matches zero rows.

## Report Output

- Findings path: `.ai/audit/13-performance/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, cross-finding analysis, roadmap, rollout safety, appendices
- Finding-ID prefix: `PERF-` — Phase 13's performance prefix; repo-wide grep
  confirms zero `PERF-NNN` identifiers in any findings document, test, or shipped
  source. The collision warning is phantom (stale, same pattern as Phase 10's
  disproven `QLT-` warning). Retained with no collision.
- Incremental append, ≤100 lines per pass; `problems-only: true` — findings only,
  every finding needs runtime evidence and the exact consequence; empty state,
  exactly: `No problems found in this phase.`
