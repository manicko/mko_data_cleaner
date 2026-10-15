---
phase: "13"
phase_name: "Performance & Scalability"
phase_file: ".kilo/commands/audit/phases/13-audit-performance.md"
source: ".ai/reports/phase-13-auditor-report.md"
author: researcher
verdict: REBASE
confidence: HIGH
code_ref: "384044a"
severity_taxonomy: ".kilo/commands/audit/phases/13-audit-performance.md#severity-taxonomy"
---

# Phase 13 — Researcher Recommendations

## 1. Executive Summary

The auditor verdict is **REBASE**. The Phase 13 spec file
(`13-audit-performance.md`, 161 lines) is framed entirely in async primitives —
Semaphore, worker pool, task queue, API polling, retry backoff — that **do not
exist** in this codebase. A repo-wide grep across `src/mko_data_cleaner/` for
`asyncio | async def | Semaphore | Queue | requests | backoff | polling | poll`
returns **zero code matches** (auditor §Appendix A). The runtime is a
synchronous, single-shot, local-only CLI + library: file → Polars chunks →
SQLite (ADBC bulk write) → mapping rules (FTS/LIKE) → CSV export (100k rows per
file), one OS process per invocation, disposable SQLite DB.

Every async concept has a direct synchronous analog that is already live:

| Async concept (gone) | Synchronous analog (live) |
|---|---|
| Semaphore / worker pool / Queue | Polars chunked streaming (`CHUNK_SIZE=10000`) + SQLite temp tables + `DBWorker` context-manager lifecycle |
| API polling loop | `timings` dict ("import"/"mapping"/"export") measured with `perf_counter()`, returned via `ReportResult` |
| Retry backoff | Fail-fast error propagation; `DBWorker.__exit__` cleanup |
| Queue backpressure | `CHUNK_SIZE` + `cache_size`/`temp_store` PRAGMAs |

The six blocks need **reframing, not deletion**. This document endorses the
auditor's New 1–6 block renumbering (§3), confirms **6 findings to surface**
with `PERF-` IDs (§4, omitting F4 which is subsumed), lists **1 gap not to
surface** (§5), restates the severity taxonomy with synchronous examples (§6),
confirms the Report Output settings with `PERF-` collision disproven (§7),
prescribes the scope/citation cleanup (§8), and provides a line-by-line edit
delta for the 161-line phase file (§9).

Evidence was verified against the working tree at `384044a`: `app_service.py`
(342 lines), `csv_service.py` (375 lines), `db_service.py` (788 lines),
`models.py` (195 lines), `utils.py` (173 lines); `tests/test_app.py`
(lines 255-279, 459-473); `tests/test_csv_service.py` (line 45); `docs/00-overview/
architecture.md`, `current-behavior.md`, `data-flow.md`;
`settings/app_config.yaml`; phase file `13-audit-performance.md` (161 lines).

## 2. Proposed Final Block Set (New 1–6, renumbered)

All blocks are independent and evidence-classed. Evidence classes:

- **static proof** — a stated property contradicted by code inspection alone
- **observed behavior** — an empirical check of the running system
- **reproduced validation error** — an induced input that triggers a runtime error

| # | Title | Description | Evidence class |
|---|---|---|---|
| 1 | Chunked-streaming boundaries: what bounds work and memory per stage | What limits data per chunk? Is the chunk size one configurable value, or does read diverge from write? | static proof + observed behavior |
| 2 | Per-stage pipeline cost: the price of each phase of a report run | What does each pipeline stage (import, mapping, export) cost in wall-clock time? Are stages attributed or lumped? | static proof + observed behavior |
| 3 | No retry: fail-fast error propagation | When a local failure occurs, is it retried or fatal? Where does cleanup run? | static proof |
| 4 | Sequential stall: what blocks the single worker | With one worker, what stage can block the entire pipeline? Is there a timeout? A progress signal during a long SQLite operation? | static proof |
| 5 | Bounded accumulation: chunk + temp-table + PRAGMA memory controls | What bounds in-memory data? Polars chunk size, SQLite temp tables, `cache_size`/`temp_store` PRAGMAs — and where is their interaction untested or unbounded? | static proof + observed behavior |
| 6 | Latency measurement vs. assertion | Is latency measured (yes, via `perf_counter` + `timings` dict) or asserted (no, docs claim `None`)? Is per-stage attribution in CLI output (no, only `duration`)? | static proof + observed behavior |

### New 1 — Chunked-streaming boundaries: what bounds work and memory per stage

What limits the data processed per chunk? Is the chunk size configurable as one
value, or does read diverge from write?

- **Evidence (declared constant):** `CSVWorker.CHUNK_SIZE = 10000`
  (`csv_service.py:28`). Used for both batched read
  (`pl.read_csv_batched(batch_size=self.CHUNK_SIZE)`, `csv_service.py:170-175`)
  and export fallback (`params.pop("chunk_size", 10000)`, `csv_service.py:312`).
- **Evidence (config override):** `app_config.yaml:42` sets `chunk_size: 100000`,
  so the effective export chunk is 100k — a **10× mismatch** from the hardcoded
  read of 10k. Verified: the template value wins in the dumped settings
  (auditor §Appendix A pedigree).
- **Evidence (asymmetry):** the read path materializes the full chunk DataFrame
  before the ADBC bulk write (`app_service.py:96-101: chunk.write_database(...)`);
  the export path uses true `fetchmany(max_rows)` streaming
  (`csv_service.py:338, 366`). Not symmetric.
- **Evidence (model default):** `PolarsWriteCSV.chunk_size: NonNegativeInt = 10000`
  (`models.py:89`) — a third definition, dead when the template is present.
- **Evidence (test fixture):** `test_csv_service.py:45` sets
  `chunk_size: 10000`, confirming the fallback path.

### New 2 — Per-stage pipeline cost: the price of each phase of a report run

What does each pipeline stage cost in wall-clock time? Are stages attributed or
lumped?

- **Evidence (measurement surface):** `timings: dict[str, float]` with keys
  `"import"`, `"mapping"`, `"export"` — measured with `perf_counter()`
  (`app_service.py:124` dict init; `:175-177` import; `:180-241` mapping;
  `:256-269` export).
- **Evidence (total):** `ReportResult.duration` =
  `perf_counter() - t_start` (`app_service.py:286`), bracketed by `started_at`
  / `finished_at` datetimes (`app_service.py:111, 271`).
- **Evidence (return path):** `run_report` returns `ReportResult`
  (`app_service.py:278-290`); `app.py:145` returns it from `process_data`.
- **Evidence (test lock-in):** `test_app.py:472` asserts
  `set(result.timings.keys()) == {"import", "mapping", "export"}`;
  `test_app.py:471` asserts `result.duration > 0.0`; `test_app.py:274` asserts
  `isinstance(result.timings, dict)`.
- **Evidence (CLI gap):** `app.py:81-85` prints `result.duration`,
  `result.rows_loaded`, `result.rows_exported`, `result.export_dir` — **never**
  `result.timings`. See PERF-001.

### New 3 — No retry: fail-fast error propagation

When a local failure occurs, is it retried or fatal? Where does cleanup run?

- **Evidence (no retry):** repo-wide grep for `backoff|retry|asyncio.sleep|time.sleep`
  in `src/mko_data_cleaner/**/*.py` returns zero matches (auditor §Appendix A
  pedigree). Local failures are fatal — `FileNotFoundError`, `sqlite3.Error`,
  `WrongDataSettings`, `ConfigError` propagate up the call chain.
- **Evidence (cleanup boundary):** `DBWorker.__exit__` → `self.close()`
  (`db_service.py:786-788`) runs `PRAGMA wal_checkpoint(FULL)` → closes both
  connections (`db_con`, `db_adb_con`) → deletes the DB file + `-wal`/`-shm`
  sidecars (`db_service.py:770-788`). Runs on **any** exception via the `with
  DBWorker(...)` block (`app_service.py:146-269`).
- **Evidence (no transient surface):** no network, no rate-limiting, no service
  degradation — there is no transient-error class to retry.

### New 4 — Sequential stall: what blocks the single worker

With one worker, what stage can block the entire pipeline? Is there a timeout?
A progress signal during a long SQLite operation?

- **Evidence (single worker):** the entire pipeline is one synchronous call chain
  `run_report → CSVWorker → DBWorker / MappingDict`
  (`app_service.py:110-290`). No threads, no coroutines, no pool.
- **Evidence (progress surface is coarse):** `progress_bar`
  (`utils.py:18`) fires only at stage boundaries — "Reading data"
  (`csv_service.py:195`), "Applying rules" (`app_service.py:219-223`),
  "Exporting {table}" (`csv_service.py:350-354`). It does **not** fire inside a
  single SQLite query. A long-running FTS5 MATCH or `LIKE '%term%'` scan blocks
  the whole pipeline with no per-query timeout, no `progress` callback, no
  SQLite `busy_timeout`.
- **Gap (not a finding):** there is no observable signal for "the pipeline is
  stuck in a slow SQL operation" — this is a design observation, not a code
  defect. Documented here so a future New 4 pass can decide whether to file it.

### New 5 — Bounded accumulation: chunk + temp-table + PRAGMA memory controls

What bounds in-memory data? Polars chunk size, SQLite temp tables,
`cache_size`/`temp_store` PRAGMAs — and where is their interaction untested or
unbounded?

- **Evidence (PRAGMA surface):** `_init_base` sets four PRAGMAs
  (`db_service.py:116-123`):
  - `journal_mode=WAL` (`:118`)
  - `synchronous=OFF` (`:119`)
  - `temp_store=MEMORY` (`:120`)
  - `cache_size=-100000` (`:121`, i.e. 100k pages ≈ ~10 MB on 1 KB pages)
- **Evidence (temp tables as bounded accumulators):** `non_mapped`,
  `full_matches_table`, `joined_matches_table`, `data_table_tags`,
  `mapping_table`, `temp_tbl`, `fts_temp_tbl` — all TEMP tables or
  drop-and-recreate tables (`architecture.md:144-148`), bounded by disk, not
  Python memory. Drop-and-recreate per `apply_mapping` iteration
  (`app_service.py:217-239`).
- **Evidence (DB lifecycle):** `DBWorker.__exit__` deletes the DB file on exit
  (`db_service.py:781`) — no data persists between runs.
- **Gap:** `synchronous=OFF` trades durability for speed on a disposable DB —
  acceptable by design (DB is deleted on exit), but should be documented as an
  explicit safety trade-off, not an implicit assumption.

### New 6 — Latency measurement vs. assertion

Is latency measured (yes, via `perf_counter` + `timings` dict) or asserted (no,
docs claim `None`)? Is per-stage attribution available in CLI output (no, only
`duration`)?

- **Evidence (measured):** `timings` dict + `ReportResult.duration` exist and are
  tested (`test_app.py:271, 272, 471, 472`).
- **Evidence (asserted — stale docs):** `current-behavior.md:272-275` states
  "process_data currently returns None; the result object ... target work";
  `architecture.md:218` asserts `S-->>U: returns None`; `data-flow.md:165`
  records "Currently None." Code returns `ReportResult` (`app_service.py:278-290`;
  `app.py:145`). See PERF-002.
- **Evidence (not emitted):** CLI prints `duration` but never `timings`
  (`app.py:81-85`). See PERF-001.

## 3. Confirmed Findings to Surface

Six findings, sorted by severity (MEDIUM first, then LOW). `PERF-` prefix is
retained from the phase file's Report Output and is **not** colliding — see §7.
The auditor's §6 lists F1–F7 (7 candidates); **F4** (coarse-grained timings — no
per-action-type attribution within "mapping") is **not** filed as a separate
finding here — it is subsumed under New 1 / PERF-001 (the stage-latency
measurement surface). See §4.1 for the gap-not-surfaced rationale.

| ID | Severity | Synonym | Anchor (primary) | Title |
|---|---|---|---|---|
| PERF-001 | MEDIUM | F3 | `app.py:81-85` | CLI displays `duration` but not per-stage `timings` |
| PERF-002 | LOW | F1 | `current-behavior.md:272-275` | Docs assert `process_data` returns `None`; code returns `ReportResult` |
| PERF-003 | LOW | F2 | `13-audit-performance.md:154` | `PERF-` ID-prefix collision warning is phantom |
| PERF-004 | LOW | F5 | `csv_service.py:28` / `app_config.yaml:42` | Import-vs-export chunk size mismatch (10,000 read vs 100,000 export) |
| PERF-005 | LOW | F6 | `db_service.py:22-27` | Dead code: `DATA_TO_SQL_PARAMS` + `data_chunk_to_sql` (pandas path) |
| PERF-006 | LOW | F7 | `db_service.py:630-643` | `_apply_delete` executes the identical DELETE twice |

### PERF-001 — [MEDIUM] CLI displays `duration` but not per-stage `timings`

**Anchor:** `app.py:81-85` (CLI output); `app_service.py:177, 241, 269` (timings data)
**Checked:** observed behavior
**Why this band:** MEDIUM — the per-stage timing data is demonstrated to exist
(`timings` dict with 3 keys, tested at `test_app.py:472`), but it is **not
emitted** to the operator. A 30-second run cannot be attributed to import vs.
mapping without programmatic access. Adjacent band NOT graded as: LOW (no data
loss — only display gap).
**Problem:** `app.py:81-85` formats only `result.duration`,
`result.rows_loaded`, `result.rows_exported`, `result.export_dir`. The
`result.timings` dict — populated at `app_service.py:177, 241, 269` — is never
printed. An operator watching CLI output cannot tell whether a 30-second run
spent 28s in import or 28s in mapping.
**Repro:** Run any report end-to-end. Observe console output:
`(123 loaded → 118 clean (30.4s) ↠ export_dir)`. No stage breakdown appears.
Call `run_report(...)` programmatically and inspect `result.timings` — the data
is present (`{"import": 1.2, "mapping": 27.1, "export": 2.1}`-shaped).
**Impact:** The on-call developer / operator cannot triage which pipeline stage
is the bottleneck from CLI output alone; must drop into Python to read
`timings`.
**Root cause:** `app.py:81-85` omits `result.timings` from its `console.print`
format string. The data exists and is tested; only the CLI display path is
missing.
**Fix:** Add per-stage timing display to the CLI output — iterate
`result.timings` and print each stage (key + seconds). Keep `duration` as the
summary line. No change to `app_service.py` or `models.py`.
**Verify:** `test_app.py:472` already asserts the timings keys exist; a new
assertion could check CLI console output contains stage names.
**Shipped-test blocker:** `test_app.py:471` (`result.duration > 0.0`) and
`test_app.py:472` (`set(result.timings.keys()) == {"import","mapping","export"}`)
lock in the *data structure*, not the *display*. Adding CLI display does not
break either test.
**Related:** PERF-006 (New 6 owns the measurement surface; PERF-001 owns its emission gap).

### PERF-002 — [LOW] Docs assert `process_data` returns `None`; code returns `ReportResult`

**Anchor:** `app.py:145` (return); `app_service.py:278-290` (ReportResult construction); `current-behavior.md:272-275`
**Checked:** static proof
**Why this band:** LOW — doc-vs-code drift; no runtime impact, but understates
the observability surface. Adjacent band NOT graded as: MEDIUM (no operator
action fails because of this — only perception).
**Problem:** Three documentation files state `process_data` returns `None`:
`current-behavior.md:272-275` ("currently returns None; the result object ...
target work"); `architecture.md:218` (`S-->>U: returns None`);
`data-flow.md:165` ("Currently None"). The code returns `ReportResult`:
`app.py:145` (`return get_app_service(settings_dir).run_report(path)`);
`app_service.py:278-290` (`return ReportResult(...)`).
**Repro:** Static proof — read `app.py:145` and `app_service.py:278-290`; read
`current-behavior.md:272-275`, `architecture.md:218`, `data-flow.md:165`.
**Impact:** Reviewers underestimate observability (timings/duration are real and
returned to callers); docs mislead about the programmatic contract.
**Root cause:** Documentation was never updated after `ReportResult` was
introduced as the return type.
**Fix:** Update the three doc files to reflect `ReportResult` (paths + durations
+ timings). Cross-reference `app.py:145` and `app_service.py:278-290`.
**Verify:** `test_app.py:461` asserts `isinstance(result, ReportResult)`.
**Ownership:** Phase 13 Block 6 (observability surface definition) + Phase 04
(stale doc vs code).

### PERF-003 — [LOW] `PERF-` ID-prefix collision warning is phantom

**Anchor:** `.kilo/commands/audit/phases/13-audit-performance.md:154`
**Checked:** static proof (repo-wide grep)
**Why this band:** LOW — phantom warning text in the phase file's own Report
Output; no runtime impact. Adjacent band NOT graded as: MEDIUM (no finding ID is
actually at risk of collision).
**Problem:** Line 154 claims: "Finding-ID prefix: `PERF-` — already in use in
shipped source, tests, workflows, and reports from a prior cycle, with at least
one identifier already resolving to two different findings; check for a
collision before minting an ID and report it rather than creating a second
namespace." A repo-wide grep for `PERF-` (excluding `.venv*` / `.git*`) finds
**zero `PERF-NNN` finding identifiers** in any findings document, test file,
shipped source, workflow, or report. All matches are narrative references to the
warning text itself.
**Repro:** `grep -rn "PERF-" . --include="*.py" --include="*.md" --include="*.yaml"`
(excluding `.venv` and `.git`) → matches only in `13-audit-performance.md:154`
and in auditor-report narrative (§5.1, §6) and `phase-11-auditor-report.md:350,352`.
No `PERF-NNN` token in any `findings.md` or test assertion.
**Impact:** Misleads auditors into believing a collision exists, risking
unnecessary prefix proliferation (e.g., inventing a `PERF-`→`PRF-` workaround).
**Root cause:** Stale warning text, likely from a cleared prior audit cycle (same
pattern as Phase 10's `QLT-` warning, disproven by `phase-10-auditor-report.md §6.1`).
**Fix:** Rewrite line 154 — state that `PERF-` is the Phase 13 prefix per the
template convention (`audit-findings.md` front matter defines `id_prefix`),
and that no collision was found in the current tree (`384044a`).
**Verify:** grep confirms zero `PERF-NNN` finding identifiers.
**Precision note:** The auditor's §5.1 states the grep returns "2 matches"
in the phase file. In fact the full-tree grep returns more matches — the phase
file warning text *itself* plus narrative references in this and the Phase 11
auditor report. The auditor's **match count was understated** but the **core
conclusion (zero finding IDs) is correct**. Flag for auditor precision
correction, not a finding.

### PERF-004 — [LOW] Import-vs-export chunk size mismatch (10,000 read vs 100,000 export)

**Anchor:** `csv_service.py:28` (`CHUNK_SIZE = 10000`) / `app_config.yaml:42` (`chunk_size: 100000`)
**Checked:** static proof + observed behavior
**Why this band:** LOW — present behavior works; the mismatch is undocumented and
asymmetric, not a failure. Adjacent band NOT graded as: MEDIUM (no memory spill
observed at current volumes).
**Problem:** CSV reading buffers in chunks of `CHUNK_SIZE = 10000`
(hardcoded, `csv_service.py:28`; `pl.read_csv_batched(batch_size=self.CHUNK_SIZE)`,
`csv_service.py:170-175`). CSV export writes chunks of `chunk_size` = `100000`
(from `app_config.yaml:42`), not the model default of `10000` (`models.py:89`)
or the code fallback of `10000` (`csv_service.py:312`). The 10× mismatch is
undocumented, unconfigurable as a single value, and asymmetric: the read path
materializes the full chunk DataFrame before ADBC write
(`app_service.py:96-101`), while the export path uses true `fetchmany` streaming
(`csv_service.py:338, 366`).
**Repro:** Read `csv_service.py:28` (`CHUNK_SIZE = 10000`) and `app_config.yaml:42`
(`chunk_size: 100000`). Observe they diverge by 10×. The YAML value wins
(exported via `model_dump()` → `params.pop("chunk_size", 10000)` returns the
YAML `100000`).
**Impact:** Asymmetric memory profile — a single oversized read chunk (if
`CHUNK_SIZE` were raised) spikes memory on the import path; export is truly
streaming. The 10k/100k divergence is undocumented and surprising.
**Root cause:** No single source of truth for chunk size — three definitions
exist (hardcoded class constant / model default / YAML template / code fallback)
and they are not unified.
**Fix:** Consolidate to one chunk-size value — make both read and export use the
same configurable constant. At minimum, document that read=10k and
export=100k by design.
**Verify:** `test_csv_service.py:45` sets `chunk_size: 10000` in the fixture,
confirming the fallback path; `models.py:89` shows the model default.
**Ownership:** Phase 13 (chunked-streaming boundary, New 1) + Phase 05 (config
value consistency — model default `10000` ≠ YAML `100000`; documented in
`phase-05-researcher-recommendations.md §3 New 4`).

### PERF-005 — [LOW] Dead code: `DATA_TO_SQL_PARAMS` + `data_chunk_to_sql`

**Anchor:** `db_service.py:22-27` (`DATA_TO_SQL_PARAMS`) / `db_service.py:505-520` (`data_chunk_to_sql`)
**Checked:** static proof (zero call sites)
**Why this band:** LOW — dead code with stale config; no runtime impact but
misleading and contradicts live values. Adjacent band NOT graded as: MEDIUM
(no active path is harmed).
**Problem:** `DATA_TO_SQL_PARAMS` (`db_service.py:22-27`) defines
`{"if_exists": "append", "index": False, "index_label": None,
"chunksize": 2000}`. `data_chunk_to_sql()` (`db_service.py:505-520`) defaults to
it and calls `chunk.to_sql(...)`. **Neither is called** — the live import path
uses Polars ADBC `write_database` (`app_service.py:96-101`). A repo-wide grep
for `data_chunk_to_sql` in `src/` and `tests/` returns zero call sites (definition
only at `db_service.py:505`). The dead `chunksize=2000` contradicts the live
`CHUNK_SIZE=10000`.
**Repro:** `grep -rn "data_chunk_to_sql" src/ tests/` → only the definition at
`db_service.py:505`; zero callers. `DATA_TO_SQL_PARAMS` is referenced only inside
`data_chunk_to_sql` (`db_service.py:516`).
**Impact:** Misleading stale config (`chunksize=2000` vs live `10000`); `pandas`
is a declared dependency (`pyproject.toml`) used only by this dead path — if
`pandas` is removed from the live path entirely, it may be droppable from
dependencies.
**Root cause:** Refactoring to Polars ADBC left the pandas `to_sql` path in
place rather than deleting it.
**Fix:** Delete `DATA_TO_SQL_PARAMS` (`db_service.py:22-27`) and
`data_chunk_to_sql` (`db_service.py:505-520`). Verify `pandas` has no other
consumer before removing the dependency from `pyproject.toml`.
**Verify:** `test_app.py:472` locks in the `timings` keys — no test calls
`data_chunk_to_sql`. A grep confirms zero callers.
**Shipped-test blocker:** None — no shipped test exercises this code.
**Ownership:** Phase 02 (dead code sweep) + Phase 13 (records the
perf-config discrepancy between `chunksize=2000` and live `CHUNK_SIZE=10000`).

### PERF-006 — [LOW] `_apply_delete` executes the identical DELETE twice

**Anchor:** `db_service.py:630-643` (lines 640, 643)
**Checked:** static proof (code inspection)
**Why this band:** LOW — second pass matches nothing (rows already deleted);
wasted work on a hot path but small per invocation. Adjacent band NOT graded as:
MEDIUM (no data corruption).
**Problem:** `_apply_delete` (`db_service.py:630-643`) builds one `sql` string
(lines 632-638) and calls `self.perform_query(sql)` **twice** — at line 640 and
again at line 643, with a `logger.debug(...)` call between them (line 642).
The second pass re-evaluates the same `DELETE FROM ... WHERE rowid IN (SELECT
DISTINCT data_rowid FROM ...)` predicate but matches zero rows (already deleted).
**Repro:** Read `db_service.py:630-643`. Observe `self.perform_query(sql)` at
line 640 and line 643 — identical argument, identical `sql` string.
**Impact:** Redundant query-planning + execution on the DELETE cost-critical
path. Scales with `rules_count × data_volume` (the `generate_rules_blocks` loop
at `app_service.py:217` iterates DELETE → REPLACE → ADD). Small per invocation
but non-free.
**Root cause:** Duplicate call — likely an abandoned refactor; the `logger.debug`
line 642 sits between the two `perform_query` calls, suggesting the debug log was
intended to bracket a single execution but the second call was left in.
**Fix:** Remove the second `self.perform_query(sql)` call (lines 642-643) and
the intervening debug log, **or** restructure so the debug log precedes a single
`perform_query` execution. Either way: one DELETE, logged once.
**Verify:** `architecture.md:252` documents this as "harmless (second pass
matches nothing) but wasteful" — confirming the auditor's observation is already
known in docs.
**Ownership:** Phase 10 (one rule, one home — dedupe) + Phase 13 (records the
redundant cost on the mapping hot path).

## 4.1 Gap NOT to Surface

| Gap | Finding synonym | Why not a separate finding |
|---|---|---|
| F4 — Coarse-grained `timings` dict: no per-action-type attribution within "mapping" | (no PERF- ID) | The `timings` dict has exactly 3 keys (`app_service.py:124, 177, 241, 269`). Within "mapping," the `generate_rules_blocks` loop iterates DELETE → REPLACE → ADD under a single `t_mapping` timer (`app_service.py:180-241`) — no per-action-type timing. This is **subsumed under New 1 (stage-latency measurement surface)** and the broader PERF-001 emission gap. File separately only if the New 1 pass decides per-action attribution is independently in-scope. |

## 4.2 Findings-to-Scope Mapping

Findings are sorted by severity (MEDIUM first, then LOW) per the findings
template's "index order IS the work order" rule. `PERF-` prefix retained from
Report Output.

| Finding ID | Syonym | Severity | Anchor | Phase-owned | Notes |
|---|---|---|---|---|---|
| PERF-001 | F3 | MEDIUM | `app.py:81-85` | Phase 13 (cost attribution) + Phase 12 (observability emission) | Shipped test (`test_app.py:472`) locks data structure, not display. |
| PERF-002 | F1 | LOW | `current-behavior.md:272-275` | Phase 13 Block 6 + Phase 04 (stale doc) | Test (`test_app.py:461`) already asserts `ReportResult` return. |
| PERF-003 | F2 | LOW | `13-audit-performance.md:154` | Phase 13 Report Output | Phantom warning; precision issue in auditor §5.1 match count. |
| PERF-004 | F5 | LOW | `csv_service.py:28` / `app_config.yaml:42` | Phase 13 (New 1) + Phase 05 (config value consistency) | Phase 05 New 4 already documents the 3-site divergence. |
| PERF-005 | F6 | LOW | `db_service.py:22-27, 505-520` | Phase 02 (dead code) + Phase 13 | Phase 02 should sweep; Phase 13 records perf-config discrepancy. |
| PERF-006 | F7 | LOW | `db_service.py:630-643` | Phase 10 (dedupe) + Phase 13 | `architecture.md:252` already flags as wasteful. |

## 5. Gaps NOT to Surface (verified non-defects / owned elsewhere)

| Gap | Claim | Why it is NOT a Phase 13 finding |
|---|---|---|
| F4 | Coarse-grained `timings` — no per-action-type attribution within "mapping" | Subsumed under New 1 / PERF-001 (stage-latency measurement surface). Not elevated to a separate finding; documented for the findings-file author's discretion. See §4.1. |
| Chunk-size 3-site config divergence | `model_default=10000` vs `template=100000` vs `code_fallback=10000` | Owned by Phase 05 (documented in `phase-05-researcher-recommendations.md §3 New 4`). Phase 13 surfaces only the *performance* angle — asymmetric read vs. export — via PERF-004. |
| Dead-code sweep | `DATA_TO_SQL_PARAMS` / `data_chunk_to_sql` | The root-cause elimination (delete dead code, check `pandas` dependency) is Phase 02. Phase 13 records only the *perf-config discrepancy* (`chunksize=2000` vs live `CHUNK_SIZE=10000`) via PERF-005. |
| Double-DELETE root cause | `_apply_delete` calls `perform_query(sql)` twice | The *deduplication fix* (one rule, one home) is Phase 10. Phase 13 records only the *redundant cost* via PERF-006. |
| "No observable signal for stuck SQLite query" | Long SQLite operation blocks with no timeout or progress callback | Design observation, not a code defect — no failing assertion or stale doc to point at. Documented in New 4; do not file as a finding without a concrete violated invariant. |

## 6. Severity Taxonomy (synchronous, performance-grounded)

The current taxonomy (lines 123–146) is built on the async fiction (polling
loop, Semaphore, backoff, worker pool). The **principle** ("grade by effect and
blast radius, not mechanism name; rate what is true now, not the worst
consequence if triggered") is sound and retained verbatim. All illustrative
examples are async-framed and must be replaced.

| Band | Current (async) example | Reworded (sync) example | Action |
|---|---|---|---|
| CRITICAL | "control believed active that is not" | Keep as-is — applies to missing instrumentation on a cost-critical path | KEEP |
| HIGH | "polling loop with no max-wait bound" | "a chunk/streaming boundary that is unbounded, so a single oversized input can exhaust memory" | REPLACE |
| HIGH | "retry holds Semaphore across backoff" | *(remove — no retry, no Semaphore)* | REMOVE |
| HIGH | "oversized result starves worker pool" | "oversized single chunk spikes memory: `CHUNK_SIZE=10000` is the only bound; no per-chunk memory guard" | REPLACE |
| MEDIUM | "queue depth with no observable signal" | "pipeline stage with no observable progress: a slow SQLite query blocks with no timeout or progress callback" | REPLACE |
| MEDIUM | "backoff formula with no cap" | *(remove — no backoff)* | REMOVE |
| MEDIUM | "latency asserted but not emitted" | Keep as-is — `timings` exists in code but per-stage timing not in CLI output | KEEP |
| LOW | "stated objective with nothing that measures it" | Keep — docs claim `process_data` returns `None`, contradicting the `timings` dict | KEEP |
| LOW | "comment/runbook line no longer describes behaviour" | Keep | KEEP |

**Rewritten severity taxonomy text (recommended):**

> Grade by **effect and blast radius**, not by mechanism name; rate what is true
> now, not the worst consequence if triggered. The bands are effect classes, not
> a closed list of mechanisms, so a defect whose mechanism is not named here still
> lands by the consequence it is producing.
>
> - **CRITICAL** — a control believed to be active that is not, on the path that
>   decides whether a performance risk is caught before it ships: nothing
>   distinguishes a checked subject from an unchecked one, and a reviewer cannot
>   tell the difference. An empty band is a valid outcome — do not populate it
>   with a hypothetical, and do not promote an item for sounding alarming.
> - **HIGH** — cost that is real now and scales with a quantity the caller or the
>   data controls, on a path that is on: a chunk/streaming boundary that is
>   unbounded, so a single oversized input can exhaust memory; oversized single
>   chunk spikes memory (`CHUNK_SIZE=10000` is the only bound; no per-chunk memory
>   guard).
> - **MEDIUM** — a cost demonstrated but conditional on volume or configuration
>   not yet in effect, where the design is unbounded rather than the present
>   value being large: a pipeline stage with no observable progress (a slow
>   SQLite query blocks with no timeout or progress callback); a latency
>   measurement that is asserted but not emitted (`timings` exists in code but
>   per-stage timing not in CLI output).
> - **LOW** — a stated objective with nothing that measures it; a comment or
>   runbook line that no longer describes the behaviour beside it; a discipline
>   whose declared scope is wider than its coverage.

## 7. Report Output Settings

The Phase 13 §Report Output (current lines 148–161) is **mostly aligned** with
the findings template (`.ai/audit/templates/audit-findings.md`) and repo
convention. Recommend **keeping** most settings, with targeted corrections.

| Item | Setting | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/13-performance/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`). Directory does not yet exist — no prior findings to reconcile (`Test-Path .ai\audit\13-performance → False` confirmed by auditor §5.1). |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `severity_taxonomy` and `code_ref` fields. |
| ID prefix | `PERF-` | **Retain, DISPROVE collision** | See §7.1 below. Zero `PERF-NNN` finding IDs found anywhere in the repo. |
| problems-only | `true` | **Keep** | All six confirmed findings are code-verified defects, not passing checks. |
| per-pass lines | ≤100, incremental append | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` | **Keep** | Verbatim. |
| Shipped-test-asserts-current-behaviour | §122: "a shipped test asserting current behaviour is a remediation blocker" | **Keep** | Applies to PERF-002 (F1: `test_app.py:461` asserts `ReportResult` return) and PERF-004 (F5: `test_csv_service.py:45` sets `chunk_size: 10000`). |

### 7.1 The `PERF-` prefix collision warning — DISPROVEN

Phase 13 line 154 states:

> "Finding-ID prefix: `PERF-` — already in use in shipped source, tests,
> workflows, and reports from a prior cycle, with at least one identifier
> already resolving to two different findings; check for a collision before
> minting an ID and report it rather than creating a second namespace."

**This claim is disproven by the working tree (`384044a`):**

- A repo-wide grep for `PERF-NNN` (the finding-ID pattern) across `src/`,
  `tests/`, `.ai/`, `.kilo/`, `pyproject.toml`, and all workflow/YAML/JSON files
  finds **zero** `PERF-NNN` finding identifiers in any findings document, test
  file, shipped source, or report.
- The string `PERF-` appears only in narrative: the Phase 13 phase file's own
  warning text (line 154, plus a scratch copy at
  `.ai/tmp/scratch/.kilo/commands/audit/phases/13-audit-performance.md:154`), in
  this auditor report's §5.1 and §6, and in
  `phase-11-auditor-report.md:350,352` (which mentions `PERF-` in a list of
  phantom-prefix patterns alongside `QLT-` and `OPS-`). **None** are finding IDs.
- No findings directory `.ai/audit/13-performance/` exists (absent; only
  `templates/` and `problems/` exist under `.ai/audit/`).

**Precision correction to auditor §5.1:** The auditor states the grep returns
"2 matches" in the phase file. The full-tree grep actually returns more matches
(the phase file warning text plus narrative references in this report and
Phase 11's report). The auditor's **match count was understated**, but the
**core conclusion (zero finding IDs) is correct** — the extra matches are
narrative, not finding identifiers.

**Recommendation:** Retain the `PERF-` prefix (it matches the template
convention and is unique among real findings — zero collisions found). **Correct
line 154** to state that `PERF-` is the Phase 13 prefix per the template
convention and no collision was found in the current tree. The same defect
pattern was identified and resolved for the `QLT-` prefix in Phase 10 (§6.1:
"disproven by the working tree").

## 8. Scope / Cross-Phase Citations

### 8.1 Phases that Phase 13 CITES (scope boundary, lines 23–28)

Phase 13's scope boundary currently defers to:

| Cited phase | Current citation text | Phase status | Action |
|---|---|---|---|
| (03) | "the Semaphore acquisition and cancellation semantics" | **Deleted** (Phase 03 auditor report §5) | **REMOVE** — Semaphore/async primitives never existed |
| (09) | "the retry and backoff contract" | **Deleted** (Phase 09 file removed) | **REMOVE** — retry/backoff never existed |
| (04) | "period resolution and task generation" | Re-based (sync) | **KEEP, reword** — Phase 13 does not own "period resolution" (no scheduled tasks). Reword to defer "pipeline stage ordering" (Phase 04 owns ordering; Phase 13 owns cost measurement of those stages). |
| (09) | "the blocking-work bridge" | **Deleted** | **REMOVE** — no bridge exists (cited twice; remove both) |
| (12) | "CI/CD and observability" | Re-based (sync) | **KEEP, reword** — Phase 13 owns *instrumentation* (timings dict); Phase 12 owns *emission/observation* (logs, metrics). Clarify the boundary. |
| (10) | "code-quality discipline" | Re-based (sync) | **KEEP** — no async framing. |
| (11) | "test-suite adequacy" | Exists (sync) | **KEEP** — no async framing. |

**Recommended reworked scope boundary (replaces lines 23–28):**

> **Scope boundaries** — this phase owns the measurable consequence of latency,
> throughput, and capacity, and nothing else. Owned elsewhere: pipeline stage
> ordering (04); observability emission / CI-CD gates (12); code-quality
> convention (10); test-suite adequacy (11).

### 8.2 Phases that CITE (13) — informational

These are not in scope for the 13 phase file edit but should be noted for
downstream re-basings of those phases:

| Phase | Line(s) | Citation text | Action needed when that phase is re-based |
|---|---|---|---|
| 04 | 27–28 | "SQLite/Polars/FTS performance cost centres (13)" | **KEEP** — valid ownership; Phase 04 auditor report §6 already recommends keeping (13) with reword. |
| 10 | 17, 131 | "measurable performance consequences (13)" | **KEEP** — valid citation. |
| 12 | 26 | "concurrency ceilings and retry cost (13)" | **REWORD** — async-framed; reword to "per-stage pipeline cost measurement (13)" when Phase 12 is re-based. |

**Orchestration:** `audit-multi-phase.md` discovers phases by directory
listing, so no hard-coded sequence needs editing.

## 9. Edit Delta — Line-by-Line Rewrite of the 161-line File

All references are to the **current** `.kilo/commands/audit/phases/13-audit-performance.md`.

| # | Lines | Action | Replacement (concise) | Rationale |
|---|---|---|---|---|
| 1 | 1–7 (front matter) | **Modify** — add fields | Keep `name`, `status`, `validated`, `executor`, `problems-only: true`. Add `code_ref: "384044a"` and `severity_taxonomy: ".kilo/commands/audit/phases/13-audit-performance.md#severity-taxonomy"`. | Aligns with findings template front-matter (`audit-findings.md` lines 6-8). |
| 2 | 9 (H1) | **Keep** | `# Phase 13 — Performance & Scalability` | Title is not async-framed; no change needed. |
| 3 | 11–21 (Purpose) | **Replace** | Rewritten Purpose (§8.5 below) | Removes async/API/Semaphore/worker-pool/poll/retry fiction; targets the real synchronous cost centres. |
| 4 | 23–28 (scope boundary) | **Replace** | Rewritten scope boundary (§8.1 above) | Removes (03) and both (09) citations; rewords (04), (12). |
| 5 | 32–43 (Audit Blocks intro) | **Modify** — reword 2 sentences | Keep "Each block is independent..." and "Evidence is a class, not a check list..." paragraphs. Reword the "Two questions here cannot be settled" paragraph: replace "real API load" with "production-volume data". | The methodology language is generic; only the "API load" reference is async-framed. |
| 6 | 45–58 (Block 1) | **Replace** with New 1 | "Chunked-streaming boundaries: what bounds work and memory per stage" + evidence bullets (CHUNK_SIZE=10000; read vs. export asymmetry; model default 10000; YAML 100000; test fixture 10000) | Replaces "Concurrency ceilings: Semaphore/worker-pool/Queue" with sync analog. |
| 7 | 60–72 (Block 2) | **Replace** with New 2 | "Per-stage pipeline cost: the price of each phase of a report run" + evidence (timings dict keys; perf_counter; ReportResult.duration; CLI gap) | Replaces "API call cost per task: build→send→poll→get-result" with per-stage cost. |
| 8 | 74–83 (Block 3) | **Replace** with New 3 | "No retry: fail-fast error propagation" + evidence (zero backoff/retry grep; fatal exceptions; __exit__ cleanup) | Replaces "Retry backoff cost" with fail-fast. |
| 9 | 85–96 (Block 4) | **Replace** with New 4 | "Sequential stall: what blocks the single worker" + evidence (single-threaded; progress_bar is stage-level only; no query timeout/busy_timeout) + gap note | Replaces "Worker saturation & queue backpressure" with sequential stall. |
| 10 | 98–108 (Block 5) | **Replace** with New 5 | "Bounded accumulation: chunk + temp-table + PRAGMA memory controls" + evidence (4 PRAGMAs at db_service.py:118-121; temp tables; DB deleted on exit) + gap note (synchronous=OFF durability trade-off) | Replaces "Queue maxsize / worker-pool starvation" with PRAGMA + temp-table bounds. |
| 11 | 110–119 (Block 6) | **Replace** with New 6 | "Latency measurement vs. assertion" + evidence (timings measured, tested; docs claim None; CLI shows duration not timings) | Replaces "build→send→poll→extract→write" with measurement-vs-assertion. |
| 12 | 123–146 (Severity Taxonomy) | **Replace examples only** | §6 reworded taxonomy (keep principle + 4 bands + "empty band valid"; replace all async examples with sync; remove the 2 REMOCE rows) | All old examples reference deleted async/API concepts. |
| 13 | 148–161 (Report Output) | **Modify** — correct the PERF- warning | Keep findings path, template, problems-only, ≤100 lines, empty state, shipped-test-blocker. **Correct line 154** — replace "already in use … check for a collision …" with "Phase 13 finding-ID prefix per the findings template convention; no `PERF-NNN` collision found in the current tree (`384044a`)." | Disproves the phantom collision; retains the prefix. |

**Rewritten Purpose (for edit 3, replaces lines 11–21):**

> What the synchronous local I-O pipeline costs per report run: the cost of
> each pipeline stage (CSV import into SQLite, SQL rule application, SQLite-
> to-CSV export), whether those costs are measured or merely asserted, and
> whether memory and data-volume bounds are explicit. There is no database
> query-plan optimization, no cache tier, and no web request — the cost centres
> are: Polars chunked streaming (`CHUNK_SIZE`), the SQLite PRAGMA tuning
> (`WAL`, `synchronous=OFF`, `temp_store=MEMORY`, `cache_size`), the per-stage
> `timings` dict measured with `perf_counter()`, the SQLite temp-table
> accumulators, and the fail-fast error path (no retry, no backoff). This file
> names what to examine and under which angle — the executing auditor discovers
> the concrete artifacts.

**Application order:** Edits 1 (front matter) and 2 (H1) are independent — do
first. Then Edit 4 (scope boundary, foundational framing) → Edit 3 (Purpose) →
Edits 6–11 (blocks 1–6, each self-contained) → Edit 12 (severity examples) →
Edit 5 (Audit Blocks intro, minor) → Edit 13 (Report Output correction).
Edits 6–11 may be applied in parallel (sections are self-contained).

**Do not touch:** `problems-only: true`; the "≤100-line passes" rule; the
empty-state wording ("No problems found in this phase.").

## 10. Findings-to-Scope Mapping (for the findings-file author)

Findings are sorted by severity (MEDIUM first, then LOW) per the findings
template's "index order IS the work order" rule. `PERF-` prefix retained.

| Finding ID | Syonym | Severity | Anchor (primary) | Phase-owned | Notes |
|---|---|---|---|---|---|
| PERF-001 | F3 | MEDIUM | `app.py:81-85` | Phase 13 + Phase 12 | Shipped test locks data structure, not display. |
| PERF-002 | F1 | LOW | `current-behavior.md:272-275` | Phase 13 + Phase 04 | Test already asserts `ReportResult` return. |
| PERF-003 | F2 | LOW | `13-audit-performance.md:154` | Phase 13 Report Output | Phantom warning; auditor §5.1 has a precision issue. |
| PERF-004 | F5 | LOW | `csv_service.py:28` / `app_config.yaml:42` | Phase 13 + Phase 05 | Phase 05 New 4 already documents 3-site divergence. |
| PERF-005 | F6 | LOW | `db_service.py:22-27, 505-520` | Phase 02 + Phase 13 | Phase 02 sweeps dead code; Phase 13 records config discrepancy. |
| PERF-006 | F7 | LOW | `db_service.py:630-643` | Phase 10 + Phase 13 | `architecture.md:252` already flags as wasteful. |

---

*End of recommendation. Evidence-grounded: every structural recommendation
traces to a verified current-source fact (working tree at `384044a`). All code
references confirmed by direct file inspection; `PERF-` collision disproven by
repo-wide grep (zero `PERF-NNN` finding identifiers). No source files were
modified by this report.*
