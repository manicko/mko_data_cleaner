---
phase: "04"
phase_name: "Data Pipeline & Report Lifecycle (re-based)"
phase_file: ".kilo/commands/audit/phases/04-audit-data-pipeline.md"
report_status: "draft"
scope: >-
  re-basing analysis of the async/Mediascope-API-framed Phase 04 audit spec
  against the current synchronous, local-only, single-shot data pipeline:
  CSV import (Polars) → SQLite (disposable, WAL, FTS5) → CSV export. No API,
  no period resolution, no task generation, no retry.
recommendation: REBASE (do not delete)
---

# Audit Report — Phase 04 Data Pipeline (Re-basing)

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a synchronous, single-shot CLI + library. A report run is
**one in-process pipeline** (`app_service.py:110-290` `AppService.run_report`),
not a set of API tasks. The real lifecycle is:

```
data_path (report dir)
 → CSVWorker.get_dictionary()      # Polars → mapping rules (DELETE/REPLACE/ADD order)
 → csv_worker.check_date_column()  # regex sample-based date detection
 → with DBWorker(...) as db:       # sqlite3 + adbc_driver_sqlite; WAL, disposable
     create_table_with_index()     # data_table + *_distinct index table + FTS5 virtual + triggers
     _import_data()                # Polars read_csv_batched (chunks of 10k) → ADBC → SQLite
     insert_matches_from_fts()/insert_matches()  # FTS5 MATCH + LIKE rules → full_matches_table
     for action, data in generate_rules_blocks():  # fixed DELETE → REPLACE → ADD
         write mapping_table → apply_mapping(action)
     build_non_mapped()            # rows with NULL extra columns
     export_sql_to_csv(..., "null_data")  # → <base_path>/null_data_*.csv.gz
     sync_with_data_table()        # UPDATE/INSERT/DELETE index↔data
     export_sql_to_csv(..., "clean_data") # → <export_path>/data_table_*.csv.gz
 → ReportResult (timestamps, durations, row counts, timings)
```

**What is absent (per task brief + code-verified):**
- No `date_filter` / `last_time` / period resolution — the run processes **all**
  CSV files in the import folder for the current report directory.
- No `asyncio.Queue`, no worker pool, no `send → wait → extract` API loop.
- No `TVTask`, no per-task YAML error files.
- No retry/backoff (no network) — that whole sub-surface belongs to Phase 09,
  which is itself async-framed and has no subject here.

Phase 03 (Semaphore/Queue/worker-pool) has been **deleted** (see
[Phase 03 auditor report](../.ai/reports/phase-03-auditor-report.md)), so the
legacy "(03)" ownership citation in this phase's Scope must be removed.

Unlike Phase 03 (pure deletion) or Phase 02 Block 2 (pure deletion of a
credential premise), **Phase 04 should be RE-BASED, not deleted**: the data
pipeline is the core of this tool and the concepts of input scope, rule-block
ordering, data-shape fidelity, export safety, and failure observability are all
live and distinct. Only the *framing* (async API, period/interval, task
generation, YAML serialisation) is obsolete.

## 2. Per-Block Assessment (current 1–6)

| # | Current block | Fit | Outdated (what is wrong) | Replacement concept |
|---|---|---|---|---|
| 1 | Period resolution & API clamping | **None** | No `date_filter`/`last_time`; no API; no "available-period" clamping; no `multiple_files` slicing. A run has one scope: the CSV files in `import_folder`. | **Run input scope & boundary detection.** Audit what inputs the run consumes (file discovery in `import_folder`), the date-column auto-detection (`CSVWorker.check_date_column`, `csv_service.py:124-157`), and whether scope is bounded (no silent truncation). |
| 2 | Task generation via Strategy | **None** | No Strategy pattern, no per-interval×audience tasks, no `check_done` skip guard, no task graph. `generate_rules_blocks()` yields rule *blocks* in a fixed order, not report tasks. | **Rule-block generation & application ordering.** Audit the `generate_rules_blocks()` iterator (fixed DELETE→REPLACE→ADD), the write-to-`mapping_table`→`apply_mapping` loop (`app_service.py:217-239`), and whether already-processed state is ever skipped (it is not — every run reprocesses). |
| 3 | Worker loop: send→wait→extract | **None** | No `send`/`wait`/`extract`, no API polling, no worker pool, no `wait_task` terminal-status set. | **Pipeline stage ordering & data flow.** Audit the actual stage sequence in `run_report` (`app_service.py:126-268`): schema → import → matches → rules → non-mapped → sync → export — and whether ordering is correct (rules applied *before* the post-sync export; non-mapped extracted *before* sync). |
| 4 | API result→DataFrame conversion | **Partial** (the DataFrame shape survives) | No "raw API result," no data strategy, no `KeyError` on a strategy column. Data originates from CSV (`pl.read_csv_batched`) and returns from SQLite via cursor→`pl.DataFrame`. | **DataFrame round-trip fidelity & empty-result handling.** Audit column-name survival across `clean_names`/`make_valid` (`utils.py`, `db_service.py:107-110`), extra-column append/detect, and the empty-table path `export_sql_to_csv` returns `0` (`csv_service.py:334-336`). |
| 5 | CSV export safety & file-existence | **Partial** (timestamped names survive) | No `mode="x"`; export uses `mkdir(parents=True, exist_ok=True)` + `df.write_csv` (chunked, `file_index`). The *real* risk is inverted: a **same-second re-run overwrites** (`get_file_name` uses `%Y%m%d-%H%M%S`, `csv_service.py:237-240`) rather than silently skips; and `null_data` exports to `base_path` while `clean_data` exports to `export_path` (`app_service.py:248-268`). | **Export path resolution, naming, & crash-safety.** Audit (a) path base consistency (why null_data uses `base_path` vs clean_data `export_path`), (b) timestamp granularity + same-second collision/overwrite, (c) non-atomic writes (no temp+rename), (d) the `file_index` chunking scheme. |
| 6 | Error serialisation & task-to-yaml | **None** | No `TVTask`, no YAML error files, no per-task failure record. | **Failure mode, crash-state preservation, observability.** Audit the `DBWorker` context manager `__exit__` → `close()` → WAL checkpoint + **delete DB files unconditionally** even on exception (`db_service.py:786-788, 762-781`); partial CSVs left on crash; `print()` progress vs structured logging; the absence of any error field on `ReportResult`. |

### Assessment summary
No block retains its async/API framing. Blocks 1, 2, 3, 6 are **Fit: None**;
Block 5 is **Partial** (the timestamped-filename concept survives but the
mechanism is different and the real defect is inverted); Block 4 is **Partial**
(DataFrames exist, but not from an API result). Every "Outdated" item is the
async/Mediascope-API premise. Every "Replacement" is a genuine, live property of
the synchronous pipeline — none is a phantom.

## 3. Proposed New Block Set (re-based, renumbered)

Each block maps a surviving concern onto the real synchronous pipeline. The
principles (`problems-only`, runtime evidence, ≤100-line passes) are unchanged.

### 1. Run input scope & boundary detection
From `data_path`, establish what files are consumed (`CSVWorker._set_files_params`,
`list_files_in_directory`), how headers are read (`n_rows=0`), and how the date
column is auto-detected (`check_date_column`, regex `^\d{4}[-/.]\d{2}[-/.]\d{2}$`,
80% threshold over 500 rows). Check whether a missing/empty import folder
fails fast (`WrongDataSettings`, `csv_service.py:71-75`) and whether the
sample-based detector can mis-classify. *Evidence: the import file list, the
detected date column, one mis-detectable column shape.*

### 2. Mapping rule-block generation & application ordering
Establish the fixed block order emitted by `MappingDict.generate_rules_blocks()`
(DELETE→REPLACE→ADD) and the per-block write→apply cycle
(`app_service.py:217-239`). Check whether skipping already-done work is ever
implemented (it is not — no `check_done` equivalent) and whether the
`mapping_table`+`joined_matches` rebuild between blocks is correct/required.
*Evidence: the block sequence; one re-run and whether prior output is reused;
the joined-matches rebuild semantics.*

### 3. Pipeline stage ordering & data-flow correctness
From `run_report` (`app_service.py:126-268`), establish the stage sequence and
verify ordering invariants: rules applied **before** the clean-data export;
non-mapped extracted **before** the index↔data sync; FTS vs LIKE match insertion
only when the respective side is non-empty. Check for redundant work and for
state that the wrong table is targeted (`target_table` vs `data_tbl_name`).
*Evidence: the stage order with line refs; one ordering invariant and its proof;
one redundant computation.*

### 4. DataFrame round-trip fidelity & empty-result handling
Audit column-name survival: CSV headers → `source_headers` →
`set_data_tbl_columns` (+ `extra_cols`) → `MappingDict.build_mapping` (column
sanitisation) → SQLite → `export_sql_to_csv` cursor→`pl.DataFrame` (all `Utf8`
via `schema_overrides`). Check whether empty tables return `0` cleanly
(`csv_service.py:334-336`) and whether a `None`/empty source is distinguished
from a real result. *Evidence: the column-name chain for one report; one empty
table and its return; one non-fatal mismatch between a source header and the
exported column.*

### 5. Export path resolution, naming, and crash-safety
Audit: (a) why `null_data` exports to `base_path` (`app_service.py:249`) while
`clean_data` exports to `export_path` (`app_service.py:267`) — are both under
`AppPaths` consistently? (b) `get_file_name` timestamp granularity
(`%Y%m%d-%H%M%S`) → same-second re-runs **overwrite**; (c) `df.write_csv` is
non-atomic (no temp+rename) → a crash mid-write leaves a partial file
(`csv_service.py:356-361`); (d) the `file_index` chunking scheme and `max_rows`
default. *Evidence: both export destinations; one same-second overwrite; one
partial-file recovery attempt.*

### 6. Failure mode, crash-state preservation & observability
Audit the `DBWorker` context manager `__exit__` → `close()` (`db_service.py:786-788`):
it checkpoints WAL, closes both connections, then **deletes the DB files
unconditionally** (`_delete_base_files`, `db_service.py:762-767`) — even on
exception. So a crash mid-run destroys the diagnostic DB but may leave a
partial `null_data_*.csv.gz` on disk. Check the `print()`-based progress
(`csv_service.py:301, 340-354`, `app_service.py:113-116, 257-261, 272-276`)
versus structured logging, and that `ReportResult` carries no error field
(`models.py`). *Evidence: the `__exit__`/`close`/`_delete_base_files` path; one
induced mid-run exception and what artefacts remain; the ReportResult fields.*

> Note — this set drops the async/API framing but keeps the **pipeline** concept,
> which is the phase's raison d'être. It adds the crash-state-preservation and
> same-second-overwrite concerns that the original (async, retry-reliant) framing
> missed entirely.

## 4. Severity Taxonomy Assessment

The **principle** ("grade by effect and blast radius; rate what is true now;
empty band is valid") is sound and **retained**. The **illustrative examples are
all obsolete** — they are built on `mode="x"`, API polling, period clamping,
and `TVTask.to_yaml`, none of which exist. Proposed re-based bands, anchored to
the synchronous pipeline:

- **CRITICAL** — the run reports success while producing no/misleading output,
  or a crash destroys diagnostic state with no record. Concretely today:
  `export_sql_to_csv` returns `0` for an empty table and `run_report` still
  returns a "successful" `ReportResult` (no error field) — a caller treating
  `rows_exported == 0` as success would silently ship an empty report. And the
  DB is **always deleted on exit** (`__exit__`→`close`→`_delete_base_files`),
  so post-mortem inspection is impossible. **Empty today** for these exact
  shapes unless a caller mis-reads `ReportResult`; a regression guard.
- **HIGH** — a same-second re-run **overwrites** prior clean/null output
  (`get_file_name`, `%Y%m%d-%H%M%S`); rules applied against the wrong table
  (`target_table` vs `data_tbl_name`); a partial CSV left on crash read as
  complete (`write_csv` non-atomic, no temp+rename).
- **MEDIUM** — non-atomic CSV writes (no temp+rename); the `null_data` vs
  `clean_data` export-path inconsistency; potential correctness gap in
  `build_non_mapped`'s predicate (`WHERE col_a | col_b IS NULL` — bitwise `|`
  on TEXT, see §5 candidate); stale Russian user-facing `print` messages
  mixed with English CLI messages (also open-questions D-5); `print()`
  progress not captured by the logging config.
- **LOW** — doc/comment drift; non-reproducible filename (second-granular
  timestamp); the empty top-level `__init__.py` (D-8 #4); the stale dev
  `__main__` block at `app_service.py:341-342` (hardcoded `data\mvideo`).

## 5. Report Output Section — Assessment

- **Findings path** `.ai/audit/04-data-pipeline/findings.md` — **KEEP.** The
  directory name and the pipeline concept are intact after re-basing.
- **Finding-ID prefix `PIPE-`** — **KEEP.** Pipeline is still the subject.
- **`problems-only: true`** — **KEEP.** There are genuine, code-verified
  problems (double DELETE, path inconsistency, non-atomic writes, unconditional
  DB deletion on crash). This phase is not an empty finding set; it should
  surface real defects, not pass.
- **Template / ≤100-line appends / empty-state wording** — unchanged.
- **No source files modified** — this report audits the phase *spec* and the
  production code behaviour, per auditor role.

## 6. Cross-Phase References (Scope note, lines 25–29)

The current ownership note reads: "Owned elsewhere: the Semaphore and Queue
concurrency (03); the retry and backoff contract (09); config defaults merge
(02); performance ceilings (13)."

- **(03) Semaphore/Queue concurrency → DELETE reference.** Phase 03 is deleted;
  this citation is void.
- **(09) retry/backoff → DELETE reference.** No network, no retries. Phase 09
  itself has no subject here (async `asyncio.to_thread` bridge, JSON credential
  file — all absent). Remove the (09) exclusion.
- **(02) config defaults merge → REWORD.** Phase 02 was re-based; there is no
  "defaults merge" (`merge_dicts` is dead code — see Phase 02 auditor report
  §2 Block 3/5). Reword to "config values (02)" — the live settings that drive
  the pipeline (`app_config.yaml` → `DataSettings`, `dict_file_settings`,
  `read_settings.from_csv`, `export_settings.to_csv`) are genuinely owned
  elsewhere.
- **(13) performance ceilings → KEEP, reword.** Phase 13 must be re-based too
  (it is async-framed), but the sync cost centres are real: Polars chunked I/O
  (`CHUNK_SIZE=10000`, `csv_service.py:28`), SQLite pragmas
  (`synchronous=OFF`, `temp_store=MEMORY`, `cache_size=-100000`,
  `db_service.py:116-123`), ADBC bulk writes. Reword to "performance cost
  centres (13)".

## 7. Gaps the Re-based Phase Should Surface (candidate findings)

These are code-verified starting points the re-based phase 1–6 blocks should
develop into findings (with runtime evidence):

1. **`DBWorker._apply_delete` executes the DELETE twice** (`db_service.py:630-643`,
   lines 640 + 643). Documented in `architecture.md:252` as "harmless (second pass
   matches nothing) but wasteful." Candidate: prove the second pass matches nothing
   and quantify the redundant work; flag the misplaced `logger.debug` between the
   two executions.
2. **Unconditional DB deletion on every exit, including errors**
   (`db_service.py:786-788, 762-767`). `__exit__` always calls `close()`→`_delete_base_files`,
   so the diagnostic DB is destroyed even when a run crashes mid-pipeline. Candidate
   finding: crash-state is unrecoverable; the only artefacts left are partial CSVs.
3. **`null_data`/`clean_data` export-path inconsistency** (`app_service.py:249`
   `f"{self.base_path}"` vs `:267` `self.export_path`). Both are under `AppPaths`, but
   writing null data to the report root while clean data goes to a sub-folder is a
   path-resolution asymmetry a caller may not expect.
4. **Same-second filename collision → silent overwrite** (`csv_service.py:237-240`,
   `%Y%m%d-%H%M%S`). A re-run within one second overwrites the prior file rather than
   producing a new one; no `check_done`-style guard prevents reprocessing.
5. **Non-atomic CSV export** (`csv_service.py:358-361`, `df.write_csv` directly to the
   final path). A crash during write leaves a truncated `.csv.gz` that a downstream
   consumer may read as complete.
6. **`build_non_mapped` predicate uses bitwise `|`** (`db_service.py:754`):
   `WHERE {' | '.join(self.extra_columns)} IS NULL`. For TEXT columns `|` coerces to
   numeric; this is very likely **not** the intended "any extra column IS NULL"
   semantics. Candidate: establish whether `non_mapped` ever captures rows; if the
   predicate is always-false for TEXT, the non-mapped extraction is dead in practice.
7. **`print()` progress on the run path** (`app_service.py:113-116, 257-261, 272-276`;
   `csv_service.py:308, 340-354`) vs the `dictConfig`'d logging
   (`app_service.py:337`). User-facing run messages are still in Russian
   (`Обработка стартовата/завершена`, `architecture.md:237-239` / open-questions D-5);
   the newer CLI messages are English — a mixed-surface i18n and observability gap.
8. **`ReportResult` carries no error/failure field** (`models.py`). A run that fails
   mid-pipeline raises and never returns; a run that succeeds with `rows_exported == 0`
   is indistinguishable from one that produced a full report — a success-is-silent
   defect.

> These do not depend on changing any source files — they are the *findings* the
> re-based phase is built to elicit. None were asserted as final defect severity
> here; each needs the runtime evidence the re-based block demands.
