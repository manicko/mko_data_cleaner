---
name: 04-data-pipeline
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 04 — Data Pipeline & Report Lifecycle

## Purpose

Audits the synchronous, single-shot data pipeline of `mko_data_cleaner`: resolve
the report directory → build `DataSettings` config → load/merge the dictionary →
open a disposable SQLite DB → import CSV (chunked Polars → SQLite via ADBC) →
build the distinct index table → apply mapping rules (DELETE→REPLACE→ADD) →
export non-mapped rows then clean data → teardown the DB.

No network, no API, no period resolution, no task graph, no retry. A report run
is one in-process chain (`AppService.run_report`, `app_service.py:110-290`); the
disposable SQLite DB (WAL, FTS5) is the only intermediate artefact and is deleted
on exit.

**Scope:** input scope & boundary detection, dictionary loading & rule-block
generation, pipeline stage ordering, DataFrame round-trip fidelity, export path
resolution / naming / crash-safety, failure-mode / crash-state / observability.
**Out of scope:** settings values (02); SQLite/Polars/FTS performance cost centres
(13).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Run input scope & boundary detection

From `data_path`, establish what the run consumes and how it fails on bad input.
`self.base_path = data_path` flows through `PathResolver.resolve` → `ensure_dir`
→ `ensure_file_parent` (`app_service.py:117-119`). File discovery enumerates
`import_folder` non-recursively and extension-gated (`CSVWorker._set_files_params`,
`csv_service.py:60-75`; `list_files_in_directory`, `utils.py:104-128`); an empty
folder raises `WrongDataSettings`. Headers come from `pl.read_csv(n_rows=0)` on the
first file only (`get_csv_headers`, `csv_service.py:77-86`). The date column is
auto-detected by regex `^\d{4}[-/.]\d{2}[-/.]\d{2}$` over a 500-row sample at 0.8
threshold (`check_date_column`/`_detect_date_column`, `csv_service.py:124-157`;
`DATE_REGEX`/`DATE_SAMPLE_SIZE` at `csv_service.py:29-30`). A missing
`merged_dictionary.csv` triggers a re-merge (`get_dictionary`/`get_merged_dictionary`,
`csv_service.py:203-231`).

*Evidence:* the import file list + detected date column; one empty `raw_data/`
(must raise `WrongDataSettings`); one sample-based mis-detect (a non-date column
matching the regex ≥80%).

### 2. Dictionary loading & rule-block generation

Establish the fixed block order and the per-block write→apply cycle, and provoke
the two ordering defects. `MappingDict.generate_rules_blocks()`
(`dict_service.py:79-95`) yields `(ActionType, DataFrame)` in a fixed
DELETE → REPLACE → ADD order; REPLACE is further split by `group_by_cols`
(`dict_service.py:63-77`) into column-shape sub-blocks. The caller loop
(`app_service.py:217-239`) writes each block to `mapping_table` (ADBC `replace`)
then calls `db_worker.apply_mapping(...)`.

*Provokable defects to surface here:*
- Gap #1: `_apply_delete` (`db_service.py:630-643`) executes the same `DELETE`
  twice (lines 640, 643) with a `logger.debug` between — the second pass matches
  nothing. Already recorded at `architecture.md:252` and
  `.ai/audit/problems/01-apply-delete-double-execution.md`; cite, don't re-derive.
- D-2 (open-questions): REPLACE-then-ADD on one column overwrites the REPLACE value
  instead of merging — `_apply_add` (`db_service.py:674-729`). Confirm via a
  one-row REPLACE→ADD case.

*Evidence:* the block sequence with line refs; one empty dictionary and its
disposition; one induced DELETE block and two `changes()` values (N then 0).

### 3. Pipeline stage ordering & data-flow correctness

Stage order in `run_report` (`app_service.py:146-268`): `__enter__` →
`set_data_tbl_columns` (`db_service.py:107-110`) → `build_mapping`
(`dict_service.py:119-142`) → `create_table_with_index` (`:166`) →
`create_rules_matches` (`:167`) → `link_search_table` (conditional on `is_empty`,
`:171-172`) → `_import_data` (`:176`; commits then `update_index_from_data`,
`db_service.py:361-377`) → FTS/LIKE match import (gated on `is_empty`,
`:171,181,196,211`) → `apply_mapping` loop (`:217-239`) → `build_non_mapped`
(`:244`) → null-data export (`:246-251`) → `sync_with_data_table` (`:262`) →
clean-data export (`:264-268`). Invariants: rules applied *before* the clean-data
export; non-mapped extracted *before* the index↔data sync; FTS vs LIKE insertion
only when the respective side is non-empty. Check `target_table`
(`db_service.py:76-79`): returns `_index_tbl_name` when `index_column` is set,
else `data_tbl_name` — confirm the no-`index_column` fallback is intentional, not a
latent mis-target (`insert_matches` scans `self.target_table`, `db_service.py:557-586`).

*Evidence:* the stage order with line refs; one invariant and its static proof;
one redundant computation (`_build_joined_matches` rebuilds per block,
`db_service.py:588-603`). DB lifecycle: created at `__enter__`, deleted on exit
(`db_service.py:786-788`) — intended per `architecture.md:136-137`.

### 4. DataFrame round-trip fidelity & empty-result handling

Audit column-name survival from CSV header to exported CSV. Chain:
`source_headers` (`get_csv_headers`, `csv_service.py:77-86`) →
`set_data_tbl_columns` (`db_service.py:107-110`) → `build_mapping`
(`dict_service.py:119-142`, renames extra cols) → SQLite `TEXT` all columns
(`create_table`, `db_service.py:134-193`) → `export_sql_to_csv` cursor→
`pl.DataFrame` with `schema_overrides = {col: pl.Utf8}` (`csv_service.py:325`).
The only name sanitizers are `clean_names` (`utils.py:81-101`, via `make_valid` at
`utils.py:73-78`) — confirm no others intercept. CSV import streams via
`pl.read_csv_batched` (10 000-row chunks, `CHUNK_SIZE` at `csv_service.py:28`)
into `_import_data` (`app_service.py:91-108`) via `chunk.write_database(...,
engine="adbc")` (legacy `data_chunk_to_sql`, `db_service.py:505-520`, uses
`to_sql` and appears unused on this path).

*Evidence:* the column-name chain for one report (incl. one sanitized name, e.g.
`COST RUB` → `COSTRUB`); one empty table and its return (`export_sql_to_csv`
returns `0`, `csv_service.py:334-336`); one non-fatal header/export mismatch.

### 5. Export path resolution, naming & crash-safety

Audit where exports land, how they are named, and whether a crash leaves partial
files. Path asymmetry: `null_data` exports to `f"{self.base_path}"` (report root,
`app_service.py:249`) while `clean_data` exports to `self.export_path`
(`base_path/clean_data`, `app_service.py:267`) — both resolve through `AppPaths`
(paths.py), but the asymmetry is unexpected. Naming: `get_file_name`
(`csv_service.py:237-240`) uses `strftime("%Y%m%d-%H%M%S")` → second-granular;
same-second re-runs **overwrite** (no collision guard) — this contradicts the doc
claim `data-flow.md:178` ("repeated runs never overwrite"). Crash-safety:
`df.write_csv` writes straight to the final path (`csv_service.py:356-361`) — no
temp+rename; a mid-write crash leaves a truncated `.csv.gz` read as complete.
`mkdir(parents=True, exist_ok=True)` at `csv_service.py:306`; `file_index` starts
at 1 per run (`csv_service.py:314`); `max_rows` defaults to 10 000 (`csv_service.py:312`).

*Evidence:* both export destinations & resolved paths; one induced same-second
overwrite; one mid-write crash and the truncated file left behind.

### 6. Failure mode, crash-state preservation & observability

Audit teardown on both happy and failure paths. `DBWorker.__exit__`
(`db_service.py:786-788`) always calls `self.close()` — no `exc_type` guard.
`close()` (`db_service.py:769-781`) checkpoints WAL (`PRAGMA wal_checkpoint(FULL)`),
closes both connections, then `_delete_base_files` (`db_service.py:762-767`)
deletes `db_file.name + "*"` **unconditionally** — so the diagnostic DB is
destroyed even on exception. Intended design (`architecture.md:136-137`,
`data-flow.md:36`, `architecture.md:230`), but a mid-run crash leaves only a
partial CSV. Progress is emitted via `print()` in a mixed Russian/English mix
(`app_service.py:113-116, 257-261, 272-276`; `csv_service.py:308`; `utils.py:34-41`
`progress_bar`) vs. the `dictConfig`'d logging at `app_service.py:337`, breaching
the English-only house rule (`project.md` rule 1). `ReportResult`
(`models.py:170-195`) has no `error`/`status` field; since `process_data` returns
it (`app.py:138`), a `rows_exported == 0` result (constructed `app_service.py:278-290`)
is indistinguishable from a full report.

*Evidence:* the `__exit__`→`close`→`_delete_base_files` path; one induced mid-run
exception and the artefacts that remain (partial CSV only); the `ReportResult`
field set.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now. An empty band is a valid outcome — do not populate it with a hypothetical.

- **CRITICAL** — the run reports success while producing no/misleading output, or a
  crash destroys diagnostic state with no record. `ReportResult` carries no
  `error`/`status` field (`models.py:170-195`) and `process_data` returns it
  (`app.py:138`) — so `rows_exported == 0` is indistinguishable from a full report;
  `export_sql_to_csv` returns `0` for an empty table without distinguishing "no
  input" from "no matches" (`csv_service.py:334-336`). The diagnostic SQLite DB is
  **always deleted on exit** (`__exit__`→`close`→`_delete_base_files`,
  `db_service.py:786-788, 762-781`), so post-mortem inspection is impossible. Both
  are latent today (no caller mis-reads the result in the test suite) — regression
  guards, not live defects.
- **HIGH** — silent data loss or a wrong target. Same-second re-run **overwrites**
  prior output (`get_file_name`, `csv_service.py:239`; `%Y%m%d-%H%M%S`, no guard); a
  partial CSV left on crash read as complete (`df.write_csv` non-atomic,
  `csv_service.py:356-361`); rules applied to the wrong table (`target_table`
  fallback when `index_column` is unset, `db_service.py:76-79`).
- **MEDIUM** — correctness gaps with bounded blast radius or observability debt.
  Non-atomic CSV writes (no temp+rename); the `null_data`/`clean_data` export-path
  asymmetry (`app_service.py:249` vs `:267`); `build_non_mapped`'s predicate uses
  bitwise `|` on TEXT (`db_service.py:754`) — if proven always-false, the
  `null_data` channel is silently dead (escalate to HIGH on evidence, Block 2);
  stale doc-vs-code claims (`data-flow.md:165` "returns None" vs. `app.py:138`;
  `data-flow.md:178` "never overwrite" vs. `csv_service.py:239`); mixed
  Russian/English `print()` vs. `dictConfig` logging (`app_service.py:337`).
- **LOW** — drift and reproducibility, no runtime consequence. Doc drift
  (`architecture.md:218` / `data-flow.md:165` stale re: return value;
  `data-flow.md:178` stale re: overwrite); non-reproducible second-granular
  filename (`csv_service.py:239`); stale dev `__main__` block hardcoding
  `data\mvideo` (`app_service.py:341-342`).

## Report Output

- Findings path: `.ai/audit/04-data-pipeline/findings.md` (singular `audit`,
  matching Phase 01 §1 and the `.ai/audit/` templates/problems layout — confirmed
  against the working tree).
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, distribution, cross-finding analysis, roadmap, rollout safety,
  appendices. The template carries a `code_ref` front-matter field; populate it
  once with the SHA this report describes, never per finding.
- Finding-ID prefix: `PIPE-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding needs
  runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker.
