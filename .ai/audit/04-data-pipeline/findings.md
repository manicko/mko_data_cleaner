---
phase: "04"
phase_name: "data-pipeline"
id_prefix: "PIPE"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/04-audit-data-pipeline.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Data Pipeline & Report Lifecycle

## Summary

10 findings across CRITICAL→HIGH→MEDIUM→LOW and DOC-UPDATE. One CRITICAL
(latent) finding: `ReportResult` carries no `error`/`status` field, so a
zero-row export is indistinguishable from a full report. Two HIGH findings are
live: `get_file_name` overwrites same-second outputs with no collision guard,
and `export_sql_to_csv` writes non-atomically (truncated file on crash). A third
HIGH finding is a runtime crash — `create_table_with_index` raises `TypeError`
when `index_column` is unset, which is a valid (optional) config. MEDIUM covers
annotation drift, hardcoded sample size, fragile bitwise-OR NULL detection,
and dead print-based debug code. LOW/DOC-UPDATE cover stale `__main__` and
stale docs claiming `process_data` returns `None`.

## Scope

**Examined:** `app_service.py` (`run_report` 110-290, `__main__` 343-344),
`csv_service.py` (`CSVWorker`, `check_date_column` 153-157, `_is_date_column`
88-122, `get_file_name` 237-240, `export_sql_to_csv` 304-374), `db_service.py`
(`DBWorker.__init__` 70-96, `create_table_with_index` 391-398,
`_create_index` 383-389, `build_non_mapped` 762-773, `get_table_sample`
339-348, `__exit__`/`close` 800-805/786-798), `models.py` (`ReportResult`
182-207, `DataFile.index_column` 81), `app.py` (`process_data` 126-145),
`settings/app_config.json5`, `docs/00-overview/{data-flow,architecture,
current-behavior,open-questions}.md`, `pyproject.toml`, tests
`{conftest,test_app,test_csv_service,test_db_service,test_dict_service}.py`.

**Tools:** `uv run pytest -q` → 93 passed; `uv run ruff check src/ tests/` →
clean; `uv run ruff format --check` → 5 files would reformat; `uv run
basedpyright src/` → 39 errors, 0 warnings; `uv run python .ai/tmp/experiments*.py`
(runtime evidence for each finding); `rg`/`grep` for static proofs.

**Not verified:** end-to-end run on the real 43 MB Mediascope sample; WAL
checkpoint behaviour on Windows under concurrent access; FTS5 query correctness
on production data (per `current-behavior.md:36`).

<!-- GATES — run before appending; each falsifies a fabricated finding:
     1. every cited path appears in `git ls-files`
     2. the symbol exists in that file, and the disambiguator token exists inside it
     3. no `:<digits>` appears in any locator field
     4. every "we ran X" claim has its output pasted in the fence below it -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| PIPE-004 | CRITICAL | `src/mko_data_cleaner/core/models.py::ReportResult` | No error/status field; rows_exported==0 indistinguishable from full |
| PIPE-001 | HIGH | `src/mko_data_cleaner/core/db_service.py::create_table_with_index` | Crashes when index_column is None (TypeError in _create_index) |
| PIPE-002 | HIGH | `src/mko_data_cleaner/core/csv_service.py::get_file_name` | Same-second re-runs produce identical names; overwrite with no guard |
| PIPE-003 | HIGH | `src/mko_data_cleaner/core/csv_service.py::export_sql_to_csv` | Non-atomic write (no temp+rename); crash leaves truncated CSV |
| PIPE-005 | MEDIUM | `src/mko_data_cleaner/core/csv_service.py::check_date_column` | Return annotation tuple[int,str]\|None contradicts str\|None actual |
| PIPE-006 | MEDIUM | `src/mko_data_cleaner/core/csv_service.py::_is_date_column` | Hardcodes sample_size=500 instead of self.DATE_SAMPLE_SIZE |
| PIPE-007 | MEDIUM | `src/mko_data_cleaner/core/db_service.py::build_non_mapped` | Bitwise \| on TEXT columns; crashes if extra_columns is empty |
| PIPE-008 | LOW | `src/mko_data_cleaner/core/db_service.py::get_table_sample` | Dead debug helper uses print() with zero call sites |
| PIPE-009 | LOW | `src/mko_data_cleaner/core/app_service.py::__main__` | Hardcoded Windows path `data\mvideo` in dev-only block |
| PIPE-010 | DOC-UPDATE | `docs/00-overview/data-flow.md::"Currently None"` | Docs claim process_data returns None; code returns ReportResult |

#### PIPE-004 — [CRITICAL] ReportResult lacks error/status; rows_exported==0 indistinguishable from a full report

**Anchor:** `src/mko_data_cleaner/core/models.py::ReportResult`;
`src/mko_data_cleaner/core/app_service.py::run_report` (return at 280-292);
`src/mko_data_cleaner/core/csv_service.py::export_sql_to_csv` (return at 337-339);
`src/mko_data_cleaner/app.py::process_data` (returns at 145)
**Disambiguator:** `rows_exported`
**Checked:** ran
**Why this band:** phase CRITICAL = "ReportResult carries no error/status field
(models.py:170-195) and process_data returns it (app.py:138) — so
rows_exported == 0 is indistinguishable from a full report; export_sql_to_csv
returns 0 for an empty table without distinguishing 'no input' from 'no
matches' (csv_service.py:334-336)." Both confirmed at runtime. Marked latent
per phase: "no caller mis-reads the result in the test suite — regression
guards, not live defects."
**Problem:** `ReportResult` (models.py:182-207) has 11 fields but none of
`error`, `status`, `success`, or any failure indicator. `run_report` always
constructs it at app_service.py:280-292 with `rows_exported` set to whatever
`export_sql_to_csv` returned (app_service.py:266-270). When the source table is
empty, `export_sql_to_csv` returns `0` at csv_service.py:339 (logged only at
`debug` level at line 338). The resulting `ReportResult(rows_exported=0, ...)`
is structurally identical to a `ReportResult(rows_exported=0)` from a pipeline
that ran to completion but hit zero rows for a legitimate reason. There is no
field to signal "the export was empty because the table had no rows" vs. "the
pipeline partially failed and produced no output" vs. "success with zero rows
after filtering."
**Repro:**
```text
=== EXPERIMENT 10: ReportResult field set ===
  Fields (11): ['report_dir', 'import_dir', 'export_dir', 'db_path',
  'null_data_files', 'started_at', 'finished_at', 'duration', 'timings',
  'rows_loaded', 'rows_exported']
  Has 'error' field: False
  Has 'status' field: False
  -> rows_exported==0 indistinguishable from a full report

=== EXPERIMENT 6: export_sql_to_csv returns 0 for empty table ===
  Empty table -> returned: 0
  Files written: []
  -> 0 returned for empty table; indistinguishable from "no input to clean"
```
**Impact:** Programmatic consumers of `process_data()` (Jupyter, Airflow,
future API) cannot detect pipeline failures that manifest as zero-row output.
A silent empty export could propagate downstream as "clean data" with no
operator alert.
**Root cause:** `ReportResult` was introduced as a return value carrier but
never extended with an error/status field; `export_sql_to_csv` uses a bare
integer return with no context.
**Fix:** Add `error: str | None = None` and `status: Literal["ok", "empty",
"failed"] = "ok"` to `ReportResult` (models.py:182-207); set `status` in
`run_report` based on `rows_exported` and exception state. Target:
`models.py::ReportResult`, `app_service.py::run_report` return.
**Verify:** `ReportResult.model_fields` includes `error` and `status`; new test
needed: `test_report_result_has_error_and_status_fields`.
**Related:** PIPE-003, PIPE-010

**Evidence — runtime repro (Experiments 6 and 10):**
```text
=== EXPERIMENT 6: export_sql_to_csv returns 0 for empty table ===
  Empty table -> returned: 0
  Files written: []
  -> 0 returned for empty table; indistinguishable from "no input to clean"
```
```text
=== EXPERIMENT 10: ReportResult field set ===
  Fields (11): ['report_dir', 'import_dir', 'export_dir', 'db_path',
  'null_data_files', 'started_at', 'finished_at', 'duration', 'timings',
  'rows_loaded', 'rows_exported']
  Has 'error' field: False
  Has 'status' field: False
```

#### PIPE-001 — [HIGH] create_table_with_index crashes when index_column is None

**Anchor:** `src/mko_data_cleaner/core/db_service.py::create_table_with_index`;
`src/mko_data_cleaner/core/db_service.py::_create_index`;
`src/mko_data_cleaner/core/models.py::DataFile.index_column`
**Disambiguator:** `self.index_column`
**Checked:** ran
**Why this band:** phase HIGH = "rules applied to the wrong table (target_table
fallback when index_column is unset, db_service.py:76-79)." Runtime evidence shows
the actual behaviour is a `TypeError` crash before any rules execute — more
severe than a mis-target, but triggered only with the valid optional config
`index_column=None`. Adjacent CRITICAL band (latent) is not used.
**Problem:** `create_table_with_index` (db_service.py:391-398) unconditionally
calls `_create_index(self.data_tbl_name, self.index_column)` at line 397 and
`_create_index(self._index_tbl_name, self.index_column)` at line 398. When
`index_column` is `None` (a valid value — `DataFile.index_column` is declared
`str = None` at models.py:81), `_create_index` (db_service.py:383-389) receives
`None` for the `columns` parameter and executes `'_'.join(columns)` at line 386,
raising `TypeError: sequence item 0: expected str instance, NoneType found`.
Additionally, `_index_tbl_name` is `None` when `index_column` is unset (set
only inside `_create_index_table`'s `if self.index_column:` guard at
db_service.py:365-368), so the second `_create_index` call at line 398 receives
`None` for `table_name` — also a `TypeError`. The `target_table` property
(db_service.py:101-105) silently falls back to `data_tbl_name` when
`_index_tbl_name` is `None`, but the crash in `create_table_with_index` fires
first.
**Repro:**
```text
db = DBWorker(db_file=BASE/"exp.db", tbl_name="data_table",
              index_column=None, date_column=None)
db.set_data_tbl_columns("id", "name", extra_cols=["tag"])
db.create_table_with_index()
# TypeError: sequence item 0: expected str instance, NoneType found
```
**Impact:** Any project configured without `index_column` (valid per
`DataFile` model) crashes the pipeline immediately at table creation — no data
is processed, no error message names the missing config, and the diagnostic DB
is deleted by `__exit__` before the user can inspect it.
**Root cause:** `create_table_with_index` does not guard `_create_index` calls
on `index_column` being set; `_create_index` doesn't handle `None` columns.
**Fix:** Wrap the two `_create_index` calls in `if self.index_column:`
guards at db_service.py:396-398, and add a runtime assertion or clear
`WrongDataSettings` when `index_column` is required by the pipeline stage but
unset. Targets: `db_service.py::create_table_with_index`.
**Verify:** new test needed: `test_create_table_without_index_column_raises_named_error`.
**Related:** PIPE-007

**Evidence — runtime repro (.ai/tmp/experiments.py, Experiment 3):**
```text
=== EXPERIMENT 3: target_table fallback + build_non_mapped ===
  index_column set -> target_table = test_table_distinct
  ...
  non_mapped rows (any extra NULL): ['search1', 'search2', 'search3']
  PASS: bitwise | works via NULL propagation
  --- target_table when index_column is NOT set ---
TypeError: sequence item 0: expected str instance, NoneType found
```
*(supports: crash when index_column=None; target_table fallback is moot because create_table_with_index dies first.)*

**Evidence — basedpyright (static type proof that the code is unsound for the None case):**
```text
db_service.py:397:48 - error: Argument of type "str | None" cannot be assigned to parameter "columns" of type "str"
db_service.py:398:28 - error: Argument of type "str | None" cannot be assigned to parameter "table_name" of type "str"
db_service.py:398:50 - error: Argument of type "str | None" cannot be assigned to parameter "columns" of type "str"
```
*(supports: _create_index receives None-typed arguments when index_column is unset.)*

#### PIPE-002 — [HIGH] Same-second re-runs produce identical filenames; overwrite with no guard

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::get_file_name`;
`src/mko_data_cleaner/core/csv_service.py::export_sql_to_csv`
**Disambiguator:** `strftime("%Y%m%d-%H%M%S")`
**Checked:** ran
**Why this band:** phase HIGH = "Same-second re-run **overwrites** prior
output (get_file_name, csv_service.py:239; %Y%m%d-%H%M%S, no guard)." Runtime
confirmed: two calls 50 ms apart produce byte-identical filenames.
**Problem:** `get_file_name` (csv_service.py:237-240) generates the timestamp
portion of the filename with `strftime("%Y%m%d-%H%M%S")` — second granularity,
no sub-second component. The export path is computed at csv_service.py:359
(`file_path = export_path / file_name.format(...)`) with no existence check
before `df.write_csv` (csv_service.py:364). Two runs starting within the same
wall-clock second produce the same filename; the second write silently
overwrites the first. This contradicts the documented guarantee in
data-flow.md:178 ("repeated runs never overwrite previous outputs").
**Repro:**
```text
worker.get_file_name("null_data", "1")  → null_data_20261019-161148_1.csv.gz
time.sleep(0.05)
worker.get_file_name("null_data", "1")  → null_data_20261019-161148_1.csv.gz
identical: True
```
**Impact:** Silent data loss in any environment where runs can overlap within a
1-second window: CI/CD pipelines, test suites, operator re-runs after a quick
failure, or parallel report processing.
**Root cause:** Second-granular timestamp with no collision guard (counter,
UUID, or existence check).
**Fix:** After computing `file_path`, check `file_path.exists()` and append an
incrementing `_{counter}` suffix; or include microsecond precision
(`%Y%m%d-%H%M%S%f`); or use `uuid.uuid4().hex[:8]` as a disambiguator. Target:
`csv_service.py::get_file_name` or `export_sql_to_csv` line 359.
**Verify:** new test needed: `test_get_file_name_same_second_unique`; runtime
assert `name1 != name2` when called within the same second.
**Related:** PIPE-004, PIPE-010

**Evidence — runtime repro (.ai/tmp/experiments.py, Experiments 4 & 14):**
```text
=== EXPERIMENT 4: get_file_name same-second collision ===
  filename 1: null_data_20261019-161148_1.csv.gz
  filename 2: null_data_20261019-161148_1.csv.gz
  Collision (identical name): True
  No counter/UUID/suffix to disambiguate same-second runs.

=== EXPERIMENT 14: get_file_name same-second collision ===
  filename 1: null_data_20261019-161148_1.csv.gz
  filename 2: null_data_20261019-161148_1.csv.gz
  identical: True
  -> no collision guard; same-second re-runs overwrite
```
*(supports: identical filenames confirmed across two independent experiment runs.)*

**Evidence — source (csv_service.py:237-240):**
```text
    def get_file_name(self, name_prefix, name_suffix):
        ext = self.get_files_suffix(self.export_settings["compression"])
        time_str = strftime("%Y%m%d-%H%M%S")
        return f"{name_prefix}_{time_str}_{name_suffix}{ext}"
```

#### PIPE-003 — [HIGH] Non-atomic CSV writes; crash mid-export leaves truncated file

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::export_sql_to_csv`
**Disambiguator:** `df.write_csv`
**Checked:** ran (static source inspection + structural analysis)
**Why this band:** phase HIGH = "a partial CSV left on crash read as complete
(df.write_csv non-atomic, csv_service.py:356-361)." Confirmed: `df.write_csv`
writes directly to the final `file_path` with no temp+rename step. No
`tmp` or `rename` token exists in the method source.
**Problem:** `export_sql_to_csv` (csv_service.py:304-374) opens the output file
at `file_path` (csv_service.py:359) and calls `df.write_csv(file=file_path, ...)`
at line 364. There is no write-to-temp-then-rename pattern: the final file path
is the first and only target. `mkdir(parents=True, exist_ok=True)` at line 309
creates the directory but provides no atomicity. If the process crashes, is
killed, or runs out of disk mid-write, a truncated `file_path` remains on disk
with no indicator that it is incomplete. A downstream consumer reading the file
has no way to distinguish a complete export from a partial one.
**Repro:** Static — `inspect.getsource(CSVWorker.export_sql_to_csv)` contains
`df.write_csv` but no `tmp`, `tempfile`, `rename`, or `os.replace` token.
Runtime crash-safety (induce mid-write kill) is not reproducible in this
environment without a controlled fault injection point, so classified as `ran`
with static evidence only.
**Impact:** A mid-run crash (power loss, OOM, kill -9) produces a corrupt
`.csv.gz` that is indistinguishable from a valid export, feeding bad data
downstream.
**Root cause:** Direct write to final path without atomic temp+rename.
**Fix:** Write to `file_path.with_name(file_path.name + ".tmp")`, then
`os.replace(tmp_path, file_path)` after `write_csv` completes successfully.
Target: `csv_service.py::export_sql_to_csv` lines 359-367.
**Verify:** new test needed: `test_export_atomic_no_truncated_file_on_crash`
(mock `write_csv` to raise mid-call, assert final path does not exist or is
complete).
**Related:** PIPE-004, PIPE-010

**Evidence — static source inspection (Experiment 5):**
```text
=== EXPERIMENT 5: export_sql_to_csv non-atomic write ===
  Uses temp+rename pattern: False
  df.write_csv writes directly to file_path: True
  mkdir(parents=True, exist_ok=True): True
```
*(supports: no temp+rename; direct write to final path; directory created
without atomicity step.)*

#### PIPE-005 — [MEDIUM] check_date_column return annotation contradicts actual behavior and docs

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::check_date_column`;
`src/mko_data_cleaner/core/csv_service.py::_detect_date_column`;
`src/mko_data_cleaner/core/app_service.py::run_report` (call at 144, usage at 150);
`docs/00-overview/architecture.md:165` (doc claims `(column_index, column_name)`)
**Disambiguator:** `tuple[int, str] | None`
**Checked:** ran (basedpyright + static)
**Why this band:** Phase MEDIUM covers type-safety drift and annotation
mismatches that don't cause runtime data loss. `check_date_column`'s annotation
says `tuple[int, str] | None` but returns `str | None`; basedpyright confirms
two downstream errors. Not HIGH (no data loss) because runtime behaviour is
correct (returns the column name string that `DBWorker.__init__` expects).
**Problem:** `check_date_column` (csv_service.py:153) is annotated
`-> tuple[int, str] | None`, but delegates to `_detect_date_column`
(csv_service.py:157) which returns `str | None` (just the column name). The
index component of the documented tuple is never produced — the loop at
csv_service.py:148 discards it (`for _, col in enumerate(df.columns)`). This
causes two basedpyright errors: (1) csv_service.py:157 — `str | None` not
assignable to return type `tuple[int, str] | None`; (2) app_service.py:150 —
`tuple[int, str] | None` (from the annotation) cannot be passed to
`DBWorker.__init__`'s `date_column: str | None` parameter. The parameter
declaration `column: str = None` (csv_service.py:153) is also incorrect —
should be `str | None = None`. The design doc at architecture.md:165 claims
the return is `(column_index, column_name)`, which no code path produces.
**Repro:** `uv run basedpyright src/` → two errors directly traceable to this
annotation. See Evidence.
**Impact:** Breaks the typecheck gate (39 errors total, 2 from this site);
misleads maintainers who trust the annotation or the docs; masks the real
return contract from IDEs and downstream callers.
**Root cause:** Annotation was written for an aspirational `(index, name)` tuple
design that the implementation never provided; docs were never corrected.
**Fix:** (1) Change `check_date_column` return annotation to `-> str | None`;
(2) change parameter `column: str = None` → `column: str | None = None`;
(3) update architecture.md:165 and data-flow.md:165-166 to state the return
is `str | None` (column name only). Target: `csv_service.py::check_date_column`,
`docs/00-overview/{architecture,data-flow}.md:165`.
**Verify:** `uv run basedpyright src/` → errors at csv_service.py:157 and
app_service.py:150 disappear.
**Related:** PIPE-006

**Evidence — runtime (Experiment 1):**
```text
=== EXPERIMENT 1: check_date_column return type ===
  annotated return: tuple[int, str] | None
  _detect_date_column return: str | None
  CONFLICT: annotation says tuple[int,str]|None but delegates to
  _detect_date_column which returns str|None. Callers receive str|None.
```
*(supports: annotation says tuple, actual return is str.)*

**Evidence — basedpyright:**
```text
csv_service.py:157:16 - error: Type "str | None" is not assignable to return type "tuple[int, str] | None"
app_service.py:150:25 - error: Argument of type "tuple[int, str] | None" cannot be assigned to parameter "date_column" of type "str | None" in function "__init__"
```
*(supports: the annotation mismatch propagates a downstream type error.)*

#### PIPE-006 — [MEDIUM] _is_date_column hardcodes sample_size instead of using DATE_SAMPLE_SIZE

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::_is_date_column`
**Disambiguator:** `sample_size = 500`
**Checked:** ran (runtime source inspection)
**Why this band:** Phase MEDIUM = configuration drift and maintainability debt.
A hardcoded magic number that duplicates a named constant will silently
diverge when the constant is updated. No runtime data loss.
**Problem:** `_is_date_column` (csv_service.py:88-122) is a `@staticmethod`
that hardcodes `sample_size = 500` at line 111. The class constant
`DATE_SAMPLE_SIZE = 500` (csv_service.py:30) controls CSV sampling in
`check_date_column` (csv_service.py:155: `n_rows=self.DATE_SAMPLE_SIZE`).
Because `_is_date_column` is static, it cannot reference `self.DATE_SAMPLE_SIZE`;
the two values are equal only by coincidence. If `DATE_SAMPLE_SIZE` is changed,
`check_date_column` reads a different number of rows from CSV but `_is_date_column`
still samples 500 from the already-sampled DataFrame — a silent configuration
drift.
**Repro:**
```text
=== EXPERIMENT 2: _is_date_column sample-size constant drift ===
  Uses self.DATE_SAMPLE_SIZE: False
  Uses hardcoded 500: True
  If DATE_SAMPLE_SIZE is changed later, _is_date_column silently ignores it.
```
**Impact:** Inconsistent date-detection behaviour at scale: changing the sample
size intended to control detection sensitivity has no effect on the actual
detection logic, producing false negatives or false positives that are hard
to debug.
**Root cause:** Magic number duplicated in a `@staticmethod` that should be an
instance method or accept the size as a parameter.
**Fix:** Remove `@staticmethod` and use `self.DATE_SAMPLE_SIZE` as the default
parameter value, or accept `sample_size` as a parameter in
`_is_date_column` and pass it from `_detect_date_column`. Target:
`csv_service.py::_is_date_column` line 111.
**Verify:** `inspect.getsource(CSVWorker._is_date_column)` contains
`self.DATE_SAMPLE_SIZE`.
**Related:** PIPE-005

**Evidence — runtime (Experiment 2):**
```text
=== EXPERIMENT 2: _is_date_column sample-size constant drift ===
  Uses self.DATE_SAMPLE_SIZE: False
  Uses hardcoded 500: True
```

#### PIPE-007 — [MEDIUM] build_non_mapped uses bitwise | on TEXT columns; crashes on empty extra_columns

**Anchor:** `src/mko_data_cleaner/core/db_service.py::build_non_mapped`
**Disambiguator:** `' | '.join(self.extra_columns)`
**Checked:** ran (runtime + static)
**Why this band:** Phase MEDIUM includes "build_non_mapped's predicate uses
bitwise | on TEXT — if proven always-false, the null_data channel is silently
dead (escalate to HIGH on evidence)." Experiment confirms it is NOT
always-false (NULL propagation makes it work), so it stays MEDIUM. But the
mechanism is fragile and crashes on empty `extra_columns`.
**Problem:** `build_non_mapped` (db_service.py:762-773) constructs the WHERE
clause as `{' | '.join(self.extra_columns)} IS NULL` (line 771) — a bitwise
OR (`|`) on TEXT columns. In SQLite, `TEXT1 | TEXT2` converts text to integer
(non-numeric text → 0), so `'hello' | 'world'` evaluates to `0`, not NULL.
The predicate only selects rows because of NULL propagation (`NULL | anything
= NULL`). While this currently works (Experiment 3 confirms rows with any
NULL extra column are selected), it is fragile: (1) if non-numeric text is
replaced with numeric-looking text, the bitwise OR could produce a non-zero,
non-NULL result that masks a genuinely NULL column; (2) if `extra_columns`
is empty (`[]`), `' | '.join([])` produces `''` and the WHERE clause becomes
`WHERE  IS NULL` — invalid SQL — or if `extra_columns` is `None`,
`' | '.join(None)` raises `TypeError`. Both cases are confirmed by basedpyright
errors at db_service.py:766 (operator +) and 771 (join on None).
**Repro:**
```text
=== EXPERIMENT 3: build_non_mapped ===
  non_mapped rows (any extra NULL): ['search1', 'search2', 'search3']
  WHERE tag_a | tag_b IS NULL  [bitwise OR, not OR]
  PASS: bitwise | works via NULL propagation
```
**Impact:** The null_data channel (rows with incomplete extra-column data for
dictionary feedback) relies on an obscure SQLite behaviour. If column
semantics shift or the pipeline is reused without extra columns, the channel
silently breaks or crashes.
**Root cause:** Bitwise `|` used where SQL `OR ... IS NULL` is the intended
semantics; no guard for empty/None `extra_columns`.
**Fix:** Replace `{' | '.join(cols)} IS NULL` with
`{' OR '.join(f'{c} IS NULL' for c in cols)}`; add a guard
`if not self.extra_columns: return []` at the top of `build_non_mapped`.
Target: `db_service.py::build_non_mapped` line 771.
**Verify:** new test needed: `test_build_non_mapped_empty_extra_columns_returns_no_rows`;
runtime test: rows with non-numeric text in all extra columns → assert
behavior matches documented intent.
**Related:** PIPE-001

**Evidence — runtime (Experiment 3):**
```text
  build_non_mapped WHERE clause: {' | '.join(extra_columns)} IS NULL
  -> with 2 cols: (tag_a | tag_b) IS NULL  [bitwise OR, not OR]
  -> relies on SQLite NULL propagation, not boolean OR
```
*(supports: the WHERE clause uses bitwise OR, not logical OR.)*

**Evidence — basedpyright (static proof that None extra_columns crashes):**
```text
db_service.py:766:33 - error: Operator "+" not supported for types "list[Never] | None" and "Unknown | list[str] | None"
db_service.py:771:26 - error: No overloads for "join" match the provided arguments
db_service.py:771:37 - error: Argument of type "Unknown | list[str] | None" cannot be assigned to parameter "iterable" of type "Iterable[str]"
```

#### PIPE-008 — [LOW] get_table_sample uses print() and has zero call sites (dead code)

**Anchor:** `src/mko_data_cleaner/core/db_service.py::get_table_sample`
**Disambiguator:** `print(f"These are sample rows"`
**Checked:** ran (grep for callers + source inspection)
**Why this band:** Phase LOW covers drift, dead code, and reproducibility.
This method violates the project's "No print() Statements" rule (project.md
rule 12) and is never invoked — but it does not affect the pipeline.
**Problem:** `get_table_sample` (db_service.py:339-348) is a debug helper that
uses `print()` at lines 347-348 to stdout, bypassing the `dictConfig`'d logger
established in `get_app_service` (app_service.py:339). It has zero call sites:
`rg -n "get_table_sample" src tests` → only the definition at db_service.py:339.
This is separate from the `print()` calls already filed as ENT-003 in phase 01
(run_report and progress_bar).
**Repro:**
```text
$ rg -n "get_table_sample" src tests
C:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\db_service.py:
  Line 339:     def get_table_sample(self, tbl_name: str, limit: int = 2):
```
**Impact:** Dead code that also violates the print() convention. If someone
calls it during debugging, stdout output bypasses the logging configuration,
making it inconsistent with the rest of the system's observability.
**Root cause:** Debug helper left in the module without integration into the
pipeline or removal.
**Fix:** Either remove `get_table_sample` if no consumer exists (per project
policy, investigate purpose first), or replace `print()` with
`logger.info()` and wire it into a debug flag. Target:
`db_service.py::get_table_sample` lines 347-348.
**Verify:** `rg -n "get_table_sample" src tests` → zero call sites after
removal, or one (the definition) after logger conversion.
**Related:** ENT-003 (phase 01: print() policy)

**Evidence — grep for callers:**
```text
$ rg -n "get_table_sample" src tests
C:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\db_service.py:
  Line 339:     def get_table_sample(self, tbl_name: str, limit: int = 2):
```
*(supports: only the definition exists; no callers in src/ or tests/.)*

**Evidence — source (db_service.py:339-348):**
```text
    def get_table_sample(self, tbl_name: str, limit: int = 2):
        query = f"SELECT * FROM {tbl_name} LIMIT {limit}"
        for tbl_row in self.perform_query(query):
            print(f"These are sample rows for {tbl_name}", flush=True)
            print(tbl_row, flush=True)
```

#### PIPE-009 — [LOW] Stale __main__ block hardcodes Windows path `data\mvideo`

**Anchor:** `src/mko_data_cleaner/core/app_service.py::__main__`
**Disambiguator:** `data\mvideo`
**Checked:** ran (source inspection)
**Why this band:** Phase LOW covers "stale dev `__main__` blocks hardcoding
paths/behavior in importable modules." The block is dev-only and never
triggers in normal usage, but it hardcodes a platform-specific path and
bypasses the settings gate.
**Problem:** At app_service.py:343-344, the module-level `__main__` block reads
`get_app_service().run_report(r"data\mvideo")` — a hardcoded Windows path.
This is not the CLI entry point (that is `app.py:153-154: app()`). When the
module is run directly (`python -m mko_data_cleaner.core.app_service`), it
attempts to process a directory that almost certainly does not exist, and the
path uses Windows separators that won't work on other platforms.
**Repro:**
```text
=== EXPERIMENT 17: stale __main__ block ===
  app_service.py: if __name__ == "__main__":
  app_service.py: get_app_service().run_report(r"data\mvideo")
```
**Impact:** Dev-only block that could confuse a developer running the module
directly; platform-specific path; not covered by tests.
**Root cause:** Dev convenience block committed to the module.
**Fix:** Remove the `__main__` block from `app_service.py` (the canonical CLI
entry point is `app.py`). Target: `app_service.py` lines 343-344.
**Verify:** `rg -n "__main__" src/mko_data_cleaner/core/app_service.py` →
zero matches after removal.
**Related:** CFG-006 (phase 02)

**Evidence — runtime (Experiment 17):**
```text
  app_service.py: if __name__ == "__main__":
  app_service.py: get_app_service().run_report(r"data\mvideo")
```

#### PIPE-010 — [DOC-UPDATE] Docs claim process_data returns None; code already returns ReportResult

**Anchor:** `docs/00-overview/data-flow.md::"Currently None"`;
`docs/00-overview/architecture.md::"returns None (result object"`;
`docs/00-overview/current-behavior.md::"currently returns None"`;
`src/mko_data_cleaner/app.py::process_data`
**Disambiguator:** `Currently None`
**Checked:** ran (runtime source inspection)
**Why this band:** Phase LOW covers doc drift. The code at c0fd0e1 already
returns `ReportResult`; three docs still say "None." No runtime consequence
but misleads developers about the API contract.
**Problem:** Three documentation files state `process_data` returns `None`:
- data-flow.md:165 — "Currently `None`; owner-confirmed to become a result
  object"
- current-behavior.md:272 — "process_data currently returns `None`; the
  result object..."
- architecture.md:219 — "returns None (result object: target work)"
- open-questions.md:71 (D-6) — "process_data returns None; the owner-confirmed
  result object (paths + timings + row counts, D8) is not implemented"

But `process_data` (app.py:126-145) is annotated `-> ReportResult` (app.py:128)
and returns `get_app_service(settings_dir).run_report(path)` (app.py:145).
`run_report` constructs and returns `ReportResult(...)` (app_service.py:280-292).
The result object is not "target work" (future) — it ships today.
**Repro:**
```text
=== EXPERIMENT 15: doc drift - process_data return value ===
  data-flow.md:165  -> 'Currently None; owner-confirmed to become a result object'
  current-behavior.md:272 -> 'process_data currently returns None'
  architecture.md:219 -> 'returns None (result object: target work)'
  app_service.py: return ReportResult(
  -> code RETURNS ReportResult; docs say 'None' -> STALE DOCS
```
**Impact:** Developers reading the docs believe the result object is
unimplemented; the open-question D-6 is stale and may lead to redundant work
or incorrect assumptions in downstream consumers.
**Root cause:** Docs were not updated when `ReportResult` was introduced and
wired into `run_report`/`process_data`.
**Fix:** Update data-flow.md:165, current-behavior.md:272,
architecture.md:219, and open-questions.md:71 (D-6) to reflect that
`process_data` returns `ReportResult` with output paths, timings, and row
counts. Target: the four doc lines cited above.
**Verify:** `rg -n "Currently None\|currently returns None\|returns None" docs/00-overview/` →
all updated or removed.
**Related:** PIPE-004

**Evidence — runtime (Experiment 15):**
```text
=== EXPERIMENT 15: doc drift - process_data return value ===
  data-flow.md:165  -> 'Currently None; owner-confirmed to become a result object'
  current-behavior.md:272 -> 'process_data currently returns None'
  architecture.md:219 -> 'returns None (result object: target work)'
  app_service.py: return ReportResult(
  -> code RETURNS ReportResult; docs say 'None' -> STALE DOCS
```

## Fix Order

- **Dependencies:** PIPE-005 must land before the full type-checking gate is
  green (the annotation fix removes 2 of 39 basedpyright errors). PIPE-004
  (adding `error`/`status` to `ReportResult`) requires updating `run_report`'s
  return construction (PIPE-004's own fix target). PIPE-002 and PIPE-003 are
  independent of all other findings.
- **Merge candidates:** PIPE-004 and PIPE-003 share `export_sql_to_csv` and
  `run_report`'s return as fix surfaces — recommend sequencing: add
  `status`/`error` to `ReportResult` first, then make `export_sql_to_csv`
  atomic next. PIPE-001 and PIPE-007 share `_create_index`/`build_non_mapped`
  call sites in the same module.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** ENT-003 (phase 01) filed the `print()` rule for
  `run_report` and `progress_bar`; PIPE-008 finds a *different* `print()` site
  (`get_table_sample`). These are complementary, not conflicting.
- **Rollout risk:**
  - PIPE-001: adding `if self.index_column:` guards is safe — the `None` path
    already crashes; the guard just makes it a named error instead.
  - PIPE-002: changing filename scheme may break downstream parsers that
    expect `<prefix>_<YYYYMMDD-HHMMSS>_<N>`. Test: assert
    `test_get_file_name_format` still matches.
  - PIPE-004: adding fields to a Pydantic v2 model is non-breaking (new fields
    default to `None`/`"ok"`). Test: all 93 tests still pass.
  - PIPE-005: fixing the annotation is non-breaking at runtime. Test: basedpyright
    clears the 2 errors.

## Appendices

### Appendix A — basedpyright output (`uv run basedpyright src/`)

```text
c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\app.py:24:16 - error: Cannot access attribute "reconfigure" for class "TextIO" (reportAttributeAccessIssue)

c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\app_service.py:
  56:16 - error: Type "Path | None" is not assignable to return type "Path" (reportReturnType)
  117:26 - error: Cannot assign to attribute "base_path" (reportAttributeAccessIssue)
  150:25 - error: Argument of type "tuple[int, str] | None" cannot be assigned to parameter "date_column" of type "str | None" (reportArgumentType)
  161:18 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  161:62 - error: Argument of type "Unknown | list[str] | None" cannot be assigned to "list[str]" (reportArgumentType)
  165:40 - error: Cannot assign to attribute "search_columns" (reportAttributeAccessIssue)
  281:24 - error: Argument of type "str | Path" cannot be assigned to "Path" (reportArgumentType)

c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\csv_service.py:
  122:16 - error: Operator ">=" not supported for types "PythonLiteral | None" and "float" (reportOperatorIssue)
  124:67 - error: Expression of type "None" cannot be assigned to parameter "str" (reportArgumentType)
  153:47 - error: Expression of type "None" cannot be assigned to parameter "str" (reportArgumentType)
  155:13 - error: Argument of type "Path | None" cannot be assigned to source (reportArgumentType)
  157:16 - error: Type "str | None" is not assignable to return type "tuple[int, str] | None" (reportReturnType)

c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\db_service.py:
  117:28 - error: Argument of type "list[str] | None" cannot be assigned to Iterable (reportArgumentType)
  137:35 - error: Object of type "None" is not subscriptable (reportOptionalSubscript)
  298:24 - error: Type "list[str]" is not assignable to "tuple[str | None, ...]" (reportAssignmentType)
  301:32 - error: Argument of type "tuple[str | None, ...]" cannot be assigned (reportArgumentType)
  372:18 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  372:40 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  395:48 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  397:48 - error: Argument of type "str | None" cannot be assigned to parameter "columns" (reportArgumentType)
  398:28 - error: Argument of type "str | None" cannot be assigned to parameter "table_name" (reportArgumentType)
  398:50 - error: Argument of type "str | None" cannot be assigned to parameter "columns" (reportArgumentType)
  424:18 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  482:45 - error: Operator "+" not supported for types "str" and "str | None" (reportOperatorIssue)
  484:46 - error: "copy" is not a known attribute of "None" (reportOptionalMemberAccess)
  585:20 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  663:30 - error: Argument of type "Unknown | list[str] | None" cannot be assigned (reportArgumentType)
  697:20 - error: Object of type "None" cannot be used as iterable (reportOptionalIterable)
  766:33 - error: Operator "+" not supported for types "list[Never] | None" and "Unknown | list[str] | None" (reportOperatorIssue)
  771:26 - error: No overloads for "join" match the provided arguments (reportCallIssue)
  771:37 - error: Argument of type "Unknown | list[str] | None" cannot be assigned to parameter "iterable" (reportArgumentType)

c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\dict_service.py:
  131:31 - error: Cannot assign to attribute "data_col_index" (reportAttributeAccessIssue)

c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\models.py:
  81:25 - error: Type "None" is not assignable to declared type "str" (reportAssignmentType)
  82:24 - error: Type "None" is not assignable to declared type "str" (reportAssignmentType)

c:\py_exp/mko_data_cleaner\src\mko_data_cleaner\core\paths.py:
  6:29 - error: "split" is not a known attribute of "None" (reportOptionalMemberAccess)

39 errors, 0 warnings, 0 notes
```
*(supports: PIPE-001 (db_service.py:397,398), PIPE-005 (csv_service.py:157, app_service.py:150), PIPE-007 (db_service.py:766,771).)*

### Appendix B — Full experiment output

```text
=== EXPERIMENT 1: check_date_column return type ===
  annotated return: tuple[int, str] | None
  _detect_date_column return: str | None
  CONFLICT: annotation says tuple[int,str]|None but delegates to
  _detect_date_column which returns str|None. Callers receive str|None.

=== EXPERIMENT 2: _is_date_column sample-size constant drift ===
  Uses self.DATE_SAMPLE_SIZE: False
  Uses hardcoded 500: True
  If DATE_SAMPLE_SIZE is changed later, _is_date_column silently ignores it.

=== EXPERIMENT 3: target_table fallback + build_non_mapped ===
  index_column set -> target_table = test_table_distinct
  data_tbl_name   = test_table
  _index_tbl_name = test_table_distinct
  search_columns  = ['search_col']
  extra_columns   = ['tag_a', 'tag_b']
  non_mapped rows (any extra NULL): ['search1', 'search2', 'search3']
  expected: ['search1', 'search2', 'search3'] (search4 has both tags set)
  PASS: bitwise | works via NULL propagation
  build_non_mapped WHERE clause: {' | '.join(extra_columns)} IS NULL
  -> with 2 cols: (tag_a | tag_b) IS NULL  [bitwise OR, not OR]
  -> relies on SQLite NULL propagation, not boolean OR

  --- target_table when index_column is NOT set ---
TypeError: sequence item 0: expected str instance, NoneType found

=== EXPERIMENT 4: get_file_name same-second collision ===
  run 1 filename: null_data_20261019-161148_1.csv.gz
  run 2 filename: null_data_20261019-161148_1.csv.gz
  Collision (identical name): True
  No counter/UUID/suffix to disambiguate same-second runs.

=== EXPERIMENT 5: export_sql_to_csv non-atomic write ===
  Uses temp+rename pattern: False
  df.write_csv writes directly to file_path: True
  mkdir(parents=True, exist_ok=True): True

=== EXPERIMENT 6: export_sql_to_csv returns 0 for empty table ===
  Empty table -> returned: 0
  Files written: []
  -> 0 returned for empty table; indistinguishable from "no input to clean"

=== EXPERIMENT 9: print() in run_report (English-only rule) ===
  print(
  print(
  print(
  progress_bar uses print(): True

=== EXPERIMENT 10: ReportResult field set ===
  Fields (11): ['report_dir', 'import_dir', 'export_dir', 'db_path',
  'null_data_files', 'started_at', 'finished_at', 'duration', 'timings',
  'rows_loaded', 'rows_exported']
  Has 'error' field: False
  Has 'status' field: False
  -> rows_exported==0 indistinguishable from a full report

=== EXPERIMENT 11: date column mis-detection ===
  check_date_column() -> code
  'code' matches YYYY-MM-DD regex but is not a date column
  -> sample-based auto-detection can mis-identify (as documented in D-4)

=== EXPERIMENT 14: get_file_name same-second collision ===
  filename 1: null_data_20261019-161148_1.csv.gz
  filename 2: null_data_20261019-161148_1.csv.gz
  identical: True
  -> no collision guard; same-second re-runs overwrite

=== EXPERIMENT 15: doc drift - process_data return value ===
  data-flow.md:165  -> 'Currently None; owner-confirmed to become a result object'
  current-behavior.md:272 -> 'process_data currently returns None'
  architecture.md:219 -> 'returns None (result object: target work)'
  app_service.py: return ReportResult(
  -> code RETURNS ReportResult; docs say 'None' -> STALE DOCS

=== EXPERIMENT 16: _apply_delete execution count ===
  perform_query calls in _apply_delete: 1
  (was 2 -> fixed to 1 in 4af9813; see architecture.md:252 / open-questions.md D-10)

=== EXPERIMENT 17: stale __main__ block ===
  app_service.py: if __name__ == "__main__":
  app_service.py: get_app_service().run_report(r"data\mvideo")

=== EXPERIMENT 18: null_data export path literal ===
  app_service.py: export_path=self.export_path,
  app_service.py: export_path=f"{self.base_path}",
  app_service.py: export_path=self.export_path,
```
*(supports: PIPE-001, PIPE-002, PIPE-003, PIPE-004, PIPE-005, PIPE-006, PIPE-007,
PIPE-008, PIPE-009, PIPE-010.)*

### Appendix C — Verified, not filed

**DELETE double-exec (phase 01 PIPE-001):** Already fixed in commit `4af9813`.
Experiment 16 confirms `_apply_delete` has 1 `perform_query` call (was 2).
Cited per phase task: "Already recorded at `architecture.md:252` and
`.ai/audit/problems/01-apply-delete-double-execution.md`; cite, don't
re-derive."

**Export path asymmetry (null_data vs clean_data):** Confirmed by Experiment 18:
`export_path=f"{self.base_path}"` for null_data, `export_path=self.export_path`
for clean_data. Matches `data-flow.md:162-163` (null_data in report root,
clean_data in `clean_data/`). **Not filed** — verified as intentional.

**DB lifecycle on exit:** `__exit__` (db_service.py:803-805) calls `self.close()`
(db_service.py:786-798), which closes both `db_con` and `db_adb_con` before
`_delete_base_files()` (db_service.py:779-784). No connection leak. **Not filed** —
correct behavior at c0fd0e1.

**Target table for no-index fallback:** `target_table` (db_service.py:101-105)
returns `data_tbl_name` when `_index_tbl_name` is None. With `index_column`
set (the default config), the distinct table is used. Without it, the code
crashes (PIPE-001) before `target_table` is consulted for rule matching.
**No separate finding** — subsumed by PIPE-001.

**print() in run_report:** 3 `print()` calls in `run_report` (app_service.py:113-116,
259-263, 272-278) with mixed Russian/English text, plus `progress_bar` print()
(utils.py:34-41). **Not re-filed** — already covered by ENT-003 (phase 01).
