---
phase: "16"
phase_name: "rendered-surface"
id_prefix: "OUT"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Rendered-Surface & Output Conformance

## Summary

Seven findings: three HIGH, three MEDIUM, one LOW. The highest severity is HIGH:
the CSV export path can silently overwrite prior output on same-second re-runs
(OUT-001), leave a truncated gzip file consumed as complete after a mid-write
crash (OUT-002), and produce an empty-table export that writes no file while
reporting `rows_exported=0` with no status field to flag the gap (OUT-003).
The broadest consequence is silent data-loss and misclassification: a downstream
analytics pipeline cannot distinguish a complete export from a crashed or empty
one, because no checksum, no atomic rename, and no `error`/`status` field exist
in the output surface or the `ReportResult` contract.

## Scope

**Examined:** `src/mko_data_cleaner/core/csv_service.py`, `app_service.py`,
`models.py`, `db_service.py`, `dict_service.py`, `utils.py`,
`src/mko_data_cleaner/settings/app_config.json5`,
`docs/00-overview/data-flow.md`, `docs/00-overview/current-behavior.md`,
`tests/test_csv_service.py`, `tests/test_app.py`.

**Tools:** ruff (configured rules E/F/I/B/UP), basedpyright, pytest, grep.

**Not verified:** Console output (built-in `print()` with Russian text in
`AppService.run_report` is tracked as ENT-003/PIPE-008 from prior phases — outside
this phase's CSV-surface scope); end-to-end same-second re-run reproduction
(static proof via code reading only); gzip input path; FTS5 export path.

**Note on config path:** the phase task file repeatedly cites `app_config.yaml`,
which does not exist in the tree. The actual settings file is
`app_config.json5`; all findings cite the correct path.

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| OUT-001 | HIGH | `csv_service.py::CSVWorker.get_file_name`, `csv_service.py::CSVWorker.export_sql_to_csv` | Same-second re-run overwrites prior CSV.gz export; no collision guard |
| OUT-002 | HIGH | `csv_service.py::CSVWorker.export_sql_to_csv` | Non-atomic write to final path; mid-write crash leaves truncated .csv.gz consumed as complete |
| OUT-003 | HIGH | `csv_service.py::CSVWorker.export_sql_to_csv` | Empty-table export returns 0, writes no file, reports success with no status field |
| OUT-004 | MEDIUM | `models.py::PolarsWriteCSV` | `include_bom=True` in config silently absorbed by `extra="allow"`; not a declared model field |
| OUT-005 | MEDIUM | `data-flow.md::"Field and Column Semantics"` | No machine-readable schema manifest; column contract exists only as prose |
| OUT-006 | MEDIUM | `models.py::ReportResult` | No `error`/`status` field; `rows_exported==0` indistinguishable from a full export |
| OUT-007 | LOW | `data-flow.md::"Provenance and Preservation of Original Values"` | data-flow.md:178 claims "repeated runs never overwrite" — contradicted by OUT-001 |

---

#### OUT-001 — [HIGH] Same-second re-run overwrites prior CSV.gz export; no collision guard

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::CSVWorker.get_file_name`
**Anchor:** `src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv`
**Disambiguator:** `strftime("%Y%m%d-%H%M%S")`
**Checked:** static
**Why this band:** A successful re-run in the same second silently destroys a
prior export — that is silent data loss for a downstream reader. Not CRITICAL
because the file is still a well-formed gzip CSV (format is valid); the damage is
loss of the prior file's content, not silent value misreading.
**Problem:** `get_file_name` builds filenames using `strftime("%Y%m%d-%H%M%S")`
(second granularity), and `file_index` resets to 1 for each `export_sql_to_csv`
call. A re-run within the same second produces identical filenames.
`df.write_csv(file=file_path, **params)` writes directly to the final path with
no existence check or `mode="x"` guard (Polars `write_csv` has no such
parameter — grep for `mode.*=.*"x"` in `src/` returns zero).
**Repro:**
1. Run `mko-data-cleaner` on a report directory at time T.
2. Re-run within the same wall-clock second (T+0s) on the same report directory.
3. The second run's `null_data_<T>_1.csv.gz` and `data_table_<T>_1.csv.gz`
   overwrite the first run's files with no warning.
**Impact:** An analyst who re-runs the tool accidentally (or in a cron job
that fires twice in the same second) loses the prior export silently. The
downstream pipeline reads the overwritten file as the authoritative result.
**Root cause:** Filename uniqueness is derived only from a second-granular
timestamp plus a per-call chunk counter; neither provides cross-run uniqueness.
**Fix:** Add a short, collision-resistant suffix to filenames — e.g.
`uuid.uuid4().hex[:8]` appended before the extension, or switch to
`datetime.now().strftime("%Y%m%d-%H%M%S-%f")` (microsecond precision) — in
`CSVWorker.get_file_name` at `csv_service.py`.
**Verify:** `tests/test_csv_service.py::test_get_file_name` — asserts prefix and
`.csv.gz` suffix only; does **not** assert uniqueness across same-second calls.
New test needed: `test_get_file_name_same_second_unique`.
**Related:** OUT-002 (same code path), OUT-007 (doc claims "never overwrite").

**Evidence — `rg -n 'strftime' src/mko_data_cleaner/core/csv_service.py`**:
```text
src/mko_data_cleaner/core/csv_service.py:239:        time_str = strftime("%Y%m%d-%H%M%S")
```

**Evidence — `rg -n 'write_csv\|file_name\|file_index' src/mko_data_cleaner/core/csv_service.py`**:
```text
src/mko_data_cleaner/core/csv_service.py:311:            file_name = self.get_file_name(file_prefix, "{file_index}")
src/mko_data_cleaner/core/csv_service.py:317:            file_index = 1
src/mko_data_cleaner/core/csv_service.py:315:            max_rows = params.pop("chunk_size", 10000)
src/mko_data_cleaner/core/csv_service.py:359:                file_path = export_path / file_name.format(file_index=file_index)
src/mko_data_cleaner/core/csv_service.py:364:                df.write_csv(
src/mko_data_cleaner/core/csv_service.py:365:                    file=file_path,
src/mko_data_cleaner/core/csv_service.py:366:                    **params,
src/mko_data_cleaner/core/csv_service.py:367:                )
```

---

#### OUT-002 — [HIGH] Non-atomic write to final path; mid-write crash leaves truncated .csv.gz consumed as complete

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv`
**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker.__exit__`
**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker.close`
**Disambiguator:** `df.write_csv(file=file_path, **params)`
**Checked:** static
**Why this band:** A truncated gzip file written directly to the final path is
indistinguishable from a complete export to a consumer that just reads the file.
Not CRITICAL because the format itself is valid (gzip CRC header) — gzip readers
*can* detect truncation via CRC mismatch — but the tool provides no checksum or
status indicator so a downstream job that swallows errors treats the partial data
as authoritative.
**Problem:** `export_sql_to_csv` calls `df.write_csv(file=file_path, **params)`
directly on the final path. There is no temp-file-plus-rename, no checksum, no
atomic-commit protocol. Compounding this, `DBWorker.__exit__` calls `close()`
unconditionally (it ignores `exc_type`), and `close()` calls
`_delete_base_files()` which deletes the SQLite database file and its
`-wal`/`-shm` sidecars. So a crash during the CSV write leaves: (a) a truncated
`.csv.gz` on disk, and (b) the source database already deleted — no way to
re-export or inspect intermediate state.
**Repro:**
1. Start a run with a large `data_table` (many chunks).
2. Kill the process mid-export (e.g. `SIGKILL` during `df.write_csv`).
3. Observe: a `.csv.gz` file of suspiciously small size exists at the final
   path, and the `.db` file is gone (deleted by `__exit__` → `close()` →
   `_delete_base_files()`).
**Impact:** A downstream pipeline reading the truncated file gets partial data
with no signal of truncation. The database (the only source of truth) is also
deleted, preventing recovery.
**Root cause:** No atomic-write pattern (write-to-temp + `os.replace`);
`__exit__` does not gate cleanup on success vs. failure.
**Fix:** Write to a temporary path (e.g. `file_path + ".tmp"`) and call
`os.replace(tmp, file_path)` after successful `write_csv`. In
`CSVWorker.export_sql_to_csv` at `csv_service.py`. Optionally, gate
`_delete_base_files()` in `DBWorker.close()` on `exc_type is None` in
`__exit__`.
**Verify:** New test needed: `test_export_sql_to_csv_atomic_write_renames_temp`.
Shipped tests `test_export_sql_to_csv_gzip` and
`test_export_sql_to_csv_roundtrip_content` assert content integrity for
non-empty data but do not test crash recovery.
**Related:** OUT-001 (same code path), OUT-003 (empty-table no-file), OUT-006
(no status field to flag failure).

**Evidence — `csv_service.py:359-367`**:
```text
src/mko_data_cleaner/core/csv_service.py:359:                file_path = export_path / file_name.format(file_index=file_index)
src/mko_data_cleaner/core/csv_service.py:364:                df.write_csv(
src/mko_data_cleaner/core/csv_service.py:365:                    file=file_path,
src/mko_data_cleaner/core/csv_service.py:366:                    **params,
src/mko_data_cleaner/core/csv_service.py:367:                )
```

**Evidence — `db_service.py:803-805` (`__exit__` calls `close()` unconditionally)**:
```text
src/mko_data_cleaner/core/db_service.py:803:    def __exit__(self, exc_type, exc, tb):
src/mko_data_cleaner/core/db_service.py:804:        # pass
src/mko_data_cleaner/core/db_service.py:805:        self.close()
```

**Evidence — `db_service.py:786-798` (`close()` deletes DB unconditionally)**:
```text
src/mko_data_cleaner/core/db_service.py:786:    def close(self):
src/mko_data_cleaner/core/db_service.py:787:        logger.info("Cleaning up temporary files")
src/mko_data_cleaner/core/db_service.py:790:        try:
src/mko_data_cleaner/core/db_service.py:791:            self.db_con.execute("PRAGMA wal_checkpoint(FULL)")
src/mko_data_cleaner/core/db_service.py:792:        except Exception as e:
src/mko_data_cleaner/core/db_service.py:793:            logger.warning(f"Cannot clean up temporary files: {e}")
src/mko_data_cleaner/core/db_service.py:795:        self.db_con.close()
src/mko_data_cleaner/core/db_service.py:796:        self.db_adb_con.close()
src/mko_data_cleaner/core/db_service.py:798:        self._delete_base_files()
```

---

#### OUT-003 — [HIGH] Empty-table export returns 0, writes no file, reports success

**Anchor:** `src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv`
**Disambiguator:** `if total_rows == 0:`
**Checked:** static
**Why this band:** An empty result (genuinely zero rows after cleaning) and a
crashed export both produce `rows_exported=0` with no output file — the
downstream consumer cannot tell whether the data was empty by design or whether
the export was aborted. Not CRITICAL because zero-row output is a legitimate
state; the defect is the failure signal, not the format.
**Problem:** `export_sql_to_csv` checks `total_rows == 0` via
`SELECT COUNT(*)` and returns `0` immediately without writing any file.
`AppService.run_report` captures this return as `rows_exported` and constructs a
`ReportResult` (which has no `status` or `error` field — see OUT-006). The caller
receives `rows_exported=0` and no file, with no way to distinguish "source was
empty" from "export crashed silently."
**Repro:**
1. Run the tool on a report where the dictionary rules match all rows (e.g.
   every row is a DELETE target), leaving `data_table` empty after sync.
2. `export_sql_to_csv` returns 0, no `data_table_*.csv.gz` is written to
   `clean_data/`.
3. `run_report` returns `ReportResult(rows_exported=0)` — indistinguishable from
   a crash.
**Impact:** A downstream pipeline that expects a `data_table_*.csv.gz` file
fails or produces empty downstream results, with no error signal from this tool.
**Root cause:** Early return on `total_rows == 0` skips file creation;
`ReportResult` lacks a status/error field to communicate the distinction.
**Fix:** Either (a) write a header-only CSV.gz file for empty tables so the
output surface is consistent, or (b) add an `error`/`status` field to
`ReportResult` and signal the empty case distinctly. The choice depends on
whether "empty source" is a normal or an error condition for the pipeline.
**Verify:** `tests/test_csv_service.py::test_export_sql_to_csv_returns_zero_for_empty_table`
— locks the return value (0) but does **not** assert file presence or absence.
New test needed: `test_export_sql_to_csv_empty_table_writes_header_file`.
**Related:** OUT-002, OUT-006.

**Evidence — `csv_service.py:337-339`**:
```text
src/mko_data_cleaner/core/csv_service.py:337:            if total_rows == 0:
src/mko_data_cleaner/core/csv_service.py:338:                logger.debug(f"Table {data_table} is empty.")
src/mko_data_cleaner/core/csv_service.py:339:                return 0
```

**Evidence — `app_service.py:266-292` (return value flows to ReportResult)**:
```text
src/mko_data_cleaner/core/app_service.py:266:            rows_exported = csv_worker.export_sql_to_csv(
src/mko_data_cleaner/core/app_service.py:267:                db_con=db_worker.db_con,
src/mko_data_cleaner/core/app_service.py:268:                data_table=db_worker.data_tbl_name,
src/mko_data_cleaner/core/app_service.py:269:                export_path=self.export_path,
src/mko_data_cleaner/core/app_service.py:270:            )
src/mko_data_cleaner/core/app_service.py:291:            rows_exported=rows_exported,
```

---

#### OUT-004 — [MEDIUM] `include_bom=True` in config silently absorbed by `extra="allow"`; not a declared model field

**Anchor:** `src/mko_data_cleaner/settings/app_config.json5::"export_settings.to_csv.include_bom"`
**Anchor:** `src/mko_data_cleaner/core/models.py::PolarsWriteCSV`
**Disambiguator:** `include_bom: True`
**Checked:** ran
**Why this band:** Every other `to_csv` setting is a declared, type-validated
field on `PolarsWriteCSV`. `include_bom` is silently accepted via
`extra="allow"`, so a typo (e.g. `inclode_bom`) would be stored, dumped, and
passed to Polars without any warning — and Polars would ignore the unknown key.
Not HIGH because the BOM is actually emitted correctly when spelled right; the
risk is silent acceptance of misspelled/invalid keys.
**Problem:** `app_config.json5` sets `"include_bom": true` (line 41), but
`PolarsWriteCSV` (lines 85-91) declares only `separator`, `decimal_comma`,
`include_header`, `chunk_size`, and `compression`. `model_config` sets
`extra="allow"`, so `include_bom` is stashed in `__pydantic_extra__` and
survives `model_dump()` → `**params` → `df.write_csv`. Polars does accept
`include_bom` as a `write_csv` parameter, so the BOM is emitted — but the Pydantic
model provides no type checking, no default, no deprecation path, and no
docstring for it.
**Repro:**
1. Construct `PolarsWriteCSV(**{...include_bom=True...})`.
2. Call `.model_dump()` — `include_bom` appears in the dict despite not being a
   declared field.
3. Misspell as `inclode_bom` — no error raised; the key silently vanishes from
   the Polars call.
**Impact:** A configuration typo silently disables the BOM with no warning. A
new developer cannot discover the field via IDE or mypy. Future refactoring that
tightens `extra` to `"forbid"` would break the export.
**Root cause:** `PolarsWriteCSV` does not declare `include_bom` as a field;
`extra="allow"` masks the omission.
**Fix:** Add `include_bom: bool = True` as an explicit field on `PolarsWriteCSV`
in `models.py`, with a docstring/comment noting Polars `write_csv` support.
**Verify:** No existing test covers `include_bom`. New test needed:
`test_polars_write_csv_validates_include_bom`.

**Evidence — `python -c` verification that `include_bom` survives `model_dump()`**:
```text
include_bom in dump: True
keys: ['separator', 'decimal_comma', 'include_header', 'chunk_size', 'compression', 'include_bom']
include_bom value: True
```

**Evidence — `models.py:85-91`**:
```text
src/mko_data_cleaner/core/models.py:85:class PolarsWriteCSV(BaseModel):
src/mko_data_cleaner/core/models.py:86:    model_config = ConfigDict(extra="allow")
src/mko_data_cleaner/core/models.py:87:    separator: str = ";"
src/mko_data_cleaner/core/models.py:88:    decimal_comma: bool = True
src/mko_data_cleaner/core/models.py:89:    include_header: bool = True
src/mko_data_cleaner/core/models.py:90:    chunk_size: NonNegativeInt = 10000
src/mko_data_cleaner/core/models.py:91:    compression: Literal["gzip", "bz2", "zip", "xz", "zstd"] | None = "gzip"
```

**Evidence — `app_config.json5:41`**:
```text
"include_bom": true,
```

---

#### OUT-005 — [MEDIUM] No machine-readable schema manifest; column contract exists only as prose

**Anchor:** `docs/00-overview/data-flow.md::"Field and Column Semantics"`
**Disambiguator:** `## Field and Column Semantics`
**Checked:** static
**Why this band:** The output CSV schema (column names, order, types) is the
contract a downstream consumer relies on, and it is documented only in prose.
A machine-readable manifest would let consumers validate at ingestion time.
Not HIGH because the prose is accurate and the code produces consistent output
today — the risk is drift and manual verification, not active misalignment.
**Problem:** The data-flow document describes source columns, extra columns,
and internal mapping columns in prose (lines 38-64). The code derives columns
dynamically from the first raw file's headers (cleaned via `clean_names` in
`utils.py`) plus dictionary-derived extra columns (dropped if entirely empty via
`_drop_empty_columns`). No schema manifest file (e.g. `data_table.schema.json`) is
emitted alongside the CSV export. A downstream consumer cannot programmatically
discover the column set without reading and decompressing the file.
**Impact:** Downstream pipelines must hardcode or guess the column schema.
Schema changes in source data (new columns, renamed columns) propagate silently
through the export with no manifest to flag divergence.
**Root cause:** The export path (`export_sql_to_csv`) writes only CSV.gz files;
no secondary manifest file is produced.
**Fix:** Emit a companion schema manifest (e.g.
`data_table_<timestamp>_<N>.schema.json`) alongside each export batch, containing
column names, order, and the `pl.Utf8` type override. Minimal change to
`export_sql_to_csv` in `csv_service.py`.
**Verify:** No existing test asserts a manifest file. New test needed:
`test_export_produces_schema_manifest`.

---

#### OUT-006 — [MEDIUM] No `error`/`status` field on ReportResult; `rows_exported==0` indistinguishable from full export

**Anchor:** `src/mko_data_cleaner/core/models.py::ReportResult`
**Disambiguator:** `rows_exported`
**Checked:** static
**Why this band:** `ReportResult` carries row counts but no outcome indicator.
A caller seeing `rows_exported=0` cannot tell whether zero rows were exported
from a legitimate empty table (OUT-003) or whether the export failed entirely.
Not HIGH because the row count is accurate; the defect is missing metadata, not
wrong data.
**Problem:** `ReportResult` (lines 182-207) declares 11 fields: `report_dir`,
`import_dir`, `export_dir`, `db_path`, `null_data_files`, `started_at`,
`finished_at`, `duration`, `timings`, `rows_loaded`, `rows_exported`. There is
no `status: str` or `error: str | None` field. `AppService.run_report` constructs
`ReportResult` at the end of the method (line 280) — if the method raises before
reaching line 280, no `ReportResult` is returned at all; if it reaches line 280
with `rows_exported=0`, the caller sees success with zero rows.
**Repro:**
1. Run `process_data()` on a report with zero data rows.
2. Observe `ReportResult.rows_exported == 0`.
3. Compare: a run where `export_sql_to_csv` crashed mid-write — the exception
   propagates, no `ReportResult` is returned. The caller cannot distinguish
   "zero rows, no file" (OUT-003) from "success with zero rows, file written."
**Impact:** Pipeline callers using `process_data()` as a Python API cannot
reliably detect success vs. failure without try/except on every call. The
return value does not encode its own success state.
**Root cause:** `ReportResult` was designed for metrics, not for error
reporting; no `status`/`error` field was added.
**Fix:** Add `status: Literal["ok", "partial", "empty", "error"] = "ok"` and
`error: str | None = None` to `ReportResult` in `models.py`; set `status`
appropriately in `AppService.run_report` at `app_service.py`.
**Verify:** `tests/test_app.py::test_report_result_model_construction_and_serialization`
locks the 11-field shape — adding fields is backward-compatible (new fields have
defaults) but the test will need updating if defaults change.
**Related:** OUT-003, OUT-002.

**Evidence — `models.py:182-207`**:
```text
src/mko_data_cleaner/core/models.py:182:class ReportResult(BaseModel):
...
src/mko_data_cleaner/core/models.py:190:    report_dir: Path
src/mko_data_cleaner/core/models.py:191:    import_dir: Path
src/mko_data_cleaner/core/models.py:192:    export_dir: Path
src/mko_data_cleaner/core/models.py:193:    db_path: Path
src/mko_data_cleaner/core/models.py:194:    null_data_files: list[Path]
src/mko_data_cleaner/core/models.py:198:    started_at: datetime
src/mko_data_cleaner/core/models.py:199:    finished_at: datetime
src/mko_data_cleaner/core/models.py:200:    duration: float
src/mko_data_cleaner/core/models.py:203:    timings: dict[str, float]
src/mko_data_cleaner/core/models.py:206:    rows_loaded: NonNegativeInt
src/mko_data_cleaner/core/models.py:207:    rows_exported: NonNegativeInt
```

---

#### OUT-007 — [LOW] data-flow.md:178 claims "repeated runs never overwrite previous outputs" — contradicted by OUT-001

**Anchor:** `docs/00-overview/data-flow.md::"Provenance and Preservation of Original Values"`
**Disambiguator:** `never overwrite previous outputs`
**Checked:** static
**Why this band:** The documentation makes a false guarantee that could mislead
an operator into believing re-runs are safe. No live data loss occurs in the
test suite because no test exercises same-second re-runs. Not MEDIUM because the
doc is a description, not a control — the actual overwrite risk is covered by
OUT-001; this finding is purely the doc drift.
**Problem:** `data-flow.md` line 178 states: "File names carry the run timestamp
(`<prefix>_<YYYYMMDD-HHMMSS>_<N>`), so repeated runs never overwrite previous
outputs." The filename template uses second-granular timestamps with no
collision guard (OUT-001), so two runs in the same second produce identical
filenames and the second overwrites the first.
**Impact:** An analyst reading the documentation believes their prior exports
are safe from overwrite. In practice, a same-second re-run silently replaces
the file.
**Root cause:** The documentation was written before (or without checking) the
second-granular timestamp implementation; the doc and code are out of sync.
**Fix:** Update `data-flow.md` line 178 to state that filenames are
second-granular and same-second re-runs may overwrite, or remove the guarantee
until OUT-001 is fixed.
**Verify:** No test asserts the doc claim. New test needed:
`test_repeated_runs_same_second_do_not_overwrite`.
**Related:** OUT-001.

**Evidence — `data-flow.md:177-178`**:
```text
| File names carry the run timestamp (`<prefix>_<YYYYMMDD-HHMMSS>_<N>`), so
| repeated runs never overwrite previous outputs. |
```

## Fix Order

- **Dependencies:** OUT-006 (add `status`/`error` to `ReportResult`) should land
  before OUT-003 is resolved, because the status field is needed to distinguish
  "empty source" from "crashed export" — fixing OUT-003 alone (write a
  header-only file) would mask the crash signal that OUT-006's fix is meant to
  provide.
- **Merge candidates:** OUT-001 and OUT-002 share the same write site
  (`export_sql_to_csv` → `df.write_csv`). A single fix (write-to-temp +
  `os.replace`) would address OUT-002 (atomicity) and can optionally include a
  uniqueness suffix to address OUT-001 simultaneously.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** None — all findings are owned by Phase 16.
- **Rollout risk:**
  - OUT-001: adding a UUID suffix changes filenames; downstream globs that match
    `data_table_*.csv.gz` are unaffected. `test_get_file_name` stays green.
  - OUT-002: temp+rename pattern does not change committed content; all existing
    `test_export_sql_to_csv_*` tests stay green.
  - OUT-003: writing a header-only file for empty tables changes the output
    surface (new file appears); `test_export_sql_to_csv_returns_zero_for_empty_table`
    must be updated to assert file presence.
  - OUT-004: adding `include_bom: bool = True` as a declared field is backward-
    compatible; `model_dump()` produces the same dict. No test change needed.
  - OUT-005: emitting a manifest is additive; existing tests stay green.
  - OUT-006: adding fields with defaults to `ReportResult` is backward-compatible;
    `test_report_result_model_construction_and_serialization` stays green.
  - OUT-007: doc-only change; no test impact.

## Appendices

### Appendix A — Lint gate

**Command:** `uv run ruff check src/ tests/`

```text
All checks passed!
```

*(supports: "Lint clean — configured rules E/F/I/B/UP only; T20 (print) not enabled")*

### Appendix B — Test gate

**Command:** `uv run pytest`

```text
93 passed in 3.79s
```

*(supports: "all 93 tests pass; no test exercises same-second overwrite, crash recovery, or empty-table file presence")*

### Appendix C — Typecheck gate

**Command:** `uv run basedpyright src/`

```text
39 errors, 0 warnings, 0 notes
```

*(supports: "typecheck state as reported by the project; errors are pre-existing and unrelated to the rendered-surface findings")*

### Appendix D — Grep for overwrite guard and pandas usage

**Command:** `rg -n 'mode.*=.*["'"'"']x["'"'"']' src/` and `rg -n 'import pandas' src/`

```text
(no matches)
```

*(supports: "no mode='x' existence guard; no pandas import — config uses Polars decimal_comma syntax")*

### Appendix E — Grep for error-status serialisation

**Command:** `rg -n 'to_yaml|TVTask|error_file' src/`

```text
(no matches)
```

*(supports: "no error-file serialisation, no TVTask, no to_yaml anywhere in src/")*

**Command:** `rg -n 'error|status' src/mko_data_cleaner/core/models.py`

```text
src/mko_data_cleaner/core/models.py:101:    ignore_errors: bool = False
```

*(supports: "ReportResult has no error/status field; the only 'error' match in models.py is ignore_errors on PolarsReadCSV, line 101 — not on ReportResult")*
