---
phase: "16"
phase_name: "Rendered-Surface & Output Conformance Measurement"
phase_file: ".kilo/commands/audit/phases/16-audit-rendered-surface.md"
source: ".ai/reports/phase-16-auditor-report.md"
author: researcher
verdict: REBASE
confidence: HIGH
code_ref: "b390d04"
severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"
---

# Phase 16 — Researcher Recommendations

## 1. Executive Summary

The auditor verdict is **REBASE**. The Phase 16 spec file
(`16-audit-rendered-surface.md`, 126 lines) is framed around concepts that
**do not exist** in this codebase: an API result pipeline, `mode="x"`
exclusive-create, UTF-8 BOM via `encoding="utf-8-sig"`, and YAML serialised
error files (`TVTask.to_yaml`). A repo-wide grep across `src/mko_data_cleaner/`
for `to_yaml|TVTask|error_file|yaml_error|mode.*=.*['"]x['"]|async def|asyncio|httpx|aiohttp|api_key`
returns **zero code matches** (auditor §Appendix A).

The runtime is a **synchronous, single-shot, local-only** CLI + library:
file → SQLite (import, FTS5/LIKE mapping) → two gzip-compressed CSV exports
(`null_data_*.csv.gz` to report root; `data_table_*.csv.gz` to `clean_data/`)
via `CSVWorker.export_sql_to_csv()` — one OS process per invocation, no
network, no API, no event loop.

The underlying concern — *does the rendered CSV output conform to a declared
surface contract* (schema, format, presence, crash-safety-readiness)? — is
**valid and alive**. Only the *framing* is obsolete. This document prescribes
the re-based block set (§2), confirms **6 findings to surface** with `OUT-` IDs
(§3), restates the severity taxonomy with CSV/null_data-grounded examples (§4),
confirms the Report Output settings with `OUT-` collision **disproven** (§5),
prescribes the scope/citation cleanup (§6), and provides a line-by-line edit
delta for the 126-line phase file (§7).

Evidence was verified against the working tree at `b390d04`:
`csv_service.py` (375 lines — `get_file_name` 237–240, `export_sql_to_csv`
269–375, direct-to-final-path write 356–361, empty-table early-return 334–336),
`app_service.py` (342 lines — `run_report` 110–290, export calls 246–268,
`ReportResult` construction 278–290), `models.py` (195 lines —
`PolarsWriteCSV` 84–90, `ReportResult` 170–195), `db_service.py` (788 lines —
`__exit__`→`close()`→`_delete_base_files` 762–788), `errors.py` (30 lines — 5
exception classes, no serialisation), `app_config.yaml` (50 lines —
`export_settings.to_csv` 40–49), `utils.py` (73–103 — `clean_names`/`make_valid`);
`tests/test_csv_service.py` (34–74 — empty-table + row-count tests);
`tests/test_app.py` (255–281, 430–473 — `ReportResult` model + end-to-end);
`docs/00-overview/data-flow.md` (:177–178 stale "never overwrite"),
`docs/00-overview/current-behavior.md` (:293 empty non-mapped → no file);
phase file `16-audit-rendered-surface.md` (126 lines — Purpose 11–21,
Blocks 31–81, Taxonomy 85–109, Report Output 111–126, Scope 25–27).

---

## 2. Proposed Final Block Set (4 blocks, re-based)

All blocks are independent and evidence-classed. Evidence classes:

- **static proof** — a stated property contradicted by code inspection alone
- **observed behavior** — an empirical check of the running system
- **reproduced validation error** — an induced input that triggers a runtime error

| # | Title (sync reframing) | Core question | Evidence class |
|---|---|---|---|
| 1 | CSV schema conformance against the source-column contract | Does the exported CSV's column set, order, and types match the cleaned source headers + dictionary extra columns? Is there a machine-readable, versioned schema manifest? | static proof |
| 2 | CSV format conformance: separator, decimal mark, compression, BOM | Does the rendered format match the `PolarsWriteCSV` settings declared in `app_config.yaml`? Is the BOM observable inside gzip? Does a comma-decimal/period mismatch risk downstream misinterpretation? | static proof + observed behavior |
| 3 | Output presence & completeness (same-second overwrite, not mode="x") | Does every successful export produce a conformant file? Replace `mode="x"` with the real same-second overwrite via second-granular timestamp + `file_index`. Does the empty-table early-return write no file and report success? | static proof + observed behavior |
| 4 | Crash-safety & output-readiness of exported files | Does `df.write_csv` write directly to the final path (non-atomic)? Is there a checksum/integrity marker? Does `DBWorker.__exit__` delete the DB unconditionally on error? Does `ReportResult` carry an error/status field? | static proof |

### New 1 — CSV schema conformance against the source-column contract

**Refocus:** From "conformance against a declared API-result contract" to
"column-name survival through the SQLite→Polars→CSV chain."

- **Evidence (column origin):** `pl.read_csv(n_rows=0, **self.reader_settings)`
  reads only headers (`csv_service.py:77-86` → `self.source_headers`). No
  separate schema manifest exists.
- **Evidence (cleaning layer):** `clean_names`/`make_valid`
  (`utils.py:73-103`) sanitise source headers before table creation
  (`db_service.py:107-110`).
- **Evidence (storage layer):** `create_table` sets every column to SQLite
  `TEXT` (`db_service.py:134-193`) — no type fidelity beyond text.
- **Evidence (export layer):** `export_sql_to_csv` reads `columns` from
  `data_cursor.description` (`csv_service.py:323`) and constructs
  `pl.DataFrame(rows, schema=columns, orient="row", schema_overrides={col: pl.Utf8})`
  (`csv_service.py:341-346`) — all text, no type information survives.
- **Evidence (extra columns):** Dictionary-derived extra columns are appended
  to the table before export; entirely empty ones are dropped
  (`dict_service.py::_drop_empty_columns`, `current-behavior.md:293`).
- **Gap:** No machine-readable, versioned schema manifest. The only contract
  is prose in `data-flow.md:38-64`. A downstream consumer must reconstruct the
  schema by reading docs.

### New 2 — CSV format conformance: separator, decimal mark, compression, BOM

**Refocus:** From `decimal=","` (pandas), `encoding="utf-8-sig"` (pandas),
`sep=";"` (pandas) to the real Polars `write_csv` parameters.

- **Evidence (separator):** `separator=";"` — `app_config.yaml:44`,
  `PolarsWriteCSV.separator` (`models.py:86`), passed at `csv_service.py:358`.
  Correct and consistent.
- **Evidence (decimal mark):** `decimal_comma=True` (Polars boolean) —
  `app_config.yaml:46`, `models.py:87`, `csv_service.py:358`. The phase spec's
  `decimal=","` is pandas syntax, not Polars.
- **Evidence (compression):** `compression="gzip"` → `.csv.gz` —
  `app_config.yaml:48`, `models.py:90`, `csv_service.py:238` (suffix via
  `get_files_suffix`). Verified Polars 1.38.1: `write_csv` has `compression`
  but no `encoding` parameter.
- **Evidence (BOM):** `include_bom=True` — `app_config.yaml:43`. **Not a
  declared field** of `PolarsWriteCSV` (`models.py:84-90`); absorbed by
  `extra="allow"` and survives `model_dump()` → `**params` → `df.write_csv`.
  With `compression="gzip"`, the BOM sits inside the gzip stream — invisible
  to any consumer that decompresses without inspecting the byte prefix. No
  test asserts the BOM's presence or absence in the rendered output.
- **Evidence (header):** `include_header=True` — `app_config.yaml:47`,
  `models.py:88`.
- **Evidence (chunk size):** `chunk_size: 100000` in config overrides the
  model default of `10000` (`models.py:89`); popped at `csv_service.py:312`
  and used as `fetchmany(max_rows)` bound (`csv_service.py:338`).

### New 3 — Output presence & completeness (same-second overwrite, not mode="x")

**Refocus:** From `mode="x"` (exclusive create, skip-existing) to the real
same-second overwrite via second-granular timestamp + `file_index`.

- **Evidence (naming):** `get_file_name()` (`csv_service.py:237-240`):
  `f"{name_prefix}_{strftime('%Y%m%d-%H%M%S')}_{name_suffix}{ext}"` with
  `file_index` starting at 1 per run (`csv_service.py:314`). The timestamp is
  **second-granular**, not higher-res.
- **Evidence (overwrite):** A re-run within the same second produces identical
  filenames: `file_name.format(file_index=file_index)` (`csv_service.py:356`)
  then `df.write_csv(file=file_path, **params)` writes directly to that path
  (`csv_service.py:358-361`). The second run **overwrites** the first — the
  *opposite* of `mode="x"` skip behaviour. No collision guard exists.
- **Evidence (two exports per run):** `null_data` export to `base_path`
  (`app_service.py:246-251`, glob `null_data_*` at `:252`); `data_table`
  export to `export_path` (`app_service.py:264-268`).
- **Evidence (empty-table silent success):** `export_sql_to_csv` returns `0`
  for an empty table (`csv_service.py:334-336`) — `df.write_csv` is never
  called, no file is written. `run_report` still constructs `ReportResult`
  with `rows_exported=0` (`app_service.py:278-290`), indistinguishable from
  a full export. Confirmed by `current-behavior.md:293`: "Empty non-mapped
  set → No `null_data` file written."
- **Evidence (test lock-in):** `test_csv_service.py:58-74` asserts the
  empty-table returns 0 but does **not** assert a file is or isn't written;
  `test_app.py:468-470` asserts `result.export_dir` exists and
  `*.gz` files exist for the clean-data path (non-empty synthetic input) —
  nothing tests the empty-null_data disposition.

### New 4 — Crash-safety & output-readiness of exported files

**Repurpose:** From "error-file conformance & content" (YAML serialisation)
to "crash-safety of the write and recoverability of state."

- **Evidence (non-atomic write):** `df.write_csv(file=file_path, **params)`
  writes **directly to the final path** (`csv_service.py:356-361`). No
  temp+rename, no checksum, no size footer. A mid-write crash (process kill,
  disk full) leaves a truncated `.csv.gz` that a downstream consumer reads
  as a complete file.
- **Evidence (DB deletion on crash):** `DBWorker.__exit__(exc_type, exc, tb)`
  → `self.close()` (`db_service.py:786-788`) runs **unconditionally** on
  exception. `close()` → `_delete_base_files()` (`db_service.py:769-781`)
  deletes the DB file + `-wal`/`-shm` sidecars regardless of `exc_type`.
  No diagnostic state is preserved for post-crash triage. Confirmed by
  `current-behavior.md:294`: "Unexpected exception during `run_report` →
  `close()` still runs ... and deletes the DB."
- **Evidence (no error/status field):** `ReportResult` (`models.py:170-195`)
  carries `rows_loaded`/`rows_exported` but **no `error` or `status` field**.
  `rows_exported == 0` is indistinguishable from a full export.
- **Evidence (no error serialisation):** `errors.py` defines 5 plain
  `Exception` subclasses (`NoReportFoundError`, `WrongDataSettings`,
  `DataValidationError`, `ConfigError`, `SettingsNotInitializedError`) —
  no `to_yaml`, no `to_dict`, no serialisation method.
- **Ownership boundary:** Phase 04 owns the *write path* and *cleanup path*.
  Phase 16 (re-based) owns the *output-readiness* question: "Is the file on
  disk safe for a downstream consumer to read?" No other phase currently
  audits this. Overlapping findings F1 and F2 must cross-file Phase 04 and
  add the output-readiness angle (checksum/integrity footer) that Phase 04
  does not cover.

---

## 3. Confirmed Findings to Surface

Six findings, sorted by severity (HIGH first, then MEDIUM) per the findings
template's "index order IS the work order" rule. `OUT-` prefix is retained
from the phase file's Report Output and is **not** colliding — see §5.

| ID | Severity | Synonym | Anchor (primary) | Title |
|---|---|---|---|---|
| OUT-001 | HIGH | F1 | `data-flow.md::"repeated runs never overwrite"` | Stale doc claims "never overwrite" — same-second re-runs overwrite (`get_file_name`, `%Y%m%d-%H%M%S`, `csv_service.py:237-240, 356-361`) |
| OUT-002 | HIGH | F2 | `csv_service.py::CSVWorker.export_sql_to_csv` | Non-atomic CSV write — `df.write_csv` writes directly to final path, no temp+rename or checksum |
| OUT-003 | HIGH | F5 | `csv_service.py::CSVWorker.export_sql_to_csv` | Empty-table export silently produces no file but reports success (returns 0, no file written, `rows_exported=0` indistinguishable from full) |
| OUT-004 | MEDIUM | F3 | `settings/app_config.yaml::export_settings.to_csv.include_bom` | `include_bom=True` not a declared `PolarsWriteCSV` field — absorbed by `extra="allow"`, unobservable inside gzip, untested |
| OUT-005 | MEDIUM | F4 | `csv_service.py::CSVWorker.get_csv_headers` | No machine-readable schema manifest — columns derive from cleaned source headers + dictionary extra columns, contract is prose only (`data-flow.md:38-64`) |
| OUT-006 | MEDIUM | F6 | `models.py::ReportResult` | `ReportResult` has no `error`/`status` field — failure is silent, `rows_exported=0` indistinguishable from full export |

### OUT-001 — [HIGH] Stale doc claims "never overwrite" — same-second re-runs overwrite

**Anchor:** `data-flow.md::"repeated runs never overwrite previous outputs"` (line 178); `csv_service.py::CSVWorker.get_file_name` (lines 237-240); `csv_service.py::CSVWorker.export_sql_to_csv` (lines 356-361)
**Checked:** static proof + observed behavior
**Why this band:** HIGH — a doc-vs-code divergence with real data-loss implications. A re-run within the same second silently destroys the prior output, and the doc falsely promises safety. Adjacent band NOT graded as: MEDIUM (no doc drift with zero runtime consequence — this one has data-loss).
**Problem:** `data-flow.md:177-178` states: "File names carry the run timestamp (`<prefix>_<YYYYMMDD-HHMMSS>_<N>`), so repeated runs never overwrite previous outputs." But `get_file_name()` uses `strftime("%Y%m%d-%H%M%S")` — **second-granular** with no sub-second component. A re-run within the same wall-clock second produces an identical filename. `export_sql_to_csv` then calls `df.write_csv(file=file_path, **params)` directly on that path (`csv_service.py:356-361`) — the second write **overwrites** the first, destroying the prior output. No collision guard, no `exists()` check, no `mode="x"` exists.
**Repro:** Static proof — read `get_file_name` (line 239: `strftime("%Y%m%d-%H%M%S")`) and the direct write at 356-361. The timestamp format `%Y%m%d-%H%M%S` has 1-second resolution. Two `export_sql_to_csv` calls within the same second → identical `file_name.format(file_index=1)` → same `file_path` → overwrite. A re-run within 1 second of the original produces `null_data_20240115-123456_1.csv.gz` twice, second overwrites first.
**Impact:** An operator re-running a report in a hurry (within the same second) silently destroys the prior export — no warning, no backup, and the doc explicitly claims this is impossible. The `null_data` export (non-mapped rows, often the audit trail of what was dropped) is the most exposed target.
**Root cause:** The timestamp granularity (`%Y%m%d-%H%M%S`) is insufficient to guarantee uniqueness, and the code relies solely on the timestamp for collision avoidance rather than an existence check.
**Fix:** Either (a) add a collision guard: check `file_path.exists()` before writing and bump `file_index` until a non-existing name is found, or (b) raise timestamp resolution to include microseconds (`%Y%m%d-%H%M%S-%f`). At minimum, correct the `data-flow.md` claim.
**Verify:** `test_csv_service.py:22-31` (`test_get_file_name`) asserts the name starts with `prefix_` and ends with `.csv.gz` but does **not** assert uniqueness across same-second calls. Update to assert no overwrite, or add a new test that calls `get_file_name` twice within the same second and confirms distinct names.
**Shipped-test blocker:** `test_get_file_name` locks the naming format but not the uniqueness invariant — no blocker.
**Related:** OUT-002 (non-atomic write); Phase 04 §5 Block 5 already found the code-side overwrite.

### OUT-002 — [HIGH] Non-atomic CSV write — `df.write_csv` writes directly to final path

**Anchor:** `csv_service.py::CSVWorker.export_sql_to_csv` (lines 356-361)
**Checked:** static proof
**Why this band:** HIGH — a mid-write crash leaves a truncated `.csv.gz` that a
downstream consumer reads as a complete file, with no way to distinguish a
partial result from a finished one. Adjacent band NOT graded as: MEDIUM (no
integrity signal at all vs. a tolerable format deviation).
**Problem:** `export_sql_to_csv` constructs the final path and writes directly:
`file_path = export_path / file_name.format(file_index=file_index)`
(`csv_service.py:356`), then `df.write_csv(file=file_path, **params)`
(`csv_service.py:358-361`). There is no temp-file + atomic rename, no checksum,
no size footer, no magic byte at the end of the gzip stream. If the process is
killed or disk fills mid-write, the `.csv.gz` is truncated — a gzip stream that
decompresses to partial or corrupted CSV. A downstream consumer has no
mechanical way to detect this.
**Repro:** Static proof — read lines 356-361. The write target is the final
`file_path`, not a temporary path. No `os.rename`, no `tempfile.NamedTemporaryFile`,
no post-write validation.
**Impact:** Downstream data pipelines consuming `clean_data/data_table_*.csv.gz`
or `null_data_*.csv.gz` may process truncated data as if it were complete,
producing silently wrong analytics.
**Root cause:** The export path was built for streaming chunks, not crash
safety. Atomicity (temp + rename) was never added.
**Fix:** Write to a temporary path (e.g., `file_path.with_suffix(".tmp")`) then
`os.replace(temp_path, file_path)` — `os.replace` is atomic on POSIX and
Windows. Optionally append a footer row count or SHA-256 of the uncompressed
content for post-hoc integrity verification.
**Verify:** No shipped test asserts atomicity. Add a test that interrupts a
write (mock `df.write_csv` to raise mid-call) and asserts no partial file
exists at the final path.
**Shipped-test blocker:** None — no test asserts the direct-to-final-path
behaviour.
**Related:** OUT-001 (same-second overwrite); Phase 04 §5 Block 5 (write
mechanics).

### OUT-003 — [HIGH] Empty-table export silently produces no file but reports success

**Anchor:** `csv_service.py::CSVWorker.export_sql_to_csv` (lines 334-336); `app_service.py::AppService.run_report` (lines 246-251, 264-268, 278-290); `models.py::ReportResult` (lines 170-195)
**Checked:** static proof + observed behavior (`current-behavior.md:293`)
**Why this band:** HIGH — the `null_data` export (non-mapped rows) can
legitimately be empty, but the consumer cannot distinguish "zero rows, no file
written, reported as success" from "export ran and produced a full file."
Adjacent band NOT graded as: MEDIUM (no data produced vs. a format deviation
that is merely unexpected).
**Problem:** `export_sql_to_csv` checks `total_rows == 0` at line 334 and
returns `0` immediately — `df.write_csv` is never called, no file is written
(`csv_service.py:334-336`). `run_report` still includes this zero in
`rows_exported` (`app_service.py:289`) and constructs `ReportResult` as
`rows_exported=0` (`app_service.py:278-290`). For the `null_data` export, this
is the expected case (no non-mapped rows), confirmed by
`current-behavior.md:293`. But the `ReportResult` carries no field indicating
that the null_data export produced zero output — `null_data_files` is a list
that may be empty, but an empty list is ambiguous (no rows vs. no export
attempted vs. export failed silently).
**Repro:** Static proof — read lines 334-336 (empty early-return) and 356-361
(non-empty write path). For the `null_data` export specifically, call
`export_sql_to_csv` on an empty `non_mapped_table` → returns `0`, no file on
disk, `null_data_files = []` after glob (`app_service.py:252`). `run_report`
reports `rows_exported=0` and returns `ReportResult` with no error/status to
distinguish this from a full export.
**Impact:** A downstream consumer of `null_data_*.csv.gz` cannot tell whether
the source data had zero non-mapped rows (expected, fine) or whether the
export path was never reached (potential bug masked as success).
**Root cause:** `ReportResult` has no `null_data_exported` or per-export status
field; the empty-table early-return conflates "legitimately empty" with
"no output produced."
**Fix:** Either (a) write a header-only CSV for empty tables so a file always
exists, or (b) add a `null_data_exported: int` field to `ReportResult` and an
explicit `export_status` enum on the result. The current `null_data_files`
list alone is ambiguous.
**Verify:** `test_csv_service.py:58-74` asserts the empty table returns 0 but
does **not** assert whether a file is or is not written. Add an assertion that
no file appears in `export_path` for the empty case, or — if the fix is to
always write a header-only file — assert the file exists.
**Shipped-test blocker:** `test_export_sql_to_csv_returns_zero_for_empty_table`
locks in the *return value* (0) but not the *output absence* — no blocker
on the return value, but a fix to write a header-only file would need a test
update.
**Related:** OUT-006 (no error/status field on ReportResult).

### OUT-004 — [MEDIUM] `include_bom=True` not a declared `PolarsWriteCSV` field

**Anchor:** `settings/app_config.yaml::export_settings.to_csv.include_bom` (line 43); `models.py::PolarsWriteCSV` (lines 84-90); `csv_service.py::CSVWorker.export_sql_to_csv` (lines 358-361)
**Checked:** static proof
**Why this band:** MEDIUM — the BOM is controlled by an undeclared,
implicitly-allowed field that no test verifies in the rendered output. With
gzip, the BOM is embedded inside the compressed stream and invisible to
decompressing consumers. Adjacent band NOT graded as: LOW (no declared contract
is broken silently — the field is undeclared, which is a Phase 05 gap, not
merely a doc drift).
**Problem:** `app_config.yaml:43` declares `"include_bom": True`. But
`PolarsWriteCSV` (`models.py:84-90`) does **not** declare `include_bom` as a
field — it is absorbed by `model_config = ConfigDict(extra="allow")`
(`models.py:85`). The value survives `model_dump()` (`app_service.py:133`) →
`params` (`csv_service.py:310`) → `**params` → `df.write_csv(file=file_path,
**params)` (`csv_service.py:358-361`) and reaches Polars. Two implicit
contracts are in play: (1) the config model does not declare `include_bom`
(even though it is a real Polars parameter), and (2) with
`compression="gzip"`, the BOM is the first 3 bytes of the *uncompressed* CSV
content inside the gzip stream — any consumer that decompresses and parses
without checking the byte prefix is either helped (Excel auto-detects) or
harmed (strict parsers reject the BOM) with no observability. No test
asserts the BOM's presence or absence in the rendered `.csv.gz`.
**Repro:** Static proof — read the `PolarsWriteCSV` model (lines 84-90):
`separator`, `decimal_comma`, `include_header`, `chunk_size`, `compression`
are declared; `include_bom` is absent. Read `app_config.yaml:43`: `include_bom:
True` is present in the YAML. `model_dump()` with `extra="allow"` includes it.
The value reaches `df.write_csv` via `**params`.
**Impact:** A downstream consumer that decompresses and strips BOMs (or rejects
them) has undefined behaviour — the BOM's presence is accidental (config-only,
undeclared in model, untested) and its location (inside gzip) makes it
non-discoverable without byte-level inspection.
**Root cause:** `include_bom` was added to config but never promoted to a
declared `PolarsWriteCSV` field. The `extra="allow"` config silently absorbs
it, masking the omission.
**Fix:** Add `include_bom: bool = True` to the `PolarsWriteCSV` model
(`models.py:84-90`), making it an explicit, type-checked field. Add a test that
decompresses a rendered `.csv.gz` and asserts the first 3 bytes are `EF BB BF`
(if BOM is intended) or absent (if not).
**Verify:** No test currently checks BOM in output. New test needed:
`test_export_includes_or_excludes_bom` — decompress a `.csv.gz` and inspect the
byte prefix.
**Shipped-test blocker:** None — no test asserts `include_bom` behaviour.
**Related:** Phase 05 §3 gap #4 (`include_bom` not a declared `PolarsWriteCSV`
field, absorbed by `extra="allow"`).

### OUT-005 — [MEDIUM] No machine-readable schema manifest

**Anchor:** `csv_service.py::CSVWorker.get_csv_headers` (lines 77-86); `csv_service.py::CSVWorker.export_sql_to_csv` (lines 323, 341-346); `utils.py::clean_names` / `make_valid` (lines 73-103); `db_service.py::create_table` (lines 134-193); `dict_service.py::_drop_empty_columns`
**Checked:** static proof
**Why this band:** MEDIUM — no automated check ensures the rendered CSV header
matches the declared contract, so column-name drift (from `clean_names`
sanitisation or dictionary column drops) is undetectable without manual
comparison. Adjacent band NOT graded as: HIGH (a downstream parse failure
from a renamed column, vs. a present-but-undocumented schema).
**Problem:** There is no machine-readable, versioned schema manifest for
exported CSVs. Column set and order are derived transitively:
`pl.read_csv(n_rows=0)` reads `source_headers` (`csv_service.py:77-86`) →
`clean_names`/`make_valid` sanitise them (`utils.py:73-103`) → SQLite
`create_table` stores all as TEXT (`db_service.py:134-193`) →
`export_sql_to_csv` reads `columns` from `data_cursor.description`
(`csv_service.py:323`) → constructs `pl.DataFrame` with `schema_overrides={col:
pl.Utf8}` (`csv_service.py:341-346`). The only contract documentation is prose
in `data-flow.md:38-64`. A downstream consumer must read this prose and
reconstruct the schema. No automated check verifies the rendered header matches
the source header after cleaning, or that a source column expected by the
dictionary was not silently dropped.
**Repro:** Static proof — trace the column flow from `get_csv_headers` (line 79,
`pl.read_csv(n_rows=0)` → `df.columns`) through `_set_data_tbl_columns` +
`clean_names` (`db_service.py:107-110`) to `create_table` (line 134, all TEXT)
to `export_sql_to_csv` (line 323, `data_cursor.description`; line 346,
`schema_overrides={col: pl.Utf8}`). No manifest file, no schema assertion in
`write_csv`, no test comparing rendered header to `data-flow.md` prose.
**Impact:** If `clean_names`/`make_valid` renames a column in a way that breaks
a downstream consumer's lookup, or if a dictionary extra column is dropped
(`_drop_empty_columns`) and a consumer expects it, the failure is silent
until the consumer's parser rejects the renamed/missing column.
**Root cause:** The schema is implicitly carried through the pipeline rather
than explicitly declared and verified.
**Fix:** Introduce a schema manifest — either a JSON/YAML file declaring the
expected column set and order for each export, or a test that asserts the
rendered CSV header matches `source_headers` after `clean_names` (with
dictionary extra columns appended). At minimum, add a test that reads a
rendered `.csv.gz` and asserts the header row matches the expected column set
from `data-flow.md:38-64`.
**Verify:** `test_app.py:468-469` asserts `result.export_dir` exists and
contains `*.gz` files but does **not** assert the header content. New test
needed: `test_exported_csv_header_matches_expected_schema`.
**Shipped-test blocker:** None.
**Related:** OUT-001 (column-name survival); Phase 05 (template-to-model
schema conformance does not cover output-schema discoverability).

### OUT-006 — [MEDIUM] `ReportResult` has no `error`/`status` field — failure is silent

**Anchor:** `models.py::ReportResult` (lines 170-195); `app_service.py::AppService.run_report` (lines 278-290); `db_service.py::DBWorker.__exit__` / `close` / `_delete_base_files` (lines 762-788)
**Checked:** static proof + observed behavior (`current-behavior.md:294`)
**Why this band:** MEDIUM — a run that fails mid-pipeline raises and never
returns a `ReportResult`; a run that succeeds with `rows_exported=0` is
indistinguishable from one that produced a full report. Adjacent band NOT
graded as: HIGH (no data loss from the missing field vs. an actual data
overwrite).
**Problem:** `ReportResult` (`models.py:170-195`) fields: `report_dir`,
`import_dir`, `export_dir`, `db_path`, `null_data_files`, `started_at`,
`finished_at`, `duration`, `timings`, `rows_loaded`, `rows_exported`. There is
**no `error` field and no `status` field**. A run that fails mid-pipeline
raises an exception and never constructs a `ReportResult` — the `DBWorker`
context manager `__exit__` → `close()` → `_delete_base_files()` (`db_service.py:
786-788, 762-781`) deletes the DB file on exit even on exception, erasing
diagnostic state. A run that succeeds with `rows_exported=0` produces the same
`ReportResult` shape as one that produced a full export — the only signal is
the integer value, which the consumer must inspect.
**Repro:** Static proof — read `ReportResult` (lines 170-195): no `error` or
`status` field. Read `run_report` (lines 278-290): constructs `ReportResult`
with `rows_exported=rows_exported` (line 289) — which is `0` if the clean-data
table is empty or if the export early-returned. No exception → no `ReportResult`;
exception → no `ReportResult` at all (only a raised error). The `DBWorker`
`__exit__` (`db_service.py:786-788`) calls `self.close()` unconditionally.
**Impact:** A consumer calling `run_report(...)` and catching an exception
cannot inspect a structured result for partial progress, error context, or
which stage failed. A successful zero-row export is structurally identical to
a full export in the `ReportResult` — only `rows_exported` differs, which is
easy to miss.
**Root cause:** `ReportResult` was designed for the happy path only; no
failure/success discriminator was added.
**Fix:** Add `status: str` (e.g., `"success" | "empty" | "failed"`) and
`error: str | None` fields to `ReportResult` (`models.py:170-195`). Populate
`status` in `run_report` (`app_service.py:278-290`) before returning. On
exception, construct a partial `ReportResult` with `status="failed"` and the
error message rather than re-raising (or at least log the structured failure
before re-raising).
**Verify:** `test_app.py:257-281` (`test_report_result_model_construction_and_serialization`)
constructs `ReportResult` with exactly the 10 current fields — adding
`status`/`error` would require a test update. `test_app.py:461` asserts
`isinstance(result, ReportResult)` — no blocker on the type itself.
**Shipped-test blocker:** `test_app.py:259-271` constructs `ReportResult` with
the exact current 10 fields — adding required fields would break this test
unless the new fields have defaults. Use `status: str = "success"` and
`error: str | None = None` to avoid breaking existing construction sites.
**Related:** OUT-003 (empty-table success indistinguishable); Phase 04 §6
Block 6 (failure-mode observability, `ReportResult` field design).

---

## 4. Gaps NOT to Surface

| Gap | Claim | Why it is NOT a Phase 16 finding |
|---|---|---|
| `mode="x"` exists | Phase 16 spec claims `mode="x"` (exclusive create) | Does not exist in Polars `write_csv` (verified Polars 1.38.1 signature) or in this codebase (grep `mode.*=.*['"]x['"]` in `src/` → zero). Already covered as OUT-001 (same-second overwrite). |
| `encoding="utf-8-sig"` exists | Phase 16 spec claims `encoding="utf-8-sig"` | Not a Polars `write_csv` parameter. BOM is `include_bom` (`models.py:88` absent, `app_config.yaml:43`). Owned as OUT-004. |
| `decimal=","` exists | Phase 16 spec claims `decimal=","` (pandas) | Code uses `decimal_comma=True` (Polars boolean, `models.py:87`). Not a defect — correct Polars syntax. Documented for spec-author awareness only. |
| YAML error files exist | Phase 16 spec claims "failed tasks serialised to YAML" | No `TVTask`, no `to_yaml`, no error files. `errors.py` = 5 plain exceptions. Already owned as OUT-006 (no error/status field). |
| API result pipeline exists | Phase 16 spec claims "converts each API result to a DataFrame" | Data flows SQLite cursor → `pl.DataFrame` (`csv_service.py:338-346`). No API. Not a defect — the concept is simply inapplicable. |
| BOM is testable in raw CSV | Consumer can check BOM in decompressed output | With `compression="gzip"`, the BOM is the first 3 bytes of the *uncompressed* stream — a consumer that only decompresses (e.g., `pandas.read_csv` with gzip) may never see it. This is the *nature* of OUT-004, not a separate gap. |

---

## 5. Report Output Settings

The Phase 16 §Report Output (current lines 111–126) is **mostly aligned** with
the findings template (`audit-findings.md`) and repo convention. Recommend
**keeping** most settings, with targeted corrections.

| Item | Setting | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/16-rendered-surface/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`, Phase 13: `.ai/audit/13-performance/findings.md`). Directory does not yet exist — no prior findings to reconcile. |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `id_prefix`, `severity_taxonomy`, `code_ref` fields (lines 4, 7-8). |
| ID prefix | `OUT-` | **Retain, collision DISPROVEN** | See §5.1 below. Zero `OUT-NNN` finding IDs found anywhere in the repo. |
| problems-only | `true` | **Keep** | All six confirmed findings (OUT-001–006) are code-verified defects, not passing checks. |
| per-pass lines | ≤100, incremental append | **Keep** | Template constraint (line 118). |
| Empty state | `No problems found in this phase.` | **Keep** | Verbatim (line 121). |
| Shipped-test-asserts-current-behaviour | line 125 | **Keep** | Applies to OUT-003 (`test_csv_service.py:58-74` locks empty-table return value) and OUT-006 (`test_app.py:259-271` locks `ReportResult` 10-field shape). New fields must have defaults to avoid breaking construction. |

### 5.1 The `OUT-` prefix collision check — DISPROVEN (with precision correction)

Phase 16 line 124 states:

> "Finding IDs use the declared bare prefix; a shipped test, configuration
> comment, or runbook may already carry an ID from a prior cycle, so check
> for a collision before minting a new one"

**This concern is disproven by the working tree (`b390d04`):**

- A repo-wide grep for `OUT-` (excluding `.venv*`, `.git*`) finds **6 matches**,
  all narrative references to the prefix itself — **zero `OUT-NNN` finding
  identifiers** in any findings document, test file, shipped source, or report.
  The matches are:
  - `.kilo/commands/audit/phases/16-audit-rendered-surface.md:117` (the phase
    file's own `Finding-ID prefix: `OUT-`` declaration)
  - `.ai/reports/phase-16-auditor-report.md` (lines 342, 465, 488, 543, 545 —
    all auditor-report narrative about the prefix)
- No findings directory `.ai/audit/16-rendered-surface/` exists (absent; only
  `templates/` and `problems/` exist under `.ai/audit/`).
- No `OUT-001` through `OUT-999` pattern appears in any tracked file.

**Precision correction to auditor §5:** The auditor states the grep returns
"1 match" in the phase file. In fact the full-tree grep returns **6 matches**
(1 in the phase file + 5 in the auditor report's own narrative). The auditor's
**match count was understated**, but the **core conclusion (zero finding IDs) is
correct** — the extra 5 matches are all narrative references to the `OUT-`
declaration, not finding identifiers. Flag for auditor precision correction.

**Recommendation:** Retain the `OUT-` prefix (matches the findings template
convention and is unique among real findings — zero collisions). **Correct line
124** — replace "a shipped test, configuration comment, or runbook may already
carry an ID from a prior cycle, so check for a collision before minting a new
one" with "verify no collision before minting; repo-wide grep for `OUT-NNN`
finding identifiers returns zero in the current tree (`b390d04`)."

---

## 6. Scope / Cross-Phase Citations

### 6.1 Phases that Phase 16 CITES (scope boundary, lines 25–27)

| Cited phase | Current citation text | Phase status | Action |
|---|---|---|---|
| (04) | "export mechanics, file-existence handling, and error-file location" | Re-based (sync) | **KEEP, reword** — "export mechanics" and "file-existence handling" are valid. **Remove "error-file location"** (no error files exist — grep `to_yaml\|TVTask\|error_file\|yaml_error` in `src/` → zero). Reword to: "crash-safety / non-atomic writes." Boundary: Phase 04 owns the *write path* (how the CSV is written, DB cleanup on `__exit__`); Phase 16 owns *output-readiness* (is the rendered file safe to consume?). |
| (05) | "template-to-model schema conformance" | Re-based (sync) | **KEEP** — valid. Phase 05 owns config↔Pydantic↔Polars conformance (e.g., `include_bom` absorbed by `extra="allow"`, `phase-05-auditor-report.md` §3 gap #4). |
| (10) | "fixed-value enum discipline" | Re-based (sync) | **KEEP** — valid. No async framing. |
| (11) | "test-suite adequacy" | Exists (sync) | **KEEP** — valid. No async framing. |

**Recommended reworked scope boundary (replaces lines 25–27):**

> "Owned here: rendered-output conformance to the declared surface contract —
> the schema, format, presence, and crash-safety-readiness of the exported CSV
> files (`null_data_*.csv.gz`, `clean_data/data_table_*.csv.gz`). Owned
> elsewhere: export mechanics and crash cleanup (04); template-to-model schema
> conformance (05); fixed-value enum discipline (10); test-suite adequacy (11)."

### 6.2 Phases that CITE (16) — informational

| Phase | Line(s) | Citation text | Action |
|---|---|---|---|
| 11 | 19 | "What the rendered outcome is, and what a derived quantity equals, are 16's and 17's" | **KEEP** — valid. Phase 11 defers rendered-outcome measurement to Phase 16; Phase 17 owns arithmetic. No async framing. |
| 13 | 18 | "a `data_table` export naming and same-second overwrite" | **KEEP** — valid reference. Phase 13 documents same-second overwrite as a performance concern; Phase 16 owns the *output-readiness* angle (OUT-001). |

**Orchestration:** `audit-multi-phase.md` discovers phases by directory
listing, so no hard-coded sequence needs editing when Phase 16 is re-based.

---

## 7. Edit Delta — Line-by-Line Rewrite of the 126-line File

All references are to the **current** `.kilo/commands/audit/phases/16-audit-rendered-surface.md`.

| # | Lines | Action | Replacement (concise) | Rationale |
|---|---|---|---|---|
| 1 | 1–7 (front matter) | **Add** fields | Add `code_ref: "b390d04"` and `severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"`. Keep `name`, `status: draft`, `validated: no`, `executor: auditor`, `problems-only: true`. | Aligns with findings template front-matter (`audit-findings.md` lines 4, 7-8) and Phase 13 precedent. |
| 2 | 9 (H1) | **Keep** | `# Phase 16 — Rendered-Surface & Output Conformance Measurement` | Title is not async-framed; no change needed. |
| 3 | 11–21 (Purpose) | **Replace** | Rewritten Purpose (§7.1 below) | Removes API/error-file/mode="x"/utf-8-sig fiction; describes the real SQLite→Polars→CSV export path. |
| 4 | 23–27 (scope boundary) | **Replace** | Rewritten scope boundary (§6.1 above) | Removes "error-file location" from (04) citation; clarifies Phase 04 vs Phase 16 ownership boundary. |
| 5 | 31–36 (Audit Blocks intro) | **Keep** | No change | Methodology language is generic; no async/API references in the intro itself. |
| 6 | 38–48 (Block 1) | **Replace** with New 1 | "CSV schema conformance against the source-column contract" + evidence bullets (headers from `pl.read_csv(n_rows=0)` 77-86; `clean_names`/`make_valid` utils.py 73-103; SQLite TEXT all db_service.py 134-193; `schema_overrides={col: pl.Utf8}` csv_service.py 325, 341-346; no manifest) | Refocus from "declared API contract" to "column-name survival through the pipeline." |
| 7 | 50–59 (Block 2) | **Replace** with New 2 | "CSV format conformance: separator, decimal mark, compression, BOM" + evidence (`separator=";"` app_config.yaml:44 / models.py:86; `decimal_comma=True` app_config.yaml:46 / models.py:87; `compression="gzip"` app_config.yaml:48 / models.py:90; `include_bom=True` absorbed by `extra="allow"` app_config.yaml:43 / models.py:84-90 / csv_service.py:358-361; `include_header=True`; `chunk_size: 100000` overrides model default) | Replace pandas parameters (`decimal=","`, `encoding="utf-8-sig"`, `sep=";"`) with real Polars settings. |
| 8 | 61–70 (Block 3) | **Replace** with New 3 | "Output presence & completeness (same-second overwrite, not mode='x')" + evidence (`get_file_name` 237-240; `%Y%m%d-%H%M%S` second-granular; overwrite at 356-361; empty-table early-return 334-336; two exports per run app_service.py 246-251, 264-268; null_data glob at 252) | Replace `mode="x"` with same-second overwrite via timestamp + `file_index`. |
| 9 | 72–81 (Block 4) | **Replace** with New 4 | "Crash-safety & output-readiness of exported files" + evidence (direct-to-final-path write 356-361; non-atomic, no temp+rename; DB deletion on `__exit__` 786-788 / close 762-781; no error/status field on ReportResult 170-195; no error serialisation errors.py 5 classes) | Repurpose from "error-file conformance" to "crash-safety / output-readiness." Ownership: Phase 04 owns write/cleanup; Phase 16 owns output-readiness. |
| 10 | 85–109 (Severity Taxonomy) | **Replace examples only** | §4 reworded taxonomy (keep principle + 4 bands + "empty band is valid"; replace *all* API/error-file/mode="x" examples with CSV/null_data-grounded examples; remove `mode="x"` HIGH example; remove "error file structure" MEDIUM example; remove "non-reproducible error-file name" LOW example) | Every illustrative example is framed around the fictional API/mode="x"/YAML-error-file model. |
| 11 | 111–126 (Report Output) | **Modify** — correct the OUT- warning & citation | Keep findings path, template, `OUT-` prefix, `≤100 lines`, empty state, shipped-test-blocker. **Correct line 124** — replace the collision warning ("may already carry an ID from a prior cycle") with "verify no collision before minting; repo-wide grep for `OUT-NNN` returns zero in the current tree (`b390d04`)." | Disproves the phantom collision; retains the prefix. |

**Application order:** Edits 1 (front matter) and 2 (H1) are independent — do
first. Then Edit 4 (scope boundary, foundational framing) → Edit 3 (Purpose) →
Edits 6–9 (blocks 1–4, each self-contained) → Edit 10 (severity examples) →
Edit 5 (Audit Blocks intro, minor) → Edit 11 (Report Output correction).
Edits 6–9 may be applied in parallel (sections are self-contained).

**Do not touch:** `problems-only: true`; the "≤100-line passes" rule; the
empty-state wording ("No problems found in this phase.").

#### Rewritten Purpose (for edit 3, replaces lines 13–18)

> "Measures the rendered CSV output of this tool —
> `clean_data/data_table_*.csv.gz` and `null_data_*.csv.gz` — against the
> output surface declared in configuration and documentation. The data pipeline
> exports SQLite table contents to gzip-compressed CSV via Polars: columns
> originate from cleaned source CSV headers (`clean_names`/`make_valid`), all
> values are rendered as text (`schema_overrides={col: pl.Utf8}`), and the
> format is governed by `PolarsWriteCSV` settings (`separator=";"`,
> `decimal_comma=True`, `compression="gzip"`, `include_bom=True` absorbed by
> `extra="allow"`). This phase audits whether what is written conforms to the
> declared surface: schema (column set, order, types), format (separator,
> decimal mark, compression, BOM), output presence (every successful export
> produces a conformant file; empty results produce none), and crash-safety
> (whether a non-atomic write can leave a truncated file consumed as complete).
> This phase does not audit the mechanics of how the write happens — that is
> Phase 04's scope — but whether what is written is safe and conformant for a
> downstream consumer."

---

## 8. Findings-to-Scope Mapping

Findings are sorted by severity (HIGH first, then MEDIUM) per the findings
template's "index order IS the work order" rule. `OUT-` prefix retained.

| Finding ID | Auditor synonym | Severity | Anchor | New block | Phase-owned | Notes |
|---|---|---|---|---|---|---|
| OUT-001 | F1 | HIGH | `data-flow.md:178` | New 3 | Phase 16 (doc-vs-rendered) + Phase 04 §5 (code-side overwrite) | `test_csv_service.py:22-31` locks name format, not uniqueness. |
| OUT-002 | F2 | HIGH | `csv_service.py:356-361` | New 4 | Phase 16 (output-readiness) + Phase 04 §5 (write mechanics) | No test asserts atomic write. |
| OUT-003 | F5 | HIGH | `csv_service.py:334-336` | New 3 | Phase 16 (output presence) + Phase 04 §6 (ReportResult fields) | `test_csv_service.py:58-74` locks return value, not file absence. |
| OUT-004 | F3 | MEDIUM | `models.py:84-90` | New 2 | Phase 16 (format conformance) + Phase 05 (model-field gap) | `include_bom` absent from model, absorbed by `extra="allow"`. |
| OUT-005 | F4 | MEDIUM | `csv_service.py:77-86, 323, 341-346` | New 1 | Phase 16 (schema conformance) | No machine-readable manifest. |
| OUT-006 | F6 | MEDIUM | `models.py:170-195` | New 4 | Phase 16 (output-readiness) + Phase 04 §6 (result-field design) | New fields must have defaults to avoid breaking `test_app.py:259-271`. |

---

## 9. Precision Corrections to Auditor Report

| Location | Auditor claim | Correction | Rationale |
|---|---|---|---|
| §5, line 342 | "Repo-wide grep for `OUT-` returns **1 match**" | Full-tree grep returns **6 matches** (1 in phase file + 5 in auditor report narrative) | Auditor's match count understated (same pattern as Phase 13 `PERF-` §7.1). Core conclusion (zero `OUT-NNN` finding IDs) is correct — the 5 extra matches are narrative references to the `OUT-` declaration, not finding identifiers. |
| §6, line 466 | "OUT- prefix (zero collisions — repo-wide grep confirms only Phase 16's own line 117 matches)" | Confirm zero finding IDs; note the 6 total `OUT-` string matches are all narrative | Same precision note. |

---

*End of recommendation. Evidence-grounded: every structural recommendation
traces to a verified current-source fact (working tree at `b390d04`). All code
references confirmed by direct file inspection; `OUT-` collision disproven by
repo-wide grep (zero `OUT-NNN` finding identifiers — the 6 string matches are
all narrative). No source files were modified by this report. The findings
OUT-001–006 are the work the re-based Phase 16 surfaces when executed against
the synchronous codebase.*
