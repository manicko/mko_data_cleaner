---
phase: "16"
phase_name: "rendered-surface"
id_prefix: "OUT"
mode: "problems-only"
report_status: "validated"
validated_at: "2026-10-19"
base_sha: "c0fd0e1"
base_sha_phase_file: "b390d04"
severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"
validator: "kilo/poolside/laguna-s-2.1:free"
---

# Validation Report — Rendered-Surface & Output Conformance Findings

## Validation Summary

All 7 findings (OUT-001 through OUT-007) in
`.ai/audit/16-rendered-surface/findings.md` were independently re-verified
against the codebase at base SHA `c0fd0e1`. Every source anchor, disambiguator
token, cited line number, runtime-claim, and test-coverage assertion was
reproduced against the actual code. The phase file (16-audit-rendered-surface.md,
code_ref `b390d04`) carries stale line numbers and cites a non-existent
`app_config.yaml`; the findings file correctly adapted both to `c0fd0e1` and
`app_config.json5`.

**Result: 7 CONFIRMED, 0 REJECTED, 0 RE-TYPED, 0 RE-GRADED.**

| ID    | Severity | Verdict       | One-line rationale                                                    |
|-------|----------|---------------|-----------------------------------------------------------------------|
| OUT-001 | HIGH   | CONFIRMED     | `strftime("%Y%m%d-%H%M%S")` + per-call file_index reset at 239,317,359-367; no existence guard |
| OUT-002 | HIGH   | CONFIRMED     | `df.write_csv` direct-to-final at 364-367; `__exit__`→`close()`→`_delete_base_files` unconditional at 803-805,786-798 |
| OUT-003 | HIGH   | CONFIRMED     | `total_rows == 0` early return at 337-339; no file, no status field |
| OUT-004 | MEDIUM | CONFIRMED     | `include_bom` not a declared field; `extra="allow"` absorbs it; Polars accepts it |
| OUT-005 | MEDIUM | CONFIRMED     | No manifest emitted; column contract is prose-only in data-flow.md:38-64 |
| OUT-006 | MEDIUM | CONFIRMED     | `ReportResult` (models.py:182-207) has 11 fields, no error/status; MEDIUM per Phase 16 rubric |
| OUT-007 | LOW    | CONFIRMED     | data-flow.md:177-178 false "never overwrite" claim; contradicted by OUT-001 |

**Baseline gates (independently re-run):**

- `uv run ruff check src/ tests/` → `All checks passed!`
- `uv run basedpyright src/` → `39 errors, 0 warnings, 0 notes`
- `uv run pytest` → `93 passed in 3.85s`
- `include_bom` survives `PolarsWriteCSV.model_dump()` → confirmed via `python -c` (lives in `__pydantic_extra__`)
- `include_bom` accepted by `pl.DataFrame.write_csv` → confirmed via `inspect.signature` (Polars 2.0.0 parameter list includes `include_bom`)
- `mode="x"` existence guard in `src/` → zero matches (grep tool)
- `import pandas` in `src/` → zero matches (grep tool)
- `to_yaml|TVTask|error_file` in `src/` → zero matches (grep tool)
- `error|status` in `models.py` → only `ignore_errors: bool = False` at line 101 (PolarsReadCSV, not ReportResult)
- `OUT-NNN` / `PIPE-NNN` identifiers in `src/` or `docs/` → zero matches (namespace clean)

---

## Per-Finding Validation

### OUT-001 — [HIGH] Same-second re-run overwrites prior CSV.gz export; no collision guard

**Verdict: CONFIRMED.**

**Evidence (re-derived):**
- `src/mko_data_cleaner/core/csv_service.py:239` — `time_str = strftime("%Y%m%d-%H%M%S")` — second-granular timestamp. Confirmed at source.
- `csv_service.py:311` — `file_name = self.get_file_name(file_prefix, "{file_index}")` — template built once, formatted per chunk.
- `csv_service.py:317` — `file_index = 1` — counter resets to 1 on every `export_sql_to_csv` call. Confirmed at source.
- `csv_service.py:359` — `file_path = export_path / file_name.format(file_index=file_index)` — final path computed.
- `csv_service.py:364-367` — `df.write_csv(file=file_path, **params)` — writes directly to final path. No `.exists()` check precedes it. Confirmed at source.
- Grep for `mode.*=.*['"]x['"]` in `src/` → zero matches. Polars `write_csv` has no `mode="x"` parameter (confirmed via `inspect.signature`).
- `tests/test_csv_service.py:91-100` — `test_get_file_name` asserts only `startswith("prefix_")` and `endswith(".csv.gz")`. Does NOT assert cross-call uniqueness. Confirmed.
- Phase 04's independent PIPE-002 validation reproduced the collision at runtime: "two calls 50 ms apart → identical filenames." The same-second overwrite is real.

**Cited line numbers vs. actual (c0fd0e1):** All match. The phase file (b390d04) cited `csv_service.py:237-240` for `get_file_name`; the findings correctly use `239` for the `strftime` call and `311, 317, 359, 364-367` for the write path — all accurate at c0fd0e1.

**Severity:** HIGH. Phase 16 rubric explicitly names this finding in the HIGH band: "a same-second re-run overwrites prior output." Correctly graded.

**Cross-phase:** Overlaps PIPE-002 (Phase 04, HIGH, VALIDATED). Same root cause, same code site, same severity. See §5.

---

### OUT-002 — [HIGH] Non-atomic write to final path; mid-write crash leaves truncated .csv.gz consumed as complete

**Verdict: CONFIRMED.**

**Evidence (re-derived):**
- `csv_service.py:364-367` — `df.write_csv(file=file_path, **params)` writes directly to the final path. No `.tmp` path, no `os.replace`, no checksum. Confirmed at source.
- Grep for `tempfile|os.replace|\.tmp|rename` in `export_sql_to_csv` source → zero matches for atomicity tokens.
- `csv_service.py:309` — `export_path.mkdir(parents=True, exist_ok=True)` creates the directory but provides no atomicity step.
- `src/mko_data_cleaner/core/db_service.py:803-805` — `__exit__(self, exc_type, exc, tb): # pass; self.close()` — calls `close()` unconditionally, `exc_type` is accepted but never referenced. Confirmed at source.
- `db_service.py:786-798` — `close()` calls `self._delete_base_files()` at line 798, after closing both connections (795-796). Confirmed at source.
- `db_service.py:779-784` — `_delete_base_files()` globs `self.db_file.name + "*"` and `unlink(missing_ok=True)` each match — deleting `.db`, `.db-wal`, `.db-shm`. Confirmed at source.
- `docs/00-overview/current-behavior.md:294` — "Unexpected exception during `run_report` → Propagates out of the `with DBWorker(...)` block; `close()` still runs (context manager) and deletes the DB." Confirms the documented behavior.

**Cited line numbers vs. actual (c0fd0e1):** All match. The phase file (b390d04) cited `db_service.py:786-788` for `__exit__`; the actual `__exit__` is at 803-805 at c0fd0e1. The findings correctly cite 803-805.

**Severity:** HIGH. Phase 16 rubric explicitly names this finding in the HIGH band: "a non-atomic write leaving a truncated `.csv.gz` consumed as complete." Correctly graded.

**Cross-phase:** Overlaps PIPE-003 (Phase 04, HIGH, VALIDATED) and shares the DB-deletion-on-crash sub-concern with PIPE-004 (Phase 04, CRITICAL). See §5.

---

### OUT-003 — [HIGH] Empty-table export returns 0, writes no file, reports success

**Verdict: CONFIRMED.**

**Evidence (re-derived):**
- `csv_service.py:334` — `count_cursor.execute(f"SELECT COUNT(*) FROM {data_table}")`.
- `csv_service.py:335` — `total_rows = count_cursor.fetchone()[0]`.
- `csv_service.py:337-339` — `if total_rows == 0: logger.debug(...); return 0`. No file written. Confirmed at source.
- `app_service.py:266-270` — `rows_exported = csv_worker.export_sql_to_csv(db_con=..., data_table=..., export_path=...)`. Confirmed at source.
- `app_service.py:280` — `return ReportResult(...)`. Constructed only on the success path; if an exception fires inside the `with DBWorker(...)` block (lines 146-271), line 280 is never reached and no `ReportResult` is returned. Confirmed at source.
- `app_service.py:291` — `rows_exported=rows_exported`. Confirmed.
- `tests/test_csv_service.py:127-144` — `test_export_sql_to_csv_returns_zero_for_empty_table` asserts `count == 0` but does NOT assert file presence or absence. Confirmed.
- `docs/00-overview/current-behavior.md:293` — "Empty non-mapped set → No `null_data` file written." Consistent with the empty-table behavior.

**Cited line numbers vs. actual (c0fd0e1):** All match. The phase file (b390d04) cited `csv_service.py:334-336`; actual is 334-339 (the `COUNT(*)` query at 334-335, the early return at 337-339). The findings correctly cite 337-339.

**Severity:** HIGH. Phase 16 rubric explicitly names this finding in the HIGH band: "an empty-table export writing no file while reporting success." Correctly graded.

**Cross-phase:** Logically depends on OUT-006/PIPE-004 (missing status field). See §5.

---

### OUT-004 — [MEDIUM] `include_bom=True` in config silently absorbed by `extra="allow"`; not a declared model field

**Verdict: CONFIRMED.**

**Evidence (re-derived):**
- `src/mko_data_cleaner/settings/app_config.json5:41` — `"include_bom": true,`. Confirmed at source. (The phase file cited `app_config.yaml:43`, which does not exist; `git ls-files` confirms only `app_config.json5` exists in the tree.)
- `src/mko_data_cleaner/core/models.py:85-91` — `PolarsWriteCSV` declares `separator`, `decimal_comma`, `include_header`, `chunk_size`, `compression`. No `include_bom` field. `model_config = ConfigDict(extra="allow")` at line 86. Confirmed at source.
- Runtime reproduction (`python -c`): constructing `PolarsWriteCSV(include_bom=True, ...)` and calling `.model_dump()` yields keys `['separator', 'decimal_comma', 'include_header', 'chunk_size', 'compression', 'include_bom']` with `include_bom` = True. It survives via `__pydantic_extra__`.
- Runtime reproduction (misspelling): `PolarsWriteCSV(inclode_bom=True)` also survives `model_dump()` with no validation error — `inclode_bom` appears in the dump dict (and Polars silently ignores it).
- `inspect.signature(pl.DataFrame.write_csv)` — `include_bom` IS a valid Polars 2.0.0 `write_csv` parameter. So when spelled correctly, the BOM is emitted. The finding's claim is accurate.

**Cited line numbers vs. actual (c0fd0e1):** `app_config.json5:41` and `models.py:85-91` both match.

**Severity:** MEDIUM. Phase 16 rubric explicitly names this finding in the MEDIUM band: "a declared model field absent from the Pydantic schema but silently absorbed by `extra='allow'`." Correctly graded.

**No existing test** covers `include_bom` validation. Confirmed (grep `include_bom` in `tests/` → zero matches).

---

### OUT-005 — [MEDIUM] No machine-readable schema manifest; column contract exists only as prose

**Verdict: CONFIRMED.**

**Evidence (re-derived):**
- `docs/00-overview/data-flow.md:38-64` — "## Field and Column Semantics" section describes source columns, extra columns, and internal mapping columns entirely in prose. No schema-manifest file referenced. Confirmed at source.
- `csv_service.py:364` — `df.write_csv(file=file_path, **params)` is the only output operation in `export_sql_to_csv`. No companion manifest file is written.
- Columns are derived dynamically from the first raw file's headers (`get_csv_headers`, csv_service.py:77-86), cleaned via `clean_names`/make_valid (`utils.py`), stored as SQLite `TEXT` (all columns TEXT per db_service.py), and exported with `schema_overrides={col: pl.Utf8}` (csv_service.py:328, 348). No type information survives.
- Empty extra columns are dropped via `_drop_empty_columns` (`dict_service.py:53`, called at `dict_service.py:111`).

**Severity:** MEDIUM. Phase 16 rubric explicitly names this finding in the MEDIUM band: "no machine-readable schema manifest (contract is prose only)." Correctly graded.

---

### OUT-006 — [MEDIUM] No `error`/`status` field on ReportResult; `rows_exported==0` indistinguishable from full export

**Verdict: CONFIRMED (with cross-phase severity note).**

**Evidence (re-derived):**
- `src/mko_data_cleaner/core/models.py:182-207` — `ReportResult` declares exactly 11 fields: `report_dir`, `import_dir`, `export_dir`, `db_path`, `null_data_files`, `started_at`, `finished_at`, `duration`, `timings`, `rows_loaded`, `rows_exported`. No `error`, `status`, `success`, or any failure indicator. Confirmed at source.
- `app_service.py:280-292` — `run_report` constructs `ReportResult(...)` only at line 280, after the `with DBWorker(...)` block completes. If the method raises before line 280, no `ReportResult` is returned; if it reaches line 280 with `rows_exported=0`, the caller sees success with zero rows. Confirmed at source.
- `src/mko_data_cleaner/app.py:126-145` — `process_data(...) -> ReportResult` returns `run_report`'s result directly (line 145). No exception-to-status mapping anywhere in the chain.
- Runtime confirmation: `ReportResult.model_fields` contains exactly the 11 keys above; `'error'` and `'status'` are absent.
- `tests/test_app.py:260-283` — `test_report_result_model_construction_and_serialization` constructs `ReportResult` with all 11 fields. Locks the 11-field shape; adding fields with defaults is backward-compatible. Confirmed.

**Cited line numbers vs. actual (c0fd0e1):** All match (`models.py:182-207`, `app_service.py:280`).

**Severity:** MEDIUM. Phase 16 rubric explicitly names this finding in the MEDIUM band: "no `error`/`status` field on `ReportResult` (`models.py:170-195`) so `rows_exported == 0` is indistinguishable from a full export." The validator cannot re-grade against Phase 16's own rubric, which places this finding precisely here. See §5 for the cross-phase severity disagreement with PIPE-004.

---

### OUT-007 — [LOW] data-flow.md:178 claims "repeated runs never overwrite previous outputs" — contradicted by OUT-001

**Verdict: CONFIRMED.**

**Evidence (re-derived):**
- `docs/00-overview/data-flow.md:177-178`:
  ```
  - File names carry the run timestamp (`<prefix>_<YYYYMMDD-HHMMSS>_<N>`), so
    repeated runs never overwrite previous outputs.
  ```
  The "never overwrite" guarantee is stated as fact. Confirmed at source.
- This is directly contradicted by OUT-001: same-second re-runs produce identical filenames (second-granular `strftime` + per-call `file_index=1` reset), and `df.write_csv` overwrites the final path without any existence check.
- No test asserts this doc claim. Confirmed (grep `never overwrite` in `tests/` → zero matches).

**Severity:** LOW. Phase 16 rubric explicitly names this finding in the LOW band: "documentation drift about output format with no runtime consequence; a stale doc claim (`data-flow.md:178` 'repeated runs never overwrite') with no live data loss in the current test suite." Correctly graded.

---

## Cross-Phase Analysis (Block 6)

Three Phase 16 findings overlap with validated Phase 04 (PIPE) findings. All were confirmed independently in `.ai/audit/99-validation/04-data-pipeline-validated-findings.md` (VALIDATED at c0fd0e1). The Phase 16 findings file's "Fix Order → Cross-phase conflicts: None" is **technically accurate but omits acknowledgment** of these overlaps.

| Phase 16 | Phase 16 Severity | Overlaps PIPE | PIPE Severity | Relationship | Disposition |
|---|---|---|---|---|---|
| OUT-001 | HIGH | PIPE-002 | HIGH | Same root cause, same code site (`get_file_name` / `csv_service.py`) | Merge candidate (cross-phase) |
| OUT-002 | HIGH | PIPE-003 | HIGH | Same root cause, same code site (`export_sql_to_csv` → `write_csv`) | Merge candidate (cross-phase) |
| OUT-003 | HIGH | (related) | PIPE-004 | OUT-003 is the runtime trigger that makes PIPE-004's missing-status-field dangerous | Cross-reference |
| OUT-006 | MEDIUM | PIPE-004 | CRITICAL | Same root cause (`ReportResult` has no `error`/`status`), same code site (`models.py:182-207`) | Severity disagreement (see below) |

**No conflicts (no opposing conclusions).** Both phases agree every defect exists. The overlaps are multi-angle: Phase 04 examines pipeline lifecycle/mechanics; Phase 16 examines output-readiness for downstream consumers.

**Severity disagreement — OUT-006 (MEDIUM) vs PIPE-004 (CRITICAL):** Both are correct per their own phase rubrics:
- Phase 04 CRITICAL band: "the run reports success while producing no/misleading output... `ReportResult` carries no `error`/`status` field... The diagnostic SQLite DB is **always deleted on exit**... so post-mortem inspection is impossible." Phase 04 bundles the missing status field WITH the unconditional DB deletion into one CRITICAL finding about pipeline observability and crash-state destruction.
- Phase 16 MEDIUM band: "no `error`/`status` field on `ReportResult` so `rows_exported == 0` is indistinguishable from a full export." Phase 16's rubric explicitly names OUT-006 in the MEDIUM band because, from the output-readiness angle, "the row count is accurate; the defect is missing metadata, not wrong data." The DB-deletion-on-crash (the part that pushes Phase 04 to CRITICAL) is captured separately under OUT-002 in Phase 16.

Neither finding is mis-graded. The Phase 16 validator does not re-grade; the rubric is the standard.

**Findings-file gap — "Cross-phase conflicts: None":** The findings file does not cross-reference PIPE-002/003/004 despite sharing the same root causes. This is a traceability gap, not a correctness defect. A reader of the validated report should be aware of the sibling findings for roadmap deduplication.

---

## Validation-Level Findings (VAL-)

### VAL-001 — [MEDIUM] Findings file does not acknowledge cross-phase overlap with validated PIPE-002/003/004

The findings' "Fix Order" section states "Cross-phase conflicts: None — all findings are owned by Phase 16." While technically correct (no opposing conclusions exist), it fails to cross-reference the three validated PIPE findings from Phase 04 that share the same root causes and code sites. A consolidated roadmap entry for `csv_service.py` write-path fixes (collision guard + atomic write + status field) would prevent duplicate work.

**Evidence:** `.ai/audit/99-validation/04-data-pipeline-validated-findings.md` confirms PIPE-002 (HIGH), PIPE-003 (HIGH), PIPE-004 (CRITICAL) as VALIDATED at c0fd0e1. `OUT-*` prefix in `src/` and `docs/` → zero matches (no namespace collision).

**Fix:** Add cross-references in the findings' "Related" fields: OUT-001↔PIPE-002, OUT-002↔PIPE-003, OUT-003/OUT-006↔PIPE-004.

### VAL-002 — [LOW] "ENT-003/PIPE-008" bundle in Not-verified section is slightly inaccurate

The findings cite "ENT-003/PIPE-008" for the `print()` in `AppService.run_report`. ENT-003 is correctly about `app_service.py::AppService.run_report` (Phase 01 findings: "print() + Russian text in core orchestrator"). However, PIPE-008 is about `db_service.py::get_table_sample` (Phase 04 findings: "Dead debug helper uses print() at db_service.py:339-348") — a different `print()` site. The bundle conflates two distinct findings about two different code locations.

**Evidence:** `.ai/audit/01-entry-architecture/findings.md:42` — ENT-003 anchors `core/app_service.py::AppService.run_report`. `.ai/audit/04-data-pipeline/findings.md:65` — PIPE-008 anchors `db_service.py::get_table_sample`. Three `print()` calls confirmed at `app_service.py:113, 259, 274`.

**Fix:** Correct the reference to "ENT-003 only" in the Not-verified note.

### VAL-003 — [LOW] Phase file (b390d04) line numbers are stale relative to c0fd0e1

The phase file `16-audit-rendered-surface.md` (code_ref `b390d04`) cites line numbers that do not match the actual c0fd0e1 source. The findings file correctly adapted to c0fd0e1. The phase file also cites `app_config.yaml` which does not exist (`app_config.json5` is the actual file). This is phase-file drift, not a findings defect, but the phase file's `severity_taxonomy` pointer and code_ref should be updated to `c0fd0e1` for reproducibility.

**Evidence:** Phase file cites `csv_service.py:358-361` (actual: 364-367); `csv_service.py:334-336` (actual: 337-339); `db_service.py:786-788` for `__exit__` (actual: 803-805); `csv_service.py:237-240` for `get_file_name` (actual: 237-240, partially matches). All findings-file citations match c0fd0e1.

---

## Rollout Analysis

**Dependencies (per findings' Fix Order, confirmed):**
- OUT-006 (add `status`/`error` to `ReportResult`) should land before OUT-003's resolution, because the status field is needed to distinguish "empty source" from "crashed export." Fixing OUT-003 alone (write a header-only file) would mask the crash signal that OUT-006's fix is meant to provide. ✓ Valid dependency.
- OUT-001 and OUT-002 share the same write site (`export_sql_to_csv` → `df.write_csv`). A single fix (write-to-temp + `os.replace` with collision-resistant name) could address both. ✓ Valid merge candidate.
- Cross-phase: PIPE-004 (add `error`/`status` to `ReportResult`) is the Phase 04 counterpart of OUT-006. Fixing once serves both phases.

**Rollout risk (per findings, verified against actual tests):**
- OUT-001: Adding a UUID suffix or microsecond precision changes filenames; downstream globs matching `data_table_*.csv.gz` are unaffected. `test_get_file_name` (lines 91-100) stays green — it only asserts prefix/suffix. ✓
- OUT-002: Temp+rename pattern does not change committed content; `test_export_sql_to_csv_roundtrip_content` (line 174) and `test_export_sql_to_csv_gzip` (line 310) stay green. ✓
- OUT-003: Writing a header-only file for empty tables changes the output surface (new file appears); `test_export_sql_to_csv_returns_zero_for_empty_table` (line 127) must be updated to assert file presence. ✓
- OUT-004: Adding `include_bom: bool = True` as a declared field is backward-compatible; `model_dump()` produces the same dict. No test change needed. ✓ (Confirmed: Pydantic v2 with `extra="allow"` — adding an explicit field that was previously in extras produces identical `model_dump()` output when the value matches.)
- OUT-005: Emitting a manifest is additive; existing tests stay green. ✓
- OUT-006: Adding fields with defaults to `ReportResult` is backward-compatible; `test_report_result_model_construction_and_serialization` (line 260) stays green. ✓ (Confirmed: Pydantic v2 — `ReportResult` with defaults accepts construction with existing 11 fields.)
- OUT-007: Doc-only change; no test impact. ✓

**No conflicting evidence** within any finding.

## Execution Validation

All fix targets and anchors still exist at c0fd0e1 (verified via grep/read):

| Target | Still exists? |
|---|---|
| `csv_service.py::CSVWorker.get_file_name` (237-240) | ✅ |
| `csv_service.py::CSVWorker.export_sql_to_csv` (269-381) | ✅ |
| `csv_service.py:337-339` (empty-table early return) | ✅ |
| `csv_service.py:359-367` (write_csv to final path) | ✅ |
| `models.py::PolarsWriteCSV` (85-91) | ✅ |
| `models.py::ReportResult` (182-207) | ✅ |
| `app_service.py:266-270, 280-292` (export call + ReportResult construction) | ✅ |
| `app.py:126-145` (process_data return) | ✅ |
| `db_service.py:803-805` (__exit__) / 786-798 (close()) / 779-784 (_delete_base_files) | ✅ |
| `app_config.json5:41` (include_bom) | ✅ |
| `data-flow.md:177-178` (never-overwrite doc) | ✅ |
| `tests/test_csv_service.py:91-100, 127-144, 174, 310` (coverage-gap tests) | ✅ |
| `tests/test_app.py:260-283` (ReportResult shape lock) | ✅ |

**Readiness:** All 7 findings are actionable against live, unmodified code at c0fd0e1. No anchor has drifted. The findings adapted correctly from the phase file's stale b390d04 references.

## Warnings

- **Cross-phase dedup opportunity:** OUT-001/002/003/006 and PIPE-002/003/004 (Phase 04) share 4 of 7 findings' root causes. A unified fix in `csv_service.py` (collision guard + atomic write) and `models.py` (status field) would resolve both phase reports' recommendations simultaneously.
- **Severity band divergence:** The same `ReportResult` status-field defect is HIGH in Phase 04's CRITICAL band and MEDIUM in Phase 16's MEDIUM band. This is correct per each phase's rubric, but a reader consuming only one report may over- or under-estimate the risk. Flag for corpus-wide severity alignment.
- **Phase file staleness:** `16-audit-rendered-surface.md` carries code_ref `b390d04` but the findings (and all other validated reports) use `c0fd0e1`. The phase file's `severity_taxonomy` self-reference and line-number evidence are stale. This affects reproducibility for future audits but not the findings' validity.
- **Unreproduced runtime claims:** OUT-001 and OUT-002 are marked "static" (correctly). The same-second overwrite was independently reproduced by Phase 04's PIPE-002 validation (50 ms apart → identical filenames). OUT-002's crash-recovery scenario could not be runtime-reproduced without fault injection — correctly classified by both phases.
- **`include_bom` visibility:** With gzip compression, the BOM is the first 3 bytes of the *uncompressed* stream and is invisible to decompressing consumers. Even if the field is properly declared (OUT-004 fix), the BOM's utility for downstream gzip consumers is questionable — worth documenting.

## Required Fixes

Same as the findings' own "Fix Order" — all 7 are confirmed and actionable:

1. **OUT-001:** Add collision-resistant suffix to `CSVWorker.get_file_name` (`csv_service.py:237-240`) — e.g. `uuid.uuid4().hex[:8]` or microsecond precision.
2. **OUT-002:** Write to temp path + `os.replace` in `export_sql_to_csv` (`csv_service.py:359-367`); gate `_delete_base_files()` on success in `__exit__` (`db_service.py:803-805`).
3. **OUT-003:** Write a header-only CSV.gz for empty tables, OR rely on OUT-006's status field to signal the empty case distinctly (`csv_service.py:337-339`).
4. **OUT-004:** Add `include_bom: bool = True` as an explicit field on `PolarsWriteCSV` (`models.py:85-91`).
5. **OUT-005:** Emit a companion schema manifest alongside each export batch (`csv_service.py:364`).
6. **OUT-006:** Add `status` and `error` fields to `ReportResult` (`models.py:182-207`); populate `status` in `run_report` (`app_service.py:280`).
7. **OUT-007:** Update `data-flow.md:177-178` to state second-granular timestamps and same-second overwrite risk.

## Advisory Recommendations

- **Cross-phase merge:** Dedupe OUT-001↔PIPE-002 and OUT-002↔PIPE-003 at the roadmap level — both are confirmed VALIDATED at the same code sites. A single `export_sql_to_csv` rewrite (temp+rename + collision guard) resolves both.
- **VAL-001 remediation:** Add `Related: PIPE-002`, `PIPE-003`, `PIPE-004` to the findings' cross-references for traceability.
- **VAL-002 remediation:** Correct "ENT-003/PIPE-008" to "ENT-003" in the Not-verified note.
- **VAL-003 remediation:** Update `16-audit-rendered-surface.md` code_ref to `c0fd0e1` and correct stale line citations for future reproducibility.
- **Test coverage:** The new tests proposed in each finding (`test_get_file_name_same_second_unique`, `test_export_sql_to_csv_atomic_write_renames_temp`, `test_export_sql_to_csv_empty_table_writes_header_file`, `test_polars_write_csv_validates_include_bom`, `test_export_produces_schema_manifest`, `test_repeated_runs_same_second_do_not_overwrite`) are all untested today and should be added before or alongside the fixes.
