---
phase: "04"
phase_name: "data-pipeline"
validated_at: "2026-10-19"
base_sha: "c0fd0e1"
validator: "kilo/poolside/laguna-s-2.1:free"
---

# Validation Report — Data Pipeline & Report Lifecycle Findings

## Validation Summary

All 10 findings in `.ai/audit/04-data-pipeline/findings.md` were independently
re-verified against the codebase at base SHA `c0fd0e1`. Each finding's source
anchor, runtime repro, and static (basedpyright) evidence was reproduced
independently — not accepted from the findings file's prior experiment output.

**Result: 10 VALIDATED, 0 REJECTED.** Two findings carry **caveats** about
repro completeness (PIPE-001) and precision of the crash mechanism (PIPE-007),
but the underlying defects are confirmed.

| ID | Severity | Verdict | Caveat |
|---|---|:---:|---|
| PIPE-004 | CRITICAL | VALIDATED | — |
| PIPE-001 | HIGH | VALIDATED | repro block incomplete (see below) |
| PIPE-002 | HIGH | VALIDATED | — |
| PIPE-003 | HIGH | VALIDATED | static evidence only (correctly classified) |
| PIPE-005 | MEDIUM | VALIDATED | — |
| PIPE-006 | MEDIUM | VALIDATED | — |
| PIPE-007 | MEDIUM | VALIDATED | crash mechanism imprecise (see below) |
| PIPE-008 | LOW | VALIDATED | — |
| PIPE-009 | LOW | VALIDATED | — |
| PIPE-010 | DOC-UPDATE | VALIDATED | — |

**Baseline gates (independently re-run):**
- `uv run pytest -q` → **93 passed** in 3.83s (matches findings).
- `uv run ruff check src/ tests/` → **All checks passed!** (matches findings: clean).
- `uv run basedpyright src/` → **39 errors, 0 warnings, 0 notes** (matches findings).
- `uv run ruff format --check src/ tests/` → **5 files would reformat** (app_service.py, db_service.py, dict_service.py, utils.py, notebook.py) — matches findings; this is a pre-existing formatting drift, not a finding.

---

## Per-Finding Validation

### PIPE-004 — [CRITICAL] ReportResult lacks error/status field; rows_exported==0 indistinguishable

**Verdict: VALIDATED.**

**Evidence:**
- `src/mko_data_cleaner/core/models.py:182-207` — `ReportResult` defines exactly 11 fields:
  `['report_dir', 'import_dir', 'export_dir', 'db_path', 'null_data_files', 'started_at', 'finished_at', 'duration', 'timings', 'rows_loaded', 'rows_exported']`. No `error` or `status` field. Independently confirmed via `ReportResult.model_fields`.
- `src/mko_data_cleaner/core/csv_service.py:337-339` — `export_sql_to_csv` returns bare `0` for an empty table (`if total_rows == 0: ... return 0`), logged at `debug` level only (line 338). Independently reproduced: empty table → returns `0`.
- `src/mko_data_cleaner/core/app_service.py:280-292` — `run_report` constructs `ReportResult(rows_exported=...)` directly from `export_sql_to_csv`'s integer return (lines 266-270). No exception-to-status mapping.
- `src/mko_data_cleaner/app.py:128,145` — `process_data(...) -> ReportResult` returns the result unchanged.

**Caveat:** The finding is latent (no caller currently mis-reads it), as the finding's own "Why this band" section states. The risk is latent-but-real for programmatic consumers (Jupyter/Airflow/future API).

**Fix target is correct:** Add `error`/`status` to `ReportResult`; set in `run_report`.

---

### PIPE-001 — [HIGH] create_table_with_index crashes when index_column is None

**Verdict: VALIDATED** (with repro caveat).

**Evidence:**
- `src/mko_data_cleaner/core/db_service.py:391-398` — `create_table_with_index` unconditionally calls `_create_index(self.data_tbl_name, self.index_column)` (line 397) and `_create_index(self._index_tbl_name, self.index_column)` (line 398) with **no guard** on `index_column` being set.
- `_create_index` (db_service.py:383-389) does `'_'.join(columns)` at line 386. When `columns=(None,)`, this raises `TypeError: sequence item 0: expected str instance, NoneType found`.
- `self.index_column` is `None` when `DataFile.index_column = None` (models.py:81 declares `index_column: str = None`).
- Independently reproduced the TypeError at runtime (with required fields satisfied):
  `TypeError: sequence item 0: expected str instance, NoneType found`.
- basedpyright 397:48, 398:28, 398:50 — all present in actual output, confirming `str | None` passed where `str` is required.

**Caveat (repro incompleteness):** The finding's stated repro block omits setting
`search_columns`. `_validate_required()` (line 393) checks `search_columns` (a
REQUIRED_FIELD) and raises `WrongDataSettings("search_columns not set")` *before*
reaching line 397, because `search_columns` defaults to `None`. The TypeError
only manifests when the required fields are satisfied but `index_column=None`.
The defect is nonetheless real and confirmed both at runtime (when required
fields are set) and statically (basedpyright 397/398). The fix recommendation
(wrap `_create_index` calls in `if self.index_column:`) is correct.

---

### PIPE-002 — [HIGH] Same-second filename collision; overwrites with no guard

**Verdict: VALIDATED.**

**Evidence:**
- `src/mko_data_cleaner/core/csv_service.py:237-240` — `get_file_name` uses
  `strftime("%Y%m%d-%H%M%S")` (second granularity, no sub-second or counter).
- `src/mko_data_cleaner/core/csv_service.py:359` — `file_path = export_path / file_name.format(...)` with no `.exists()` check before `df.write_csv` (line 364).
- Independently reproduced: two calls 50 ms apart → identical filenames
  `null_data_20261019-164809_1.csv.gz`. Confirmed second-granularity and no collision guard.

---

### PIPE-003 — [HIGH] Non-atomic CSV writes; truncated file on crash

**Verdict: VALIDATED** (static evidence; runtime crash-safety not reproducible without fault injection, correctly classified).

**Evidence:**
- `src/mko_data_cleaner/core/csv_service.py:359-367` — `df.write_csv(file=file_path, ...)` writes directly to the final path.
- No `tempfile`, `os.replace`, `.tmp`, or `rename` token exists in `export_sql_to_csv` source (independently confirmed).
- `export_path.mkdir(parents=True, exist_ok=True)` (line 309) creates the directory but provides no atomicity step.

---

### PIPE-005 — [MEDIUM] check_date_column annotation contradicts actual behavior

**Verdict: VALIDATED.**

**Evidence:**
- `src/mko_data_cleaner/core/csv_service.py:153` — `def check_date_column(self, column: str = None) -> tuple[int, str] | None`. Annotated return is `tuple[int, str] | None`; parameter `column: str = None` is incorrect (should be `str | None = None`).
- `src/mko_data_cleaner/core/csv_service.py:157` — `return self._detect_date_column(df, column)`.
- `_detect_date_column` (csv_service.py:124) is annotated `-> str | None` and returns only a column-name string (the `for _, col in enumerate(...)` loop at line 148 discards the index).
- basedpyright `csv_service.py:157:16` (type `str | None` not assignable to `tuple[int, str] | None`) and `app_service.py:150:25` (tuple cannot be passed to `date_column: str | None`) — both present in actual basedpyright output.

**Note:** The finding also cites `architecture.md:165`. The actual doc at architecture.md:165 is a different sentence than found in data-flow.md. The architecture.md:219 claim ("returns None (result object: target work)") IS present and stale (see PIPE-010). The annotation mismatch is confirmed regardless.

---

### PIPE-006 — [MEDIUM] _is_date_column hardcodes sample_size instead of using DATE_SAMPLE_SIZE

**Verdict: VALIDATED.**

**Evidence:**
- `src/mko_data_cleaner/core/csv_service.py:88` — `_is_date_column` is a `@staticmethod` (confirmed via `inspect.getattr_static` → `staticmethod`).
- `src/mko_data_cleaner/core/csv_service.py:111` — `sample_size = 500` hardcoded.
- `CSVWorker.DATE_SAMPLE_SIZE = 500` (csv_service.py:30) is used only in `check_date_column` (line 155: `n_rows=self.DATE_SAMPLE_SIZE`).
- A `@staticmethod` cannot reference `self`, so it cannot use `self.DATE_SAMPLE_SIZE`. Independently confirmed: source contains hardcoded `sample_size = 500` and does NOT contain `self.DATE_SAMPLE_SIZE`.

---

### PIPE-007 — [MEDIUM] build_non_mapped uses bitwise | on TEXT columns; crashes on empty extra_columns

**Verdict: VALIDATED** (with crash-mechanism caveat).

**Evidence:**
- `src/mko_data_cleaner/core/db_service.py:762-773` — `build_non_mapped` constructs the WHERE clause as `{' | '.join(self.extra_columns)} IS NULL` (line 771), a bitwise-OR (`|`) on TEXT columns, not SQL `OR`.
- `self.search_columns + self.extra_columns` at line 766 — if `extra_columns` is `None`, `+` raises `TypeError: can only concatenate list (not "NoneType") to list`.
- Independent runtime reproduction:
  - Empty `extra_columns=[]` → `' | '.join([])` = `''` → WHERE becomes `WHERE  IS NULL` → `sqlite3.OperationalError: near "IS": syntax error` (not a Python TypeError as the finding implies).
  - `None` `extra_columns` → `TypeError: can only concatenate list (not "NoneType") to list` at line 766.
- basedpyright `db_service.py:766:33` (operator `+` on `list[Never] | None` and `list[str] | None`) and `771:26`/`771:37` (join overloads) — all present in actual output.

**Caveat:** The finding states the empty-`extra_columns` case "crashes" with `' | '.join(None)` raising `TypeError` — but for the empty-list case (the realistic post-`set_data_tbl_columns` state), the crash is actually `sqlite3.OperationalError` (invalid SQL), not a Python `TypeError`. For the `None` case (pre-init state), it is a `TypeError` but at line 766 (`search_columns + extra_columns`), not at the `join`. The defect is confirmed; the precise crash path differs slightly from the finding's wording. The bitwise-OR-on-TEXT claim is confirmed.

**Important operational note:** In the normal pipeline flow, `_validate_required()` (called by `create_table_with_index`) rejects an empty `extra_columns` list because `not []` is truthy-True — so `build_non_mapped` with empty extra_columns is unreachable through the standard flow. The fragility is real in the code but gated by an adjacent falsy-empty check.

---

### PIPE-008 — [LOW] get_table_sample is dead code using print()

**Verdict: VALIDATED.**

**Evidence:**
- `src/mko_data_cleaner/core/db_service.py:339-348` — `get_table_sample` uses `print(f"These are sample rows for {tbl_name}", flush=True)` (line 347) and `print(tbl_row, flush=True)` (line 348). Confirmed via `inspect.getsource` → contains `print(`.
- Repo-wide grep for `get_table_sample` (`src/`, `tests/`, `scripts/`) → only the definition at db_service.py:339. **Zero call sites** (the only other matches are inside `findings.md` itself). Confirmed.
- Violates the project "No `print()` Statements" rule (project.md rule 12) and bypasses the `dictConfig`'d logger.

---

### PIPE-009 — [LOW] Stale __main__ block hardcodes Windows path `data\mvideo`

**Verdict: VALIDATED.**

**Evidence:**
- `src/mko_data_cleaner/core/app_service.py:343-344`:
  ```python
  if __name__ == "__main__":
      get_app_service().run_report(r"data\mvideo")
  ```
  Independently confirmed via `inspect.getsource`.
- The canonical CLI entry point is `src/mko_data_cleaner/app.py:153-154: app()` (the `mko-data-cleaner` console script). The `app_service.py` block is a dev-only, non-ported convenience that would attempt to process a non-existent Windows path when run via `python -m mko_data_cleaner.core.app_service`.

---

### PIPE-010 — [DOC-UPDATE] Docs claim process_data returns None; code returns ReportResult

**Verdict: VALIDATED.**

**Stale doc claims (confirmed present):**
- `docs/00-overview/data-flow.md:165` — "Currently `None`; owner-confirmed to become a result object with paths + timings + clean row counts (D8)"
- `docs/00-overview/current-behavior.md:272` — "`process_data` currently returns `None`; the result object (paths +..."
- `docs/00-overview/architecture.md:219` — `S-->>U: returns None (result object: target work)`
- `docs/00-overview/open-questions.md:71` — "process_data returns `None`; the owner-confirmed result object (paths + timings + row counts, D8) is not implemented"

**Code reality (confirmed):**
- `src/mko_data_cleaner/app.py:126-145` — `process_data(...) -> ReportResult` (annotation confirmed via `inspect.signature`), returns `get_app_service(settings_dir).run_report(path)`.
- `src/mko_data_cleaner/core/app_service.py:280-292` — `run_report` constructs and returns `ReportResult(...)`.

The result object (D8) **ships today**; four docs + the open-question D-6 are stale.

---

## Rollout Analysis

**Dependencies:**
- PIPE-005 must land before the type-check gate is fully green (it removes 2 of 39 basedpyright errors: csv_service.py:157 and app_service.py:150). Non-breaking at runtime.
- PIPE-004 (add `error`/`status` to `ReportResult`) touches `models.py` + `app_service.py:run_report` return construction. Pydantic v2 model — adding fields with defaults is non-breaking; 93 tests remain green.
- PIPE-001 and PIPE-007 share `db_service.py` (`_create_index` / `build_non_mapped` call sites). Independently fixable.
- PIPE-002 and PIPE-003 share `csv_service.py` (`get_file_name` / `export_sql_to_csv`). Independently fixable.

**Rollout risks (from findings, confirmed):**
- PIPE-002 filename scheme change may break downstream parsers expecting `<prefix>_<YYYYMMDD-HHMMSS>_<N>`.
- PIPE-007 `build_non_mapped` is gated by `_validate_required`'s falsy-empty check in the normal flow — fixing it to emit proper `OR ... IS NULL` SQL will not be exercised unless extra_columns is non-empty (which is the normal case anyway).

**No conflicting evidence.** PIPE-008's `print()` is complementary to ENT-003 (phase 01), not conflicting.

---

## Execution Validation

All fix targets and anchors still exist at c0fd0e1 (verified by grep/inspect):

| Target | Still exists? |
|---|---|
| `models.py::ReportResult` (models.py:182-207) | ✅ |
| `app_service.py::run_report` return (280-292) | ✅ |
| `csv_service.py::check_date_column` (153) / `_detect_date_column` (124) / `_is_date_column` (88-122) / `get_file_name` (237-240) / `export_sql_to_csv` (269-381) | ✅ |
| `db_service.py::create_table_with_index` (391-398) / `_create_index` (383-389) / `build_non_mapped` (762-773) / `get_table_sample` (339-348) | ✅ |
| `app_service.py::__main__` (343-344) | ✅ |
| `app.py::process_data` (126-145) | ✅ |
| 4 stale doc lines + open-questions.md:71 | ✅ (all stale) |

**Readiness:** All findings are actionable against live, unmodified code at the validated SHA. No target has drifted.

---

## Required Fixes (consolidated)

1. **PIPE-004** — Add `error: str | None = None` and `status: Literal["ok","empty","failed"] = "ok"` to `ReportResult` (models.py:182-207); populate `status` in `run_report` based on `rows_exported` and exception state.
2. **PIPE-001** — Guard both `_create_index` calls in `create_table_with_index` (db_service.py:397-398) with `if self.index_column:`.
3. **PIPE-002** — Add a collision guard in `export_sql_to_csv` (csv_service.py:359) after computing `file_path` — existence check + incrementing counter, or microsecond precision.
4. **PIPE-003** — Make `df.write_csv` (csv_service.py:364) atomic: write to a `.tmp` path, then `os.replace` on success.
5. **PIPE-005** — Change `check_date_column` return annotation to `-> str | None`; fix `column: str = None` → `column: str | None = None` (csv_service.py:153).
6. **PIPE-006** — Remove `@staticmethod` from `_is_date_column` (or accept `sample_size` as a parameter) and use `self.DATE_SAMPLE_SIZE`.
7. **PIPE-007** — Replace `' | '.join(self.extra_columns) IS NULL` with `{' OR '.join(f'{c} IS NULL' for c in self.extra_columns)}`; add `if not self.extra_columns: return []` guard; fix the `search_columns + extra_columns` `+` operand (None-safety).
8. **PIPE-008** — Remove `get_table_sample` or convert `print()` → `logger.info()` behind a debug flag.
9. **PIPE-009** — Remove the `__main__` block (app_service.py:343-344).
10. **PIPE-010** — Update data-flow.md:165, current-behavior.md:272, architecture.md:219, open-questions.md:71 (D-6) to reflect `process_data` returns `ReportResult`.

---

## Advisory Recommendations

- **Basedpyright hygiene:** PIPE-005, PIPE-006, PIPE-007 collectively account for 5 of the 39 basedpyright errors tied to this phase. Fixing them (plus PIPE-001's 3 errors) removes **8 of 39** type errors — a meaningful gate improvement. The remaining 31 errors (paths.py, dict_service.py, models.py:81-82, app_service.py:56/117/161/165/281, etc.) are out of scope for this phase but documented.
- **PIPE-001 repro fix:** The findings file's repro block should set `search_columns` (or remove `_validate_required`) to reproduce the TypeError directly. Recommend annotating the repro with `# must set search_columns to pass _validate_required`.
- **ruff format gate:** 5 files are unformatted (`ruff format --check`). If format is added as a CI/gate check, these must pass before merge. Not a data-pipeline defect but a rollout blocker if the gate is enforced.
- **PIPE-006 design note:** Converting `_is_date_column` from `@staticmethod` to an instance method widens its surface beyond the original intent; an alternative is to pass `sample_size` as a parameter from `_detect_date_column`. Evaluate before implementing.
