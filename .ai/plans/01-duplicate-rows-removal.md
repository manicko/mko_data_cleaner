---
id: execution-plan-01-duplicate-rows-removal
domain: plan
tags:
  - execution-plan
  - deduplication
related:
  - problems/08_duplicate_rows_removal_spec
---

# Execution Plan — 01 Duplicate Rows Removal

**Source specification:** `.ai/problems/08_duplicate_rows_removal_spec.md` (T1–T7, FR-1…FR-17, NFR-1…NFR-6, D16–D27)
**Audit context:** `ses_e063b4b2dffeOlOARdg9lM2Vpu` (see §"Audit findings" below)
**Status:** ready for execution

---

## 1. Scope summary

Add optional, deterministic, pre-rule **global exact de-duplication** to the Mediascope
cleaner pipeline:

- `deduplicate: bool = False` on `DataSettings` + `app_config.json5` template (T1)
- deterministic `data_files` ordering in `CSVWorker` (T2)
- `CSVWorker.load_deduplicated()` global primitive using one `scan_csv` +
  `.unique(keep="first", maintain_order=True)` (T3)
- `AppService` integration + 2 new `ReportResult` metrics + log line (T4)
- `--deduplicate` CLI flag + `process_data(deduplicate=...)` override (T5)
- test suite D1–D18 + `make_report` factory (T6)
- documentation (T7 — Doc-specialist, run separately in Phase 4)

Out of scope: per §12 of the spec (no `--no-deduplicate`, no column subset, no fuzzy
matching, no post-rule dedup, no raw-file rewrite, no rule-engine/schema changes, no
`timings["deduplicate"]`, no full-pipeline CI scale test, do not touch the 4 pre-existing
`test_notebook.py` failures).

---

## 2. Execution blocks and sequencing

Blocks are sequential; each block depends on the prior one. T1+T2 are independent of each
other and both precede T3.

| Block | Tasks | Depends on | Agent(s) | Risk |
|---|---|---|---|---|
| B1 | T1, T2 | — | Implementor | Low |
| B2 | T3 | B1 | Implementor | Medium (Polars API, empty-frame guard) |
| B3 | T4 | B2 | Implementor | High (metrics invariant, monkeypatch trap) |
| B4 | T5 | B3 | Implementor | Medium (CLI tri-state, substring compat) |
| B5 | T6 | B4 | Implementor + Validator | High (D1–D18 coverage) |
| B6 | T7 | B1–B5 | Doc-specialist (Phase 4) | Low |
| — | Final validation | B1–B6 | Validator (Phase 5) | — |

Risk-based agent assignment: no block triggers the full auditor/researcher/planner cascade
(the spec already contains the research R1–R3 and the audit confirmed reality). B3 is the
only high-risk block; the implementor gets explicit watch-items from the audit (below). The
Validator covers B5 and the final pass.

---

## 3. Audit findings carried into implementation (watch-items)

These are binding constraints for the Implementor. Source: audit `ses_e063b4b2dffeOlOARdg9lM2Vpu`.

1. **CRITICAL — `test_app.py:316-321` monkeypatch will break.** It patches
   `mko_data_cleaner.app.get_app_service` with
   `lambda settings_dir=None, dict_path=None, clear_export_folder=None, force=None`.
   When T5 forwards `deduplicate=deduplicate`, that lambda raises `TypeError`. **Must**
   add `deduplicate=None` to the lambda signature (C2 permits fixing tests).
2. **`_import_data` return-type change ripples to one site.** Currently `int`
   (`app_service.py:231-249`, consumed at `run_report` `:342`). T4 → `tuple[int,int]`;
   update `:342` to `rows_loaded, rows_removed = self._import_data(...)`. No other callers.
3. **Empty-frame guard (T3/D6).** Current chunk loop never writes an empty frame. The dedup
   path calls `df.write_database` once — guard `if df.is_empty(): skip write` to handle
   header-only input (D6: 0/0/0/0).
4. **`rows_resulted` vs `rows_exported`.** `rows_resulted = rows_loaded −
   rows_removed_duplicates` (post-dedup). `rows_exported` stays the final export count.
   Do not conflate.
5. **No `--no-deduplicate` (D16).** Existing `force`/`clear_export_folder` use dual
   `--x/--no-x`; dedup uses a **single** `--deduplicate` defaulting to `None`
   (tri-state). Do not copy the dual form.
6. **Summary line substring compat (D25/FR-16).** `test_app.py:529` asserts
   `"3 loaded → 2 clean"`. Append ` · {rows_removed_duplicates:,} duplicate(s) removed`
   ONLY when dedup is active; emit nothing extra when inactive.
7. **T2 genuinely missing.** `list_files_in_directory` (utils.py:104-128) returns unsorted
   `Path.glob` results. Sort inside `_set_files_params` (csv_service.py:60-65), not in the
   helper (it is shared with dict/settings listing).
8. **Polars 2.0.0** pinned + installed — matches spec. `scan_csv([...], new_columns=...)`,
   `.unique(subset=None, keep="first", maintain_order=True)`, `empty_string_is_null=True`
   default all hold.
9. **Pre-existing red baseline: 4 failed / 129 passed** — all `test_notebook.py`
   FileNotFound (renamed notebook). Clean gate: `uv run pytest -q
   --ignore=tests/test_notebook.py` → expect all pass.
10. **Existing Russian `print()` in `run_report`** (app_service.py:254-257/425-429/440-444)
    is pre-existing and OUT OF SCOPE. Do not "fix" it. The new dedup log line must be
    English `logger.info`.
11. **`ReportResult` defaults load-bearing.** Add `rows_removed_duplicates: NonNegativeInt =
    0` and `rows_resulted: NonNegativeInt = 0` so existing construction sites
    (test_app.py:270-282/301-313/537-551) keep passing. Use `NonNegativeInt` for
    consistency with `rows_loaded`/`rows_exported`.
12. **`col_names` for dedup** = `db_worker.data_tbl_columns[:col_count]` where
    `col_count = len(csv_worker.source_headers)` — source columns only, cleaned, excluding
    added tag/extra columns (satisfies FR-1).
13. **README/docs are Russian-first.** Add new identifiers/rows; do not rewrite existing
    prose (minimal diff). `data-flow.md:154` states the OLD behavior and MUST be updated.

---

## 4. Task blocks (Implementor briefs)

### B1 — Settings model + template + deterministic file order (T1 + T2)

**Files.**
- `src/mko_data_cleaner/core/models.py` — `DataSettings`
- `src/mko_data_cleaner/settings/app_config.json5`
- `src/mko_data_cleaner/core/csv_service.py` — `CSVWorker._set_files_params`

**T1 changes.**
- `models.py`: add `deduplicate: bool = False` to `DataSettings` after `force` (:178). No
  `model_config`, no validator, no enum.
- `app_config.json5`: add `"deduplicate": false` after `"force": false` (:49). Preserve
  CRLF and absence of trailing newline (C9).
- Model + template MUST land together (R-1: Pydantic `extra="ignore"` silently drops an
  unknown key — adding to template without the model is a silent no-op).

**T2 changes.**
- `csv_service.py` `_set_files_params`: sort `self.data_files` after discovery (sorted by
  path) so "first occurrence" is deterministic (D27/FR-5). Do not modify
  `utils.list_files_in_directory` (shared helper).

**Acceptance.**
- `DataSettings(**config_without_key).deduplicate is False`.
- `DataSettings(**config_with_true).deduplicate is True`.
- Shipped template parses → `deduplicate is False`.
- Two files discovered in reverse creation order are processed in sorted path order.

**Out of scope:** anything in T3–T7.

---

### B2 — Global de-duplication primitive (T3)

**File.** `src/mko_data_cleaner/core/csv_service.py` — new method on `CSVWorker`.

**Change.**
```python
def load_deduplicated(self, col_names: list[str]) -> tuple[pl.DataFrame, int, int]:
    """Read all raw files, drop exact duplicate rows, return (df, rows_read, rows_removed)."""
```
Implementation:
- one `pl.scan_csv(self.data_files, new_columns=col_names, **self.reader_settings)`;
- `.unique(subset=None, keep="first", maintain_order=True)`;
- `.collect()`;
- `rows_removed = rows_read - df.height`.
- **Empty-frame guard**: header-only / zero-row input must not call `write_database` on an
  empty frame (D6). The method itself returns the empty frame + counts; the caller (B3)
  guards the write.

**Acceptance.**
- Two files sharing one row → `rows_removed == 1`.
- Duplicate separated by more than `CHUNK_SIZE` removed (D3).
- `null` ≠ `""`; case-sensitive (D11a–D11c).
- 1 000 000 rows de-duplicate < 1 s at primitive level (bounded; not a CI gate).

**Out of scope:** pipeline wiring, metrics, CLI (B3/B4).

---

### B3 — Pipeline integration + metrics (T4)

**Files.**
- `src/mko_data_cleaner/core/app_service.py` — `AppService`, `_import_data`, `run_report`,
  `get_app_service`
- `src/mko_data_cleaner/core/models.py` — `ReportResult`

**Changes.**
- `AppService.__init__(..., deduplicate: bool | None = None)` → `self._deduplicate`.
- `_resolved_deduplicate(self) -> bool` mirroring `_resolved_clear_export_folder` (:56-61):
  return `self._deduplicate` if not `None` else `self.app_config.deduplicate`.
- `_import_data(db_worker, csv_worker) -> tuple[int, int]`: when dedup active, call
  `csv_worker.load_deduplicated(col_names)` with
  `col_names = db_worker.data_tbl_columns[:len(csv_worker.source_headers)]`, guard empty
  frame (skip write), write the frame once via `df.write_database(engine="adbc")`, commit,
  then `db_worker.update_index_from_data()`; return `(rows_read, rows_removed)`. When
  inactive, keep the existing chunk loop and return `(rows_count, 0)`.
- `run_report`: `rows_loaded, rows_removed = self._import_data(...)`;
  `rows_resulted = rows_loaded - rows_removed`; pass both new fields to `ReportResult`.
- `get_app_service(..., deduplicate: bool | None = None)` threaded to `AppService`.
- `ReportResult`: add `rows_removed_duplicates: NonNegativeInt = 0` and
  `rows_resulted: NonNegativeInt = 0`.
- Logging: one `logger.info` line naming removed / loaded / resulted counts (English).

**Acceptance.**
- `rows_resulted == rows_loaded − rows_removed_duplicates` in every scenario (D18).
- `rows_loaded` == raw file row count when dedup on.
- dedup off → `rows_removed_duplicates == 0`, `rows_resulted == rows_loaded`.
- `set(result.timings.keys()) == {"import", "mapping", "export"}` preserved.
- `update_index_from_data` yields the same distinct table with/without dedup for exact
  duplicates.

**Watch-items:** #1, #2, #3, #4, #10, #11 above.

---

### B4 — CLI + public API override (T5)

**Files.**
- `src/mko_data_cleaner/app.py` — `run`, `process_data`, summary line
- `src/mko_data_cleaner/__init__.py` — unchanged

**Changes.**
- `run`: add `deduplicate: bool | None = typer.Option(None, "--deduplicate", help="Remove
  duplicate rows (identical across all columns) before cleaning.")` after `force`
  (:86-90); forward to `get_app_service(...)`. **Single flag, not dual** (watch-item #5).
- `process_data(..., deduplicate: bool | None = None)` with docstring; forward.
- Summary line: append ` · {rows_removed_duplicates:,} duplicate(s) removed` when dedup
  active (watch-item #6). Preserve existing substring.
- Update the monkeypatched lambda at `test_app.py:316-321` to accept `deduplicate=None`
  (watch-item #1).

**Acceptance.**
- `--deduplicate` present → on even when config `false`.
- flag absent + config `false` → off.
- flag absent + config `true` → on.
- `process_data(deduplicate=False)` overrides config `true`.
- `--no-deduplicate` rejected by Typer (exit 2) — documented, not a defect.

---

### B5 — Test suite (T6)

**Files.**
- `tests/conftest.py` — add `make_report` factory (files, rows, dict lines); do NOT modify
  `synthetic_report` (its 3/2 contract is a no-dedup regression guard).
- `tests/test_csv_service.py` — `load_deduplicated` unit tests (within file, across files,
  null/empty/case semantics, empty input).
- `tests/test_app.py` — end-to-end matrix D1–D18, CLI precedence, `process_data`
  precedence, raw-files-untouched (byte-compare), metrics invariant.
- No `pyproject.toml` marker changes; scale test is a plain test.

**Coverage matrix (D1–D18):** see spec §13. Implementor must cover every row.

**Acceptance.** All checks in spec §13 DoD (tests).

**Watch-items:** #1 (already fixed in B4), #6, #8, #9.

---

## 5. Non-regression / quality gates

- `uv run pytest -q` → no additional failures beyond the 4 pre-existing `test_notebook.py`.
- `uv run pytest -q --ignore=tests/test_notebook.py` → all pass.
- `uv run ruff check src/ tests/`
- `uv run ruff format --check src/ tests/`
- `uv run basedpyright src/`

---

## 6. Agent routing

- B1, B2, B3, B4: **Implementor** (sequential, one at a time; each commits its own block).
- B5: **Implementor** (tests) then **Validator** covers coverage.
- B6: **Doc-specialist** (Phase 4).
- Final: **Validator** (Phase 5).
- No Auditor/Researcher/Planner needed per block — spec + audit already cover the analysis.

---

## 7. Commit convention

Each block commits only its own files:
```
git add <specific-files>
git commit -m "{type}({scope}): {description}"
```
Never `git reset`/`git checkout`; never rewrite history.