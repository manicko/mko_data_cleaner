# Test Coverage Remediation — Execution Plan

- **Status:** Active (execution phase)
- **Created:** 2026-10-18 by Tech Lead (orchestrator)
- **Source spec:** `.ai/audit/tests/audit_report_01.md` (37 validated findings)
- **Code context:** `.ai/audit/tests/code_context_analysis.md` (auditor inspection)
- **Original plan:** `.ai/plans/04-test-coverage-remediation.md`
- **Current HEAD:** `a41a445` (26 commits ahead of audit baseline `f6ac8e1`)
- **Test gate:** `uv run pytest`
- **Lint:** `uv run ruff check src/ tests/`
- **Constraint:** Single implementor only, max 2 parallel subagents, no background agents

---

## Critical Corrections from Auditor (D1–D6)

| ID | Finding | Impact on Execution |
|---|---|---|
| **D2** | Plan's T-RW-01 analysis is wrong: `get_app_service(settings_dir)` is at `app.py:77`, **outside** the try/except block (lines 79-89). `SettingsNotInitializedError` propagates uncaught; `CliRunner` stores the original exception in `result.exception` with `exit_code=1`. | **Do NOT remove** `isinstance(result.exception, SettingsNotInitializedError)`. Instead: **add** `result.exit_code == 1` and check error message in `result.output`. Keep existing isinstance assertion. |
| **D3** | FINDING-33 reclassified from `[TEST-UPDATE]` to `[TEST-REWRITE]` in plan — correct, since test doesn't exist. | Proceed with T-RW-10 as a creation task in Phase 1. |
| **D5** | `SettingsNotInitializedError(FileNotFoundError)` — both `is_dir()` guard and settings check raise `FileNotFoundError` subtypes. | T-UP-07 (nonexistent dir test) can safely use `pytest.raises(FileNotFoundError, match="Directory not found")`. |
| **D6** | Audit counted 18 test_app.py tests; current tree has 24. Extra tests include `test_config_to_dict_*` and `test_process_data_error_*`. | No action needed — just context. |
| **T-UP-12** | Already covered by existing `test_pyproject_uses_spdx_license` (test_packaging.py:87-91). | **SKIP** T-UP-12 — finding already addressed, no new test needed. |
| **D1** | Line-number drift (audit baseline `f6ac8e1` vs current `a41a445`). | Implementors should grep for function names, not rely on line numbers. |

---

## Adjusted Task List (36 active tasks — T-UP-12 removed)

### Phase 0: Cleanup (9 tasks — all independent, zero production impact)

| Task ID | Action | File | Description |
|---|---|---|---|
| T-DEL-01 | Delete | `tests/conftest.py` | Remove `sample_mapping_df` fixture (unused, wrong column layout) |
| T-DEL-02 | Delete | `tests/conftest.py` | Remove `tmp_db_path` fixture (unused) |
| T-DEL-03 | Delete | `tests/conftest.py` | Remove `db_worker_memory` fixture (unused, supersedes FINDING-05) |
| T-DEL-04 | Delete | `tests/test_app.py` | Remove `test_null_data_files_excludes_stale` (tautological) |
| T-DEL-05 | Delete | `tests/test_app.py` | Remove `test_init_project_with_settings_dir` (duplicates test_initialize_settings_threads_settings_dir) |
| T-DEL-06 | Delete | `tests/test_app.py` | Remove `test_init_project_none_uses_app_paths` (duplicates CLI/python path tests) |
| T-DEL-07 | Delete | `tests/test_app.py` | Remove `test_top_level_no_import_side_effects` (duplicates test_import_app_does_not_create_settings_dir) |
| T-DEL-08 | Verify | `tests/test_dict_service.py` | Confirm `test_build_match_query_single` absent — no-op |
| T-DEL-09 | Verify | `tests/test_packaging.py` | Confirm `test_pyproject_validates_successfully` absent — no-op |

### Phase 1: High-Impact Rewrites (11 tasks — depends on Phase 0)

| Task ID | Action | File | Description | Dependencies |
|---|---|---|---|---|
| T-RW-01 | Rewrite | `tests/test_app.py` | Add `exit_code == 1` + output check to `test_run_cli_without_settings_raises_settings_error`. **Keep** `isinstance(result.exception, SettingsNotInitializedError)` — D2 corrected. | Phase 0 |
| T-RW-02 | Rewrite | `tests/test_app.py` | Replace trivially-True `is not None` with concrete config values in `test_get_app_service_constructs_after_initialization` | Phase 0 |
| T-RW-03 | Rewrite | `tests/test_app.py` | Replace bare `except Exception: pass` in `test_process_data_past_gate_does_not_raise_settings_error` with `synthetic_report` + result assertions | Phase 0 |
| T-RW-04 | Rewrite | `tests/test_app.py` | Replace `SimpleNamespace` mock with `MagicMock` + `assert_called_once_with` in `test_process_data_returns_report_result` | Phase 0 |
| T-RW-05 | Rewrite | `tests/test_app.py` | Add CSV content read-back to `test_process_data_end_to_end` (Brand2 absent, Brand1 present, added_by_brand tag) | Phase 0 |
| T-FIX-01 | Rewrite | `tests/test_dict_service.py` | Replace vacuous `is_in().all()` with specific expected column-name set in `test_mapping_build` | Phase 0 |
| T-RW-06 | Create | `tests/test_dict_service.py` | Create `test_build_search_like_pattern_ends_with` (asserts `%APPLE`, leading `%` only) | Phase 0, T-UP-01 |
| T-RW-07 | Create | `tests/test_dict_service.py` | Create `test_build_search_like_pattern_starts_with` (asserts `APPLE%`, trailing `%` only) | Phase 0 |
| T-RW-08 | Create | `tests/test_dict_service.py` | Create `test_build_search_like_pattern_contains` (3 cases: FULL=term, PARTIAL=%term%, STARTS_WITH=term%) | Phase 0 |
| T-RW-09 | Create | `tests/test_csv_service.py` | Create `test_export_sql_to_csv_gzip` (gzip format + content verification) | Phase 0 |
| T-RW-10 | Create | `tests/test_csv_service.py` | Create `test_import_csv_no_delimiter_raises` (asserts `WrongDataSettings`, not `RuntimeError`) | Phase 0 |

### Phase 2: Missing Coverage (11 tasks — T-UP-10 prod change can run first)

| Task ID | Action | File | Description | Dependencies |
|---|---|---|---|---|
| T-UP-10 | **Production** | `src/mko_data_cleaner/core/models.py` | Change `rows_loaded`/`rows_exported` from `int` to `NonNegativeInt` in `ReportResult` | None (doesn't break existing tests) |
| T-UP-01 | Update | `tests/conftest.py` | Add ENDS_WITH (`e`) row to `sample_dictionary` fixture | None |
| T-UP-04 | Update | `tests/test_dict_service.py` | Add `assert "%КОЛА" in patterns` to `test_pattern_generation` | T-UP-01 |
| T-UP-02 | Create | `tests/test_dict_service.py` | Create `test_generate_rules_blocks_ordering` (DELETE → REPLACE → ADD) | None |
| T-UP-03 | Create | `tests/test_dict_service.py` | Create `test_build_match_query_format` (exact `col:"(value)"` string) | None |
| T-UP-05 | Update | `tests/test_csv_service.py` | Extend `test_get_files_suffix` with all missing branches | None |
| T-UP-06 | Update | `tests/test_csv_service.py` | **Merged into T-UP-05** — all assertions use explicit `assert` | T-UP-05 |
| T-UP-07 | Create | `tests/test_app.py` | Create `test_process_data_nonexistent_directory_raises_filenotfound` | None |
| T-UP-08 | Create | `tests/test_app.py` | Create `test_run_cli_success` (exit_code==0, "Report complete", "3 loaded → 2 clean") | None |
| T-UP-09 | Create | `tests/test_app.py` | Create `test_report_result_rejects_negative_row_counts` (pytest.raises ValidationError) | T-UP-10 |
| T-UP-11 | Update | `tests/test_metadata.py` | Add version cross-check against `pyproject.toml` via `tomllib` | None |
| ~~T-UP-12~~ | **SKIP** | — | Already covered by `test_pyproject_uses_spdx_license` | — |

### Phase 3: New Tests & Production Fix (3 tasks — depends on Phase 0 + Phase 2)

| Task ID | Action | File | Description | Dependencies |
|---|---|---|---|---|
| BP-03 | **Production** | `src/mko_data_cleaner/core/db_service.py` | Add `self.__dict__.pop("column_index", None)` to `set_data_tbl_columns`; add regression test | Phase 0 |
| T-WR-01 | Create | `tests/test_db_service.py` | Create `test_apply_add_concatenates_tags` (56 lines of `_apply_add` covered) | Phase 0, T-UP-01 |

### Phase 4: Documentation Fixes (2 tasks — independent)

| Task ID | Action | File | Description | Dependencies |
|---|---|---|---|---|
| DOC-01 | Fix | `tests/conftest.py` | Update `synthetic_report` docstring: lines 112-160→112-168, remove deleted test ref | T-DEL-04 |
| DOC-02 | Fix | `tests/test_app.py` | Fix docstring "10 fields" → "11 fields" in `test_report_result_model_construction_and_serialization` | None |

---

## Execution Sequence (single implementor, sequential)

```
Step 1: Phase 0 (conftest.py + test_app.py deletions) → commit
Step 2: T-UP-10 (models.py NonNegativeInt) → commit
Step 3: Phase 2 coverage (conftest.py + test_dict_service.py + test_csv_service.py + test_app.py + test_metadata.py) → commit
Step 4: Phase 1 rewrites (test_app.py + test_dict_service.py + test_csv_service.py) → commit
Step 5: Phase 3 (db_service.py fix + test_db_service.py + models.py already done) → commit
Step 6: Phase 4 docs (conftest.py + test_app.py docstrings) → commit
Step 7: Final validation (Validator)
```

**Rationale for T-UP-10 before Phase 2 coverage:** It's a production change that doesn't break existing tests. Doing it first unblocks T-UP-09 (negative validation test) which is in Phase 2.

**Rationale for Phase 2 before Phase 1:** T-UP-01 (ENDS_WITH fixture) is needed by T-RW-06 and T-UP-04. T-UP-09 (Phase 2) depends on T-UP-10 (Phase 2). Phase 1 rewrites can run after Phase 2 fixtures are ready, since T-RW-06 explicitly depends on T-UP-01.

---

## Verification Steps

### After each phase:
1. `uv run pytest` — verify all tests pass
2. `uv run ruff check src/ tests/` — verify no lint errors
3. If production code changed: `uv run basedpyright src/mko_data_cleaner/core/<module>.py` — verify no type errors

### Environment note:
This is a standalone uv project. Tests run via `uv run pytest` with SQLite in `tmp_path`
fixtures — no Docker, no PostgreSQL, no external services required.
