# Test Coverage Remediation Plan

- **Status:** Draft (planning phase — read-only; no code changes in this task)
- **Created:** 2026-10-18 by Dependency-Aware Planner
- **Source spec:** `.ai/audit/tests/audit_report_01.md` (37 validated findings, audit baseline HEAD `f6ac8e1`)
- **Current HEAD:** `a41a445` (repository state may have evolved since audit baseline — see §Appendix: Findings Referencing Tests Not in Current Tree)
- **Test gate:** `uv run pytest`
- **Scope:** `tests/` directory — 9 test modules, 7 test files

---

## Objective

Sequence the 37 audit findings (classified into `[TEST-DELETE]`, `[TEST-REWRITE]`, `[TEST-UPDATE]`, `[TEST-WRITE]`, `[BEST-PRACTICE]`, `[DOC-UPDATE]`) into a dependency-safe, phased execution plan of atomic, reviewable work units. The plan must preserve `pytest` green status after each phase and isolate production-code changes into their own reviewable tasks with regression tests.

---

## Prerequisites

| # | Prerequisite | Rationale |
|---|---|---|
| P1 | Verify current git HEAD and test baseline | Audit baseline is `f6ac8e1`; repo is at `a41a445`. Some referenced tests may not exist in the current tree — see Appendix. |
| P2 | Run baseline test gate | `uv run pytest` must pass before any changes. Captures post-deletion regression baseline. |
| P3 | Run baseline lint | `uv run ruff check src/ tests/` must be clean — ensures test deletions/rewrites don't introduce lint errors. |
| P4 | Confirm single-implementor constraint | Only one implementor agent at a time (per `.kilo/rules/commands.md`); max 2 parallel subagents. Sequence accordingly. |
| P5 | Review `ReportResult` consumers | Before changing `int` → `NonNegativeInt`, confirm all construction sites pass non-negative values (app_service.py:280-292, test_app.py:257-280, test_app.py:286-312). |
| P6 | Review `column_index` usage | Before fixing the `cached_property` invalidation, confirm `get_col_names()` consumers in production (db_service.py:130-131) — currently no production caller exists; only `test_column_index_and_get_col_names` exercises it. |

---

## Findings Cross-Reference (37 total)

| Task ID | Phase | Finding | Classification | Test File | Production File | Effort | Tests Exist? |
|---|---|---|---|---|---|---|---|
| T-DEL-01 | P0 | FINDING-01 | TEST-DELETE | conftest.py:82-96 | — | trivial | ✅ Yes |
| T-DEL-02 | P0 | FINDING-03 | TEST-DELETE | conftest.py:99-105 | — | trivial | ✅ Yes |
| T-DEL-03 | P0 | FINDING-04 | TEST-DELETE | conftest.py:108-119 | — | trivial | ✅ Yes |
| T-DEL-04 | P0 | FINDING-13 | TEST-DELETE | test_app.py:452-463 | — | trivial | ✅ Yes |
| T-DEL-05 | P0 | FINDING-14 | TEST-DELETE | test_app.py:343-349 | — | trivial | ✅ Yes |
| T-DEL-06 | P0 | FINDING-15 | TEST-DELETE | test_app.py:372-378 | — | trivial | ✅ Yes |
| T-DEL-07 | P0 | FINDING-16 | TEST-DELETE | test_app.py:406-425 | — | trivial | ✅ Yes |
| T-DEL-08 | P0 | FINDING-27 | TEST-DELETE | test_dict_service.py | — | trivial | ❌ No — verify |
| T-DEL-09 | P0 | FINDING-37 | TEST-DELETE | test_packaging.py | — | trivial | ❌ No — verify |
| T-RW-01 | P1 | FINDING-08 | TEST-REWRITE | test_app.py:166-174 | app.py:55-89 | small | ✅ Yes |
| T-RW-02 | P1 | FINDING-09 | TEST-REWRITE | test_app.py:219-223 | app_service.py:36-52 | small | ✅ Yes |
| T-RW-03 | P1 | FINDING-10 | TEST-REWRITE | test_app.py:226-236 | app.py:126-145 | small | ✅ Yes |
| T-RW-04 | P1 | FINDING-11 | TEST-REWRITE | test_app.py:286-312 | app.py:126-145 | small | ✅ Yes |
| T-RW-05 | P1 | FINDING-12 | TEST-REWRITE | test_app.py:431-449 | db_service.py:647-659 | medium | ✅ Yes |
| T-RW-06 | P1 | FINDING-28 | TEST-REWRITE | test_dict_service.py | dict_service.py:226-227 | trivial | ❌ No — create |
| T-RW-07 | P1 | FINDING-29 | TEST-REWRITE | test_dict_service.py | dict_service.py:224-225 | trivial | ❌ No — create |
| T-RW-08 | P1 | FINDING-30 | TEST-REWRITE | test_dict_service.py | dict_service.py:222-223 | trivial | ❌ No — create |
| T-RW-09 | P1 | FINDING-32 | TEST-REWRITE | test_csv_service.py | csv_service.py:269-381 | small | ❌ No — create |
| T-RW-10 | P1 | FINDING-33 | TEST-REWRITE | test_csv_service.py | csv_service.py:71-75 | small | ❌ No — create |
| T-FIX-01 | P1 | FINDING-26 | TEST-REWRITE | test_dict_service.py:42 | dict_service.py:119-142 | trivial | ✅ Yes (vacuous assert) |
| T-UP-01 | P2 | FINDING-06 | TEST-UPDATE | conftest.py:46-69 | dict_service.py:226-227 | small | N/A (fixture) |
| T-UP-02 | P2 | FINDING-24 | TEST-UPDATE | test_dict_service.py | dict_service.py:79-94 | small | N/A (new test) |
| T-UP-03 | P2 | FINDING-25 | TEST-UPDATE | test_dict_service.py | dict_service.py:264-279 | small | N/A (new test) |
| T-UP-04 | P2 | FINDING-23 | TEST-UPDATE | test_dict_service.py | dict_service.py:226-227 | small | N/A (new test) |
| T-UP-05 | P2 | FINDING-31 | TEST-UPDATE | test_csv_service.py:8-11 | csv_service.py:242-263 | trivial | ✅ Yes |
| T-UP-06 | P2 | FINDING-34 | TEST-UPDATE | test_csv_service.py | csv_service.py:242-263 | trivial | ✅ Yes |
| T-UP-07 | P2 | FINDING-17 | TEST-UPDATE | test_app.py | app.py:142-144 | trivial | N/A (new test) |
| T-UP-08 | P2 | FINDING-18 | TEST-UPDATE | test_app.py | app.py:55-89 | small | N/A (new test) |
| T-UP-09 | P2 | FINDING-19 | TEST-UPDATE | test_app.py | models.py:182-207 | trivial | N/A (new test) |
| T-UP-10 | P2 | FINDING-19 | TEST-UPDATE | models.py:206-207 | models.py:182-207 | trivial | N/A (prod change) |
| T-UP-11 | P2 | FINDING-35 | TEST-UPDATE | test_metadata.py:10-13 | __init__.py:10-15 | small | ✅ Yes |
| T-UP-12 | P2 | FINDING-36 | TEST-UPDATE | test_packaging.py | pyproject.toml:6,15 | small | ✅ Partial (existing) |
| T-WR-01 | P3 | FINDING-21 | TEST-WRITE | test_db_service.py | db_service.py:690-745 | medium | N/A (new test) |
| BP-01 | P0* | FINDING-02 | BEST-PRACTICE | conftest.py:82-96 | — | superseded | — |
| BP-02 | P0* | FINDING-05 | BEST-PRACTICE | conftest.py:108-119 | — | superseded | — |
| BP-03 | P3 | FINDING-22 | BEST-PRACTICE | test_db_service.py | db_service.py:124-139 | trivial | N/A (prod fix) |
| DOC-01 | P4 | FINDING-07 | DOC-UPDATE | conftest.py:122-135 | smoke_install.py | trivial | N/A |
| DOC-02 | P4 | FINDING-20 | DOC-UPDATE | test_app.py:257 | models.py:182-207 | trivial | N/A |

> *BP-01 and BP-02 are superseded by T-DEL-01 and T-DEL-03 respectively — no separate action needed. They are listed for traceability.

---

## Work Breakdown Structure (WBS)

### Phase 0: Cleanup [~2-3h]

**Purpose:** Remove dead fixtures and redundant tests. Zero production impact. Reduces test suite noise and CI runtime. All 9 tasks are independent and can execute in parallel (subject to the single-implementor constraint).

#### T-DEL-01 — Delete `sample_mapping_df` fixture (FINDING-01)

**Description:** Remove the `sample_mapping_df` fixture from `tests/conftest.py:82-96`. The fixture has zero consumers — `grep` across `tests/` returns only the definition itself. Its docstring already states "Reserved for future integration tests of `group_by_cols`" but `group_by_cols` is already exercised by `test_group_by_cols` (inline DataFrame) and `test_build_non_mapped_with_extra_cols` (uses `extra_cols` fixture).

**Goals:**
- Remove 16 lines (fixture + docstring) from conftest.py
- Zero test coverage loss (fixture was unused)
- No import or reference remains

**Files:** `tests/conftest.py` — remove `sample_mapping_df` function (lines 82-96)

**Acceptance Criteria:**
- `grep sample_mapping_df tests/` returns no matches
- `pytest` passes with same count (minus any tests that somehow imported it — none found)
- `ruff check tests/conftest.py` clean

#### T-DEL-02 — Delete `tmp_db_path` fixture (FINDING-03)

**Description:** Remove the `tmp_db_path` fixture from `tests/conftest.py:99-105`. Zero consumers — `db_worker` fixture already provides a `tmp_path`-backed `DBWorker`.

**Files:** `tests/conftest.py` — remove `tmp_db_path` function (lines 99-105)

**Acceptance Criteria:**
- `grep tmp_db_path tests/` returns no matches
- `pytest` passes

#### T-DEL-03 — Delete `db_worker_memory` fixture (FINDING-04)

**Description:** Remove the `db_worker_memory` fixture from `tests/conftest.py:108-119`. Zero consumers — `db_worker` fixture provides equivalent coverage with realistic config. This also supersedes BEST-PRACTICE finding #05 (`db_worker_memory` hardcodes values).

**Files:** `tests/conftest.py` — remove `db_worker_memory` function (lines 108-119)

**Acceptance Criteria:**
- `grep db_worker_memory tests/` returns no matches
- `pytest` passes

#### T-DEL-04 — Delete `test_null_data_files_excludes_stale` (FINDING-13)

**Description:** Remove `test_null_data_files_excludes_stale` from `tests/test_app.py:452-463`. The test is tautological: it pre-seeds a stale file and asserts `stale_file not in result.null_data_files`, but `null_data_files` is populated from a freshly-created list (app_service.py:246-254) that never contained the stale file. The assertion is always True.

**Files:** `tests/test_app.py` — remove `test_null_data_files_excludes_stale` function (lines 452-463)

**Acceptance Criteria:**
- `grep test_null_data_files_excludes_stale tests/` returns no matches
- `pytest` passes

#### T-DEL-05 — Delete `test_init_project_with_settings_dir` (FINDING-14)

**Description:** Remove `test_init_project_with_settings_dir` from `tests/test_app.py:343-349`. It duplicates `test_initialize_settings_threads_settings_dir` (lines 318-324) with identical assertions. `initialize_settings` (app.py:148-150) is a one-line delegation to `init_project`.

**Files:** `tests/test_app.py` — remove `test_init_project_with_settings_dir` function (lines 343-349)

**Acceptance Criteria:**
- Test removed; `test_initialize_settings_threads_settings_dir` still covers `init_project` path
- `pytest` passes

#### T-DEL-06 — Delete `test_init_project_none_uses_app_paths` (FINDING-15)

**Description:** Remove `test_init_project_none_uses_app_paths` from `tests/test_app.py:372-378`. It duplicates `test_init_cli_creates_settings_in_tmp_dir` (lines 203-213) — same monkeypatch, same assertions, same code path. CLI path is also covered by `test_init_cli_with_settings_dir` (lines 355-366).

**Files:** `tests/test_app.py` — remove `test_init_project_none_uses_app_paths` function (lines 372-378)

**Acceptance Criteria:**
- Test removed; `test_init_cli_creates_settings_in_tmp_dir` and `test_init_cli_with_settings_dir` cover both paths
- `pytest` passes

#### T-DEL-07 — Delete `test_top_level_no_import_side_effects` (FINDING-16)

**Description:** Remove `test_top_level_no_import_side_effects` from `tests/test_app.py:406-425`. It duplicates `test_import_app_does_not_create_settings_dir` (lines 81-101) — both run a subprocess that monkeypatches `platformdirs.user_config_dir`, import the package, and assert no `settings/` dir is created. The top-level import delegates to `app.py` (line 5-8 in `__init__.py`), so `__init__.py` import is fully covered by the `app.py` test.

**Files:** `tests/test_app.py` — remove `test_top_level_no_import_side_effects` function (lines 406-425)

**Acceptance Criteria:**
- Test removed; `test_import_app_does_not_create_settings_dir` and `test_import_survives_stdout_without_reconfigure` cover the import-time side-effect gate
- `pytest` passes

#### T-DEL-08 — Verify/delete `test_build_match_query_single` (FINDING-27)

**Description:** The audit references `test_build_match_query_single` as a redundant duplicate of `test_build_match_query_multiple`. **This test does NOT exist in the current tree** (HEAD `a41a445`). Verify with `grep` and skip if absent. If found (e.g., on a different branch), delete it.

**Files:** `tests/test_dict_service.py`

**Acceptance Criteria:**
- `grep test_build_match_query_single tests/` confirms absence
- If present, removed; `pytest` passes

#### T-DEL-09 — Verify/delete `test_pyproject_validates_successfully` (FINDING-37)

**Description:** The audit references `test_pyproject_validates_successfully` as a duplicate of `test_metadata_contains_required_fields`. **This test does NOT exist in the current tree** (HEAD `a41a445`). The existing metadata tests in `test_packaging.py` are `test_pyproject_no_stale_license_classifier`, `test_pyproject_no_stale_project_name`, `test_pyproject_uses_spdx_license`, and `test_pyproject_declares_package_data`. Verify absence and skip.

**Files:** `tests/test_packaging.py`

**Acceptance Criteria:**
- `grep test_pyproject_validates_successfully tests/` confirms absence
- No action needed if absent; `pytest` passes

---

### Phase 1: High-Impact Rewrites [4-6h]

**Purpose:** Strengthen weak assertions and fix tautological/circular tests that mask operational errors. All 10 rewrite tasks depend on Phase 0 completing (cleaned test suite). Some pattern tests (T-RW-06, T-RW-07, T-RW-08) also depend on T-UP-01 (ENDS_WITH fixture row) from Phase 2 — this dependency is flagged below.

#### T-RW-01 — Rewrite `test_run_cli_without_settings_raises_settings_error` (FINDING-08)

**Description:** The current test asserts `isinstance(result.exception, SettingsNotInitializedError)` but never checks `result.exit_code`. In Typer/Click, the `run` command (app.py:79-89) wraps `service.run_report` in `try/except Exception` and calls `raise typer.Exit(1)` on any error. This means `SettingsNotInitializedError` is caught by the broad `except` at line 87, printed as a red error, and converted to `typer.Exit(1)` — the exception does NOT propagate to `result.exception` as the original type. Instead, `result.exit_code` is 1 and `result.exception` is a `SystemExit`.

**Rewrite to:**
- Assert `result.exit_code == 1`
- Assert the error message contains `"settings"` or `"SettingsNotInitializedError"` in `result.output` (matching the `console.print` at app.py:88)
- Remove the `isinstance(result.exception, ...)` assertion or adjust it to account for `typer.Exit(1)` wrapping

**Files:** `tests/test_app.py` — `test_run_cli_without_settings_raises_settings_error` (lines 166-174)

**Acceptance Criteria:**
- `result.exit_code == 1`
- Error message visible in `result.output`
- Test fails against pre-fix code if `exit_code` is not checked
- `pytest tests/test_app.py::test_run_cli_without_settings_raises_settings_error -v` passes

#### T-RW-02 — Rewrite `test_get_app_service_constructs_after_initialization` (FINDING-09)

**Description:** The current test asserts three trivially-True conditions: `service.app_config is not None`, `service.log_config is not None`, and `service.app_config.data_paths.import_folder is not None`. Since `get_app_service` already calls `_require_settings` (app_service.py:337), the properties are guaranteed to be valid model instances upon test entry. `import_folder` has a default of `Path("raw_data")` (models.py:64) — always truthy.

**Rewrite to:**
- Assert concrete config values parsed from the shipped template: `service.app_config.data_file_settings.index_column == "AdId"`, `service.app_config.database_settings.table_name == "data_table"`, `service.app_config.data_file_settings.extension == DataFileExtension.gz`
- Assert `service.log_config.version == 1`
- Assert `service.app_config.data_paths.import_folder == Path("raw_data")` and `export_folder == Path("clean_data")`

**Files:** `tests/test_app.py` — `test_get_app_service_constructs_after_initialization` (lines 219-223)

**Acceptance Criteria:**
- No `is not None` assertions remain
- All assertions check concrete values against the shipped `app_config.json5` template
- Test passes with `populated_settings` fixture

#### T-RW-03 — Rewrite `test_process_data_past_gate_does_not_raise_settings_error` (FINDING-10)

**Description:** The current test wraps `process_data` in `try/except SettingsNotInitializedError: pytest.fail()`, then catches ALL other exceptions with `except Exception: pass`. This masks operational errors (database failures, config bugs, assertion errors) behind a passing test. The empty report dir (`empty_report_dir`) is a valid directory, so `process_data` proceeds to `get_app_service` → `run_report` → `CSVWorker.__init__` → `_set_files_params()` which raises `WrongDataSettings` (no data files in `raw_data/`), swallowed by the bare `except`.

**Rewrite to** use `synthetic_report` fixture:
- Call `process_data(synthetic_report, settings_dir=populated_settings)`
- Assert the result is a `ReportResult` with `rows_loaded >= 0`, `rows_exported >= 0`
- Assert `result.duration > 0.0` and `set(result.timings.keys()) == {"import", "mapping", "export"}`
- If the empty-directory scenario is still desired, assert the specific expected exception: `pytest.raises(WrongDataSettings)`

**Files:** `tests/test_app.py` — `test_process_data_past_gate_does_not_raise_settings_error` (lines 226-236)

**Acceptance Criteria:**
- No `except Exception: pass` pattern remains
- Test asserts specific expected behavior, not just "no SettingsNotInitializedError"
- `pytest tests/test_app.py::test_process_data_past_gate_does_not_raise_settings_error -v` passes

#### T-RW-04 — Rewrite `test_process_data_returns_report_result` (FINDING-11)

**Description:** The current test mocks `get_app_service` to return a `SimpleNamespace(run_report=lambda path: fake_result)` and then asserts `isinstance(result, ReportResult)`, `result.rows_loaded == 42`, `result.report_dir == empty_report_dir`. These are circular — the mock returns exactly what was just constructed. The `is_dir()` check and delegation logic in `process_data` (app.py:142-145) are never exercised.

**Rewrite to** use a MagicMock spy:
- Replace `SimpleNamespace` with `MagicMock()` for the fake service
- Set `fake_service.run_report.return_value = fake_result`
- After calling `process_data`, assert `fake_service.run_report.assert_called_once_with(empty_report_dir)`
- Keep the `isinstance(result, ReportResult)` and `rows_loaded`/`rows_exported` assertions as secondary checks
- Verify `report_dir` argument matches the `path` passed to `process_data`

**Files:** `tests/test_app.py` — `test_process_data_returns_report_result` (lines 286-312)

**Acceptance Criteria:**
- `run_report` is asserted to be called with the correct `Path` argument
- Mock is a `MagicMock`, not `SimpleNamespace(lambda: ...)`
- Test fails if `process_data` passes a wrong path to `run_report`
- `pytest tests/test_app.py::test_process_data_returns_report_result -v` passes

#### T-RW-05 — Rewrite `test_process_data_end_to_end` (FINDING-12)

**Description:** The current test asserts `rows_loaded == 3` and `rows_exported == 2` with a comment `# Brand2 row deleted` — but never verifies that `Brand2` was the row actually deleted. A bug in `_apply_delete` (db_service.py:647-659) that deletes by rowid instead of by match would still satisfy the count assertion. The ADD rule (`added_by_brand` tag) is also unverified.

**Rewrite to** inspect exported CSV content:
- After `process_data` completes, read the exported `.csv.gz` files from `result.export_dir` using `pl.read_csv`
- Assert `"Brand2"` is **absent** from any cell in the exported data
- Assert `"Brand1"` is **present** in at least one row
- Assert the `added_by_brand` column (or tag value) is present and populated for Brand1 rows (the ADD rule at dict line 4: `"a;f;4;NULL;Brand1;added_by_brand"`)
- Keep existing assertions (paths, timings, durations)

**Files:** `tests/test_app.py` — `test_process_data_end_to_end` (lines 431-449)

**Acceptance Criteria:**
- Exported CSV content is read back and verified for specific row values
- DELETE rule verified by absence of Brand2, not just count
- ADD rule verified by presence of added_by_brand tag
- E2E test still passes with `synthetic_report` + `populated_settings` fixtures

#### T-RW-06 — Create `test_build_search_like_pattern_ends_with` (FINDING-28)

**Description:** The audit references `test_build_search_like_pattern_ends_with` which asserted the wrong pattern for ENDS_WITH. **This test does NOT exist in the current tree.** Create it with the correct assertion.

The production code `_build_search_like_pattern` (dict_service.py:208-229) produces `%term` for `MatchType.ENDS_WITH` (leading `%` only, no trailing `%`):
```python
.when(pl.col(match_type_col) == MatchType.ENDS_WITH)
.then(pl.concat_str([pl.lit("%"), pl.col(term_col)]))
```

**Create** `test_build_search_like_pattern_ends_with` following the pattern of `test_like_pattern_full` and `test_like_pattern_partial`:
```python
def test_build_search_like_pattern_ends_with():
    expr = MappingDict._build_search_like_pattern("match", "term")
    df = pl.DataFrame({"match": [MatchType.ENDS_WITH], "term": ["APPLE"]})
    result = df.with_columns(expr.alias("pattern"))
    assert result["pattern"][0] == "%APPLE"  # leading %, NO trailing %
```

**Dependency:** Requires T-UP-01 (ENDS_WITH row in `sample_dictionary`) for integration-style assertions. Can be created as a standalone unit test without it, but for consistency with `test_pattern_generation`, T-UP-01 should precede.

**Files:** `tests/test_dict_service.py` — new test function

**Acceptance Criteria:**
- Asserts `result["pattern"][0] == "%APPLE"` (leading `%` only)
- Does NOT assert `"%APPLE%"` (with trailing `%`)
- `pytest tests/test_dict_service.py::test_build_search_like_pattern_ends_with -v` passes

#### T-RW-07 — Create `test_build_search_like_pattern_starts_with` (FINDING-29)

**Description:** The audit references `test_build_search_like_pattern_starts_with` which asserted the wrong pattern. **Does NOT exist in current tree.** Create with correct assertion.

Production code produces `term%` for `MatchType.STARTS_WITH` (term first, trailing `%` only):
```python
.when(pl.col(match_type_col) == MatchType.STARTS_WITH)
.then(pl.concat_str([pl.col(term_col), pl.lit("%")]))
```

**Create:**
```python
def test_build_search_like_pattern_starts_with():
    expr = MappingDict._build_search_like_pattern("match", "term")
    df = pl.DataFrame({"match": [MatchType.STARTS_WITH], "term": ["APPLE"]})
    result = df.with_columns(expr.alias("pattern"))
    assert result["pattern"][0] == "APPLE%"  # trailing %, NO leading %
```

**Files:** `tests/test_dict_service.py` — new test function

**Acceptance Criteria:**
- Asserts `result["pattern"][0] == "APPLE%"` (trailing `%` only)
- `pytest tests/test_dict_service.py::test_build_search_like_pattern_starts_with -v` passes

#### T-RW-08 — Create `test_build_search_like_pattern_contains` (FINDING-30 + FINDING-34)

**Description:** The audit references `test_build_search_like_pattern_contains` with wrong assertions and missing `assert` on 3 cases. **Does NOT exist in current tree.** Create with correct assertions for all 3 cases (FULL_MATCH, PARTIAL_MATCH/STARTS_WITH) and explicit `assert` on each.

Production code produces:
- `FULL_MATCH` → `term` (no `%`)
- `PARTIAL_MATCH` → `%term%` (both `%`)
- `STARTS_WITH` → `term%` (trailing `%` only)

**Create** with all 3 cases explicitly asserted:
```python
def test_build_search_like_pattern_contains():
    expr = MappingDict._build_search_like_pattern("match", "term")
    df = pl.DataFrame({
        "match": [MatchType.FULL_MATCH, MatchType.PARTIAL_MATCH, MatchType.STARTS_WITH],
        "term": ["APPLE", "APP", "APPLE"],
    })
    result = df.with_columns(expr.alias("pattern"))
    patterns = result["pattern"].to_list()
    assert patterns[0] == "APPLE"        # FULL: no %
    assert patterns[1] == "%APP%"        # PARTIAL: both %
    assert patterns[2] == "APPLE%"       # STARTS_WITH: trailing % only
```

**Dependency:** T-UP-01 (ENDS_WITH fixture row) is NOT required for this standalone test (it uses inline DataFrame, not `sample_dictionary`).

**Files:** `tests/test_dict_service.py` — new test function

**Acceptance Criteria:**
- All 3 match types tested with explicit `assert`
- No bare expression calls (FINDING-34 satisfied)
- Pattern assertions are correct per production code
- `pytest tests/test_dict_service.py::test_build_search_like_pattern_contains -v` passes

#### T-RW-09 — Create `test_export_sql_to_csv_gzip` (FINDING-32)

**Description:** The audit references `test_export_sql_to_csv_gzip` which asserted only row count + file existence. **Does NOT exist in current tree.** The existing `test_export_sql_to_csv_roundtrip_content` (test_csv_service.py:155-206) already verifies CSV content read-back, but it does not specifically test **gzip compression** integrity (binary `.gz` decompression + content verification).

**Create** `test_export_sql_to_csv_gzip`:
- Create a SQLite table with 3 rows
- Export with `compression: "gzip"` and `chunk_size: 1`
- Read back exported `.csv.gz` files using `pl.read_csv(f, separator=";")` (polars auto-decompresses `.gz`)
- Assert: column headers correct, 3 rows total, specific cell values preserved (including null handling)
- Assert exported files exist and have `.csv.gz` extension

**Note:** This overlaps with `test_export_sql_to_csv_roundtrip_content`. To avoid redundancy, either (a) delete the roundtrip test if it adequately covers gzip, or (b) make this test specifically assert gzip decompression success (e.g., verify `gzip.open(f, 'rt')` succeeds, or `f.endswith('.csv.gz')`).

**Files:** `tests/test_csv_service.py` — new test function

**Acceptance Criteria:**
- Exported `.gz` files are read back with correct content
- Compression format verified (`.csv.gz` extension, valid gzip binary)
- Cell values and null handling verified
- `pytest tests/test_csv_service.py::test_export_sql_to_csv_gzip -v` passes

#### T-RW-10 — Create `test_import_csv_no_delimiter_raises` (FINDING-33)

**Description:** The audit references `test_import_csv_no_delimiter_raises` which used generic `RuntimeError`. **Does NOT exist in current tree.** Production code at `csv_service.py:71-75` raises `WrongDataSettings` when no files with the specified extension are found in the data directory.

**Create** `test_import_csv_no_delimiter_raises`:
- Create a `CSVWorker` with a data path containing no matching files (or a directory with files of wrong extension)
- Assert `pytest.raises(WrongDataSettings)` (NOT `RuntimeError`)
- Import `WrongDataSettings` from `mko_data_cleaner.core.errors`
- Assert the error message contains `"No files with extension"` (matching csv_service.py:72)

**Files:** `tests/test_csv_service.py` — new test function

**Acceptance Criteria:**
- Uses `WrongDataSettings` exception type, not `RuntimeError`
- Triggers the no-files-found path in `_set_files_params`
- `pytest tests/test_csv_service.py::test_import_csv_no_delimiter_raises -v` passes

#### T-FIX-01 — Fix vacuous `is_in()` assertion in `test_mapping_build` (FINDING-26)

**Description:** At `test_dict_service.py:42`, the assertion `df[MappingColumns.column_name].is_in(main_columns).all()` is vacuously True — `build_mapping` (dict_service.py:119-142) sets `data_col_index` from `self._table_columns` (which equals `main_columns`), so `_build_query` joins on the search index and resolves `column_name` from `main_columns` by construction. The assertion can never fail regardless of bugs in filtering, joining, or column resolution.

**Rewrite to:**
- Replace with a specific assertion on expected column names
- Given `sample_dictionary` search indices 1 → "researchDate"... wait, let me check: `main_columns = ["adId", "researchDate", "programName", "channelName", "brand"]` and search indices in `sample_dictionary` are 3 (Coca-Cola → channelName), 1 (канал → researchDate), 1 (Первый → researchDate), 5 (Удалить → out of range), 4 (Спонсор → brand), "0|1" (Apple|Company → FTS, not in like_data).
- Assert: `set(df[MappingColumns.column_name].drop_nulls().unique().to_list()) == {"channelName", "researchDate", "brand"}`
  - search index 3 → channelName
  - search index 1 → researchDate
  - search index 4 → brand
  - search index 5 (Удалить DELETE row) → null (out of range, no column)
  - FTS row excluded from like_data

**Files:** `tests/test_dict_service.py` — `test_mapping_build` (line 42)

**Acceptance Criteria:**
- `is_in().all()` assertion replaced with specific expected column-name set
- Test fails if column resolution is broken (e.g., wrong search index mapping)
- `pytest tests/test_dict_service.py::test_mapping_build -v` passes

---

### Phase 2: Missing Coverage [3-4h]

**Purpose:** Add new test cases for uncovered code paths and missing fixture data. Minimal production changes. Several tasks depend on T-UP-01 (ENDS_WITH fixture row) — this task must complete before the ENDS_WITH-related tests.

#### T-UP-01 — Add ENDS_WITH row to `sample_dictionary` fixture (FINDING-06)

**Description:** The `MatchType` enum (models.py:124-129) defines `ENDS_WITH="e"`, but `sample_dictionary` (conftest.py:46-69) has no ENDS_WITH row. The `_build_search_like_pattern` method (dict_service.py:226-227) has a dedicated ENDS_WITH branch (`%term` pattern) that is never exercised by any test.

**Add** an ENDS_WITH row to `sample_dictionary`:
```python
["a", "e", 3, None, "Кола", "cola_end"],
```
- `action="a"` (ADD), `match="e"` (ENDS_WITH), `search=3` (→ channelName column), `extra_col=None`, `term="Кола"`, `tag="cola_end"`

**Impact analysis — tests that use `sample_dictionary` and may need assertion updates:**
- `test_mapping_build` (test_dict_service.py:7-42) — asserts `like_data`/`fts_data` non-empty; ENDS_WITH row goes to `like_data` (match != "fts"); no assertion change needed but T-FIX-01 (vacuous is_in) should account for it
- `test_pattern_generation` (test_dict_service.py:45-83) — asserts specific patterns; ENDS_WITH row adds a `%КОЛА` pattern. Must add assertion: `assert "%КОЛА" in patterns`
- `test_build_query_no_strict_column` (test_dict_service.py:165-176) — only checks column names, no assertion change needed
- `test_build_search_like_pattern_real_cases` (test_dict_service.py:86-118) — filters by `MatchType.FULL_MATCH`/`PARTIAL_MATCH`/`STARTS_WITH`; ENDS_WITH is excluded from these filters; no assertion change needed

**Dependency:** Must complete before T-RW-06 (ENDS_WITH pattern test) and T-UP-04 (ENDS_WITH coverage).

**Files:** `tests/conftest.py` — `sample_dictionary` fixture (line 49-56)

**Acceptance Criteria:**
- `sample_dictionary` contains a row with `match="e"` (ENDS_WITH)
- `test_pattern_generation` adds `assert "%КОЛА" in patterns`
- All existing `test_dict_service.py` tests still pass with the new fixture data
- `pytest tests/test_dict_service.py -v` passes

#### T-UP-02 — Add `generate_rules_blocks` ordering test (FINDING-24)

**Description:** `generate_rules_blocks` (dict_service.py:79-94) iterates actions in order `[DELETE, REPLACE, ADD]`. For REPLACE, it calls `group_by_cols` which groups by column set and yields each group separately. In `apply_mapping` (db_service.py:626-645), the `mapping_table` is written with `if_table_exists="replace"` (app_service.py:229), meaning only the last group survives. The ordering of yielded blocks determines which rules are ultimately applied. No test asserts the yield order.

**Create** `test_generate_rules_blocks_ordering`:
- Build a `MappingDict` with multi-action dictionary data (DELETE, multiple REPLACE groups, ADD rows)
- Call `list(mapping.generate_rules_blocks())`
- Assert: (a) DELETE blocks appear before any REPLACE block, (b) REPLACE blocks appear before any ADD block, (c) within REPLACE, `group_by_cols` groups are yielded in order (by first non-None column set)

**Files:** `tests/test_dict_service.py` — new test function

**Acceptance Criteria:**
- Asserts action ordering: DELETE → REPLACE → ADD
- Asserts REPLACE groups respect `group_by_cols` output order
- `pytest tests/test_dict_service.py::test_generate_rules_blocks_ordering -v` passes

#### T-UP-03 — Add FTS query string format test (FINDING-25)

**Description:** `build_match_query` (dict_service.py:264-279) constructs FTS query strings with specific formatting: wraps terms in parentheses and quotes (`safe_value = f'"({safe_value})"'`, line 276), produces `col_name:"(value)"` format (line 278), joins parts with spaces (line 279). Existing tests only assert `fts_data` is non-empty — never inspect the actual query strings.

**Create** `test_build_match_query_format`:
- Construct a `MappingDict` with `sample_dictionary` + `dict_indexes`
- Call `build_mapping` with main columns
- Call `mapping_dict.build_match_query([0, 1], ["Apple", "Company"])`
- Assert the result matches `f'{col_0}:"(Apple)" {col_1}:"(Company)"'` (exact format with quotes and parentheses)
- Assert `search_columns` set is updated as a side effect (contains `col_0` and `col_1`)

**Files:** `tests/test_dict_service.py` — new test function

**Acceptance Criteria:**
- Query string format `column:"(value)"` verified with exact string match
- `search_columns` side-effect verified
- `pytest tests/test_dict_service.py::test_build_match_query_format -v` passes

#### T-UP-04 — Add `MatchType.ENDS_WITH` coverage (FINDING-23)

**Description:** The ENDS_WITH branch in `_build_search_like_pattern` (dict_service.py:226-227) is never exercised by integration tests. After T-UP-01 adds the ENDS_WITH row to `sample_dictionary`, the `test_pattern_generation` test should verify that the ENDS_WITH pattern is generated correctly.

**Add** assertion to `test_pattern_generation`:
```python
assert "%КОЛА" in patterns  # ENDS_WITH → %term (leading % only)
```
- Verify the ENDS_WITH row's pattern has leading `%` but no trailing `%`
- Verify it appears in `like_data` (not `fts_data`)

**Dependency:** Requires T-UP-01 (ENDS_WITH fixture row).

**Files:** `tests/test_dict_service.py` — `test_pattern_generation` (lines 45-83)

**Acceptance Criteria:**
- ENDS_WITH pattern `%КОЛА` asserted in `test_pattern_generation`
- No trailing `%` on ENDS_WITH pattern
- `pytest tests/test_dict_service.py::test_pattern_generation -v` passes

#### T-UP-05 — Test remaining `get_files_suffix` branches (FINDING-31)

**Description:** `get_files_suffix` (csv_service.py:242-263) has a `match` statement with 6 cases + default, plus 3 pre-match branches (`infer`, `dict`, fallback to `str().lower()`). The test at `test_csv_service.py:8-11` only covers `"gzip"`, `"bz2"`, and `None`. Missing: `"infer"`, dict-form compression, `"xz"`, `"zip"`, `"zstd"`, and default fallback for unrecognized values.

**Extend** `test_get_files_suffix`:
```python
# infer → .csv
assert CSVWorker.get_files_suffix("infer") == ".csv"
# dict-form compression
assert CSVWorker.get_files_suffix({"method": "gzip"}) == ".csv.gz"
# xz
assert CSVWorker.get_files_suffix("xz") == ".csv.xz"
# zip
assert CSVWorker.get_files_suffix("zip") == ".csv.zip"
# zstd
assert CSVWorker.get_files_suffix("zstd") == ".csv.zst"
# unrecognized → default .csv
assert CSVWorker.get_files_suffix("unknown") == ".csv"
assert CSVWorker.get_files_suffix("parquet") == ".csv"
assert CSVWorker.get_files_suffix("json") == ".csv"
assert CSVWorker.get_files_suffix("csv") == ".csv"
```

**Files:** `tests/test_csv_service.py` — `test_get_files_suffix` (lines 8-11)

**Acceptance Criteria:**
- All `match` branches covered: gzip, gz, bz2, xz, zip, zstd, default
- All pre-match branches covered: infer, dict-form, None
- Every assertion explicitly uses `assert` keyword
- `pytest tests/test_csv_service.py::test_get_files_suffix -v` passes

#### T-UP-06 — Ensure all `get_files_suffix` test cases have explicit `assert` (FINDING-34)

**Description:** FINDING-34 notes that a variant of `test_get_files_suffix` had 3 bare expressions (no `assert`). In the **current tree** (HEAD `a41a445`), all 3 assertions are correct. This finding is subsumed by T-UP-05 which rewrites the test with all explicit `assert` statements.

**Action:** No separate task — T-UP-05 includes explicit `assert` on every case.

**Files:** `tests/test_csv_service.py`

**Acceptance Criteria:**
- No bare `CSVWorker.get_files_suffix(...)` calls without `assert`
- Merged into T-UP-05

#### T-UP-07 — Add `process_data` non-existent directory test (FINDING-17)

**Description:** `process_data` (app.py:142-144) checks `if not path.is_dir()` and raises `FileNotFoundError(f"Directory not found: {path}")`. No test exercises this guard — existing tests pass an existing (empty) directory.

**Create** `test_process_data_nonexistent_directory_raises_filenotfound`:
```python
def test_process_data_nonexistent_directory_raises_filenotfound(populated_settings):
    nonexistent = Path("/nonexistent/path/that/does/not/exist")
    with pytest.raises(FileNotFoundError, match="Directory not found"):
        process_data(nonexistent, settings_dir=populated_settings)
```
- The `is_dir()` guard at app.py:143 fires before `get_app_service` / settings gate
- Asserts `FileNotFoundError` with `match="Directory not found"`

**Files:** `tests/test_app.py` — new test function

**Acceptance Criteria:**
- `FileNotFoundError` raised before settings gate
- Error message contains "Directory not found"
- `pytest tests/test_app.py::test_process_data_nonexistent_directory_raises_filenotfound -v` passes

#### T-UP-08 — Add CLI `run` success test (FINDING-18)

**Description:** The CLI `run` command (app.py:55-89) is tested only for failure paths. No test invokes `app run` with populated settings and valid data to verify the success path (console output, exit code 0, ReportResult printing).

**Create** `test_run_cli_success`:
```python
def test_run_cli_success(populated_settings, synthetic_report):
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", str(synthetic_report), "--settings-dir", str(populated_settings)],
    )
    assert result.exit_code == 0, result.output
    assert "Report complete" in result.output
    assert "3 loaded" in result.output
    assert "2 clean" in result.output
```
- Exercises full CLI path: `run` → `get_app_service` → `service.run_report` → `console.print` success → exit 0
- Verifies the console output format at app.py:82-85

**Files:** `tests/test_app.py` — new test function

**Acceptance Criteria:**
- `result.exit_code == 0`
- `"Report complete"` in output
- Row count format `"3 loaded → 2 clean"` verified
- `pytest tests/test_app.py::test_run_cli_success -v` passes

#### T-UP-09 — Add `ReportResult` negative validation tests (FINDING-19, part 1)

**Description:** `ReportResult` (models.py:182-207) uses `rows_loaded: int` and `rows_exported: int` — allowing negative values, which is logically impossible for row counts. No test verifies that negative values are rejected.

**Create** `test_report_result_rejects_negative_row_counts`:
```python
def test_report_result_rejects_negative_row_counts():
    with pytest.raises(ValidationError):
        ReportResult(
            report_dir=Path("/tmp/report"),
            ...  # all fields from existing test
            rows_loaded=-1,
            rows_exported=95,
        )
    with pytest.raises(ValidationError):
        ReportResult(
            ...
            rows_loaded=100,
            rows_exported=-5,
        )
```

**Dependency:** Requires T-UP-10 (NonNegativeInt change) to be applied first, otherwise `int` accepts negatives.

**Files:** `tests/test_app.py` — new test function

**Acceptance Criteria:**
- `ValidationError` raised for `rows_loaded=-1`
- `ValidationError` raised for `rows_exported=-5`
- `pytest tests/test_app.py::test_report_result_rejects_negative_row_counts -v` passes

#### T-UP-10 — Change `ReportResult.rows_loaded`/`rows_exported` from `int` to `NonNegativeInt` (FINDING-19, part 2)

**Description:** Change `rows_loaded: int` → `rows_loaded: NonNegativeInt` and `rows_exported: int` → `rows_exported: NonNegativeInt` in `models.py:206-207`. `NonNegativeInt` is already imported (models.py:14) and used elsewhere. This is a **Pydantic validation change** — not a database schema change. No migration needed.

**Impact analysis:**
- `ReportResult` is a Pydantic `BaseModel`, not a Django model — no DB migration required
- `NonNegativeInt` is `Annotated[int, Field(ge=0)]` — backward compatible for all positive values
- `run_report` (app_service.py:280-292) computes `rows_loaded` and `rows_exported` from non-negative operations (counts, CSV export return) — no production impact
- Existing tests (`test_report_result_model_construction_and_serialization` with `rows_loaded=100, rows_exported=95`) still pass — positive values are valid

**Production code change** — separate from test work:

**Files:** `src/mko_data_cleaner/core/models.py` — `ReportResult` class (lines 206-207)

```python
# Before:
rows_loaded: int
rows_exported: int

# After:
rows_loaded: NonNegativeInt
rows_exported: NonNegativeInt
```

**Dependency:** Must be applied before T-UP-09 (negative validation test). Can be done in parallel with Phase 0/1 test work since it doesn't break existing tests.

**Acceptance Criteria:**
- `NonNegativeInt` used instead of plain `int`
- All positive-value constructions still work (no regression)
- `ruff check src/mko_data_cleaner/core/models.py` clean
- `mypy src/mko_data_cleaner/core/models.py` clean
- `pytest tests/test_app.py::test_report_result_model_construction_and_serialization -v` passes

#### T-UP-11 — Add `test_version_is_non_empty_string` cross-check with `pyproject.toml` (FINDING-35)

**Description:** `test_version_is_non_empty_string` (test_metadata.py:10-13) asserts `__version__` is a non-empty string but does NOT verify it matches `pyproject.toml`'s `version = "0.2.0"` (line 7). The `__init__.py` fallback (line 15: `__version__ = "0.2.0"`) could drift from `pyproject.toml` without detection.

**Extend** `test_version_is_non_empty_string`:
```python
import tomllib

# Cross-check against pyproject.toml
with open(ROOT / "pyproject.toml", "rb") as f:
    pyproject = tomllib.load(f)
expected_version = pyproject["project"]["version"]
assert mko_data_cleaner.__version__ == expected_version, (
    f"__version__ ({mko_data_cleaner.__version__}) != "
    f"pyproject.toml version ({expected_version})"
)
```
- Use `ROOT = Path(__file__).resolve().parent.parent` (same pattern as `test_packaging.py`)
- Import `tomllib` (Python 3.12+ stdlib, same as `test_packaging.py`)

**Files:** `tests/test_metadata.py` — `test_version_is_non_empty_string` (lines 10-13)

**Acceptance Criteria:**
- `__version__` cross-checked against `pyproject.toml` `project.version`
- Fails if fallback version drifts from `pyproject.toml`
- `pytest tests/test_metadata.py::test_version_is_non_empty_string -v` passes

#### T-UP-12 — Add `test_metadata_contains_required_fields` license cross-check (FINDING-36)

**Description:** The audit references `test_metadata_contains_required_fields` which doesn't exist in the current tree. The closest existing test is `test_pyproject_uses_spdx_license` (test_packaging.py:87-91) which asserts `metadata["license"] == "BSD-3-Clause"`.

**Action:** Verify the existing `test_pyproject_uses_spdx_license` test covers the license cross-check:
- ✅ Asserts `metadata["license"] == "BSD-3-Clause"` (pyproject.toml:15)
- ✅ Asserts `metadata["license-files"] == ["LICENSE.txt"]` (pyproject.toml:16)

If these assertions exist and are correct, the finding is already addressed. If not, add a dedicated `test_metadata_contains_required_fields` test that cross-references `name`, `version`, `license`, and `license-files` from `pyproject.toml`.

**Files:** `tests/test_packaging.py` — verify or extend `test_pyproject_uses_spdx_license`

**Acceptance Criteria:**
- `pyproject.toml` `license` field cross-checked against `"BSD-3-Clause"`
- `license-files` field cross-checked against `["LICENSE.txt"]`
- `pytest tests/test_packaging.py::test_pyproject_uses_spdx_license -v` passes

---

### Phase 3: New Tests & Production Fix [4-6h]

**Purpose:** Write tests for entirely untested production code (`_apply_add`) and fix the latent `column_index` cache invalidation bug. Both tasks depend on Phase 0 (cleaned fixtures) and Phase 2 (T-UP-01 EndsWith fixture row).

#### T-WR-01 — Write direct unit tests for `_apply_add` (FINDING-21)

**Description:** `_apply_add` (db_service.py:690-745, 56 lines) is entirely untested. Sibling methods `_apply_delete` and `_apply_replace` are both directly tested (test_db_service.py:385, 437). The `_apply_add` method:
1. Iterates over `self.extra_columns`
2. Builds UNION queries selecting non-null, non-empty tag values from `_joined_matches_table`
3. Inserts into `data_table_tags` table (via `INSERT OR IGNORE`)
4. Retrieves distinct column names from `data_table_tags`
5. For each used column, builds an UPDATE with `GROUP_CONCAT(DISTINCT value, separator)` to concatenate values into the target table

**Create** `test_apply_add_concatenates_tags`:
- Follow the pattern of `test_apply_replace_picks_highest_mapping_index` (test_db_service.py:437-505):
  1. Set up target table with rows having extra columns
  2. Set `db_worker.extra_columns` via `set_data_tbl_columns` with `extra_cols`
  3. Create `_joined_matches_table` with matching rows containing tag values
  4. Call `db_worker._ensure_tags_table()` then `db_worker._apply_add(separator=", ")`
  5. Assert the target table's extra column was populated with `GROUP_CONCAT` output

**Test scenario:**
- Target table: `id`, `brand`, `tag` (3 rows: ad1/BrandX/None, ad2/BrandX/None, ad3/BrandX/None)
- `_joined_matches_table`: 2 entries for ad1's rowid with `tag="fruit_tag"` and `tag="sponsor_tag"`
- After `_apply_add(separator=", ")`: `tag` column for ad1 should be `"fruit_tag, sponsor_tag"` (GROUP_CONCAT DISTINCT, ordered)
- Rows ad2 and ad3 should remain unchanged (no matches)

**Files:** `tests/test_db_service.py` — new test function

**Acceptance Criteria:**
- `_apply_add` called with correct separator
- `data_table_tags` populated with distinct tag values
- Target table extra column updated with concatenated values
- Unmatched rows remain unchanged
- `pytest tests/test_db_service.py::test_apply_add_concatenates_tags -v` passes

#### BP-03 — Fix `DBWorker.column_index` cache invalidation bug (FINDING-22)

**Description:** `column_index` is a `@cached_property` (db_service.py:124-128) that caches a `{index: name}` dict built from `self.data_tbl_columns` (which returns `self._data_tbl_columns`). The `set_data_tbl_columns` method (db_service.py:133-139) reassigns `self._data_tbl_columns` but does NOT invalidate the `column_index` cache. `cached_property` stores its result in `self.__dict__["column_index"]`, which persists across subsequent `set_data_tbl_columns` calls.

**Impact:** Currently latent — `run_report` (app_service.py:154-156) calls `set_data_tbl_columns` once per `DBWorker` instance. But any future code that calls `set_data_tbl_columns` twice on the same instance (or any test reusing a `db_worker` fixture) would get stale column names via `get_col_names()`.

**Fix in `set_data_tbl_columns`:**
```python
def set_data_tbl_columns(self, *main_cols, extra_cols: list | None = None):
    extra_cols = extra_cols or []
    self._data_tbl_columns = clean_names(*main_cols, *extra_cols)
    if extra_cols:
        self._extra_columns = self.data_tbl_columns[-len(extra_cols):]
    else:
        self._extra_columns = []
    # Invalidate cached_property column_index so it reflects updated columns
    self.__dict__.pop("column_index", None)
```

**Add regression test** `test_column_index_reflects_second_set_data_tbl_columns`:
- Call `set_data_tbl_columns("id", "name")` → assert `column_index == {0: "id", 1: "name"}`
- Access `column_index` to trigger caching
- Call `set_data_tbl_columns("id", "name", "value", "tag")` → assert `column_index == {0: "id", 1: "name", 2: "value", 3: "tag"}`
- Verify `get_col_names(2) == "value"` and `get_col_names(3) == "tag"` (would return stale/null without fix)

**Files:** `src/mko_data_cleaner/core/db_service.py` — `set_data_tbl_columns` method (lines 133-139); `tests/test_db_service.py` — new test function

**Acceptance Criteria:**
- `self.__dict__.pop("column_index", None)` added to `set_data_tbl_columns`
- Regression test fails on pre-fix code, passes post-fix
- Existing `test_column_index_and_get_col_names` still passes
- `pytest tests/test_db_service.py -v` passes (all DBWorker tests)
- `ruff check src/mko_data_cleaner/core/db_service.py tests/test_db_service.py` clean

---

### Phase 4: Documentation Fixes [~0.5h]

**Purpose:** Correct docstring inaccuracies. Both tasks are independent and can be done in parallel. Both depend on Phase 0 (T-DEL-04 removes `test_null_data_files_excludes_stale`, which is referenced in DOC-01's docstring).

#### DOC-01 — Fix `synthetic_report` docstring line references (FINDING-07)

**Description:** The `synthetic_report` fixture docstring (conftest.py:122-135) references `scripts/smoke_install.py (lines 112-160)`. The actual e2e scenario in `smoke_install.py` spans lines 112-168 (line 112: comment block `# (v) End-to-end on synthetic sample`, line 168: final print statement). The docstring also references `tests/test_app.py::test_null_data_files_excludes_stale` which is deleted in Phase 0 (T-DEL-04) — this reference must be removed.

**Fix:**
- Change `scripts/smoke_install.py (lines 112-160)` → `scripts/smoke_install.py (lines 112-168)`
- Remove reference to `test_null_data_files_excludes_stale` (deleted in Phase 0) from the "keep in sync" list
- Add `test_run_cli_success` (T-UP-08) to the "keep in sync" list if it references `synthetic_report`

**Files:** `tests/conftest.py` — `synthetic_report` docstring (lines 124-135)

**Acceptance Criteria:**
- Line reference updated to 112-168
- Deleted test reference removed
- Docstring accurate for current state
- `ruff check tests/conftest.py` clean

#### DOC-02 — Fix `ReportResult` docstring field count (FINDING-20)

**Description:** `test_report_result_model_construction_and_serialization` (test_app.py:257-258) has docstring `"""ReportResult accepts all 10 fields and serializes via model_dump()."""` — but `ReportResult` has 11 fields (models.py:182-207): `report_dir`, `import_dir`, `export_dir`, `db_path`, `null_data_files`, `started_at`, `finished_at`, `duration`, `timings`, `rows_loaded`, `rows_exported`.

**Fix:** Change `10 fields` → `11 fields` in the docstring.

**Files:** `tests/test_app.py` — `test_report_result_model_construction_and_serialization` docstring (line 258)

**Acceptance Criteria:**
- Docstring says "11 fields"
- Count matches `ReportResult` model definition
- `ruff check tests/test_app.py` clean

---

## Execution Order & Dependencies (DAG)

```
Phase 0: Cleanup (parallelizable)
┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
│ T-DEL-01    │ T-DEL-02    │ T-DEL-03    │ T-DEL-04    │ T-DEL-05    │ T-DEL-06    │
│ (sample_    │ (tmp_db_    │ (db_worker_ │ (test_null_ │ (test_init_ │ (test_init_ │
│ mapping_df) │ path)       │ memory)     │ data_files_)│ project_    │ project_    │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
                                                           │
                                                           ▼
                                                  T-DEL-07 (test_top_level_...)
                                                           │
                                                           ▼
                                                  T-DEL-08, T-DEL-09 (verify)
                                                           │
                                                           ▼
  ┌───────────────────────────────────────────────────────┴────────────────────────────────┐
  │                                                                            Phase 1: Rewrites                                                                                   │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
  T-RW-01   T-RW-02   T-RW-03   T-RW-04   T-RW-05   T-FIX-01   T-RW-07   T-RW-09   T-RW-10
  (CLI exit) (config vals) (past gate) (mock spy) (CSV content) (is_in fix) (starts_with) (gzip) (no_delimiter)
     │         │         │         │         │         │         │         │         │
     └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
                            │
                            ▼
                    ┌───────────────────────┐
                    │  T-UP-01: ENDS_WITH   │  ← Must precede T-RW-06, T-UP-04
                    │  row in sample_dict   │
                    └───────────────────────┘
                            │
                            ▼
               T-RW-06          T-UP-04
               (ends_with)      (ENDS_WITH coverage)

Phase 2: Missing Coverage (parallelizable within dependencies)
┌─────────────────────────────────────────────────────────────────────────┐
│ T-UP-02 (rules ordering)  T-UP-03 (FTS query)  T-UP-05 (suffix branches) │
│ T-UP-06 (merged w/ T-UP-05) T-UP-07 (nonexist dir) T-UP-08 (CLI run)   │
│ T-UP-11 (version check)   T-UP-12 (license check)                      │
└─────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
 Phase 3: New Tests & Production Fix
 ┌──────────────────────────────────────────────────┐
 │ T-WR-01: _apply_add tests   BP-03: column_index │
 │ (depends on Phase 0/2)      fix + regression    │
 └──────────────────────────────────────────────────┘
                                 │
                                 ▼
 Phase 4: Documentation Fixes
 ┌──────────────────────────────────────────────────┐
 │ DOC-01: synthetic_report docstring               │
 │ DOC-02: ReportResult field count (10 → 11)       │
 └──────────────────────────────────────────────────┘

Cross-phase dependency: T-UP-10 (NonNegativeInt prod change) must
precede T-UP-09 (negative validation test). T-UP-10 can run in
parallel with Phase 0-1 work since it doesn't break existing tests.
```

**Dependency summary:**

| Task | Depends On | Reason |
|---|---|---|
| T-RW-06 | T-UP-01 | Needs ENDS_WITH row in `sample_dictionary` fixture |
| T-UP-04 | T-UP-01 | Needs ENDS_WITH row in `sample_dictionary` fixture |
| T-UP-09 | T-UP-10 | Needs `NonNegativeInt` change to reject negatives |
| T-WR-01 | Phase 0, T-UP-01 | Uses `db_worker` fixture (needs clean fixtures); ENDS_WITH row adds coverage |
| BP-03 | Phase 0 | Clean fixtures for regression test |
| DOC-01 | T-DEL-04 (Phase 0) | Docstring references deleted `test_null_data_files_excludes_stale` |

---

## Risk Register

| Task ID | Risk | Severity | Mitigation |
|---|---|---|---|
| T-RW-03 | Replacing bare `except Exception: pass` with `synthetic_report` changes test semantics from "settings gate only" to "full pipeline" — may expose latent operational bugs | HIGH | Run full `test_app.py` after; if pipeline fails, diagnose separately from the rewrite |
| T-RW-05 | CSV content inspection may fail if export format differs from expectation (`.gz` auto-decompression, separator) | MEDIUM | Use `pl.read_csv` with explicit `separator=";"` matching `app_config.json5` export_settings |
| T-UP-01 | Adding ENDS_WITH row to `sample_dictionary` may break `test_mapping_build`, `test_pattern_generation`, `test_build_search_like_pattern_real_cases` if they have implicit assumptions about row count or pattern set | HIGH | Run `test_dict_service.py` after; update assertions in `test_pattern_generation` for new `%КОЛА` pattern |
| T-UP-10 | Changing `int` → `NonNegativeInt` in `ReportResult` could break code paths that pass negative counts | LOW | Verify `run_report` always produces non-negative counts; run full suite after change |
| T-WR-01 | `_apply_add` test setup is complex (56 lines of SQL) — test may be brittle or test wrong things | MEDIUM | Follow established pattern from `test_apply_replace_picks_highest_mapping_index`; spy on `perform_query`; assert on both SQL and observable outcome |
| BP-03 | Production code change to `set_data_tbl_columns` — `self.__dict__.pop("column_index", None)` could affect caching behavior in unexpected ways | LOW | No production caller of `column_index` or `get_col_names` exists; change is cache invalidation only; existing tests unaffected |
| T-RW-09 | `test_export_sql_to_csv_gzip` overlaps with existing `test_export_sql_to_csv_roundtrip_content` — risk of redundancy | LOW | Make gzip test specifically assert compression format (`.csv.gz` extension, gzip binary validity) in addition to content |
| T-RW-10 | `test_import_csv_no_delimiter_raises` scenario may not trigger `WrongDataSettings` if files exist but are empty | MEDIUM | Create directory with wrong-extension files (e.g., `.txt`) to trigger the "no files found" path in `_set_files_params` |
| T-RW-08 | Merging FINDING-30 (rewrite) and FINDING-34 (missing assert) into one test — risk of incomplete coverage if not all 3 cases tested | LOW | Explicitly assert all 3 match types: FULL_MATCH → no %, PARTIAL_MATCH → %term%, STARTS_WITH → term% |
| T-UP-12 | `test_metadata_contains_required_fields` doesn't exist — finding may be already addressed by `test_pyproject_uses_spdx_license` | LOW | Verify existing test covers license cross-check; if yes, close as complete; if no, add dedicated test |

---

## Acceptance Criteria

### Phase 0 — Cleanup
| Criterion | Verification |
|---|---|
| 3 unused fixtures removed from conftest.py | `grep` returns no matches for `sample_mapping_df`, `tmp_db_path`, `db_worker_memory` |
| 5 redundant tests removed from test_app.py | `grep` confirms absence of all 5 test names |
| 2 non-existent tests verified absent | `grep` confirms `test_build_match_query_single` and `test_pyproject_validates_successfully` are absent |
| `pytest` still green after deletions | `uv run pytest` passes; same pass count minus deleted tests |
| `ruff check` clean | `uv run ruff check tests/` clean |

### Phase 1 — High-Impact Rewrites
| Criterion | Verification |
|---|---|
| `test_run_cli_without_settings` asserts `exit_code == 1` | Test rewritten; `result.exit_code` checked |
| `test_get_app_service_constructs` asserts concrete config values | No `is not None` assertions; specific values from `app_config.json5` |
| `test_process_data_past_gate` has no bare `except Exception: pass` | Code reviewed; only specific exception types caught |
| `test_process_data_returns_report_result` spies on `run_report` call args | `MagicMock.assert_called_once_with` used |
| `test_process_data_end_to_end` reads back CSV content | `pl.read_csv` on exported `.gz` files; Brand2 absent, Brand1 present |
| 3 pattern tests created with correct assertions | `ENDS_WITH` → `%TERM`, `STARTS_WITH` → `TERM%`, `CONTAINS` → `%TERM%` |
| Vacuous `is_in()` assertion replaced with specific column names | `test_mapping_build` asserts exact expected set |
| `pytest tests/test_app.py tests/test_dict_service.py tests/test_csv_service.py -v` all pass | Full test run |

### Phase 2 — Missing Coverage
| Criterion | Verification |
|---|---|
| `sample_dictionary` has ENDS_WITH row | Fixture contains row with `match="e"` |
| `generate_rules_blocks` ordering tested | DELETE → REPLACE → ADD asserted |
| FTS query string format tested | `build_match_query` output asserted as `col:"(value)"` format |
| ENDS_WITH pattern covered in integration test | `%КОЛА` pattern asserted in `test_pattern_generation` |
| `get_files_suffix` all branches covered | infer, dict, xz, zip, zstd, default cases asserted |
| `process_data` non-existent dir raises `FileNotFoundError` | `pytest.raises(FileNotFoundError, match="Directory not found")` |
| CLI `run` success path tested | `exit_code == 0`, `"Report complete"` in output |
| `ReportResult` rejects negative row counts | `pytest.raises(ValidationError)` for negative values |
| `ReportResult` uses `NonNegativeInt` | `rows_loaded`/`rows_exported` type changed in models.py |
| `__version__` cross-checked with pyproject.toml | `tomllib` parse + equality assertion |
| License field cross-checked | `BSD-3-Clause` asserted against pyproject.toml |
| Full test suite passes | `uv run pytest` green |

### Phase 3 — New Tests & Production Fix
| Criterion | Verification |
|---|---|
| `_apply_add` has direct unit test (56 lines covered) | `test_apply_add_concatenates_tags` tests GROUP_CONCAT behavior |
| `column_index` cache invalidation fixed in production | `set_data_tbl_columns` calls `self.__dict__.pop("column_index", None)` |
| Regression test fails pre-fix, passes post-fix | Test asserts second `set_data_tbl_columns` call reflects new columns |
| `test_column_index_and_get_col_names` still passes | No regression in existing cache behavior |
| `ruff check src/mko_data_cleaner/core/db_service.py` clean | Lint passes |
| `mypy src/mko_data_cleaner/core/db_service.py` clean | Type check passes |

### Phase 4 — Documentation Fixes
| Criterion | Verification |
|---|---|
| `synthetic_report` docstring line refs updated | `112-168` (not `112-160`); deleted test ref removed |
| `ReportResult` docstring says "11 fields" | Not "10 fields" |
| `ruff check` clean on modified files | No lint errors |

---

## Verification Steps

### After each phase:
1. Run `uv run pytest` (fast gate) — verify all tests pass
2. Run `uv run ruff check src/ tests/` — verify no lint errors
3. If production code changed: run `uv run mypy src/mko_data_cleaner/core/<module>.py` — verify no new type errors

### Targeted test runs per phase:

| Phase | Tests to run |
|---|---|
| Phase 0 | `pytest tests/test_app.py tests/test_dict_service.py tests/test_packaging.py tests/conftest.py` |
| Phase 1 | `pytest tests/test_app.py tests/test_dict_service.py tests/test_csv_service.py -v` |
| Phase 2 | `pytest tests/test_app.py tests/test_dict_service.py tests/test_csv_service.py tests/test_metadata.py tests/test_packaging.py -v` |
| Phase 3 | `pytest tests/test_db_service.py src/mko_data_cleaner/core/models.py -v` (plus full suite) |
| Phase 4 | `pytest tests/conftest.py tests/test_app.py -v` (docstring changes only) |

### Single test execution:
```powershell
uv run pytest -k test_name
```
(Per project convention — see `.kilo/rules/commands.md` §Tests)

---

## Appendix: Findings Referencing Tests Not in Current Tree

The audit was conducted on baseline HEAD `f6ac8e1`. The current repository HEAD is `a41a445`. The following 7 findings reference tests that **do not exist** in the current `tests/` directory:

| Finding | Referenced Test | Status | Action |
|---|---|---|---|
| FINDING-27 | `test_build_match_query_single` (test_dict_service.py) | Absent | T-DEL-08: verify absence; no-op |
| FINDING-28 | `test_build_search_like_pattern_ends_with` | Absent | T-RW-06: create with correct assertion (`%TERM`) |
| FINDING-29 | `test_build_search_like_pattern_starts_with` | Absent | T-RW-07: create with correct assertion (`TERM%`) |
| FINDING-30 | `test_build_search_like_pattern_contains` | Absent | T-RW-08: create with correct assertions for 3 cases; also satisfies FINDING-34 |
| FINDING-32 | `test_export_sql_to_csv_gzip` | Absent | T-RW-09: create with content verification (gzip-specific angle vs existing `test_export_sql_to_csv_roundtrip_content`) |
| FINDING-33 | `test_import_csv_no_delimiter_raises` | Absent | T-RW-10: create with `WrongDataSettings` (not `RuntimeError`) |
| FINDING-36 | `test_metadata_contains_required_fields` | Absent | T-UP-12: verify `test_pyproject_uses_spdx_license` covers license cross-check; if not, add dedicated test |
| FINDING-37 | `test_pyproject_validates_successfully` | Absent | T-DEL-09: verify absence; no-op |

**Note on FINDING-26** (`is_in()` vacuous assertion): Classified as `[TEST-REWRITE]` in the audit but not explicitly listed in the task's TEST-REWRITE roster. Included here as **T-FIX-01** in Phase 1 for completeness — it is a high-value fix that replaces a zero-verification assertion with a concrete expected-value check.

**Note on FINDING-33** (`test_import_csv_no_delimiter_raises`): Classified as `[TEST-UPDATE]` (MEDIUM) in the audit but moved to `[TEST-REWRITE]` (TEST-REWRITE #10) in the task's classification. Placed in Phase 1 (Rewrites) per the task's categorization.

**Note on FINDING-34** (`test_get_files_suffix` missing assert): Classified as `[TEST-UPDATE]` (LOW) in the audit. In the current tree, all 3 assertions are correct (no bare expressions). Subsumed by T-UP-05 which rewrites the test with all explicit `assert` statements.

---

## Effort Summary

| Phase | Tasks | Est. Time |
|---|---|---|
| Phase 0 (Cleanup) | 9 tasks | 2-3h |
| Phase 1 (Rewrites) | 11 tasks | 4-6h |
| Phase 2 (Missing Coverage) | 12 tasks (incl. production NonNegativeInt change) | 3-4h |
| Phase 3 (New Tests + Prod Fix) | 3 tasks (2 test writes + 1 prod fix) | 4-6h |
| Phase 4 (Docs) | 2 tasks | 0.5h |
| **Total** | **37 findings** | **14-19h** |

> Phase 2 includes 1 production-code change (T-UP-10: `NonNegativeInt`). It is a Pydantic validation type change — no database migration required. Can be done first within Phase 2 to unblock the negative-validation test (T-UP-09).
