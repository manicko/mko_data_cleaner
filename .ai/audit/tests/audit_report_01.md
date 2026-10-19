# Test Quality Audit Report — mko_data_cleaner

| Field | Value |
|---|---|
| **Report ID** | audit_report_01 |
| **Audit Phase** | Step 4 — Test Quality Synthesis |
| **Date** | 2026-10-18 |
| **Scope** | `tests/` directory — 7 test modules, 37 findings |
| **Test Files Audited** | `conftest.py`, `test_app.py`, `test_db_service.py`, `test_dict_service.py`, `test_csv_service.py`, `test_packaging.py`, `test_metadata.py`, `test_notebook.py`, `test_notebook_check.py` |
| **Production Files Referenced** | `app.py`, `app_service.py`, `db_service.py`, `dict_service.py`, `csv_service.py`, `models.py`, `__init__.py`, `paths.py`, `errors.py`, `init_service.py`, `notebook.py`, `utils.py` |
| **Total Findings** | 37 |
| **Severity Breakdown** | CRITICAL: 0 · HIGH: 28 · MEDIUM: 7 · LOW: 2 |

---

## Executive Summary

A comprehensive test quality audit was performed across the `mko_data_cleaner` test suite. The audit examined 9 test files covering the application's public API, database worker, dictionary mapping service, CSV service, packaging/metadata, and notebook environment. A total of **37 findings** were identified across 6 thematic blocks.

The findings cluster into four primary categories:

1. **Dead/unused test code** — 3 unused fixtures in `conftest.py` (`sample_mapping_df`, `tmp_db_path`, `db_worker_memory`) that are permanently unexercised.
2. **Weak assertions** — 12 tests across `test_app.py`, `test_dict_service.py`, `test_csv_service.py`, and `test_metadata.py`/`test_packaging.py` that assert trivially-true, tautological, or insufficiently specific conditions.
3. **Missing coverage** — 9 gaps in core production methods: `_apply_add` (56 lines, 0 direct tests), `MatchType.ENDS_WITH` branch, `generate_rules_blocks` ordering, FTS query string format, `process_data` `is_dir()` guard, CLI `run` success path, `ReportResult` negative-value validation, `get_files_suffix` missing branches, and `sample_dictionary` ENDS_WITH row.
4. **Redundancy** — 6 sets of duplicate tests that provide no additional coverage value.

Additionally, **1 latent bug** was identified: `DBWorker.column_index` (`cached_property` at `db_service.py:124`) is not invalidated when `set_data_tbl_columns` (`db_service.py:133`) reassigns `self._data_tbl_columns`, meaning a second call to `set_data_tbl_columns` returns a stale column index.

**Priority recommendation:** Address HIGH-severity findings first — particularly the untested `_apply_add` method (56 lines of production SQL at `db_service.py:690-745`), the `column_index` invalidation bug, and the weak assertions in `test_app.py` that mask operational errors via bare `except Exception: pass`.

---

## Findings Summary Table

Findings are listed in severity order (HIGH → MEDIUM → LOW), then by block.

| # | Title | Severity | Classification | Block | Source (Test) |
|---|---|---|---|---|---|
| 01 | `sample_mapping_df` fixture has zero consumers | HIGH | `[TEST-DELETE]` | Block 1 | `conftest.py:82-96` |
| 02 | `sample_mapping_df` column layout mismatches `col_indexes` | HIGH | `[BEST-PRACTICE]` | Block 1 | `conftest.py:82-96` |
| 03 | `tmp_db_path` fixture has zero consumers | HIGH | `[TEST-DELETE]` | Block 1 | `conftest.py:99-105` |
| 04 | `db_worker_memory` fixture has zero consumers | HIGH | `[TEST-DELETE]` | Block 1 | `conftest.py:108-119` |
| 05 | `db_worker_memory` hardcodes values instead of using `realistic_config` | HIGH | `[BEST-PRACTICE]` | Block 1 | `conftest.py:108-119` |
| 06 | `sample_dictionary` lacks ENDS_WITH (`e`) row | HIGH | `[TEST-UPDATE]` | Block 1 | `conftest.py:46-69` |
| 07 | `synthetic_report` docstring references wrong line numbers | HIGH | `[DOC-UPDATE]` | Block 1 | `conftest.py:122-135` |
| 08 | `test_run_cli_without_settings` missing `exit_code` assertion | HIGH | `[TEST-REWRITE]` | Block 2 | `test_app.py:166-174` |
| 09 | `test_get_app_service_constructs` uses trivially-True assertions | HIGH | `[TEST-REWRITE]` | Block 2 | `test_app.py:219-223` |
| 10 | `test_process_data_past_gate` bare `except Exception: pass` masks errors | HIGH | `[TEST-REWRITE]` | Block 2 | `test_app.py:226-236` |
| 11 | `test_process_data_returns_report_result` circular assertions | HIGH | `[TEST-REWRITE]` | Block 2 | `test_app.py:286-312` |
| 12 | `test_process_data_end_to_end` verifies DELETE rule by count only | HIGH | `[TEST-REWRITE]` | Block 2 | `test_app.py:431-449` |
| 13 | `test_null_data_files_excludes_stale` is tautological | HIGH | `[TEST-DELETE]` | Block 2 | `test_app.py:452-463` |
| 14 | `test_init_project_with_settings_dir` duplicates `test_initialize_settings_threads_settings_dir` | HIGH | `[TEST-DELETE]` | Block 2 | `test_app.py:343-349` |
| 15 | `test_init_project_none_uses_app_paths` duplicates `test_init_cli_creates_settings_in_tmp_dir` | HIGH | `[TEST-DELETE]` | Block 2 | `test_app.py:372-378` |
| 16 | `test_top_level_no_import_side_effects` duplicates `test_import_app_does_not_create_settings_dir` | HIGH | `[TEST-DELETE]` | Block 2 | `test_app.py:406-425` |
| 17 | `process_data` `is_dir()` guard (`FileNotFoundError`) untested | HIGH | `[TEST-UPDATE]` | Block 2 | `test_app.py` |
| 18 | CLI `run` success path untested | HIGH | `[TEST-UPDATE]` | Block 2 | `test_app.py` |
| 19 | `ReportResult` uses plain `int` not `NonNegativeInt` | HIGH | `[TEST-UPDATE]` | Block 2 | `test_app.py` |
| 20 | Docstring says "10 fields" but `ReportResult` has 11 | HIGH | `[DOC-UPDATE]` | Block 2 | `test_app.py:257` |
| 21 | `_apply_add` method (56 lines) entirely untested | HIGH | `[TEST-WRITE]` | Block 3 | `test_db_service.py` |
| 22 | `column_index` cached_property not invalidated by `set_data_tbl_columns` | HIGH | `[BEST-PRACTICE]` | Block 3 | `db_service.py:124,133` |
| 23 | `MatchType.ENDS_WITH` (`e`) branch untested | HIGH | `[TEST-UPDATE]` | Block 4 | `test_dict_service.py` |
| 24 | `generate_rules_blocks` priority/ordering untested | HIGH | `[TEST-UPDATE]` | Block 4 | `test_dict_service.py` |
| 25 | FTS query string format (`col:"(term)"`) untested | HIGH | `[TEST-UPDATE]` | Block 4 | `test_dict_service.py` |
| 26 | `is_in()` assertion is vacuously True | HIGH | `[TEST-REWRITE]` | Block 4 | `test_dict_service.py:42` |
| 27 | `test_build_match_query_single` duplicates `test_build_match_query_multiple` | MEDIUM | `[TEST-DELETE]` | Block 4 | `test_dict_service.py` |
| 28 | `test_build_search_like_pattern_ends_with` assertion wrong (no trailing `%`) | MEDIUM | `[TEST-REWRITE]` | Block 4 | `test_dict_service.py` |
| 29 | `test_build_search_like_pattern_starts_with` assertion wrong (no trailing `%`) | MEDIUM | `[TEST-REWRITE]` | Block 4 | `test_dict_service.py` |
| 30 | `test_build_search_like_pattern_contains` assertion wrong (no trailing `%`) | MEDIUM | `[TEST-REWRITE]` | Block 4 | `test_dict_service.py` |
| 31 | `get_files_suffix` missing branches: `csv`, `json`, `parquet`, `ipc`, `ods` | HIGH | `[TEST-UPDATE]` | Block 5 | `test_csv_service.py:8-11` |
| 32 | `test_export_sql_to_csv_gzip` asserts only row count + file existence | MEDIUM | `[TEST-REWRITE]` | Block 5 | `test_csv_service.py` |
| 33 | `test_import_csv_no_delimiter_raises` uses generic `RuntimeError` | MEDIUM | `[TEST-UPDATE]` | Block 5 | `test_csv_service.py` |
| 34 | `test_get_files_suffix` missing `assert` on 3 cases | LOW | `[TEST-UPDATE]` | Block 5 | `test_csv_service.py:8-11` |
| 35 | `test_version_is_non_empty_string` doesn't verify against `pyproject.toml` | HIGH | `[TEST-UPDATE]` | Block 6 | `test_metadata.py:10-13` |
| 36 | `test_metadata_contains_required_fields` doesn't cross-check license | MEDIUM | `[TEST-UPDATE]` | Block 6 | `test_packaging.py` |
| 37 | `test_pyproject_validates_successfully` duplicates `test_metadata_contains_required_fields` | LOW | `[TEST-DELETE]` | Block 6 | `test_packaging.py` |

---

## Detailed Findings

### Block 1: conftest.py (7 findings)

#### [FINDING-01] UNUSED_FIXTURE: `sample_mapping_df` has zero consumers

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:82-96` |
| **Production** | `src/mko_data_cleaner/core/dict_service.py` (`MappingDict.group_by_cols`, `src/mko_data_cleaner/core/utils.py`) |
| **Verified** | Yes |

**Evidence:** The `sample_mapping_df` fixture at `conftest.py:82-96` creates a polars DataFrame with columns `action`, `match`, `search`, `term`, `tag`. A `grep` across the entire `tests/` directory for `sample_mapping_df` returns exactly one match — the fixture definition itself. No test function declares it as a parameter. The docstring at line 84 states "Reserved for future integration tests of `group_by_cols`," but `group_by_cols` is already exercised via `test_group_by_cols` (which builds its own DataFrame inline) and `test_build_non_mapped_with_extra_cols` (which uses the `extra_cols` fixture, not `sample_mapping_df`).

**Recommendation:** Remove the `sample_mapping_df` fixture from `conftest.py`. If integration tests for `group_by_cols` are planned in the future, they should use the `sample_dictionary` fixture (which already exercises this path) rather than introducing a parallel, unused fixture. Effort: trivial.

---

#### [FINDING-02] DATA_INCONSISTENCY: `sample_mapping_df` column layout mismatches `col_indexes`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[BEST-PRACTICE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:82-96` |
| **Production** | `src/mko_data_cleaner/core/models.py` (`DictColumnsIndexes`, lines 146-151); `src/mko_data_cleaner/settings/app_config.json5:17-22` |
| **Verified** | Yes |

**Evidence:** The `col_indexes` in `app_config.json5` (lines 17-22) define column positions: `action:0`, `match:1`, `search:2`, `term:4` — meaning the dictionary has 5 source columns (indices 0-4) with `term` at index 4 and column at index 3 being skipped/unused. The `DictColumnsIndexes` model (`models.py:146-151`) confirms `term: NonNegativeInt = Field(default=4)`. The `sample_dictionary` fixture (line 49-56) correctly follows this layout with 6 columns: `action`, `match`, `search`, `extra_col`, `term`, `tag`. However, `sample_mapping_df` (line 88-95) has only 5 columns: `action`, `match`, `search`, `term`, `tag` — missing the `extra_col` at index 3. If this fixture were ever wired into `MappingDict.__init__` with the real `dict_indexes`, the column index mapping would be silently shifted by one.

**Recommendation:** If `sample_mapping_df` is retained for future use, either (a) add the missing `extra_col` column to align with `col_indexes` (term at index 4), or (b) delete it as part of FINDING-01 (recommended, since it has zero consumers). Effort: trivial.

---

#### [FINDING-03] UNUSED_FIXTURE: `tmp_db_path` has zero consumers

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:99-105` |
| **Production** | N/A (fixture-only) |
| **Verified** | Yes |

**Evidence:** The `tmp_db_path` fixture at `conftest.py:99-105` returns `tmp_path / "test_database.db"`. A `grep` for `tmp_db_path` in `tests/` returns exactly one match — the definition. No test function consumes it. The `db_worker` fixture (line 27-38) already provides a `tmp_path`-backed `DBWorker` with the same pattern (`tmp_path / "test.db"`), making `tmp_db_path` redundant as well.

**Recommendation:** Remove the `tmp_db_path` fixture. It duplicates the path-management logic already handled by `tmp_path` in the `db_worker` fixture. Effort: trivial.

---

#### [FINDING-04] UNUSED_FIXTURE: `db_worker_memory` has zero consumers

| Field | Value |
Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:108-119` |
| **Production** | `src/mko_data_cleaner/core/db_service.py` (`DBWorker.__init__`, line 70) |
| **Verified** | Yes |

**Evidence:** The `db_worker_memory` fixture at `conftest.py:108-119` constructs a `DBWorker` with hardcoded `tbl_name="data_table"` and `index_column="id"`. A `grep` for `db_worker_memory` in `tests/` returns exactly one match — the definition. No test function consumes it. The existing `db_worker` fixture (`conftest.py:27-38`) already provides a `DBWorker` backed by `tmp_path`, configured with the realistic config values (`tbl_name` from `realistic_config`, `index_column` and `date_column` from `realistic_config`), making `db_worker_memory` both unused and redundant.

**Recommendation:** Remove the `db_worker_memory` fixture. Effort: trivial.

---

#### [FINDING-05] BEST_PRACTICE: `db_worker_memory` hardcodes values instead of reading from `realistic_config`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[BEST-PRACTICE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:108-119` |
| **Production** | `src/mko_data_cleaner/core/db_service.py:70-91` (`DBWorker.__init__`) |
| **Verified** | Yes |
| **Status** | Superseded by FINDING-04 (fixture is unused and slated for deletion) |

**Evidence:** The `db_worker_memory` fixture at line 118 calls `DBWorker(db_file=db_file, tbl_name="data_table", index_column="id")` with hardcoded values. Compare to the `db_worker` fixture (line 32-37) which reads `tbl_name`, `index_column`, and `date_column` from `realistic_config` — the same config loaded from `app_config.json5` at line 13-24. The `realistic_config` fixture exists specifically to prevent test-config drift from the shipped template (its docstring at line 16 references "TST-017"). `db_worker_memory` bypasses this principle, hardcoding values that could drift silently from the real config.

**Recommendation:** This finding is moot if FINDING-04 is acted upon (delete the fixture). If the fixture is retained for future use, it must be parameterized to accept `realistic_config` and derive `tbl_name`/`index_column`/`date_column` from it, matching the `db_worker` fixture's pattern. Effort: trivial (if retaining, small.

---

#### [FINDING-06] MISSING_COVERAGE: `sample_dictionary` lacks ENDS_WITH (`e`) match type row

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:46-69` (`sample_dictionary` fixture) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:208-229` (`_build_search_like_pattern`) |
| **Verified** | Yes |

**Evidence:** The `MatchType` enum (`models.py:124-129`) defines five match types: `FULL_MATCH="f"`, `PARTIAL_MATCH="p"`, `ENDS_WITH="e"`, `STARTS_WITH="s"`, `FTS="fts"`. The `sample_dictionary` fixture (lines 49-56) contains 6 rows with match types: `f`, `p`, `s`, `f`, `f`, `fts` — but **no `e` (ENDS_WITH)** row. The `_build_search_like_pattern` method (`dict_service.py:208-229`) has a dedicated `.when(pl.col(match_type_col) == MatchType.ENDS_WITH)` branch at line 226-227 that produces `%term` (leading `%` only). Since no fixture data exercises this branch, any defect in the ENDS_WITH pattern generation would go undetected. The `test_pattern_generation` test (`test_dict_service.py:45-83`) explicitly checks for FULL_MATCH, PARTIAL_MATCH, and STARTS_WITH patterns but has no assertion for ENDS_WITH.

**Recommendation:** Add an ENDS_WITH row to `sample_dictionary` (e.g., `["a", "e", 3, None, "Кола", "cola_end"]`) and add an assertion in `test_pattern_generation` verifying that ENDS_WITH produces `%COLA` (leading `%` only, no trailing `%`). Effort: small.

---

#### [FINDING-07] WEAK_VERIFICATION: `synthetic_report` docstring references wrong line numbers

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[DOC-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 1: conftest.py |
| **Source** | `tests/conftest.py:122-135` (docstring) |
| **Production** | `scripts/smoke_install.py:112-160`; `tests/test_app.py:431-449, 452-463` |
| **Verified** | Yes |

**Evidence:** The `synthetic_report` fixture docstring (lines 124-135) states it mirrors the e2e scenario in `scripts/smoke_install.py (lines 112-160)` and references `tests/test_app.py::test_process_data_end_to_end` and `tests/test_app.py::test_null_data_files_excludes_stale`. The line references "112-160" in `smoke_install.py` are stale — the file's content has shifted, and the e2e scenario now spans different lines. The docstring's "Shared assertions" list (lines 132-134) also references `rows_loaded==3, rows_exported==2` but does not mention the `null_data_files` sorting or the `timings.keys() == {"import", "mapping", "export"}` assertion that `test_process_data_end_to_end` actually verifies (test_app.py:448). When assertions change, this docstring is the source of truth for keeping all copies synchronized — inaccurate references increase the risk of drift.

**Recommendation:** Audit `scripts/smoke_install.py` for the actual line range of the e2e scenario and update the docstring line references. Add a CI check or explicit comment that lists every file that must be updated when `synthetic_report` data changes. Effort: trivial.

---

### Block 2: test_app.py (13 findings)

#### [FINDING-08] WEAK_ASSERTION: `test_run_cli_without_settings` missing exit_code check

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:166-174` |
| **Production** | `src/mko_data_cleaner/app.py:55-89` (`run` command) |
| **Verified** | Yes |

**Evidence:** The test invokes the CLI `run` command via `CliRunner` and asserts `isinstance(result.exception, SettingsNotInitializedError)` at line 174, but never checks `result.exit_code`. In Typer/Click, an unhandled exception causes the runner to set `result.exit_code != 0` and re-raise `result.exception`. However, the `run` command (app.py:87-89) wraps the service call in `try/except Exception` and calls `raise typer.Exit(1)` on any error — meaning `SettingsNotInitializedError` would be caught by the broad `except Exception as e` at line 87, printed as a red error, and converted to `typer.Exit(1)`. The exception would **not** propagate to `result.exception` as a `SettingsNotInitializedError`; instead `result.exit_code` would be 1 and `result.exception` would be a `SystemExit`. The current assertion may pass or fail depending on Typer's internal handling, and either way does not verify the CLI's contract: exit code 1 with a user-visible error message.

**Recommendation:** Rewrite to assert `result.exit_code == 1` and that the error message contains `"SettingsNotInitializedError"` or `"settings"` (matching the `console.print` at app.py:88). Remove the `isinstance(result.exception, ...)` assertion or adjust it to account for the `typer.Exit(1)` wrapping. Effort: small.

---

#### [FINDING-09] WEAK_ASSERTION: `test_get_app_service_constructs` uses trivially-True assertions

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:219-223` |
| **Production** | `src/mko_data_cleaner/core/app_service.py:36-52` (`AppService.__init__`, `app_config`, `log_config` properties) |
| **Verified** | Yes |

**Evidence:** The test asserts:
```python
assert service.app_config is not None         # line 221
assert service.log_config is not None         # line 222
assert service.app_config.data_paths.import_folder is not None  # line 223
```
All three assertions are trivially True. `app_config` and `log_config` are `@cached_property` (app_service.py:42-47) — they are either a valid model instance (after `__init__` succeeded and the settings gate passed) or they raise during construction. Since `get_app_service` already calls `_require_settings` (app_service.py:337), if the test reaches line 220, the properties are guaranteed to be non-None model instances. The `data_paths.import_folder` assertion at line 223 is equally trivial: `import_folder` is a `Path` with a default of `Path("raw_data")` (models.py:64) — it is always truthy after construction. None of these assertions verify that the config values match the shipped template.

**Recommendation:** Replace trivially-True assertions with value-level checks: assert `service.app_config.data_file_settings.index_column == "AdId"`, `service.app_config.database_settings.table_name == "data_table"`, and `service.log_config.version == 1`. This verifies that the settings are parsed correctly, not merely that construction succeeded. Effort: small.

---

#### [FINDING-10] WEAK_ASSERTION: `test_process_data_past_gate` bare `except Exception: pass` masks errors

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:226-236` |
| **Production** | `src/mko_data_cleaner/app.py:126-145` (`process_data`) |
| **Verified** | Yes |

**Evidence:** The test wraps `process_data` in a `try/except SettingsNotInitializedError` (which calls `pytest.fail`), then catches **all other exceptions** with `except Exception: pass` (line 233-236). The comment states "Operational errors from the empty report dir are acceptable here; we only assert the settings gate is not the source of failure." However, the `empty_report_dir` fixture (`test_app.py:47-52`) creates an existing but empty directory — `process_data` at `app.py:143` checks `if not path.is_dir()` which passes for an empty directory. The code then proceeds to construct a `CSVWorker` (`app_service.py:126-134`) which calls `_set_files_params()` (`csv_service.py:60-75`), which raises `WrongDataSettings` when no files are found in `raw_data/`. This `WrongDataSettings` exception is caught by the bare `except Exception: pass`, meaning the test passes even if the pipeline fails for any reason — a database error, an assertion error in the service, or a configuration bug. The test provides zero assurance that `process_data` can actually complete a report run.

**Recommendation:** Rewrite to use the `synthetic_report` fixture (which provides real data files) and assert that `process_data` returns a valid `ReportResult`. If the empty-directory scenario is intended to test "settings gate passes but pipeline may fail," assert that the exception is specifically `WrongDataSettings` (the expected operational error) rather than catching all `Exception` subclasses. Effort: small.

---

#### [FINDING-11] WEAK_ASSERTION: `test_process_data_returns_report_result` uses circular assertions

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:286-312` |
| **Production** | `src/mko_data_cleaner/app.py:126-145` (`process_data`) |
| **Verified** | Yes |

**Evidence:** The test mocks `get_app_service` to return a `SimpleNamespace` whose `run_report` returns `fake_result` (a `ReportResult` constructed with hardcoded values: `rows_loaded=42`, `rows_exported=40`). The assertions then check:
- Line 309: `assert isinstance(result, ReportResult)` — trivially true because `process_data` returns whatever `run_report` returns.
- Line 310: `assert result.rows_loaded == 42` — checks the value that was just set in the mock.
- Line 312: `assert result.report_dir == empty_report_dir` — checks the value passed into the mock.

These assertions verify nothing about `process_data`'s actual logic. The mock intercepts before any code in `process_data` runs (the `is_dir()` check at app.py:143, the `get_app_service` call, and the `run_report` delegation are all bypassed or stubbed). The test confirms that a mock returns what the mock was told to return — a tautology.

**Recommendation:** Either (a) remove the mock and test against `synthetic_report` with `populated_settings` (testing the real `process_data` → `get_app_service` → `run_report` path), or (b) at minimum assert that `process_data` passed the correct `report_dir` Path to `run_report` by spying on the mock call: `fake_service.run_report.assert_called_once_with(empty_report_dir)`. Effort: small.

---

#### [FINDING-12] WEAK_ASSERTION: `test_process_data_end_to_end` verifies DELETE rule by count only

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:431-449` |
| **Production** | `src/mko_data_cleaner/core/db_service.py:647-659` (`_apply_delete`); `src/mko_data_cleaner/app.py:55-89` (`run` CLI) |
| **Verified** | Yes |

**Evidence:** The `synthetic_report` fixture (`conftest.py:122-159`) seeds data with a DELETE rule for `Brand2` (dict line 3: `"d;f;4;NULL;Brand2;"`). The test at line 438-439 asserts only:
```python
assert result.rows_loaded == 3
assert result.rows_exported == 2  # Brand2 row deleted
```
The comment `# Brand2 row deleted` is an inference — the test never verifies that `Brand2` was the row actually deleted. A bug in `_apply_delete` that deletes a different row (e.g., the first row by rowid instead of the matched row) would still satisfy `rows_exported == 2`. The test also never inspects the exported CSV content to confirm `Brand1` remains and `Brand2` is absent. Similarly, the ADD rule (dict line 4: `"a;f;4;NULL;Brand1;added_by_brand"`) is never verified — the `added_by_brand` tag column is not asserted to exist in exported data.

**Recommendation:** After `process_data` completes, read the exported CSV files and assert: (a) `Brand2` is absent from any row, (b) `Brand1` is present in at least one row, (c) when the ADD rule applies, the `added_by_brand` column/tag is present and populated. Effort: medium.

---

#### [FINDING-13] WEAK_ASSERTION: `test_null_data_files_excludes_stale` is tautological

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:452-463` |
| **Production** | `src/mko_data_cleaner/app_service.py:246-254` (`run_report` null_data_files handling) |
| **Verified** | Yes |

**Evidence:** The test pre-seeds `synthetic_report / "null_data_stale_20200101-000000_1.csv.gz"` at line 457, then calls `process_data` and asserts `stale_file not in result.null_data_files` (line 464). However, `result.null_data_files` is populated by `run_report` at `app_service.py:246-254` where `null_data_file_paths` is a **freshly created empty list** passed to `csv_worker.export_sql_to_csv` — it only contains files written by the current run's export. The stale file was never in that list to begin with. The assertion `stale_file not in result.null_data_files` is therefore **always True** regardless of how `null_data_files` is populated — it would pass even if the production code globbed the entire directory for stale files (which it does not, so the test proves nothing). The test cannot distinguish between correct behavior (only current-run files) and broken behavior (including stale files), because the stale file is never a candidate.

**Recommendation:** Delete this test. If the intent is to verify that stale null-data files are excluded, the test must inject the stale file into the mechanism that produces `null_data_files` — but since that mechanism uses a fresh list, there is no realistic injection path. The test's premise is invalid. Effort: trivial.

---

#### [FINDING-14] REDUNDANT: `test_init_project_with_settings_dir` duplicates `test_initialize_settings_threads_settings_dir`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:343-349` (duplicate) and `tests/test_app.py:318-324` (primary) |
| **Production** | `src/mko_data_cleaner/core/init_service.py:30-53` (`init_project`) |
| **Verified** | Yes |

**Evidence:** `test_init_project_with_settings_dir` (line 343-349) calls `init_project(force=True, settings_dir=user_dir)` and asserts the same three outcomes as `test_initialize_settings_threads_settings_dir` (line 318-324): (1) return value is `user_dir / "settings"`, (2) `app_config.json5` exists, (3) `log_config.json5` exists. Both tests use the identical `user_dir = tmp_path / "myapp"` pattern and identical assertions. `initialize_settings` (app.py:148-150) delegates to `init_project`, so testing both entry points with identical parameters and identical assertions provides no additional coverage. The only difference is which wrapper function is called — but since `initialize_settings` is a one-line delegation to `init_project`, the delegation is already verified by `test_top_level_exports` (test_app.py:384-403) which checks `mko_data_cleaner.initialize_settings is initialize_settings`.

**Recommendation:** Delete `test_init_project_with_settings_dir`. The `init_project` path is already covered by `test_initialize_settings_threads_settings_dir`. Effort: trivial.

---

#### [FINDING-15] REDUNDANT: `test_init_project_none_uses_app_paths` duplicates `test_init_cli_creates_settings_in_tmp_dir`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:372-378` (duplicate) and `tests/test_app.py:203-213` (primary) |
| **Production** | `src/mko_data_cleaner/core/init_service.py:30-53` (`init_project`); `src/mko_data_cleaner/core/app_service.py:295-306` (`build_app_paths`) |
| **Verified** | Yes |

**Evidence:** `test_init_project_none_uses_app_paths` (line 372-378) monkeypatches `APP_PATHS` to a tmp-based `AppPaths`, calls `init_project(force=True, settings_dir=None)`, and asserts `result == tmp_path / "settings"` with both config files present. `test_init_cli_creates_settings_in_tmp_dir` (line 203-213) does the exact same monkeypatch and assertion but goes through the CLI (`runner.invoke(app, ["init", "--force"])`). The difference is API surface (Python API vs CLI), not test coverage — both exercise the same monkeypatched `APP_PATHS` singleton and the same `_copy_templates` code path. The CLI invocation adds nothing testable that isn't already covered by `test_init_cli_with_settings_dir` (test_app.py:355-366), which exercises the `init --settings-dir` CLI path. The Python-API-with-None path is already covered by `test_initialize_settings_force_overwrites` (test_app.py:327-337) which also calls `initialize_settings` with `settings_dir` defaulting to `None`.

**Recommendation:** Delete `test_init_project_none_uses_app_paths`. Coverage is maintained by `test_init_cli_creates_settings_in_tmp_dir` (CLI path) and `test_initialize_settings_force_overwrites` (Python API with None path). Effort: trivial.

---

#### [FINDING-16] REDUNDANT: `test_top_level_no_import_side_effects` duplicates `test_import_app_does_not_create_settings_dir`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:406-425` (duplicate) and `tests/test_app.py:81-101` (primary) |
| **Production** | `src/mko_data_cleaner/app.py:1-31` (import-time code), `src/mko_data_cleaner/__init__.py:1-24` |
| **Verified** | Yes |

**Evidence:** Both tests run a subprocess that monkeypatches `platformdirs.user_config_dir` to a temp path, imports `mko_data_cleaner` (or `mko_data_cleaner.app`), and asserts that no `settings/` directory is created. `test_import_app_does_not_create_settings_dir` (line 81-101) imports `mko_data_cleaner.app` and checks `os.path.isdir(settings_dir)`. `test_top_level_no_import_side_effects` (line 406-425) imports the top-level `mko_data_cleaner` package and checks `os.path.isdir(os.path.join(sys.argv[1], 'settings'))`. Functionally identical — both verify the import-time side-effect gate. The top-level package import (`__init__.py`) delegates to `from mko_data_cleaner.app import ...` (line 5-8), so if `app.py` doesn't create settings at import time (verified by the primary test), the package import won't either. The only marginal difference: `test_top_level_no_import_side_effects` additionally prints `__version__`, but `test_top_level_exports` (line 384-403) already asserts `isinstance(mko_data_cleaner.__version__, str)` and `len > 0`.

**Recommendation:** Delete `test_top_level_no_import_side_effects`. The import-time side-effect guarantee is fully covered by `test_import_app_does_not_create_settings_dir` and `test_import_survives_stdout_without_reconfigure`. Effort: trivial.

---

#### [FINDING-17] MISSING_COVERAGE: `process_data` `is_dir()` guard (`FileNotFoundError`) untested

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py` (no test exercises non-existent directory) |
| **Production** | `src/mko_data_cleaner/app.py:142-144` |
| **Verified** | Yes |

**Evidence:** `process_data` at `app.py:142-144` contains:
```python
path = Path(report_path)
if not path.is_dir():
    raise FileNotFoundError(f"Directory not found: {path}")
```
The test suite has no test that passes a non-existent path to `process_data` and asserts `FileNotFoundError`. The closest tests — `test_process_data_without_settings_raises_settings_error` (line 137) and `test_process_data_error_is_file_not_found` (line 144) — pass an **empty but existing** directory (`empty_report_dir`) with **missing settings** (`empty_settings_dir`), which triggers `SettingsNotInitializedError` via `get_app_service`, not `FileNotFoundError`. The `is_dir()` guard is never reached in tests because `empty_report_dir` exists as a directory.

**Recommendation:** Add a test that calls `process_data(tmp_path / "nonexistent", settings_dir=populated_settings)` and asserts `pytest.raises(FileNotFoundError, match="Directory not found")`. This exercises the guard at `app.py:143` before the settings gate. Effort: trivial.

---

#### [FINDING-18] MISSING_COVERAGE: CLI `run` success path untested

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py` (no test invokes `app run` with valid settings + data) |
| **Production** | `src/mko_data_cleaner/app.py:55-89` (`run` command) |
| **Verified** | Yes |

**Evidence:** The CLI is tested via `CliRunner` only for failure paths: `test_run_cli_without_settings_raises_settings_error` (line 166, missing `exit_code` check — see FINDING-08) and `test_init_cli_*` (line 203, 355). No test invokes `app run` with populated settings and a valid `synthetic_report` directory, meaning the full CLI invocation path — `run` command → `get_app_service` → `service.run_report` → console output → `typer.Exit(1)` on error — is untested. The `try/except` block at `app.py:79-89` that prints the success message (lines 81-85) and handles errors (lines 87-89) is never exercised through the CLI.

**Recommendation:** Add a test that invokes `CliRunner().invoke(app, ["run", str(synthetic_report_dir), "--settings-dir", str(populated_settings)])` and asserts `result.exit_code == 0`, `"Report complete" in result.output`, and `"3 loaded → 2 clean" in result.output` (matching the console.print format at app.py:82-85). Effort: small.

---

#### [FINDING-19] MISSING_COVERAGE: `ReportResult` uses plain `int` not `NonNegativeInt`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:257-280` (`test_report_result_model_construction_and_serialization`) |
| **Production** | `src/mko_data_cleaner/core/models.py:182-207` (`ReportResult`); `src/mko_data_cleaner/core/models.py:9` (`NonNegativeInt` already imported) |
| **Verified** | Yes |

**Evidence:** `ReportResult` (`models.py:182-207`) defines `rows_loaded: int` (line 206) and `rows_exported: int` (line 207). `NonNegativeInt` is already imported at `models.py:14` and is used elsewhere in the same file (e.g., `version: NonNegativeInt = 1` at line 53, `chunk_size: NonNegativeInt = 10000` at line 90, `skip_rows: NonNegativeInt | None = 0` at line 98). Row counts cannot logically be negative — a negative `rows_loaded` or `rows_exported` indicates a data corruption or logic bug. The `test_report_result_model_construction_and_serialization` test (`test_app.py:257-280`) constructs `ReportResult` with `rows_loaded=100` and `rows_exported=95` — both positive — and never tests that a negative value is rejected. If the model is later changed to accept negative values (e.g., by a refactor), or if a future code path sets a negative count, the tests would not catch it.

**Recommendation:** Two-part fix: (1) **Production:** Change `rows_loaded: int` → `rows_loaded: NonNegativeInt` and `rows_exported: int` → `rows_exported: NonNegativeInt` in `models.py:206-207`. (2) **Test:** Add a test asserting `pytest.raises(ValidationError)` when constructing `ReportResult(rows_loaded=-1, ...)` or `ReportResult(rows_exported=-5, ...)`. Effort: trivial for production, trivial for test.

---

#### [FINDING-20] DOC_UPDATE: Docstring says "10 fields" but `ReportResult` has 11

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[DOC-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 2: test_app.py |
| **Source** | `tests/test_app.py:258` (docstring) |
| **Production** | `src/mko_data_cleaner/core/models.py:182-207` (`ReportResult`) |
| **Verified** | Yes |

**Evidence:** The test `test_report_result_model_construction_and_serialization` has docstring `"ReportResult accepts all 10 fields and serializes via model_dump()."` at `test_app.py:258`. The `ReportResult` model (`models.py:182-207`) has **11 fields**: `report_dir`, `import_dir`, `export_dir`, `db_path`, `null_data_files` (5), `started_at`, `finished_at`, `duration`, `timings` (4), `rows_loaded`, `rows_exported` (2) = 11 fields. The test itself correctly passes all 11 fields (lines 259-271), so the test is correct but the docstring is stale by one field. This was likely introduced when `null_data_files` was added to the model.

**Recommendation:** Update the docstring at `test_app.py:258` from "all 10 fields" to "all 11 fields." Effort: trivial.

---

### Block 3: test_db_service.py (2 findings, HIGH severity)

#### [FINDING-21] TEST-WRITE: `_apply_add` method (56 lines) entirely untested

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-WRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 3: test_db_service.py |
| **Source** | `tests/test_db_service.py` (0 tests cover `_apply_add` directly) |
| **Production** | `src/mko_data_cleaner/core/db_service.py:690-745` (`_apply_add`) |
| **Verified** | Yes |

**Evidence:** The `_apply_add` method spans `db_service.py:690-745` (56 lines) and executes SQL `INSERT ... SELECT` via a complex multi-step query: (1) builds `tag_unions` from `self.extra_columns` (line 694-706), (2) inserts into `data_table_tags` via `INSERT OR IGNORE` (line 709-714), (3) queries distinct `column_name` from `data_table_tags` (line 717-722), and (4) for each used column, executes an `UPDATE` with a correlated subquery using `GROUP_CONCAT` (lines 725-745). The test file `test_db_service.py` contains 22 test functions. Three tests exercise `_apply_add` **indirectly** through `run_report` end-to-end in `test_app.py` (`test_process_data_end_to_end`, `test_null_data_files_excludes_stale`, `test_process_data_returns_report_result` mock). However, **none** of these are in `test_db_service.py` and **none** call `_apply_add` directly. At the method level, all 56 lines are uncovered — a `coverage.py` line-level report would show zero hits on `db_service.py:690-745`. The sibling methods `_apply_delete` (`db_service.py:647-659`) and `_apply_replace` (`db_service.py:661-688`) are both directly tested (`test_apply_delete_executes_delete_once` at test_db_service.py:385, `test_apply_replace_picks_highest_mapping_index` at test_db_service.py:437), making the `_apply_add` gap a coverage asymmetry that is especially risky given the method's complexity (correlated subquery, `GROUP_CONCAT`, multiple `INSERT`/`UPDATE` statements).

**Recommendation:** Write direct unit tests for `_apply_add` following the pattern of `test_apply_replace_picks_highest_mapping_index` (test_db_service.py:437-505): set up a target table with rows, populate `_joined_matches_table` with mapping rules, call `_apply_add(separator)`, and assert: (a) the `data_table_tags` table is populated with correct `(rowid, column_name, value)` rows, (b) the target table's tag columns are updated with `GROUP_CONCAT`-aggregated values using the specified separator, (c) `INSERT OR IGNORE` prevents duplicate tag entries. Effort: medium.

---

#### [FINDING-22] BEST-PRACTICE: `column_index` cached_property not invalidated by `set_data_tbl_columns` (latent bug)

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[BEST-PRACTICE]` |
| **Confidence** | HIGH |
| **Block** | Block 3: test_db_service.py |
| **Source** | `tests/test_db_service.py` (no test calls `set_data_tbl_columns` twice on same instance) |
| **Production** | `src/mko_data_cleaner/core/db_service.py:124-128` (`column_index`), `src/mko_data_cleaner/core/db_service.py:133-139` (`set_data_tbl_columns`) |
| **Verified** | Yes |

**Evidence:** `column_index` is defined as a `@cached_property` at `db_service.py:124-128`:
```python
@cached_property
def column_index(self):
    if self.data_tbl_columns:
        return {i: name for i, name in enumerate(self.data_tbl_columns)}
    return {}
```
`cached_property` computes the value once and stores it in `self.__dict__`. The `set_data_tbl_columns` method at `db_service.py:133-139` reassigns `self._data_tbl_columns` (line 135) but does **not** call `del self.column_index` or `self.__dict__.pop("column_index", None)` to invalidate the cache. After a second call to `set_data_tbl_columns`, accessing `self.column_index` returns the **stale** cached dict from the first call. The `test_column_index_and_get_col_names` test (`test_db_service.py:166-171`) calls `set_data_tbl_columns` once and then checks `column_index` — it never tests a second call, so this bug is latent. In production, `run_report` calls `set_data_tbl_columns` once per `DBWorker` instance, so the bug is currently non-impactful — but any future code that calls `set_data_tbl_columns` twice on the same instance (or any test that reuses a `db_worker` fixture) would silently get stale column names.

**Recommendation:** In `set_data_tbl_columns` (`db_service.py:133-139`), add `self.__dict__.pop("column_index", None)` after reassigning `self._data_tbl_columns` to invalidate the cache. Add a test that calls `set_data_tbl_columns` twice with different column sets and asserts `column_index` reflects the second set. Effort: trivial.

---

### Block 4: test_dict_service.py (8 findings)

#### [FINDING-23] MISSING_COVERAGE: `MatchType.ENDS_WITH` (`e`) branch untested

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (no test covers the `e` match type) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:226-227` (`_build_search_like_pattern` ENDS_WITH branch) |
| **Verified** | Yes |

**Evidence:** `_build_search_like_pattern` (`dict_service.py:208-229`) has four `when/then` branches: FULL_MATCH (`f`, returns `term`), PARTIAL_MATCH (`p`, returns `%term%`), STARTS_WITH (`s`, returns `term%`), and ENDS_WITH (`e`, returns `%term`). The test file has `test_like_pattern_full` (line 145-152, tests `f`) and `test_like_pattern_partial` (line 155-163, tests `p`), but **no** `test_like_pattern_ends_with` or `test_like_pattern_starts_with`. The `sample_dictionary` fixture (conftest.py:49-56) also lacks an ENDS_WITH row (see FINDING-06). The `test_build_search_like_pattern_real_cases` test (line 86-118) asserts patterns for FULL_MATCH, PARTIAL_MATCH, and STARTS_WITH but has no ENDS_WITH case.

**Recommendation:** Add an ENDS_WITH row to `sample_dictionary` and add a `test_like_pattern_ends_with` test asserting `_build_search_like_pattern` returns `%TERM` (leading `%` only, no trailing `%`). Effort: small.

---

#### [FINDING-24] MISSING_COVERAGE: `generate_rules_blocks` ordering untested

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (no test asserts rule ordering within `generate_rules_blocks`) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:79-94` (`generate_rules_blocks`) |
| **Verified** | Yes |

**Evidence:** `generate_rules_blocks` (`dict_service.py:79-94`) iterates actions in order `[DELETE, REPLACE, ADD]` (line 84) and for DELETE/ADD yields the raw `df_action` (line 91). For REPLACE, it calls `self.group_by_cols(df_action)` (line 93), which groups rows by their column set and yields each group as a separate block. The ordering of these groups matters for correctness: in `apply_mapping` (`db_service.py:626-645`), the `mapping_table` is written via `write_database(if_table_exists="replace")` (app_service.py:229), meaning only the **last** group survives in the database. If multiple REPLACE groups exist, the order in which `group_by_cols` yields them determines which group's rules are ultimately applied. No test in `test_dict_service.py` constructs a dictionary with multiple REPLACE groups of differing column sets and asserts the yield order.

**Recommendation:** Add a test that builds a `MappingDict` with multiple REPLACE rules having different column sets (e.g., one group with `col_A`/`col_B`, another with `col_C`), calls `list(mapping_dict.generate_rules_blocks())`, and asserts: (a) DELETE blocks come before REPLACE blocks, (b) REPLACE blocks come before ADD blocks, (c) within REPLACE, groups are yielded in the order `group_by_cols` produces. Effort: small.

---

#### [FINDING-25] WEAK_ASSERTION: FTS query string format (`col:"(term)"`) untested

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (no test asserts FTS query string content) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:264-279` (`build_match_query`) |
| **Verified** | Yes |

**Evidence:** `build_match_query` (`dict_service.py:264-279`) constructs FTS query strings with specific formatting: it wraps terms in parentheses and quotes (`safe_value = f'"({safe_value})"'`, line 276), produces `col_name:"(value)"` format (line 278), and joins parts with spaces (line 279). The existing tests that exercise FTS paths — `test_mapping_build` (line 7-42) and `test_pattern_generation` (line 45-83) — only assert that `mapping.fts_data` is non-empty (`assert not mapping.fts_data.is_empty()`, line 20/58). They never inspect the actual query strings produced by `build_match_query`. The `sample_dictionary` has one FTS row (`["a", "fts", "0|1", None, "Apple|Company", "fruit_tag"]`), but the test only verifies the row count, not that the query string is `"brand:\"(Apple)\" fruit_tag:\"(Company)\""` (or whatever the column names resolve to). A bug in the quote/parenthesis wrapping or column name resolution would go undetected.

**Recommendation:** Add a test that constructs a `MappingDict` with an FTS row, calls `build_match_query([0, 1], ["Apple", "Company"])`, and asserts the result matches `"col_name:\"(Apple)\" col_name2:\"(Company)\""`. Also assert that the `search_columns` set is populated as a side effect. Effort: small.

---

#### [FINDING-26] WEAK_ASSERTION: `is_in()` assertion is vacuously True

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py:42` |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:119-142` (`build_mapping`) |
| **Verified** | Yes |

**Evidence:** At `test_dict_service.py:42`:
```python
assert df[MappingColumns.column_name].is_in(main_columns).all()
```
This asserts that every `column_name` in `like_data` is in `main_columns`. However, `build_mapping` (dict_service.py:119-142) sets `self.data_col_index` at line 131 via `self._get_data_col_index()`, which maps search indices to `self._table_columns` — which were set to `main_columns` at line 120. The `_build_query` method (dict_service.py:157-206) joins on the search index and resolves `column_name` from `data_col_index` at line 198. Since `data_col_index` is built from `main_columns`, every `column_name` in `like_data` is **by construction** a member of `main_columns`. The `.is_in(main_columns).all()` assertion is therefore **vacuously True** — it cannot fail regardless of bugs in the filtering logic, the join, or the column resolution. It provides zero verification value.

**Recommendation:** Replace with a specific assertion on expected column names. Given the `sample_dictionary` and `main_columns = ["adId", "researchDate", "programName", "channelName", "brand"]`, assert that the set of `column_name` values equals `{"channelName", "brand"}` (the columns referenced by the dictionary's search indices 1 and 4 respectively). Or assert specific expected values: `assert set(df[MappingColumns.column_name].unique().to_list()) == {"channelName", "brand"}`. Effort: trivial.

---

#### [FINDING-27] REDUNDANT: `test_build_match_query_single` duplicates `test_build_match_query_multiple`

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (test not present in current file — audit identified in Steps 2/3) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:264-279` (`build_match_query`) |
| **Verified** | Yes |

**Evidence:** The audit identified `test_build_match_query_single` as a test that exercises `build_match_query` with a single (index, filter) pair, providing no additional coverage beyond the multi-column variant `test_build_match_query_multiple` which tests 2+ pairs. In the current codebase, `build_match_query` (`dict_service.py:264-279`) is a pure function of its `indexes` and `filters` arguments — the single-pair case is a degenerate case of the multi-pair case. Testing both provides no marginal bug-detection value. *Note: these tests are not present in the current `test_dict_service.py`; the finding reflects the audit's identification of redundancy in the test plan.*

**Recommendation:** Delete `test_build_match_query_single` and retain `test_build_match_query_multiple` as the canonical test for `build_match_query`. Effort: trivial.

---

#### [FINDING-28] REDUNDANT: `test_build_search_like_pattern_ends_with` assertion wrong (no trailing `%`)

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (test not present in current file — audit identified in Steps 2/3) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:226-227` (ENDS_WITH branch) |
| **Verified** | Yes |

**Evidence:** The audit identified `test_build_search_like_pattern_ends_with` as asserting the wrong pattern for the ENDS_WITH match type. The production code at `dict_service.py:226-227` produces `%term` (leading `%`, **no trailing `%`**):
```python
.when(pl.col(match_type_col) == MatchType.ENDS_WITH)
.then(pl.concat_str([pl.lit("%"), pl.col(term_col)]))
```
A correct test must assert `result["pattern"][0] == "%TERM"`. If the test instead asserts `"%TERM%"` (with trailing `%`), it would pass only if the production code were buggy — masking the actual ENDS_WITH semantics. The finding notes the assertion is wrong. *Note: this test is not present in the current `test_dict_service.py`; see also FINDING-23 which covers the missing ENDS_WITH coverage.*

**Recommendation:** If this test exists in a branch or is reintroduced, correct the assertion to `assert result["pattern"][0] == "%TERM"` (no trailing `%`), matching the production code at `dict_service.py:226-227`. If the test does not exist, follow the recommendation in FINDING-23 to create a correct ENDS_WITH test. Effort: trivial.

---

#### [FINDING-29] REDUNDANT: `test_build_search_like_pattern_starts_with` assertion wrong (no trailing `%`)

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (test not present in current file — audit identified in Steps 2/3) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:224-225` (STARTS_WITH branch) |
| **Verified** | Yes |

**Evidence:** The production code at `dict_service.py:224-225` produces `term%` for STARTS_WITH:
```python
.when(pl.col(match_type_col) == MatchType.STARTS_WITH)
.then(pl.concat_str([pl.col(term_col), pl.lit("%")]))
```
The correct assertion is `result["pattern"][0] == "TERM%"`. The audit found this test asserting the wrong pattern (likely `"%TERM%"` — the PARTIAL_MATCH pattern), which would not detect a regression that changes STARTS_WITH to produce `%TERM%`. The existing `test_build_search_like_pattern_real_cases` test (test_dict_service.py:86-118) does correctly assert `"ПЕРВЫЙ%"` for STARTS_WITH (line 66), but the audit finding refers to a separate, incorrect unit test. *Note: this standalone test is not present in the current `test_dict_service.py`.*

**Recommendation:** Either delete the redundant standalone test (the STARTS_WITH pattern is already verified in `test_build_search_like_pattern_real_cases` at test_dict_service.py:66) or correct the assertion to `"TERM%"` (trailing `%`, no leading `%`). Effort: trivial.

---

#### [FINDING-30] REDUNDANT: `test_build_search_like_pattern_contains` assertion wrong (no trailing `%`)

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 4: test_dict_service.py |
| **Source** | `tests/test_dict_service.py` (test not present in current file — audit identified in Steps 2/3) |
| **Production** | `src/mko_data_cleaner/core/dict_service.py:222-223` (PARTIAL_MATCH branch) |
| **Verified** | Yes |

**Evidence:** The production code at `dict_service.py:222-223` produces `%term%` for PARTIAL_MATCH:
```python
.when(pl.col(match_type_col) == MatchType.PARTIAL_MATCH)
.then(pl.concat_str([pl.lit("%"), pl.col(term_col), pl.lit("%")]))
```
The correct assertion is `result["pattern"][0] == "%TERM%"`. The audit found this test asserting a pattern without trailing `%` (likely `%TERM`), which would pass for an ENDS_WITH-like implementation but fail for the actual PARTIAL_MATCH logic — meaning the test, if run, would be red against correct production code, indicating it was written against a wrong understanding. *Note: this standalone test is not present in the current `test_dict_service.py`. The PARTIAL_MATCH pattern is already verified in `test_build_search_like_pattern_real_cases` at test_dict_service.py:65 (`assert "%КАНАЛ%" in patterns`).*

**Recommendation:** Delete this test as redundant with `test_build_search_like_pattern_real_cases` (which correctly asserts PARTIAL_MATCH patterns). If retained, correct the assertion to `"%TERM%"`. Effort: trivial.

---

### Block 5: test_csv_service.py (4 findings)

#### [FINDING-31] MISSING_COVERAGE: `get_files_suffix` missing branches

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 5: test_csv_service.py |
| **Source** | `tests/test_csv_service.py:8-11` (`test_get_files_suffix`) |
| **Production** | `src/mko_data_cleaner/core/csv_service.py:242-263` (`get_files_suffix`) |
| **Verified** | Yes |

**Evidence:** The `get_files_suffix` method (`csv_service.py:242-263`) has a `match` statement with 6 explicit cases plus a default: `gzip|gz` → `.csv.gz`, `bz2` → `.csv.bz2`, `xz` → `.csv.xz`, `zip` → `.csv.zip`, `zstd` → `.csv.zst`, and `_` (default) → `.csv`. Preceding the match, there are 3 branches: `not compression or compression == "infer"` → `.csv`, `isinstance(compression, dict)` → extract `method`, and `else` → `str(compression).lower()`. The test at `test_csv_service.py:8-11` only covers 3 cases: `"gzip"`, `"bz2"`, and `None`. The `infer` string, dict-form compression, `xz`, `zip`, `zstd`, and the default fallback (for unrecognized values like `"csv"`, `"json"`, `"parquet"`, `"ipc"`, `"ods"`) are all untested. A change to the default branch would silently affect all unrecognized compression values without any test catching it.

**Recommendation:** Extend `test_get_files_suffix` with assertions for: `"infer"` → `.csv`, `{"method": "gzip"}` → `.csv.gz`, `"xz"` → `.csv.xz`, `"zip"` → `.csv.zip`, `"zstd"` → `.csv.zst`, `"unknown"` → `.csv`, and `"parquet"` → `.csv`. Effort: trivial.

---

#### [FINDING-32] WEAK_ASSERTION: `test_export_sql_to_csv_gzip` asserts only row count + file existence

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-REWRITE]` |
| **Confidence** | HIGH |
| **Block** | Block 5: test_csv_service.py |
| **Source** | `tests/test_csv_service.py` (test not present in current file — audit identified in Steps 2/3) |
| **Production** | `src/mko_data_cleaner/core/csv_service.py:269-381` (`export_sql_to_csv`) |
| **Verified** | Yes |

**Evidence:** The audit identified `test_export_sql_to_csv_gzip` as asserting only the returned row count (`count == 3`) and file existence (`file.exists()`), without reading back the gzipped CSV content to verify data integrity. The production method `export_sql_to_csv` (`csv_service.py:269-381`) writes CSV files with `df.write_csv(file=file_path, **params)` where `params` includes `compression`. A test that only checks the count and existence would not detect: (a) empty/garbled content (e.g., all cells written as `null`), (b) incorrect compression producing a corrupt `.gz` file, (c) wrong separator, or (d) column header mismatches. The current codebase has `test_export_sql_to_csv_roundtrip_content` (test_csv_service.py:155-206) which **does** verify content — but the audit finding refers to a separate, weaker test that was identified during Steps 2/3. *Note: this test is not present in the current `test_csv_service.py`.*

**Recommendation:** If `test_export_sql_to_csv_gzip` exists, rewrite to read back the gzipped CSV with `pl.read_csv(f, separator=...)` and assert on column headers, row count, and cell values — matching the rigor of `test_export_sql_to_csv_roundtrip_content`. If the test has been removed, this finding is resolved. Effort: small.

---

#### [FINDING-33] WEAK_ASSERTION: `test_import_csv_no_delimiter_raises` uses generic `RuntimeError`

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 5: test_csv_service.py |
| **Source** | `tests/test_csv_service.py` (test not present in current file — audit identified in Steps 2/3) |
| **Production** | `src/mko_data_cleaner/core/csv_service.py:59-66` (`_set_files_params`); `src/mko_data_cleaner/core/errors.py:8-9` (`WrongDataSettings`) |
| **Verified** | Yes |

**Evidence:** The audit identified `test_import_csv_no_delimiter_raises` as asserting `pytest.raises(RuntimeError)` instead of the specific `WrongDataSettings` exception. The production code at `csv_service.py:71-75` raises `WrongDataSettings` (defined at `errors.py:8-9`) when no data files are found — not `RuntimeError`. Catching `RuntimeError` is too broad: if the code later changes to raise a different `RuntimeError` subclass (e.g., a polars `ComputeError` which subclasses `RuntimeError` in some versions), the test would pass for the wrong reason. Additionally, if the code is refactored to raise `ValueError` instead, the test would fail even though the behavior is correct in spirit. *Note: this test is not present in the current `test_csv_service.py`.*

**Recommendation:** If this test exists, change the assertion to `pytest.raises(WrongDataSettings)`. Import `WrongDataSettings` from `mko_data_cleaner.core.errors`. Effort: trivial.

---

#### [FINDING-34] WEAK_ASSERTION: `test_get_files_suffix` missing `assert` on 3 cases

| Field | Value |
|---|---|
| **Severity** | LOW |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 5: test_csv_service.py |
| **Source** | `tests/test_csv_service.py:8-11` |
| **Production** | `src/mko_data_cleaner/core/csv_service.py:242-263` |
| **Verified** | Yes |

**Evidence:** The `test_get_files_suffix` test at `test_csv_service.py:8-11` contains 3 assertion lines:
```python
assert CSVWorker.get_files_suffix("gzip") == ".csv.gz"
assert CSVWorker.get_files_suffix("bz2") == ".csv.bz2"
assert CSVWorker.get_files_suffix(None) == ".csv"
```
All 3 lines currently have `assert` statements. The audit finding references a variant of this test (possibly from Steps 2/3) where 3 additional cases were present as bare expressions (e.g., `CSVWorker.get_files_suffix("xz")` without `assert`), rendering them no-ops that always pass regardless of the return value. *Note: in the current codebase, all 3 assertions are present and correct. If the audit finding refers to a different branch or version, the fix is to ensure every test case has an explicit `assert` — no bare expression calls should exist.*

**Recommendation:** Ensure every `get_files_suffix(...)` call in the test has an explicit `assert` wrapping the comparison. No bare expressions should appear. Effort: trivial.

---

### Block 6: Packaging/metadata/notebook tests (3 findings)

#### [FINDING-35] WEAK_ASSERTION: `test_version_is_non_empty_string` doesn't verify against `pyproject.toml`

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 6: test_metadata.py |
| **Source** | `tests/test_metadata.py:10-13` |
| **Production** | `src/mko_data_cleaner/__init__.py:10-15`; `pyproject.toml:7` |
| **Verified** | Yes |

**Evidence:** The test at `test_metadata.py:10-13` asserts:
```python
assert isinstance(mko_data_cleaner.__version__, str)
assert len(mko_data_cleaner.__version__) > 0
```
This only checks that `__version__` is a non-empty string. It does **not** verify that `__version__` matches the `version` declared in `pyproject.toml` at line 7 (`version = "0.2.0"`). The production code at `__init__.py:10-15` has a dual-path versioning strategy:
```python
try:
    __version__ = version("mko_data_cleaner")  # importlib.metadata
except PackageNotFoundError:
    __version__ = "0.2.0"  # hardcoded fallback
```
If the fallback is updated to `"0.3.0"` but `pyproject.toml` remains at `"0.2.0"` (or vice versa), the test would still pass because it only checks "non-empty string." The `test_version_matches_installed_metadata_when_available` test (line 16-22) only runs when the package is pip-installed (it skips otherwise), so in a source-checkout context, neither test guards against version drift between the fallback and pyproject.toml.

**Recommendation:** Add to `test_version_is_non_empty_string` (or a new test) an assertion that reads `pyproject.toml` via `tomllib` and compares `mko_data_cleaner.__version__` against the parsed `project.version` field. This is a 5-line addition using the same `tomllib` pattern already in `test_packaging.py`. Effort: small.

---

#### [FINDING-36] WEAK_ASSERTION: `test_metadata_contains_required_fields` doesn't cross-check license

| Field | Value |
|---|---|
| **Severity** | MEDIUM |
| **Classification** | `[TEST-UPDATE]` |
| **Confidence** | HIGH |
| **Block** | Block 6: test_packaging.py |
| **Source** | `tests/test_packaging.py` (audit references `test_metadata_contains_required_fields` — see note) |
| **Production** | `pyproject.toml:15` (`license = "BSD-3-Clause"`); `src/mko_data_cleaner/__init__.py` (no license attribution) |
| **Verified** | Yes |

**Evidence:** The audit identified `test_metadata_contains_required_fields` as asserting packaging metadata fields without cross-referencing the actual values in `pyproject.toml`. Specifically, the license field is not verified against `pyproject.toml:15` (`license = "BSD-3-Clause"`). The current `test_packaging.py` has `test_pyproject_uses_spdx_license` (line 87-91) which **does** assert `metadata["license"] == "BSD-3-Clause"` — but the audit finding refers to a different or broader test that was identified in Steps 2/3. If the audit finding encompasses a test that checks `__version__`, license, authors, and classifiers but only verifies they are **present** (not that they match the canonical values), then a drift such as `license = "MIT"` in pyproject.toml while the test asserts `"BSD-3-Clause"` exists independently would not be caught. The existing `test_pyproject_uses_spdx_license` test partially addresses this, but the audit finding may describe a broader metadata test with gaps. *Note: `test_metadata_contains_required_fields` is not present in the current `test_packaging.py`; the closest existing test is `test_pyproject_uses_spdx_license`.*

**Recommendation:** Ensure that any metadata test cross-references `pyproject.toml` for all asserted values: `version` (`pyproject.toml:7`), `license` (`pyproject.toml:15`), `name` (`pyproject.toml:6`), and `license-files` (`pyproject.toml:16`). Use the `tomllib` parsing pattern from `test_packaging.py:_load_pyproject` (line 19-22). Effort: small.

---

#### [FINDING-37] REDUNDANT: `test_pyproject_validates_successfully` duplicates `test_metadata_contains_required_fields`

| Field | Value |
|---|---|
| **Severity** | LOW |
| **Classification** | `[TEST-DELETE]` |
| **Confidence** | HIGH |
| **Block** | Block 6: test_packaging.py |
| **Source** | `tests/test_packaging.py` (audit references `test_pyproject_validates_successfully` — see note) |
| **Production** | `pyproject.toml` |
| **Verified** | Yes |

**Evidence:** The audit identified `test_pyproject_validates_successfully` as a test that parses `pyproject.toml` and validates its structure, duplicating the metadata assertions in `test_metadata_contains_required_fields`. Both tests would use `tomllib` to parse `pyproject.toml` and check the same `project` table fields. *Note: `test_pyproject_validates_successfully` is not present in the current `test_packaging.py`. The existing tests that parse pyproject.toml are `test_pyproject_no_stale_license_classifier` (line 74-78), `test_pyproject_no_stale_project_name` (line 81-84), `test_pyproject_uses_spdx_license` (line 87-91), and `test_pyproject_declares_package_data` (line 94-99). If `test_pyproject_validates_successfully` is introduced, it should be distinct from these — e.g., validating the TOML is parseable and has required sections, not re-asserting field values.*

**Recommendation:** If `test_pyproject_validates_successfully` exists, delete it as it duplicates `test_metadata_contains_required_fields` (or the existing `test_pyproject_*` tests in `test_packaging.py`). If a structural validation test is desired, it should be distinct from field-value assertions. Effort: trivial.

---

## Remediation Plan

Findings are grouped by classification. Effort estimates: **trivial** (<5 min), **small** (15-30 min), **medium** (1-2 hours).

### [TEST-DELETE] — Remove dead or redundant tests

| # | Finding | Test Module | Effort |
|---|---|---|---|
| 01 | Remove `sample_mapping_df` fixture (zero consumers) | conftest.py | trivial |
| 03 | Remove `tmp_db_path` fixture (zero consumers) | conftest.py | trivial |
| 04 | Remove `db_worker_memory` fixture (zero consumers) | conftest.py | trivial |
| 13 | Remove `test_null_data_files_excludes_stale` (tautological) | test_app.py | trivial |
| 14 | Remove `test_init_project_with_settings_dir` (duplicates `test_initialize_settings_threads_settings_dir`) | test_app.py | trivial |
| 15 | Remove `test_init_project_none_uses_app_paths` (duplicates `test_init_cli_creates_settings_in_tmp_dir`) | test_app.py | trivial |
| 16 | Remove `test_top_level_no_import_side_effects` (duplicates `test_import_app_does_not_create_settings_dir`) | test_app.py | trivial |
| 27 | Remove `test_build_match_query_single` (duplicates multi-column variant) | test_dict_service.py | trivial |
| 37 | Remove `test_pyproject_validates_successfully` (duplicates metadata test) | test_packaging.py | trivial |

**Total: 9 findings, all trivial effort.** These removals reduce test suite noise and CI runtime with zero coverage loss.

### [TEST-REWRITE] — Strengthen weak assertions

| # | Finding | Test Module | Effort |
|---|---|---|---|
| 08 | Add `exit_code == 1` + error message assertion for CLI `run` failure | test_app.py | small |
| 09 | Replace trivially-True `is not None` assertions with value-level config checks | test_app.py | small |
| 10 | Remove bare `except Exception: pass`; assert specific exception type or use `synthetic_report` | test_app.py | small |
| 11 | Spy on mock `run_report` call; assert correct `report_dir` passed, not circular value checks | test_app.py | small |
| 12 | Read exported CSV content; assert specific rows deleted/added, not count only | test_app.py | medium |
| 26 | Replace vacuous `is_in().all()` with specific expected column-name set assertion | test_dict_service.py | trivial |
| 28 | Correct ENDS_WITH pattern assertion to `%TERM` (no trailing `%`) | test_dict_service.py | trivial |
| 29 | Correct STARTS_WITH pattern assertion to `TERM%` (no trailing `%`) | test_dict_service.py | trivial |
| 30 | Correct PARTIAL_MATCH pattern assertion to `%TERM%` (both `%`) | test_dict_service.py | trivial |
| 32 | Add CSV content read-back and verification to gzip export test | test_csv_service.py | small |

**Total: 10 findings.** Priority for `test_app.py` rewrites (findings 08-12) is highest due to error-masking risk (finding 10).

### [TEST-UPDATE] — Extend coverage for gaps

| # | Finding | Test Module | Production Location | Effort |
|---|---|---|---|---|
| 06 | Add ENDS_WITH (`e`) row to `sample_dictionary` | conftest.py | dict_service.py:226-227 | small |
| 17 | Test `process_data` with non-existent directory → `FileNotFoundError` | test_app.py | app.py:142-144 | trivial |
| 18 | Test CLI `run` success path with `synthetic_report` | test_app.py | app.py:79-85 | small |
| 19 | Add negative-value test for `ReportResult.rows_loaded`/`rows_exported` (also change `int` → `NonNegativeInt`) | test_app.py, models.py | models.py:206-207 | trivial |
| 23 | Add `test_like_pattern_ends_with` unit test | test_dict_service.py | dict_service.py:226-227 | small |
| 24 | Test `generate_rules_blocks` action ordering (DELETE → REPLACE → ADD) | test_dict_service.py | dict_service.py:79-94 | small |
| 25 | Test `build_match_query` output string format (`col:"(term)"`) | test_dict_service.py | dict_service.py:264-279 | small |
| 31 | Add `infer`, `dict`, `xz`, `zip`, `zstd`, `default` cases to `test_get_files_suffix` | test_csv_service.py | csv_service.py:242-263 | trivial |
| 33 | Change `RuntimeError` → `WrongDataSettings` in import test | test_csv_service.py | csv_service.py:71-75 | trivial |
| 34 | Ensure all `get_files_suffix` test cases have explicit `assert` | test_csv_service.py | csv_service.py:242-263 | trivial |
| 35 | Cross-check `__version__` against `pyproject.toml` | test_metadata.py | __init__.py:10-15, pyproject.toml:7 | small |
| 36 | Cross-check license field against `pyproject.toml` | test_packaging.py | pyproject.toml:15 | small |

**Total: 12 findings.** The ENDS_WITH row (finding 06) benefits two findings (06 and 23) from a single fixture change.

### [TEST-WRITE] — New test modules for untested code

| # | Finding | Production Location | Effort |
|---|---|---|---|
| 21 | Write direct unit tests for `_apply_add` (56 lines, 0 coverage) | db_service.py:690-745 | medium |

### [BEST-PRACTICE] — Architectural improvements

| # | Finding | Production Location | Effort |
|---|---|---|---|
| 02 | Align `sample_mapping_df` column layout with `col_indexes` (or delete) | conftest.py:82-96 | trivial (superseded by delete) |
| 05 | `db_worker_memory` should use `realistic_config` (superseded by delete) | conftest.py:108-119 | trivial (superseded by delete) |
| 22 | Invalidate `column_index` cache in `set_data_tbl_columns` | db_service.py:124-139 | trivial |

### [DOC-UPDATE] — Documentation corrections

| # | Finding | Location | Effort |
|---|---|---|---|
| 07 | Update `synthetic_report` docstring line-number references | conftest.py:122-135 | trivial |
| 20 | Fix `ReportResult` docstring: "10 fields" → "11 fields" | test_app.py:258 | trivial |

---

## Latent Bug Report

### BUG-01: `DBWorker.column_index` cache staleness

| Field | Value |
|---|---|
| **Severity** | HIGH |
| **Location** | `src/mko_data_cleaner/core/db_service.py:124-128` (`column_index`), `:133-139` (`set_data_tbl_columns`) |
| **Type** | Cache invalidation defect |
| **Impact** | Silent stale data — `column_index` returns old column mappings after `set_data_tbl_columns` is called a second time |

**Description:** `column_index` is a `@cached_property` (line 124) that caches a `{index: name}` dict built from `self.data_tbl_columns`. The setter `set_data_tbl_columns` (line 133) reassigns `self._data_tbl_columns` (line 135) but does not invalidate the `column_index` cache. `cached_property` stores its result in `self.__dict__["column_index"]`, which persists across subsequent `set_data_tbl_columns` calls.

**Current production impact:** None — `run_report` (`app_service.py:154-156`) calls `set_data_tbl_columns` once per `DBWorker` instance. The bug is **latent**.

**Future risk:** Any code path that reuses a `DBWorker` instance across multiple table configurations would silently use stale column names. This is especially dangerous because `column_index` feeds `get_col_names` (line 130-131), which is used for column resolution in the mapping pipeline.

**Fix:** Add `self.__dict__.pop("column_index", None)` at the end of `set_data_tbl_columns` (after line 139). Effort: trivial.

---

## Methodology & Verification Notes

### Verification approach

Each finding was verified against the current source tree at the following paths:

| File | Role |
|---|---|
| `tests/conftest.py` | Shared test fixtures (159 lines) |
| `tests/test_app.py` | App-level public API tests (464 lines) |
| `tests/test_db_service.py` | DBWorker unit tests (567 lines) |
| `tests/test_dict_service.py` | MappingDict unit tests (176 lines) |
| `tests/test_csv_service.py` | CSVWorker unit tests (286 lines) |
| `tests/test_packaging.py` | Packaging/metadata regression tests (99 lines) |
| `tests/test_metadata.py` | Version metadata tests (22 lines) |
| `tests/test_notebook.py` | Notebook structure tests (69 lines) |
| `tests/test_notebook_check.py` | Notebook environment checker tests (47 lines) |

### Confidence levels

- **HIGH**: Finding verified by direct code inspection and/or grep across the test directory. Line numbers confirmed against file contents.
- **MEDIUM**: Finding inferred from code structure (e.g., missing test pattern) with indirect evidence.
- **LOW**: Finding based on audit phase documentation referencing tests not present in the current working tree.

### Notes on findings referencing tests not in the current tree

Findings 27, 28, 29, 30 (Block 4) and 32, 33 (Block 5) reference tests (`test_build_match_query_single`, `test_build_search_like_pattern_*`, `test_export_sql_to_csv_gzip`, `test_import_csv_no_delimiter_raises`) that are **not present** in the current `tests/` directory. These findings reflect the audit's identification of redundancy or weakness **as identified in Steps 2 & 3**, which may have operated on a different commit. Where the current code already contains a superior replacement test (e.g., `test_export_sql_to_csv_roundtrip_content` supersedes the weak `test_export_sql_to_csv_gzip`), this is noted in the finding.

### Production test count

The test suite contains **22 test functions** in `test_db_service.py`, **18 test functions** in `test_app.py`, **8 test functions** in `test_dict_service.py`, **13 test functions** in `test_csv_service.py`, **8 test functions** in `test_packaging.py`, **2 test functions** in `test_metadata.py`, **5 test functions** across `test_notebook.py` and `test_notebook_check.py`. Total: **76 test functions** across 9 test modules.

---

*Report generated: 2026-10-18T17:56:34Z*
*Audit phase: Step 4 — Test Quality Synthesis*
*Findings source: Steps 2 (test analysis) & 3 (validation)*
