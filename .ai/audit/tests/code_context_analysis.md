# Code Context Analysis — Test Coverage Remediation Plan

**Audit Date:** 2026-10-18  
**Current HEAD:** `a41a4451fc9427f46bef02455f0fcfc5b1e7c05f` (26 commits ahead of audit baseline `f6ac8e1`)  
**Plan File:** `.ai/plans/04-test-coverage-remediation.md`  
**Scope:** 37 findings across 4 phases (P0–P4)

---

## Repository State Summary

| Aspect | Value |
|---|---|
| Audit baseline HEAD | `f6ac8e1` |
| Current HEAD | `a41a445` |
| Commits elapsed | 26 |
| Test files in scope | 7 (conftest.py + 6 test modules) |
| Test functions in scope | 70 total |

### Test function counts (current tree vs. audit report)

| Test File | Audit Claim | Actual Count | Discrepancy |
|---|---|---|---|
| `test_app.py` | 18 | 24 | +6 functions (repo evolved) |
| `test_db_service.py` | 22 | 22 | ✅ match |
| `test_dict_service.py` | 8 | 8 | ✅ match |
| `test_csv_service.py` | 13 | 13 | ✅ match |
| `test_packaging.py` | 8 | 8 | ✅ match |
| `test_metadata.py` | 2 | 2 | ✅ match |

Two additional test files exist outside the plan scope: `test_notebook.py` (5 tests) and `test_notebook_check.py` (4 tests).

---

## Available Fixtures (verified)

| Fixture | File:Line | Description | Consumers |
|---|---|---|---|
| `realistic_config` | `conftest.py:12-24` | Loads shipped `app_config.json5` via importlib.resources | `db_worker`, `dict_indexes` |
| `db_worker` | `conftest.py:27-38` | `DBWorker` backed by `tmp_path/"test.db"`, configured from `realistic_config` | All `test_db_service.py` tests |
| `dict_indexes` | `conftest.py:42-43` | `DictColumnsIndexes` from `realistic_config["dict_file_settings"]["col_indexes"]` | All `test_dict_service.py` tests |
| `sample_dictionary` | `conftest.py:46-69` | 6-row polars DataFrame (search types: f, p, s, f, f, fts) | `test_mapping_build`, `test_pattern_generation`, `test_build_search_like_pattern_real_cases`, `test_build_query_no_strict_column` |
| `extra_cols` | `conftest.py:72-79` | Returns `["tag"]` | `test_build_non_mapped_with_extra_cols` |
| `sample_mapping_df` | `conftest.py:82-96` | **Unused** — zero consumers | None |
| `tmp_db_path` | `conftest.py:99-105` | **Unused** — zero consumers | None |
| `db_worker_memory` | `conftest.py:108-119` | **Unused** — zero consumers | None |
| `synthetic_report` | `conftest.py:122-159` | E2E report dir with 3-row CSV + 2-rule dict (DELETE Brand2, ADD tag for Brand1) | `test_process_data_end_to_end`, `test_null_data_files_excludes_stale` |
| `populated_settings` | `test_app.py:30-38` | User dir with `app_config.json5` + `log_config.json5` copied from templates | Multiple test_app.py tests |
| `empty_settings_dir` | `test_app.py:42-44` | User dir with no `settings/` subdir | `test_process_data_without_settings_*`, `test_run_cli_without_settings_*` |
| `empty_report_dir` | `test_app.py:48-52` | Existing but empty report directory | Multiple tests |

---

## Phase 0: Cleanup Findings

### T-DEL-01 — Delete `sample_mapping_df` fixture (FINDING-01)

**Status:** ✅ Exists, zero consumers  
**Location:** `tests/conftest.py:82-96`  

**Exact code:**
```python
@pytest.fixture
def sample_mapping_df():
    """Reserved for future integration tests of ``group_by_cols``.

    No current consumer (TST-016).
    """
    return pl.DataFrame(
        {
            "action": ["a", "r", "d"],
            "match": ["f", "p", "s"],
            "search": [0, 1, 0],
            "term": ["Apple", "Sam", "Test"],
            "tag": ["fruit", "brand", "delete"],
        }
    )
```

**Evidence:** `grep -rn "sample_mapping_df" tests/` returns only the definition at line 83. No test function lists it as a parameter. The fixture's column layout (5 cols: action, match, search, term, tag) omits the `extra_col` at index 3 that `DictColumnsIndexes` in `app_config.json5` expects — confirming FINDING-02's data inconsistency claim. If wired into `MappingDict`, column indices would be silently shifted by one.

**Imports needed:** `pl` (already imported at conftest.py:4)

---

### T-DEL-02 — Delete `tmp_db_path` fixture (FINDING-03)

**Status:** ✅ Exists, zero consumers  
**Location:** `tests/conftest.py:99-105`

**Exact code:**
```python
@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Path to a temporary sqlite database file.

    Reserved for future integration tests. No current consumer (TST-016).
    """
    return tmp_path / "test_database.db"
```

**Evidence:** `grep -rn "tmp_db_path" tests/` returns only the definition at line 100. The `db_worker` fixture (lines 27-38) already provides a `tmp_path`-backed `DBWorker` with `tmp_path / "test.db"`, making this redundant.

**Imports needed:** `Path` (already imported at conftest.py:2)

---

### T-DEL-03 — Delete `db_worker_memory` fixture (FINDING-04/FINDING-05)

**Status:** ✅ Exists, zero consumers  
**Location:** `tests/conftest.py:108-119`

**Exact code:**
```python
@pytest.fixture
def db_worker_memory(tmp_path):
    """DBWorker backed by an on-disk sqlite file (``tmp_path``).

    Reserved for future integration tests that need an isolated worker.
    No current consumer (TST-016).
    """

    db_file = tmp_path / "memory.db"

    with DBWorker(db_file=db_file, tbl_name="data_table", index_column="id") as worker:
        yield worker
```

**Evidence:** `grep -rn "db_worker_memory" tests/` returns only the definition at line 109. The fixture hardcodes `tbl_name="data_table"` and `index_column="id"` instead of reading from `realistic_config` (FINDING-05). The `db_worker` fixture (lines 27-38) already provides equivalent coverage with realistic config values extracted from `app_config.json5`. This supersedes BEST-PRACTICE FINDING-05.

**Imports needed:** `DBWorker` (already imported at conftest.py:8)

---

### T-DEL-04 — Delete `test_null_data_files_excludes_stale` (FINDING-13)

**Status:** ✅ Exists  
**Location:** `tests/test_app.py:452-463`

**Exact code:**
```python
def test_null_data_files_excludes_stale(populated_settings: Path, synthetic_report: Path):
    """null_data_files reports only files from the current run, not stale files
    left in the report directory from a previous run.
    """
    # Pre-seed a stale null_data file (would be picked up by the old glob approach)
    stale_file = synthetic_report / "null_data_stale_20200101-000000_1.csv.gz"
    stale_file.write_bytes(b"stale")

    result = process_data(synthetic_report, settings_dir=populated_settings)

    assert isinstance(result.null_data_files, list)
    # Stale file must NOT appear — only paths written by this run's export
    assert stale_file not in result.null_data_files
```

**Analysis — tautology confirmed:** `result.null_data_files` is populated at `app_service.py:246-254` where `null_data_file_paths` is a **freshly created empty list** (line 246: `null_data_file_paths: list[Path] = []`) passed to `csv_worker.export_sql_to_csv(..., output_files=null_data_file_paths)`. The stale file was never added to this list. The assertion `stale_file not in result.null_data_files` is **always True** — it cannot fail regardless of production behavior. No injection path exists because the mechanism uses a fresh list, not a directory glob.

**Production context** (`src/mko_data_cleaner/core/app_service.py:246-254`):
```python
null_data_file_paths: list[Path] = []
csv_worker.export_sql_to_csv(
    db_con=db_worker.db_con,
    file_prefix="null_data",
    export_path=f"{self.base_path}",
    data_table=db_worker.non_mapped_table,
    output_files=null_data_file_paths,
)
null_data_files = sorted(null_data_file_paths)
```

---

### T-DEL-05 — Delete `test_init_project_with_settings_dir` (FINDING-14)

**Status:** ✅ Exists  
**Location:** `tests/test_app.py:343-349`

**Exact code:**
```python
def test_init_project_with_settings_dir(tmp_path: Path):
    """init_project(force=True, settings_dir=...) writes to settings_dir / settings."""
    user_dir = tmp_path / "myapp"
    result = init_project(force=True, settings_dir=user_dir)
    assert result == user_dir / "settings"
    assert (result / "app_config.json5").exists()
    assert (result / "log_config.json5").exists()
```

**Analysis — duplication confirmed:** The primary `test_initialize_settings_threads_settings_dir` (lines 318-324) calls `initialize_settings(settings_dir=user_dir)` and asserts the same three outcomes. `initialize_settings` (app.py:148-150) is a one-line delegation: `return init_project(force=force, settings_dir=settings_dir)`. The only difference is `force=True` vs `force=False` (default), but since `user_dir` is a fresh `tmp_path`, `force` has no observable effect. The delegation is also verified by `test_top_level_exports` (line 384-403) which asserts `mko_data_cleaner.initialize_settings is initialize_settings`.

---

### T-DEL-06 — Delete `test_init_project_none_uses_app_paths` (FINDING-15)

**Status:** ✅ Exists  
**Location:** `tests/test_app.py:372-379` (plan says 372-378 — **off by 1**)

**Exact code:**
```python
def test_init_project_none_uses_app_paths(tmp_path: Path, monkeypatch):
    """init_project(force, settings_dir=None) uses monkeypatchable APP_PATHS."""
    tmp_app_paths = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=tmp_path)
    monkeypatch.setattr("mko_data_cleaner.core.init_service.APP_PATHS", tmp_app_paths)
    result = init_project(force=True, settings_dir=None)
    assert result == tmp_path / "settings"
    assert (result / "app_config.json5").exists()
```

**Analysis — duplication confirmed:** Same monkeypatch pattern and assertions as `test_init_cli_creates_settings_in_tmp_dir` (lines 203-213), but via Python API instead of CLI. The CLI path is also covered by `test_init_cli_with_settings_dir` (lines 355-366). The Python-API-with-None path is covered by `test_initialize_settings_force_overwrites` (lines 327-337).

**Discrepancy:** Plan states lines 372-378; actual function spans 372-379 (8 content lines + closing blank). Minor off-by-one in plan.

---

### T-DEL-07 — Delete `test_top_level_no_import_side_effects` (FINDING-16)

**Status:** ✅ Exists  
**Location:** `tests/test_app.py:406-425`

**Exact code:**
```python
def test_top_level_no_import_side_effects(tmp_path: Path):
    """importing the top-level package must not create a settings dir."""
    target = str(tmp_path)
    script = (
        "import platformdirs, os, sys\n"
        "platformdirs.user_config_dir = lambda *a, **k: sys.argv[1]\n"
        "import mko_data_cleaner\n"
        "print(mko_data_cleaner.__version__)\n"
        "if os.path.isdir(os.path.join(sys.argv[1], 'settings')):\n"
        "    print('FAIL: settings dir created', file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "print('OK')\n"
        "sys.exit(0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, target],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
```

**Analysis — duplication confirmed:** Functionally identical to `test_import_app_does_not_create_settings_dir` (lines 81-101). Both run a subprocess that monkeypatches `platformdirs.user_config_dir`, import the package, and assert no `settings/` dir is created. The top-level `__init__.py:5` delegates to `from mko_data_cleaner.app import ...` — so if `app.py` import doesn't create settings (verified by primary test), the package import won't either. The `__version__` print is already covered by `test_top_level_exports` (line 397-398).

---

### T-DEL-08 — Verify/delete `test_build_match_query_single` (FINDING-27)

**Status:** ✅ Confirmed absent from current tree  
**Search:** `grep -rn "test_build_match_query_single" tests/` → **no matches in test files**  
**Note:** The audit report (line 605) states: "these tests are not present in the current `test_dict_service.py`." The plan correctly flags this as a no-op verification. No `test_build_match_query_multiple` exists either — the canonical test for `build_match_query` in the current tree is `test_build_search_like_pattern_real_cases` (test_dict_service.py:86), which exercises FTS patterns indirectly.

---

### T-DEL-09 — Verify/delete `test_pyproject_validates_successfully` (FINDING-37)

**Status:** ✅ Confirmed absent from current tree  
**Search:** `grep -rn "test_pyproject_validates_successfully" tests/` → **no matches**  
**Search:** `grep -rn "test_metadata_contains_required_fields" tests/` → **no matches**  

The audit references two tests that do NOT exist: `test_pyproject_validates_successfully` (FINDING-37) and `test_metadata_contains_required_fields` (FINDING-36). The actual tests in `test_packaging.py` (8 functions total):
- `test_template_json5_accessible_as_package_data` (line 25)
- `test_template_json5_parse` (line 44)
- `test_manifest_in_no_stale_project_name` (line 61)
- `test_manifest_in_includes_json5_templates` (line 66)
- `test_pyproject_no_stale_license_classifier` (line 74)
- `test_pyproject_no_stale_project_name` (line 81)
- `test_pyproject_uses_spdx_license` (line 87) — **this covers the license cross-check**
- `test_pyproject_declares_package_data` (line 94)

---

## Phase 1: High-Impact Rewrites

### T-RW-01 — Rewrite `test_run_cli_without_settings_raises_settings_error` (FINDING-08)

**Status:** ✅ Test exists at test_app.py:166-174  
**Production:** app.py:55-89 (`run` command)

**Current test (lines 166-174):**
```python
def test_run_cli_without_settings_raises_settings_error(
    empty_report_dir: Path, empty_settings_dir: Path
):
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", str(empty_report_dir), "--settings-dir", str(empty_settings_dir)],
    )
    assert isinstance(result.exception, SettingsNotInitializedError)
```

**Critical discrepancy in plan analysis:** The plan claims at lines 188-189:
> "the `run` command (app.py:79-89) wraps `service.run_report` in `try/except Exception` and calls `raise typer.Exit(1)` on any error. This means `SettingsNotInitializedError` is caught by the broad `except` at line 87..."

**This is INCORRECT.** The actual `run` function (app.py:54-89):
```python
def run(report, verbose, settings_dir=None):  # line 55
    ...
    service = get_app_service(settings_dir)  # line 77 — OUTSIDE try/except
    try:                                        # line 79
        result = service.run_report(report)    # line 80
        console.print(...)                      # lines 81-85
    except Exception as e:                      # line 87
        console.print(...)                      # line 88
        raise typer.Exit(1) from e             # line 89
```

`get_app_service(settings_dir)` is called at **line 77**, which is **outside** the `try/except` block (lines 79-89). `SettingsNotInitializedError` is raised inside `get_app_service` → `_require_settings` (app_service.py:337), and propagates **uncaught** through the `run` function. Typer/Click's `CliRunner` then catches it, sets `result.exit_code = 1`, and stores the original `SettingsNotInitializedError` in `result.exception`.

**Impact on plan:** The plan's rationale ("the exception does NOT propagate to `result.exception` as the original type") is wrong. The current assertion `assert isinstance(result.exception, SettingsNotInitializedError)` is actually **correct** — the exception type IS preserved. However, the plan's recommendation to still assert `result.exit_code == 1` and check `result.output` for the error message remains valid best practice (strengthening, not correcting a wrong assertion).

**Recommended test rewrite (corrected):**
- Assert `result.exit_code == 1` (valid addition — currently missing)
- Assert `"SettingsNotInitializedError"` or `"settings"` in `result.output` (valid addition)
- Keep `assert isinstance(result.exception, SettingsNotInitializedError)` (the plan incorrectly suggests removing it)

---

### T-RW-02 — Rewrite `test_get_app_service_constructs_after_initialization` (FINDING-09)

**Status:** ✅ Test exists at test_app.py:219-223  
**Production:** app_service.py:36-52 (`AppService.__init__`, `app_config`, `log_config`)

**Current test (lines 219-223):**
```python
def test_get_app_service_constructs_after_initialization(populated_settings: Path):
    service = get_app_service(settings_dir=populated_settings)
    assert service.app_config is not None
    assert service.log_config is not None
    assert service.app_config.data_paths.import_folder is not None
```

**Analysis:** All 3 assertions are trivially True:
- `service.app_config` is a `@cached_property` that either returns a valid `DataSettings` instance or raises during construction (app_service.py:46-52). Since `_require_settings` (app_service.py:337) already ran, it cannot be None.
- `service.log_config` similarly either returns `LoggingSettings` or raises.
- `import_folder` has default `Path("raw_data")` (models.py:64) — always truthy.

**Expected values from shipped template** (`src/mko_data_cleaner/settings/app_config.json5`):
| Config field | Expected value | Source |
|---|---|---|
| `data_file_settings.index_column` | `"AdId"` | app_config.json5:10 |
| `database_settings.table_name` | `"data_table"` | app_config.json5:25 |
| `data_file_settings.extension` | `DataFileExtension.gz` | app_config.json5:9 |
| `log_config.version` | `1` | log_config.json5:4 / models.py:53 |
| `data_paths.import_folder` | `Path("raw_data")` | app_config.json5:3 / models.py:64 |
| `data_paths.export_folder` | `Path("clean_data")` | app_config.json5:4 / models.py:65 |

**Imports needed in test:** `DataFileExtension` from `mko_data_cleaner.core.models`

---

### T-RW-03 — Rewrite `test_process_data_past_gate_does_not_raise_settings_error` (FINDING-10)

**Status:** ✅ Test exists at test_app.py:226-236  
**Production:** app.py:126-145 (`process_data`)

**Current test (lines 226-236):**
```python
def test_process_data_past_gate_does_not_raise_settings_error(
    populated_settings: Path, empty_report_dir: Path
):
    try:
        process_data(empty_report_dir, settings_dir=populated_settings)
    except SettingsNotInitializedError:
        pytest.fail("process_data raised SettingsNotInitializedError despite settings present")
    except Exception:
        # Operational errors from the empty report dir are acceptable here
        pass
```

**Analysis — bare `except Exception: pass` confirmed:** The `empty_report_dir` fixture creates an existing but empty directory. `process_data` (app.py:142-145) passes the `is_dir()` check, then calls `get_app_service(settings_dir).run_report(path)`. `run_report` (app_service.py:110-292) constructs a `CSVWorker` at line 126, whose `__init__` calls `_set_files_params()` (csv_service.py:60-75). With no files in `raw_data/`, this raises `WrongDataSettings`. The bare `except Exception: pass` swallows this — and would swallow any other error (DB failures, config bugs, assertion errors).

**Imports needed:** `WrongDataSettings` from `mko_data_cleaner.core.errors` (not currently imported in test_app.py)

---

### T-RW-04 — Rewrite `test_process_data_returns_report_result` (FINDING-11)

**Status:** ✅ Test exists at test_app.py:286-312  
**Production:** app.py:126-145

**Current test (lines 286-312):**
```python
def test_process_data_returns_report_result(
    monkeypatch, populated_settings: Path, empty_report_dir: Path
):
    fake_result = ReportResult(...)  # rows_loaded=42, rows_exported=40
    fake_service = SimpleNamespace(run_report=lambda path: fake_result)
    monkeypatch.setattr(
        "mko_data_cleaner.app.get_app_service",
        lambda settings_dir=None: fake_service,
    )
    result = process_data(empty_report_dir, settings_dir=populated_settings)
    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 42
    assert result.rows_exported == 40
    assert result.report_dir == empty_report_dir
```

**Analysis — circular assertions confirmed:** The mock returns `fake_result` (constructed with hardcoded values 42/40), then the test asserts those same values were returned. The `is_dir()` check (app.py:143) and the delegation to `run_report(path)` are never verified — the mock intercepts the call.

**Fix needed:** Replace `SimpleNamespace(run_report=lambda path: fake_result)` with `MagicMock()` and use `assert_called_once_with(empty_report_dir)`.

**Imports needed:** `from unittest.mock import MagicMock` (not currently imported; `SimpleNamespace` from `types` is imported at test_app.py:8)

---

### T-RW-05 — Rewrite `test_process_data_end_to_end` (FINDING-12)

**Status:** ✅ Test exists at test_app.py:431-449  
**Production:** db_service.py:647-659 (`_apply_delete`), app.py:55-89

**Current test (lines 431-449):**
```python
def test_process_data_end_to_end(populated_settings: Path, synthetic_report: Path):
    report_dir = synthetic_report
    result = process_data(report_dir, settings_dir=populated_settings)
    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 3
    assert result.rows_exported == 2  # Brand2 row deleted  ← line 437
    assert result.rows_exported <= result.rows_loaded
    assert result.export_dir == report_dir / "clean_data"
    assert result.import_dir == report_dir / "raw_data"
    assert result.report_dir == report_dir
    assert (result.export_dir).exists()
    assert len(list(result.export_dir.glob("*.gz"))) > 0
    assert isinstance(result.null_data_files, list)
    assert result.duration > 0.0
    assert set(result.timings.keys()) == {"import", "mapping", "export"}
    assert all(v >= 0.0 for v in result.timings.values())
```

**Line number discrepancy:** Plan says assertions at lines 438-439; actual `rows_loaded`/`rows_exported` assertions are at lines 436-437 (off by 1, due to repo evolution since audit baseline).

**Analysis — count-only assertion confirmed:** The test asserts `rows_exported == 2` with comment `# Brand2 row deleted` but never verifies `Brand2` is actually the deleted row. A bug in `_apply_delete` (db_service.py:647-659) that deletes by rowid instead of by match would still produce count 2.

**Fix needed:** Read back exported CSV with `pl.read_csv(f, separator=";")` and assert:
- `"Brand2"` absent from any cell
- `"Brand1"` present in at least one row
- `added_by_brand` tag present for Brand1 rows (ADD rule: dict line 4 `"a;f;4;NULL;Brand1;added_by_brand"`)

---

### T-RW-06 — Create `test_build_search_like_pattern_ends_with` (FINDING-28)

**Status:** ✅ Does NOT exist — needs creation  
**Production:** dict_service.py:208-229 (`_build_search_like_pattern`)

**Production code (lines 208-229):**
```python
@staticmethod
def _build_search_like_pattern(match_type_col: str, term_col: str) -> pl.Expr:
    return (
        pl.when(pl.col(match_type_col) == MatchType.FULL_MATCH)
        .then(pl.col(term_col))
        .when(pl.col(match_type_col) == MatchType.PARTIAL_MATCH)
        .then(pl.concat_str([pl.lit("%"), pl.col(term_col), pl.lit("%")]))
        .when(pl.col(match_type_col) == MatchType.STARTS_WITH)
        .then(pl.concat_str([pl.col(term_col), pl.lit("%")]))
        .when(pl.col(match_type_col) == MatchType.ENDS_WITH)         # line 226
        .then(pl.concat_str([pl.lit("%"), pl.col(term_col)]))        # line 227
        .otherwise(None)
    )
```

**Expected pattern for ENDS_WITH:** `%APPLE` (leading `%` only, no trailing `%`)

**Dependency:** T-UP-01 (ENDS_WITH row in `sample_dictionary`) for integration test. Can be standalone unit test without it.

**Imports needed:** `MatchType`, `MappingDict` from `mko_data_cleaner.core.dict_service` / `models`

---

### T-RW-07 — Create `test_build_search_like_pattern_starts_with` (FINDING-29)

**Status:** ✅ Does NOT exist — needs creation  
**Production:** dict_service.py:224-225 (STARTS_WITH branch)

**Expected pattern:** `APPLE%` (trailing `%` only, no leading `%`)

**Note:** The existing `test_build_search_like_pattern_real_cases` (test_dict_service.py:86-118) already correctly asserts `"ПЕРВЫЙ%"` at line 66 for STARTS_WITH. The standalone test adds explicit unit-level coverage.

---

### T-RW-08 — Create `test_build_search_like_pattern_contains` (FINDING-30 + FINDING-34)

**Status:** ✅ Does NOT exist — needs creation  
**Production:** dict_service.py:222-223 (PARTIAL_MATCH branch)

**Expected pattern:** `%APP%` (both `%` wrapping)

**Note:** The existing `test_build_search_like_pattern_real_cases` (line 65) already asserts `"%КАНАЛ%"` for PARTIAL_MATCH. This standalone test covers FULL_MATCH + PARTIAL_MATCH + STARTS_WITH in one test. This subsumes FINDING-34 (missing `assert` on 3 cases).

---

### T-RW-09 — Create `test_export_sql_to_csv_gzip` (FINDING-32)

**Status:** ✅ Does NOT exist — needs creation  
**Production:** csv_service.py:269-381 (`export_sql_to_csv`)

**Note:** The existing `test_export_sql_to_csv_roundtrip_content` (test_csv_service.py:155-206) already verifies content read-back with gzip decompression. The plan recommends making the new test specifically assert gzip format (`.csv.gz` extension, binary validity via `gzip.open`).

---

### T-RW-10 — Create `test_import_csv_no_delimiter_raises` (FINDING-33)

**Status:** ✅ Does NOT exist — needs creation  
**Production:** csv_service.py:60-75 (`_set_files_params`)

**Production code (lines 60-75):**
```python
def _set_files_params(self):
    ext = self.data_settings.get("extension", "csv")
    self.data_files = list_files_in_directory(
        self.data_path,
        extensions=(ext,),
    )
    if self.data_files and len(self.data_files) > 0:
        self.sample_data_file = self.data_files[0]
        self.source_headers = self.get_csv_headers(self.sample_data_file)
    else:
        raise WrongDataSettings(
            f"Please check settings. "
            f"No files with extension: '{ext}' "
            f"found in: '{self.data_path}'. "
        )
```

**Classification discrepancy:** Audit classifies FINDING-33 as `[TEST-UPDATE]` (line 71), but the plan reclassifies it as `[TEST-REWRITE]` (T-RW-10 in the plan's task table). The test doesn't exist, so it must be created — not updated. The plan's classification is more accurate.

**Expected exception:** `WrongDataSettings` (errors.py:8-9), NOT `RuntimeError`  
**Expected message fragment:** `"No files with extension"` (csv_service.py:73)

**Imports needed:** `WrongDataSettings` from `mko_data_cleaner.core.errors`

---

### T-FIX-01 — Fix vacuous `is_in()` assertion in `test_mapping_build` (FINDING-26)

**Status:** ✅ Test exists at test_dict_service.py:42  
**Production:** dict_service.py:119-142 (`build_mapping`), dict_service.py:157-206 (`_build_query`)

**Current assertion (line 42):**
```python
assert df[MappingColumns.column_name].is_in(main_columns).all()
```

**Analysis — vacuous True confirmed:** `build_mapping` (line 119-125) sets `self._table_columns = list(tbl_columns)` where `tbl_columns` = `main_columns`. Then `_get_data_col_index` (line 144-155) builds `{column_name: self._table_columns}` — i.e., the column names ARE `main_columns` by construction. The `_build_query` join (line 198) resolves `column_name` from this dict, so every `column_name` in `like_data` is by construction a member of `main_columns`.

**Expected replacement assertion:**
```python
# sample_dictionary search indices in main_columns:
#   index 3 → channelName (Coca-Cola, Удалить, Спонсор → "f")
#   index 1 → researchDate (канал, Первый → "p", "s")
#   index 4 → brand (Apple|Company → "fts", excluded from like_data)
#   index 5 → out of range → null column_name (Удалить DELETE row has match="f")
#   index 4 → brand (Спонсор, "f")
assert set(df[MappingColumns.column_name].drop_nulls().unique().to_list()) == {"channelName", "researchDate", "brand"}
```

**After T-UP-01:** Adding ENDS_WITH row `["a", "e", 3, None, "Кола", "cola_end"]` (search=3 → channelName) would NOT change the expected set — `channelName` is already present.

---

## Phase 2: Missing Coverage

### T-UP-01 — Add ENDS_WITH row to `sample_dictionary` (FINDING-06)

**Status:** ✅ Fixture exists at conftest.py:46-69  
**Production:** dict_service.py:226-227 (ENDS_WITH branch)

**Current `sample_dictionary` data (lines 49-56):**
```python
data = [
    ["a", "f", 3, None, "Coca-Cola", "brand"],          # FULL_MATCH → channelName
    ["r", "p", 1, None, "канал", "channelName"],         # PARTIAL → researchDate
    ["r", "s", 1, None, "Первый", "channelName"],        # STARTS_WITH → researchDate
    ["d", "f", 5, None, "Удалить", None],                # DELETE, index 5 out of range → null
    ["a", "f", 4, None, "Спонсор", "sponsor_tag"],        # FULL_MATCH → brand
    ["a", "fts", "0|1", None, "Apple|Company", "fruit_tag"], # FTS → excluded from like_data
]
```

**No ENDS_WITH (`e`) row exists.** `MatchType.ENDS_WITH = "e"` is defined at models.py:127 but never exercised.

**Proposed addition (after line 55, before the FTS row):**
```python
["a", "e", 3, None, "Кола", "cola_end"],  # ENDS_WITH → channelName, pattern "%КОЛА"
```

**Impact on existing tests:**
| Test | Impact |
|---|---|
| `test_mapping_build` (line 7) | New row goes to `like_data`; `column_name` = channelName (already in set); pattern `%КОЛА` added to patterns list. No assertion change needed. |
| `test_pattern_generation` (line 45) | Pattern `%КОЛА` added. **Must add assertion**: `assert "%КОЛА" in patterns`. |
| `test_build_search_like_pattern_real_cases` (line 86) | ENDS_WITH is excluded from both filters (FULL_MATCH only, and PARTIAL_MATCH/STARTS_WITH). No assertion change needed. |
| `test_build_query_no_strict_column` (line 165) | Only checks `"strict" not in mapping.like_data.columns`. No change needed. |

**`MatchType` enum (models.py:124-129):**
```python
class MatchType(StrEnum):
    FULL_MATCH = "f"
    PARTIAL_MATCH = "p"
    ENDS_WITH = "e"
    STARTS_WITH = "s"
    FTS = "fts"
```

---

### T-UP-02 — Add `generate_rules_blocks` ordering test (FINDING-24)

**Status:** ✅ Production exists at dict_service.py:79-94  
**Test needed:** ✅ Create new test

**Production code (lines 79-94):**
```python
def generate_rules_blocks(self) -> Generator[tuple[str, pl.DataFrame]]:
    keep_cols = [
        MappingColumns.mapping_index,
        *self.extra_col_names,
    ]
    actions = [ActionType.DELETE, ActionType.REPLACE, ActionType.ADD]  # line 84
    for action in actions:
        df_action = self.data.filter(
            pl.col(MappingColumns.action) == action
        ).select(keep_cols)
        match action:
            case ActionType.DELETE | ActionType.ADD:
                yield action, df_action
            case ActionType.REPLACE:
                for df in self.group_by_cols(df_action):
                    yield action, df
```

**Key behavior:** DELETE blocks first, then REPLACE (via `group_by_cols`), then ADD. For REPLACE, `group_by_cols` (line 64-77) groups rows by their non-None column set, yielding each group separately. In `apply_mapping` (db_service.py:626-645), the `mapping_table` is written with `if_table_exists="replace"` (app_service.py:229), so only the last REPLACE group survives — ordering matters.

**Test approach:** Build a `MappingDict` with multiple REPLACE rules having different column sets, call `list(mapping.generate_rules_blocks())`, assert:
1. All DELETE actions precede REPLACE actions
2. All REPLACE actions precede ADD actions
3. Within REPLACE, groups are yielded in `group_by_cols` output order

---

### T-UP-03 — Add FTS query string format test (FINDING-25)

**Status:** ✅ Production exists at dict_service.py:264-279  
**Test needed:** ✅ Create new test

**Production code (lines 264-279):**
```python
def build_match_query(self, indexes: list[int], filters: list[str]) -> str:
    parts = []
    columns_list: list[str] = self.data_col_index[MappingColumns.column_name]
    for col_idx, value in zip(indexes, filters, strict=True):
        col_name = columns_list[col_idx]
        self.search_columns.add(col_name)
        safe_value = value.replace('"', "")
        if "(" not in safe_value:
            safe_value = f'"({safe_value})"'
        parts.append(f"{col_name}:{safe_value}")
    return " ".join(parts)
```

**Expected format:** `col_name:"(value) col_name2:"(value2)"`  
Quote/parenthesis wrapping at line 276: `safe_value = f'"({safe_value})"'`  
Column:value join at line 278: `f"{col_name}:{safe_value}"`  
Space-join at line 279: `" ".join(parts)`

**Note:** `search_columns` side-effect at line 270: `self.search_columns.add(col_name)`

**Test approach:** After `build_mapping`, call `build_match_query([0, 1], ["Apple", "Company"])` and assert exact string format. With `main_columns = ["adId", "researchDate", "programName", "channelName", "brand"]`:
- col_idx 0 → "adId"  
- col_idx 1 → "researchDate"  
- Expected: `adId:"(Apple)" researchDate:"(Company)"`

---

### T-UP-04 — Add ENDS_WITH coverage in `test_pattern_generation` (FINDING-23)

**Status:** ✅ Test exists at test_dict_service.py:45-83  
**Depends on:** T-UP-01 (ENDS_WITH row)

**Current assertions (lines 64-66):**
```python
assert "COCA-COLA" in patterns, "FULL_MATCH should produce term without %"
assert "%КАНАЛ%" in patterns, "PARTIAL_MATCH should wrap term in %...%"
assert "ПЕРВЫЙ%" in patterns, "STARTS_WITH should produce term%"
```

**Required addition (after T-UP-01 adds ENDS_WITH row):**
```python
assert "%КОЛА" in patterns, "ENDS_WITH should produce %term (leading % only)"
```

The ENDS_WITH row `["a", "e", 3, None, "Кола", "cola_end"]` will produce pattern `%КОЛА` (uppercase, line 202: `pattern_expr.str.to_uppercase()`).

---

### T-UP-05 — Extend `test_get_files_suffix` with missing branches (FINDING-31)

**Status:** ✅ Test exists at test_csv_service.py:8-11  
**Production:** csv_service.py:242-263 (`get_files_suffix`)

**Current test (lines 8-11):**
```python
def test_get_files_suffix():
    assert CSVWorker.get_files_suffix("gzip") == ".csv.gz"
    assert CSVWorker.get_files_suffix("bz2") == ".csv.bz2"
    assert CSVWorker.get_files_suffix(None) == ".csv"
```

**Production code (lines 242-263):**
```python
@staticmethod
def get_files_suffix(compression: str | dict | None) -> str:
    base = ".csv"
    if not compression or compression == "infer":     # line 245 — pre-match branch
        return base
    if isinstance(compression, dict):                 # line 247 — dict-form branch
        method = compression.get("method", "").lower()
    else:                                             # line 249 — str branch
        method = str(compression).lower()
    match method:
        case "gzip" | "gz":                           # line 252
            return base + ".gz"
        case "bz2":                                   # line 254
            return base + ".bz2"
        case "xz":                                    # line 256
            return base + ".xz"
        case "zip":                                   # line 258
            return base + ".zip"
        case "zstd":                                  # line 260
            return base + ".zst"
        case _:                                       # line 262 — default
            return base
```

**Missing branches to test:**
| Input | Expected output | Branch |
|---|---|---|
| `"infer"` | `.csv` | line 245 (pre-match) |
| `{"method": "gzip"}` | `.csv.gz` | line 247 (dict-form) |
| `"xz"` | `.csv.xz` | line 256 |
| `"zip"` | `.csv.zip` | line 258 |
| `"zstd"` | `.csv.zst` | line 260 |
| `"unknown"` | `.csv` | line 262 (default) |
| `"csv"` | `.csv` | line 262 (default) |
| `"json"` | `.csv` | line 262 (default) |
| `"parquet"` | `.csv` | line 262 (default) |

**Subsumes FINDING-34:** The audit notes FINDING-34 ("missing `assert` on 3 cases") — in the current tree all 3 assertions have `assert`. T-UP-05 rewrites the test with all explicit `assert` statements, covering both FINDING-31 and FINDING-34.

---

### T-UP-07 — Add `process_data` non-existent directory test (FINDING-17)

**Status:** ✅ Test needed (new)  
**Production:** app.py:142-144

**Production code (lines 142-145):**
```python
path = Path(report_path)
if not path.is_dir():
    raise FileNotFoundError(f"Directory not found: {path}")
return get_app_service(settings_dir).run_report(path)
```

**Key detail:** The `is_dir()` guard fires BEFORE `get_app_service` (settings gate). So passing a nonexistent path with `populated_settings` will raise `FileNotFoundError`, NOT `SettingsNotInitializedError`.

**Note:** `SettingsNotInitializedError` is a subclass of `FileNotFoundError` (errors.py:20). The existing `test_process_data_error_is_file_not_found` (test_app.py:144-150) passes an **existing** empty dir, so `is_dir()` passes and the settings gate raises `SettingsNotInitializedError` (which IS a `FileNotFoundError`). The new test passes a **non-existent** path, so `is_dir()` raises `FileNotFoundError` directly before reaching `get_app_service`.

---

### T-UP-08 — Add CLI `run` success test (FINDING-18)

**Status:** ✅ Test needed (new)  
**Production:** app.py:55-89

**Success output format (lines 81-85):**
```python
console.print(
    f"[green]✅ Report complete:[/green] "
    f"{result.rows_loaded:,} loaded → {result.rows_exported:,} clean "
    f"({result.duration:.1f}s) ↠ {result.export_dir}"
)
```

With `synthetic_report` (3 rows, 2 exported):
- `"Report complete"` in output ✅
- `"3 loaded → 2 clean"` in output ✅
- `result.exit_code == 0` ✅

---

### T-UP-09 — Add `ReportResult` negative validation test (FINDING-19)

**Status:** ✅ Test needed (new, depends on T-UP-10)  
**Production:** models.py:182-207

**Current `ReportResult` fields (lines 191-207):**
```python
class ReportResult(BaseModel):
    report_dir: Path               # 1
    import_dir: Path               # 2
    export_dir: Path               # 3
    db_path: Path                  # 4
    null_data_files: list[Path]    # 5
    started_at: datetime           # 6
    finished_at: datetime          # 7
    duration: float                # 8
    timings: dict[str, float]      # 9
    rows_loaded: int               # 10 ← should be NonNegativeInt
    rows_exported: int             # 11 ← should be NonNegativeInt
```

**Field count:** 11 fields (confirmed). The docstring at test_app.py:258 incorrectly says "10 fields".

---

### T-UP-10 — Change `int` → `NonNegativeInt` in `ReportResult` (FINDING-19)

**Status:** ✅ Production exists at models.py:206-207  
**Production change needed**

**Current code (lines 206-207):**
```python
    rows_loaded: int
    rows_exported: int
```

**Change to:**
```python
    rows_loaded: NonNegativeInt
    rows_exported: NonNegativeInt
```

**Verification:**
- `NonNegativeInt` is already imported at models.py:14: `NonNegativeInt,` (in the `from pydantic import (...)` block, lines 9-17)
- `NonNegativeInt` is already used elsewhere: `version: NonNegativeInt = 1` (line 53), `chunk_size: NonNegativeInt = 10000` (line 90), `skip_rows: NonNegativeInt | None = 0` (line 98), `action: NonNegativeInt = Field(default=0)` (line 148), etc.
- `ReportResult` is a Pydantic `BaseModel`, not a Django model — **no DB migration needed**
- `run_report` (app_service.py:280-292) computes `rows_loaded = self._import_data(...)` (line 276, returns `rows_count` from `chunk.write_database()` which returns `int`) and `rows_exported = csv_worker.export_sql_to_csv(...)` (line 266, returns `int` count). Both are non-negative in practice.

**Production consumers of ReportResult construction:**
- `app_service.py:280-292` — `run_report` return value (lines_loaded/rows_exported always ≥ 0) ✅
- `test_app.py:257-271` — `test_report_result_model_construction_and_serialization` (rows_loaded=100, rows_exported=95) ✅ positive
- `test_app.py:290-302` — `test_process_data_returns_report_result` (rows_loaded=42, rows_exported=40) ✅ positive

---

### T-UP-11 — Add `__version__` cross-check with `pyproject.toml` (FINDING-35)

**Status:** ✅ Test exists at test_metadata.py:10-13  
**Production:** `__init__.py:10-15`, `pyproject.toml:7`

**Current test (lines 10-13):**
```python
def test_version_is_non_empty_string():
    """__version__ resolves to a non-empty string in all execution modes."""
    assert isinstance(mko_data_cleaner.__version__, str)
    assert len(mko_data_cleaner.__version__) > 0
```

**Production code (`__init__.py:10-15`):**
```python
try:
    __version__ = version("mko_data_cleaner")
except PackageNotFoundError:
    # Fallback for source checkout / pythonpath=src (not pip-installed).
    # NOTE: must track the top-level ``version`` in pyproject.toml.
    __version__ = "0.2.0"
```

**pyproject.toml:** Line 7: `version = "0.2.0"` ✅ matches fallback

**Risk:** If fallback `"0.2.0"` is changed but `pyproject.toml` is not updated (or vice versa), the current test passes but versions drift. In a source-checkout context (test runner), `PackageNotFoundError` is raised (package not installed), so the fallback is used — the `test_version_matches_installed_metadata_when_available` test (line 16-22) skips when not installed.

**Fix:** Add `tomllib` parse of `pyproject.toml` and assert `mko_data_cleaner.__version__ == pyproject["project"]["version"]`.

**Existing pattern in test_packaging.py:14:** `ROOT = Path(__file__).resolve().parent.parent` and `PYPROJECT = ROOT / "pyproject.toml"`

---

### T-UP-12 — Verify license cross-check in `test_packaging.py` (FINDING-36)

**Status:** ✅ Existing test `test_pyproject_uses_spdx_license` at test_packaging.py:87-91 covers it

**Current test (lines 87-91):**
```python
def test_pyproject_uses_spdx_license():
    """pyproject.toml uses PEP 639 SPDX license expression (B3/D4)."""
    metadata = _load_pyproject()["project"]
    assert metadata["license"] == "BSD-3-Clause"
    assert metadata["license-files"] == ["LICENSE.txt"]
```

**pyproject.toml verification:**
- Line 15: `license = "BSD-3-Clause"` ✅
- Line 16: `license-files = ["LICENSE.txt"]` ✅

**Note:** `test_metadata_contains_required_fields` (referenced by audit FINDING-36) does NOT exist. The existing `test_pyproject_uses_spdx_license` already covers the license cross-check. This finding is addressed — no new test needed. The plan's acceptance criterion ("`pyproject.toml` `license` field cross-checked against `"BSD-3-Clause"`") is already met.

---

## Phase 3: New Tests & Production Fix

### T-WR-01 — Write direct unit tests for `_apply_add` (FINDING-21)

**Status:** ✅ Production exists at db_service.py:690-745  
**Test needed:** ✅ Create `test_apply_add_concatenates_tags`

**Production code (lines 690-745):**
```python
def _apply_add(self, separator=", "):
    # line 690
    logger.debug(f"Apply add rules to {self.target_table}")
    tag_unions = []
    for col in self.extra_columns:                          # line 696
        tag_unions.append(f"""
            SELECT rm.data_rowid AS rowid,
                   '{col}' AS column_name,
                   rm.{col} AS value
            FROM {self._joined_matches_table} rm
            WHERE rm.{col} IS NOT NULL
            AND rm.{col} != ''
        """)
    tag_expand_query = "\nUNION ALL\n".join(tag_unions)     # line 707
    insert_sql = f"""
        INSERT OR IGNORE INTO data_table_tags(rowid, column_name, value)
        {tag_expand_query}
    """                                                        # line 709-712
    self.perform_query(insert_sql)                             # line 714
    # получаем реально используемые колонки
    cols_query = """
                 SELECT DISTINCT column_name
                 FROM data_table_tags
                 """
    used_columns = self.perform_query(cols_query).fetchall()  # line 721
    used_columns = [row[0] for row in used_columns]           # line 722
    for col in used_columns:                                   # line 725
        update_sql = f"""
            UPDATE {self.target_table}
            SET {col} = (
                SELECT GROUP_CONCAT(value, '{separator}')
                FROM (
                    SELECT DISTINCT value
                    FROM data_table_tags
                    WHERE rowid = {self.target_table}.rowid
                    AND column_name = '{col}'
                    ORDER BY value
                )
            )
            WHERE rowid IN (
                SELECT rowid
                FROM data_table_tags
                WHERE column_name = '{col}'
            )
        """                                                     # line 726-743
        self.perform_query(update_sql)                          # line 745
```

**Key design points:**
- Iterates `self.extra_columns` (set via `set_data_tbl_columns(..., extra_cols=...)`)
- Builds UNION SELECT from `_joined_matches_table` filtering non-null, non-empty values
- Inserts into `data_table_tags` table (created by `_ensure_tags_table()` at line 747-758)
- Uses `GROUP_CONCAT(DISTINCT value, separator)` ordered alphabetically
- Only updates rows whose `rowid` appears in `data_table_tags`

**Sibling tests for pattern reference:**
- `test_apply_delete_executes_delete_once` (test_db_service.py:385-434) — uses `mocker.spy(db_worker, "perform_query")`
- `test_apply_replace_picks_highest_mapping_index` (test_db_service.py:437-505) — same spy pattern, same `_joined_matches_table` setup

**Imports needed:** `mocker` (pytest-mock fixture, already used in sibling tests)

---

### BP-03 — Fix `DBWorker.column_index` cache invalidation bug (FINDING-22)

**Status:** ✅ Bug confirmed — fix needed at db_service.py:133-139  
**Production code:**

`column_index` cached_property (lines 124-128):
```python
@cached_property
def column_index(self):
    if self.data_tbl_columns:
        return {i: name for i, name in enumerate(self.data_tbl_columns)}
    return {}
```

`set_data_tbl_columns` (lines 133-139):
```python
def set_data_tbl_columns(self, *main_cols, extra_cols: list | None = None):
    extra_cols = extra_cols or []
    self._data_tbl_columns = clean_names(*main_cols, *extra_cols)
    if extra_cols:
        self._extra_columns = self.data_tbl_columns[-len(extra_cols):]
    else:
        self._extra_columns = []
```

**Bug:** No `self.__dict__.pop("column_index", None)` after reassigning `self._data_tbl_columns`. `cached_property` stores result in `self.__dict__["column_index"]`, which persists.

**Confirmed:** `grep -n "__dict__.pop" src/mko_data_cleaner/core/db_service.py` → **no matches** — the invalidation is missing.

**Production impact:** Currently latent — `run_report` (app_service.py:154-156) calls `set_data_tbl_columns` once per `DBWorker` instance. No production code calls `column_index` or `get_col_names` directly (grep confirms only `test_column_index_and_get_col_names` at test_db_service.py:166-171 exercises them).

**Fix:** Add `self.__dict__.pop("column_index", None)` as the last line of `set_data_tbl_columns`.

**Regression test:** `test_column_index_reflects_second_set_data_tbl_columns`:
1. Call `set_data_tbl_columns("id", "name")` → assert `column_index == {0: "id", 1: "name"}`
2. Access `column_index` (triggers caching)
3. Call `set_data_tbl_columns("id", "name", "value", "tag")` → assert `column_index == {0: "id", 1: "name", 2: "value", 3: "tag"}`
4. Assert `get_col_names(2) == "value"` and `get_col_names(3) == "tag"`

**Existing test that must still pass:** `test_column_index_and_get_col_names` (test_db_service.py:166-171).

---

## Phase 4: Documentation Fixes

### DOC-01 — Fix `synthetic_report` docstring line references (FINDING-07)

**Status:** ✅ Docstring exists at conftest.py:122-135  
**Issue 1 — Line range:** conftest.py:126 says `scripts/smoke_install.py (lines 112-160)`.  
**Actual:** The e2e section in smoke_install.py spans **lines 112-168**:
- Line 112: comment `# (v) End-to-end on synthetic sample`
- Line 168: `print(f"[PASS] (v) end-to-end: loaded={_result.rows_loaded}, ...")`

**Issue 2 — Deleted test reference:** conftest.py:131 lists `tests/test_app.py::test_null_data_files_excludes_stale` in the "keep in sync" list. This test is deleted in T-DEL-04. The reference must be removed after deletion.

**Issue 3 — Missing success test reference (optional):** T-UP-08 (`test_run_cli_success`) also uses `synthetic_report` — should be added to the "keep in sync" list after creation.

**Docstring (lines 124-135):**
```python
"""Create a tiny synthetic report directory for end-to-end tests.

Mirrors the e2e scenario in scripts/smoke_install.py (lines 112-160)
and tests/test_app.py::test_process_data_end_to_end. When either the
data or the assertions change, update ALL copies:
  1. scripts/smoke_install.py (maintenance comment)
  2. tests/test_app.py::test_process_data_end_to_end
  3. tests/test_app.py::test_null_data_files_excludes_stale
Shared assertions: rows_loaded==3, rows_exported==2,
export_dir/import_dir/report_dir resolved, export dir exists with
.gz files, null_data_files is list, duration>0, timings keys.
"""
```

---

### DOC-02 — Fix `ReportResult` docstring field count (FINDING-20)

**Status:** ✅ Docstring exists at test_app.py:258  
**Issue:** Docstring says "all 10 fields" but `ReportResult` has 11 fields (models.py:182-207).

**Current docstring (line 258):**
```python
"""ReportResult accepts all 10 fields and serializes via model_dump()."""
```

**Fix:** Change `"10 fields"` → `"11 fields"`.

**Field enumeration:**
1. `report_dir: Path` (line 191)
2. `import_dir: Path` (line 192)
3. `export_dir: Path` (line 193)
4. `db_path: Path` (line 194)
5. `null_data_files: list[Path]` (line 195)
6. `started_at: datetime` (line 198)
7. `finished_at: datetime` (line 199)
8. `duration: float` (line 200)
9. `timings: dict[str, float]` (line 203)
10. `rows_loaded: int` (line 206)
11. `rows_exported: int` (line 207)

**Test construct at lines 259-271** correctly passes all 11 fields — only the docstring is stale.

---

## Cross-Cutting Discrepancies & Risks

### D1. Line-number drift between audit plan and current tree

The plan was written against audit baseline `f6ac8e1` (26 commits behind `a41a445`). Several line references are off by small margins:

| Plan reference | Actual line | Delta |
|---|---|---|
| T-DEL-06: test_app.py:372-378 | 372-379 | +1 line |
| T-RW-05: test_app.py:438-439 (assertions) | 436-437 | -2 lines |
| T-RW-04: test_app.py:257 (docstring) | 258 | +1 line |

These are cosmetic — the functions exist at the referenced ranges but content shifted slightly. Implementations should grep for the function name, not rely on line numbers.

### D2. Plan's T-RW-01 analysis is incorrect about control flow

**Severity:** HIGH (could lead to removing a correct assertion)

The plan (lines 188-189) claims `SettingsNotInitializedError` is caught by the `except Exception` at app.py:87 and converted to `typer.Exit(1)`. **This is wrong** — `get_app_service(settings_dir)` is called at line 77, **outside** the try/except block (lines 79-89). The exception propagates uncaught; `CliRunner` catches it and stores the original `SettingsNotInitializedError` in `result.exception` with `exit_code=1`.

**Impact:** If an implementor follows the plan's suggestion to "Remove the `isinstance(result.exception, ...)` assertion," they would remove a **valid** assertion. The correct rewrite keeps the `isinstance` check AND adds `exit_code == 1` + output verification.

### D3. FINDING-33 reclassified from TEST-UPDATE to TEST-REWRITE

The audit report (line 71) classifies FINDING-33 as `[TEST-UPDATE]` because it references a test that should exist. The plan (T-RW-10) reclassifies it as `[TEST-REWRITE]` and notes: "Placed in Phase 1 (Rewrites) per the task's categorization" (plan line 1007). Since the test doesn't exist, it must be created — the plan's classification is more accurate.

### D4. `app.py __all__` includes unregistered exports

`app.py:157-165` declares:
```python
__all__ = [
    "process_data", "initialize_settings", "init", "run",
    "list_settings", "app", "get_app_service",
]
```
But `__init__.py`'s `__all__` (lines 17-24) does NOT include `init`, `run`, `list_settings`, `app`, or `get_app_service`. These are importable via `from mko_data_cleaner.app import run` but NOT via `from mko_data_cleaner import run`. The `test_top_level_exports` test (test_app.py:384-403) only checks the package-level `__all__`, not module-level. This is not flagged by the audit but is a latent API consistency issue.

### D5. `SettingsNotInitializedError` is a subclass of `FileNotFoundError`

`errors.py:20`: `class SettingsNotInitializedError(FileNotFoundError)`. This means the `is_dir()` guard in `process_data` (app.py:143-144) and the settings check (`_require_settings` in app_service.py:337) both raise `FileNotFoundError` subtypes. The existing `test_process_data_error_is_file_not_found` (test_app.py:144-150) catches this ambiguity correctly — it asserts `isinstance(exc_info.value, FileNotFoundError)` AND `not isinstance(exc_info.value, ValidationError)`.

### D6. Test count discrepancy in audit report

The audit report (line 958) claims 76 test functions total, but the current tree has 81 (70 in scope + 11 in notebook tests). The audit counted 18 for test_app.py; the actual count is 24. The 6 extra tests in test_app.py likely include the `test_config_to_dict_*` and `test_process_data_error_*` tests that were added after the audit baseline.

---

## Exact Imports Required Per Test

| Test file | Existing imports | Needed additions |
|---|---|---|
| `test_app.py` | `SimpleNamespace` (types), `SettingsNotInitializedError`, `ReportResult`, `CliRunner`, `ValidationError` | `MagicMock` (unittest.mock), `WrongDataSettings` (core.errors), `DataFileExtension` (core.models) |
| `test_dict_service.py` | `pl`, `MappingDict`, `MappingColumns`, `MatchType` | None (all already imported) |
| `test_csv_service.py` | `sqlite3`, `pl`, `CSVWorker` | `WrongDataSettings` (core.errors), `gzip` (for T-RW-09) |
| `test_db_service.py` | `DBWorker`, `mocker` (via pytest-mock) | None |
| `test_metadata.py` | `PackageNotFoundError`, `version`, `pytest`, `mko_data_cleaner` | `tomllib`, `Path` (for T-UP-11) |
| `test_packaging.py` | `tomllib`, `Path`, `_load_pyproject()` helper | None (T-UP-12 is already covered) |

---

## Execution Ordering Summary (from plan DAG)

The plan's dependency graph is correct:
1. **Phase 0** (9 tasks) — all independent, sequential within single implementor
2. **Phase 1** (11 tasks) — depends on Phase 0; T-RW-06/07/08 can be standalone
3. **Phase 2**: T-UP-01 (ENDS_WITH row) must precede T-RW-06, T-UP-04; T-UP-10 (prod change) must precede T-UP-09
4. **Phase 3**: T-WR-01 and BP-03 depend on Phase 0 + Phase 2
5. **Phase 4**: DOC-01 depends on T-DEL-04 (Phase 0); DOC-02 is independent

**T-UP-10 (NonNegativeInt production change)** can run in parallel with Phase 0/1 since it doesn't break existing tests (all positive values remain valid).
