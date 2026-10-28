---
phase: "11"
phase_name: "test-coverage"
id_prefix: "TST"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/11-audit-test-coverage.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Test Coverage

## Summary

Six findings; the highest severity is CRITICAL. The broadest consequence is false assurance: the green suite proves nothing about a core shipped feature with zero test coverage (TST-001) nor about user-config preservation on a default re-init (TST-010), and the type-safety gate is red yet never blocks merges (TST-002) — so feature, config, and type regressions can ship while CI stays green.

## Scope

**Examined:** `src/mko_data_cleaner/` (`app_service`, `csv_service`, `dict_service`, `db_service`, `models`, `paths`, `utils`, `errors`, `init_service`), `app.py`, `notebook.py`, `tests/` (`conftest` + `test_app`, `test_csv_service`, `test_db_service`, `test_dict_service`, `test_notebook`, `test_notebook_check`, `test_metadata`, `test_packaging`), `.github/workflows/ci.yml`, `pyproject.toml`, `scripts/smoke_install.py`, `examples/notebooks/*`, `MANIFEST.in`.
**Tools:** `pytest --cov=mko_data_cleaner --cov-report=term-missing`, `ruff check`, `mypy`, `basedpyright`, `select-string`/`git ls-files`, `git rev-parse`.
**Not verified:** CI jobs at runtime (`build`, `install-smoke`, `notebook` kernel execution) — read only from `ci.yml` text, not executed in this sandbox (no GitHub Actions). Real user reports with FTS rules or date-less CSVs — production behaviour beyond the synthetic fixture.

> IDs TST-001, TST-002, TST-007–010. The 003–006 range is vacant: it is referenced only by stale prior-cycle cross-references inside test docstrings (e.g. `tests/test_db_service.py:456 (TST-003)`); per the phase file the collision warning is phantom, and those numbers are left vacant rather than reused so the current cycle's IDs are unambiguous. (The `TST-` prefix collision warning in the phase file is itself disproven by this same grep: `grep -rn 'TST-0[0-9]' tests/` returns the stale cross-references above — no findings document or shipped source collides. TST-010 is minted adjacent to the vacant 003–006 range, not within it.)

<!-- GATES — verified: every cited path is in `git ls-files`; every disambiguator token exists in its primary anchor file; no `:<digits>` in any locator. -->

## Findings

| ID | Severity | Anchor | Title |
|---|---|---|---|
| TST-001 | CRITICAL | `src/mko_data_cleaner/core/db_service.py::DBWorker.insert_matches_from_fts` | [B1] FTS matches-bridge never exercised by the suite |
| TST-002 | HIGH | `.github/workflows/ci.yml::"Type check (non-blocking)"` | [B7] Type-safety gate is red and never blocks a merge |
| TST-007 | MEDIUM | `src/mko_data_cleaner/core/db_service.py::DBWorker.update_index_from_data` | [B4] `date_column=None` boundary crossed only on the date-present side |
| TST-008 | MEDIUM | `pyproject.toml::[tool.pytest.ini_options]` | [B5] No test-timeout guard; one hang stalls the run |
| TST-009 | LOW | `scripts/smoke_install.py::"keep in sync with"` | [B8] Fixture-data sync comment names a deleted test |
| TST-010 | MEDIUM | `src/mko_data_cleaner/core/init_service.py::_copy_templates` | [B1/B4] `force=False` re-init skip-existing (don't overwrite user settings) unobserved |

#### TST-001 — [CRITICAL] FTS matches-bridge never exercised by the suite

**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker.insert_matches_from_fts`
**Anchor:** `src/mko_data_cleaner/core/app_service.py::AppService.run_report`
**Disambiguator:** `_full_matches_table`
**Checked:** ran (coverage) + static (grep)
**Why this band:** Meets the CRITICAL second clause — a decision the system acts on (`if not mapping_dict.fts_data.is_empty()` → `insert_matches_from_fts`) whose effect (FTS rules producing matches so they fire) no assertion anywhere can observe, received by FTS-rule users. Adjacent NOT graded as HIGH ("boundary asserted from one side only") because the matches side is not merely one-sided — it has *zero* coverage (not even a direct unit test), and it is the sole path that makes the shipped FTS feature fire.
**Problem:** `insert_matches_from_fts` (db_service.py:559-571) is the bridge `INSERT INTO fts_full_matches SELECT ... FROM fts_table JOIN mapping_table ... MATCH ...`. It has no test. Its only caller is `run_report`'s FTS branch (app_service.py:171-172, 181-194), whose `if not mapping_dict.fts_data.is_empty()` decision is never True in the suite: `synthetic_report` (conftest.py:119-123) ships only `d`/`a` rows with `match="f"`, so `fts_data` is always empty in tests. FTS *looks* covered (`link_search_table`, triggers, `build_match_query` are unit-tested) but the extraction that feeds every FTS rule is not.
**Repro:** 1) `uv run pytest --cov=mko_data_cleaner --cov-report=term-missing -q` 2) read `app_service.py` row → `Missing 51-52, 62-64, 172, 182-194, 305, 344` and `db_service.py` row → `Missing … 560-571 …`. 3) `grep insert_matches_from_fts tests/` → 0 hits. 4) `conftest.py::synthetic_report` `dict_lines` contains only `d;f;4` and `a;f;4` — no `fts` match row.
**Impact:** A defect in the FTS→matches SQL (wrong `MATCH` column, wrong join key, wrong table name) ships undetected; users whose dictionary uses FTS rules get rules that silently never match.
**Root cause:** No integration fixture exercises the FTS rule path through `run_report`, and `insert_matches_from_fts` has no direct unit test.
**Fix:** Add a direct unit test for `insert_matches_from_fts` (populate `fts_temp_tbl`+`full_matches_table`, assert rows land via the MATCH join); add an `fts` row to `synthetic_report`'s `dict_lines` so `run_report`'s FTS branch fires end-to-end.
**Verify:** new test needed: `test_insert_matches_from_fts_populates_matches` + FTS assertion in `test_process_data_end_to_end`.
**Related:** Phase 04 (FTS SQL); OQ-5 (owner notes "any FTS5 rule path" not executed).
**Evidence — `uv run pytest --cov=mko_data_cleaner --cov-report=term-missing -q`** *(supports: run_report FTS branch + insert_matches_from_fts uncovered)*:
```text
src/mko_data_cleaner/core/app_service.py      148     11    93%   51-52, 62-64, 172, 182-194, 305, 344
src/mko_data_cleaner/core/db_service.py       318     37    88%   51-52, 128, 158, 198, 230-234, 259-262, 281-283, 294-301, 345-348, 560-571, 641, 645-646, 783-784, 792-793
```
**Evidence — `select-string -Path tests -Pattern insert_matches_from_fts`** *(supports: no test calls the bridge)*:
```text
(no matches)
```

#### TST-002 — [HIGH] Type-safety gate is red and never blocks a merge

**Anchor:** `.github/workflows/ci.yml::"Type check (non-blocking)"`
**Anchor:** `pyproject.toml::[tool.mypy]`
**Anchor:** `pyproject.toml::[tool.basedpyright]`
**Disambiguator:** `continue-on-error`
**Checked:** ran (mypy, basedpyright, ruff, pytest) + static (ci.yml)
**Why this band:** HIGH — "a property the harness depends on that nothing holds it to" (project rule: Type Safety Everywhere) and "a gate declared and never loaded": CI's mypy step is `continue-on-error: true` (red but ignored), and the locally-documented `Typecheck` command (basedpyright) is red and absent from CI. Adjacent NOT graded as CRITICAL (no shipped assertion is false — CI green is accurate because the type step is explicitly allowed to fail) and not MEDIUM (the redness is not a "known accepted-noise class"; the project set these strict bars itself via `disallow_any_generics`, `strict_optional`, etc.).
**Problem:** CI runs `uv run mypy src/mko_data_cleaner` under `continue-on-error: true` (ci.yml:27-29) and fails with 45 errors; the step never fails the job, so type errors ship unchecked. Meanwhile `.kilo/commands` documents the type check as `uv run basedpyright src/` (39 errors), which is not referenced by CI at all — two type checkers, neither green, neither blocking.
**Repro:** 1) `uv run mypy src/mko_data_cleaner` → `Found 45 errors in 6 files`. 2) `uv run basedpyright src/` → `39 errors, 0 warnings, 0 notes`. 3) `uv run ruff check src/ tests/` → `All checks passed!`. 4) `uv run pytest` → `93 passed in 3.76s`. 5) `ci.yml:27-29` — the type step is the only `continue-on-error` step; ruff/pytest are blocking.
**Impact:** A contributor running the documented Typecheck sees red and faces no CI block; CI's own type errors are silently ignored; so type regressions (e.g. the `tuple[int,str]`/`str | None` lie in `check_date_column`, or the `Optional` mismatches) accumulate with no gate.
**Root cause:** The type gate was wired non-blocking (`continue-on-error: true`) in CI; the documented local typecheck (basedpyright) was never promoted to CI; the two checkers are not unified.
**Fix:** Promote a single type checker to a blocking CI step and drive it to zero errors (remove `continue-on-error: true` on the mypy step and fix the 45 errors — Phase 04 owns the diagnostics — or replace mypy with a blocking basedpyright step). Do not keep two red type checkers.
**Verify:** `uv run mypy src/mko_data_cleaner` (or `basedpyright`) returns 0 errors AND `ci.yml` type step has no `continue-on-error: true`.
**Related:** Phase 04 (populates the 45/39 diagnostics); QLT-007 (basedpyright config rationale).
**Evidence — `uv run mypy src/mko_data_cleaner`** *(supports: CI type gate is red)*:
```text
Found 45 errors in 6 files (checked 13 source files)
```
**Evidence — `uv run basedpyright src/`** *(supports: locally-documented Typecheck is red and not in CI)*:
```text
39 errors, 0 warnings, 0 notes
```
**Evidence — `.github/workflows/ci.yml:27-29`** *(supports: mypy is non-blocking in CI)*:
```text
      - name: Type check (non-blocking)
        continue-on-error: true
        run: uv run mypy src/mko_data_cleaner
```

#### TST-007 — [MEDIUM] `date_column=None` boundary crossed only on the date-present side

**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker.update_index_from_data`
**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker._create_index_table`
**Anchor:** `src/mko_data_cleaner/core/csv_service.py::CSVWorker._detect_date_column`
**Disambiguator:** `if self.date_column`
**Checked:** ran (coverage) + static (grep of fixtures)
**Why this band:** MEDIUM — bounded and real with a specific audience (reports that contain no date-like column). By blast radius this is MEDIUM, not CRITICAL: the date-present path is genuinely tested and no shipped assertion is false. Adjacent NOT graded as HIGH ("boundary asserted from one side only") describes the mechanism, but the untested side is a trivial, documented branch (`if self.date_column:` skip) with a limited audience, so effect/blast-radius lands it in MEDIUM.
**Problem:** `update_index_from_data` (db_service.py:400-416) and `_create_index_table` (db_service.py:368-381) branch on `if self.date_column:`. The False branch omits `MAX(date)` and the date column from the distinct index table. `run_report` reaches it only when `check_date_column` returns `None`. But `_detect_date_column`'s `return None` (csv_service.py:151 — coverage-missing) never executes: the `db_worker` fixture (conftest.py:31-38) always sets `date_column` from config (`"researchDate"`), and `synthetic_report` ships `researchDate` rows, so the date-absent side is unreachable through the harness.
**Repro:** 1) `uv run pytest --cov=mko_data_cleaner --cov-report=term-missing -q` → `csv_service.py … 151 …` is in the missing list (the `return None` when no date column is found). 2) `grep -rn 'update_index_from_data' tests/` → 0 direct calls. 3) `grep -rn 'date_column=None' tests/` → 0. 4) `conftest.py:36` — `date_column=realistic_config["data_file_settings"]["date_column"]`.
**Impact:** A regression in the no-date path (e.g. the index table built without a date column but `update_index_from_data` still referencing one) would break date-less reports only, with no test to surface it.
**Root cause:** The shared `db_worker` and `synthetic_report` fixtures hard-code the date-present universe; no fixture manufactures the date-absent state (a block-3 fixture-data constraint absorbed).
**Fix:** Add a case to `test_process_data_end_to_end` (or a new test) with a CSV that has no date-like column, asserting the distinct index is built without a date column and `MAX(date)` is not referenced.
**Verify:** new test needed: `test_run_report_without_date_column_skips_max_date`.
**Related:** TST-001 (same run_report branch-coverage gap class); OQ-5 (no-date path); block 3 (fixture absorbs the `None` state).
**Evidence — `uv run pytest --cov=mko_data_cleaner --cov-report=term-missing -q`** *(supports: csv_service `return None` at 151 is uncovered)*:
```text
src/mko_data_cleaner/core/csv_service.py      162     16    90%   84-86, 145-146, 151, 182-184, 205, 226-228, 379-381
src/mko_data_cleaner/core/db_service.py       318     37    88%   ... (405/374 line-covered but date=None branch never taken) ...
```
**Evidence — `select-string -Path tests -Pattern 'date_column=None|update_index_from_data'`** *(supports: no test crosses the absent branch)*:
```text
(no matches)
```

#### TST-008 — [MEDIUM] No test-timeout guard; one hang stalls the run

**Anchor:** `pyproject.toml::[tool.pytest.ini_options]`
**Disambiguator:** `--strict-markers`
**Checked:** ran (pytest plugin list + grep) + static (ci.yml)
**Why this band:** MEDIUM — exact MEDIUM descriptor "a resource left unbounded so that one test can take the run down." Adjacent NOT graded as HIGH (no test currently hangs, so this is latent, not an active false-green) and not LOW (a hung run stalls the whole suite, not "no runtime consequence").
**Problem:** `addopts` (pyproject.toml:159-165) has `--import-mode=importlib`, `-ra`, `-v`, `--strict-markers` — no `--timeout`. The `dev` dependency group has no `pytest-timeout`, and `.github/workflows/ci.yml` declares no `timeout-minutes` on the `test` job. A test that hangs (e.g. a DB deadlock via ADBC, or an infinite `progress_bar`) blocks the local run indefinitely and the CI job up to GitHub's 360-minute default.
**Repro:** 1) `select-string pyproject.toml -Pattern timeout|pytest-timeout` → no match. 2) `git ls-files .github/**` → `.github/workflows/ci.yml` only; `select-string ci.yml -Pattern timeout` → no match. 3) `uv run pytest -q` → `plugins: cov-7.1.0, mock-3.15.1` (no timeout plugin).
**Impact:** On-call/developer running `uv run pytest` on a branch that introduces a hang gets a silent, unrecoverable stall — no failure, no artifact, no timeout log.
**Root cause:** The harness never acquired a timeout primitive; the only loaded plugins are `cov` (undeclared in pyproject dev deps) and `mock`.
**Fix:** Add `pytest-timeout` to the `dev` group and `--timeout=30 --timeout-method=thread` to `addopts`; add `timeout-minutes:` to the CI `test` job as a backstop.
**Verify:** `uv run pytest` raises a `Timeout` failure (not a hang) when a test exceeds the bound; CI `test` job declares `timeout-minutes`.
**Related:** TST-002 (gates); Scope (the `cov-7.1.0` plugin is present in the venv but undeclared in pyproject dev deps — a non-reproducible measurement).
**Evidence — `pyproject.toml:159-165`** *(supports: addopts has no timeout flag)*:
```text
addopts = [
    "--import-mode=importlib",
    "-ra",
    "-v",
    "--strict-markers",
]
```
**Evidence — `uv run pytest -q` plugin line** *(supports: no timeout plugin loaded)*:
```text
plugins: cov-7.1.0, mock-3.15.1
```

#### TST-009 — [LOW] Fixture-data sync comment names a deleted test

**Anchor:** `scripts/smoke_install.py::"DUPLICATED — keep in sync with"`
**Anchor:** `tests/conftest.py::synthetic_report docstring`
**Disambiguator:** `test_null_data_files_excludes_stale`
**Checked:** ran (grep)
**Why this band:** LOW — self-description/hygiene, no runtime consequence today: the two copies of the synthetic sample (conftest fixture and smoke_install.py) currently agree on data, and CI's `install-smoke` job runs `smoke_install.py` green. Adjacent NOT graded as MEDIUM (no behaviour gap yet, only a stale sync list).
**Problem:** `smoke_install.py:114-117` documents its synthetic sample as "DUPLICATED — keep in sync with: tests/test_app.py::test_process_data_end_to_end, tests/test_app.py::test_null_data_files_excludes_stale." The second name was deleted from the tree — `grep -rn test_null_data_files_excludes_stale tests/` returns 0. The authoritative contract in `conftest.py::synthetic_report` (lines 91-103) lists the correct, current trio of sync sites, so the drift is isolated to smoke_install.py's comment.
**Repro:** 1) `select-string -Path scripts/smoke_install.py -Pattern test_null_data_files_excludes_stale` → `116: #   - tests/test_app.py::test_null_data_files_excludes_stale`. 2) `select-string -Path tests -Pattern test_null_data_files_excludes_stale` → 0 matches.
**Impact:** A maintainer updating the synthetic sample follows smoke_install.py's list, greps for a removed test, and may miss the real co-evolving site `test_run_cli_success` (absent from smoke_install.py's list, present in conftest.py's) — raising future drift risk.
**Root cause:** The sync list in smoke_install.py was not updated when `test_null_data_files_excludes_stale` was deleted during a prior-cycle remediation; the data itself stayed in sync.
**Fix:** Edit `scripts/smoke_install.py:114-117` to match `conftest.py::synthetic_report` docstring's current sync list (drop the deleted test, add `test_run_cli_success` so the two lists agree).
**Verify:** `grep -rn test_null_data_files_excludes_stale scripts/ tests/` → 0 matches after update; both docstrings list the same trio.
**Related:** Block 3 (two-home fixture data); block 8 (stale comment misdescribes the harness).
**Evidence — `scripts/smoke_install.py:114-117`** *(supports: stale sync list)*:
```text
# DUPLICATED — keep in sync with:
#   - tests/test_app.py::test_process_data_end_to_end
#   - tests/test_app.py::test_null_data_files_excludes_stale
#   (both use tests/conftest.py::synthetic_report fixture)
```
**Evidence — `select-string -Path tests -Pattern test_null_data_files_excludes_stale`** *(supports: named test is absent)*:
```text
(no matches)
```

#### TST-010 — [MEDIUM] `force=False` re-init skip-existing branch unobserved

**Anchor:** `src/mko_data_cleaner/core/init_service.py::_copy_templates`
**Disambiguator:** `if target_path.exists() and not force`
**Checked:** ran (coverage) + static (grep)
**Why this band:** MEDIUM — a config-preservation decision the system acts on (`initialize_settings()` / CLI `init` default to `force=False`, so `_copy_templates` must skip an existing user settings file) whose effect no assertion observes; bounded to the re-init audience. Adjacent NOT graded as HIGH because the untested side is a single guard statement with limited blast radius (the copy path is fully covered via the `--force` tests); NOT graded as LOW because it carries real runtime consequence (not overwriting user-customised config).
**Problem:** `_copy_templates` (init_service.py:23-24) guards `if target_path.exists() and not force: continue` so a non-force re-init leaves an existing `app_config.json5`/`log_config.json5` untouched. The `continue` (line 24) is the only uncovered statement in `_copy_templates` (`Missing 20-21, 24, 57`). Every test uses either a fresh dir (`test_initialize_settings_threads_settings_dir`) or `--force` (`test_initialize_settings_force_overwrites`, `test_init_cli_creates_settings_in_tmp_dir`); no test pre-seeds an existing settings file with `force=False` and asserts preservation.
**Repro:** 1) `uv run pytest --cov=mko_data_cleaner.core.init_service --cov-report=term-missing -q` → `Missing 20-21, 24, 57`. 2) `select-string -Path tests -Pattern 'force=False'` → `(no matches)`. 3) `grep -rn 'force=False' tests/` → 0.
**Impact:** If the guard regresses (e.g. the `not force` term is dropped), `mko-data-cleaner init` / `initialize_settings()` silently overwrites a user's customised `app_config.json5` on every subsequent run — config/data loss for the operator re-initialising.
**Root cause:** The init tests cover the write/overwrite side only; the non-overwrite (preservation) contract has no fixture that pre-seeds an existing config and asserts non-overwrite.
**Fix:** Add a test that pre-creates `app_config.json5` with a sentinel string, calls `initialize_settings(force=False)` (and the `init` CLI without `--force`), and asserts the sentinel is intact. Target: `src/mko_data_cleaner/core/init_service.py::_copy_templates` line 24.
**Verify:** new test needed: `test_initialize_settings_no_force_preserves_existing`.
**Related:** TST-001 (same untested-branch class in a shipped path); Block 3 (no fixture manufactures the pre-existing-state case).

**Evidence — `uv run pytest --cov=mko_data_cleaner.core.init_service --cov-report=term-missing -q`** *(supports: line 24 skip-branch uncovered)*:
```text
src\mko_data_cleaner\core\init_service.py      25      4    84%   20-21, 24, 57
```
**Evidence — `select-string -Path tests -Pattern 'force=False'`** *(supports: no test exercises the non-force preservation branch)*:
```text
(no matches)
```

## Fix Order

- **Dependencies:** TST-002 Fix (make a type gate blocking) depends on Phase 04 fixing the 45 mypy / 39 basedpyright diagnostics first (Phase 04 owns the diagnostics; TST-002 owns the gate).
- **Merge candidates:** TST-001 and TST-007 share a root cause (synthetic_report exercises only a subset of `run_report` branches); their fixes are independent but both expand the e2e fixture.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** None. TST-002 complements Phase 04 (diagnostics vs. gate); TST-001 complements Phase 04 (FTS SQL correctness vs. coverage). Neither re-files another phase's defect.
- **Rollout risk:** TST-001 Fix — adding an `fts` row to `synthetic_report` changes `rows_loaded`/`rows_exported`, so `test_process_data_end_to_end` (`==3`/`==2`) and `test_run_cli_success` (`"3 loaded → 2 clean"`) must co-change or the suite breaks. TST-002 Fix — removing `continue-on-error` makes CI red until mypy is 0, blocking merges until Phase 04 lands. TST-007/TST-008/TST-009 Fix — new test / config-only / comment-only, non-breaking. TST-010 Fix — adds a non-force re-init preservation test; independent and non-breaking.

## Appendices

### Appendix A — Gate inventory (block 7 evidence)

```text
$ uv run ruff check src/ tests/            # CI: also lints scripts/ — both green
All checks passed!

$ uv run mypy src/mko_data_cleaner         # CI type gate (continue-on-error: true)
Found 45 errors in 6 files (checked 13 source files)

$ uv run basedpyright src/                 # documented local Typecheck — NOT in CI
39 errors, 0 warnings, 0 notes

$ uv run pytest                            # CI blocking gate
93 passed in 3.76s         (plugins: cov-7.1.0, mock-3.15.1)
```

CI gates that **block** merges: `ruff check src/ tests/ scripts/` (ci.yml:26), `pytest` (ci.yml:30), plus `build`/`twine check`/`check-wheel-contents`/wheel-content asserts (ci.yml:42-48), `install-smoke` runs `scripts/smoke_install.py` (ci.yml:72), and the `notebook` job executes the example notebook via nbclient (ci.yml:84-94). The **type** step is the sole `continue-on-error: true` step — the only declared gate that is loaded and red and explicitly does not block.

### Appendix B — Clean-block assessments (blocks 2, 3, 6)

- **Block 2 (doubles):** The doubles inventory was checked. Every replaced collaborator has its property observed elsewhere: the `MagicMock`-for-`AppService` double (`test_process_data_returns_report_result`) replaces the real pipeline, but `test_process_data_end_to_end` observes the real transformation; the `CSVWorker.__new__` bypasses of `__init__` are covered by `test_import_csv_no_delimiter_raises` (the `__init__` no-files guard); the `find_spec` pyarrow stand-ins are observed by `test_pyarrow_guard_noop_when_absent` / `test_pyarrow_broken_guard_raises_actionable_error`; the notebook cell-text inspection is backed by CI's `notebook` execution job. No block-2 finding.
- **Block 3 (fixture data contract):** `sample_dictionary`, `dict_indexes`, and `realistic_config` are consistent (`dict_indexes = DictColumnsIndexes(**realistic_config["dict_file_settings"]["col_indexes"])`); the only divergence is the stale sync-list in `smoke_install.py` (TST-009). Not a separate finding.
- **Block 6 (reproducibility):** The suite is reproducible — fixed synthetic data, per-test `tmp_path` SQLite, and the `_isolate_logging` autouse fixture restoring root/loggers. Per-run variation is the `strftime` timestamp in export filenames, recovered by `export_dir.glob("*.gz")` (not asserted as an exact name). No stray `log.log` was left in the tree after a run (`git status --short` clean of code artifacts). No block-6 finding.
