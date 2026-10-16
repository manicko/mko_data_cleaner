# Execution Plan 03 — Test Coverage Remediation (TST-001…TST-018, SR-01…SR-04)

- **Status:** Draft (planning phase — read-only; no code changes)
- **Created:** 2026-10-16 by Tech Lead (multi-agent workflow)
- **Source spec:** `.ai/audit/99-validation/11-test-coverage-validated-findings.md` (18 validated findings, HEAD `f6ac8e1`, 17 confirmed, TST-006 re-graded HIGH→LOW)
- **Supplementary findings (auditor):** SR-01 (spurious `strict` column), SR-02 (`[-0:]` slice bug), SR-03 (polars 2.0 pin stale), SR-04 (scripts/ ungated)
- **Baseline HEAD:** `f6ac8e1`
- **Test gate:** `uv run pytest` → 65 passed / 0 warnings; `ruff check src/ tests/` clean; mypy 48 src / 50 test errors (mypy 1.19.1)

> **⚠ Corrections per Auditor inspection (Step 0):**
> - mypy version is **1.19.1** (not 2.4.0 as cited in plan/finding text). Pin targets 1.19.1.
> - mypy test error count is **50** (not 7). OD-2 Option D rationale corrected accordingly.
> - B03 FTS row requires changing `sample_dictionary` `search` column schema from `pl.Int64` to `pl.String` (B03 block updated below).
> - B17 Cyrillic count is **51 lines** (6 conftest + 20 test_dict + 25 test_db), not 65.
> - B10: `check_date_column` return-type annotation `-> tuple[int, str] | None` is incorrect — method returns `str | None` via `_detect_date_column`. Noted in B10.

---

## Overview

This plan decomposes the 18 validated TST findings + 4 supplementary SR findings into 20
execution blocks, ordered so that `pytest` stays green (65 passed) after every commit.
Production-code changes are isolated into their own blocks, each with a dedicated
regression test. Test-only changes are grouped into thematic clusters that can land
without production modifications.

Five findings require Tech Lead resolution **before** implementation can begin (see
[Open Decisions](#open-decisions-tech-lead-resolution)). Blocks dependent on an open
decision are marked with a ⚠ GATE.

---

## Global Constraints

- **C1.** All comments, strings, and identifiers in modified code must be English (project rule 1; `.kilo/rules/project.md` §1).
- **C2.** Semantic anchors only — never line numbers. Use `{file}::{symbol}`.
- **C3.** Each production-code block must include a regression test that fails pre-fix and passes post-fix (mirror plan 02's B1 pattern).
- **C4.** The `pytest` suite must pass after every individual commit (65 passed baseline).
- **C5.** Production-code blocks must not be mixed with unrelated test-only changes in the same commit.
- **C6.** No changes to `.ai/audit/` files; no changes to existing plan files.
- **C7.** Gate: changes to `pyproject.toml` (dependency changes, mypy config, ruff scope) are CI-visible — the CI `test` job runs `uv run ruff check src/ tests/` + `uv run pytest`.
- **C8.** The `scripts/` directory is excluded from CI ruff and pytest scope (`testpaths = ["tests"]`, CI ruff `src/ tests/`).
- **C9.** Do not introduce speculative abstractions; keep changes minimal and targeted.
- **C10.** Use StrEnum for all fixed values (project rule 10); Pydantic v2 for all models (rule 11).

---

## Block Decomposition (Dependency Order)

| ID  | Phase | Findings                          | Summary                                          | Depends on                          | Risk   | Agents                     |
|-----|-------|-----------------------------------|--------------------------------------------------|-------------------------------------|--------|----------------------------|
| B01 | P1    | SR-01                             | Fix spurious `strict` column in `_build_query`   | —                                   | Low    | Implementor + Validator    |
| B02 | P1    | SR-02                             | Fix `[-0:]` empty-slice bug in `set_data_tbl_columns` | —                            | Low    | Implementor + Validator    |
| B03 | P2    | TST-001, TST-002                  | Add FTS row to fixture + un-guard assertions     | B01 (clean data)                    | Low    | Implementor                |
| B04 | P2    | TST-016, TST-006                  | Wire `extra_cols` fixture + multi-extra-col test | B03, B02 (SR-02)                   | Low    | Implementor                |
| B05 | P3    | TST-003                           | Add REPLACE action test                           | —                                   | Low    | Implementor                |
| B06 | P3    | TST-005 (+ optional drop_triggers) | FTS5 trigger firing test                          | —                                   | Low    | Implementor                |
| B07 | P4    | TST-018                           | Extract shared E2E helper                         | ⚠ GATE: TST-018 decision            | Low    | Planner (decision)         |
| B08 | P4    | TST-010                           | Remove pre-created merged file + merge test       | B07; ⚠ GATE: column-name investigation | Medium | Auditor → Implementor      |
| B09 | P4    | TST-004                           | CSV roundtrip content test                        | B08                                 | Low    | Implementor                |
| B10 | P5    | TST-009                           | DATE_REGEX reference + both verdicts              | —                                   | Low    | Implementor                |
| B11 | P6    | TST-008                           | Fix `null_data_files` path attribution            | —                                   | Medium | Implementor + Validator    |
| B12 | P6    | TST-011                           | `drop_table` contract fix                         | ⚠ GATE: TST-011 decision            | Low    | Planner (decision)         |
| B13 | P7    | TST-012                           | `slow` marker: mark-real-use or delete            | —                                   | Low    | Implementor                |
| B14 | P7    | TST-007                           | Add mypy to CI                                    | ⚠ GATE: TST-007 strategy            | High   | Researcher → Planner       |
| B15 | P7    | SR-03                             | Lift polars 2.0 pin                               | B01, B14 (after mypy strategy)      | Medium | Researcher → Planner       |
| B16 | P7    | SR-04                             | Add `scripts/` to ruff/CI scope                   | —                                   | Low    | Implementor                |
| B17 | P8    | TST-015                           | Translate Cyrillic comments                       | ⚠ GATE: TST-015 scope               | Low    | Planner (decision)         |
| B18 | P8    | TST-017                           | Load template values via `importlib.resources`    | B17 (same files)                    | Low    | Implementor                |
| B19 | P8    | TST-013                           | Fix notebook docstring gate references            | —                                   | Low    | Implementor                |
| B20 | P8    | TST-014                           | Replace source-text assertions                    | —                                   | Low    | Implementor                |

**Phases:** P1 = Production inert-bug fixes · P2 = Fixture-contract cluster · P3 = db_service test coverage · P4 = E2E cluster · P5 = CSV service test coverage · P6 = Production-defect fixes · P7 = Gate/config · P8 = Test hygiene

---

## Open Decisions (Tech Lead Resolution Required)

These decisions must be made **before** the corresponding block can be implemented.

### OD-1 · TST-011: `drop_table` contract — production fix or test/docstring fix?

**Finding:** `DBWorker.drop_table` (db_service.py) executes `DROP TABLE IF EXISTS`
and returns `bool(self.perform_query(query))`. The cursor is always truthy on
success, so it always returns `True` — even for a non-existent table. The docstring
promises "True or False." `test_drop_tables` asserts `result["non_existent"] is True`.

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (fix production)** | Before `DROP TABLE IF EXISTS`, check `self.tbl_exists(table_name)`; return `False` for missing tables. | Behavioral change — callers relying on `True` for non-existent tables would break. `drop_tables` (plural) returns `dict[str, bool]`; any caller checking for `False` would now get it. |
| **B (fix test + docstring)** | Update docstring to state "returns True when the DROP statement executes successfully (IF EXISTS suppresses errors); always True in practice." Keep test asserting `True`. | Zero production impact; honest contract; test is already correct. |
| **C (check rowcount)** | After `DROP TABLE IF EXISTS`, return `self.db_con.total_changes > 0` or check cursor `rowcount`. | Fragile — `DROP TABLE IF EXISTS` may report `rowcount == -1` or `0` depending on sqlite3 driver version; unreliable signal. |

**Recommendation:** Option B (fix test + docstring). The `IF EXISTS` clause makes
"table doesn't exist → no-op success" intentional, not a bug. The docstring should
document the actual contract. Option A introduces a behavioral change for zero user
benefit (nobody depends on getting `False` from a no-op drop).

### OD-2 · TST-007: mypy CI gate strategy

**Finding:** `pyproject.toml` has `[tool.mypy]` configured but CI's `test` job only
runs `ruff check src/ tests/` + `pytest`. No mypy step exists. Running mypy manually
reports 48 errors in src/ and 50 in tests/ (mypy 1.19.1). No `files` key is set; the
dev dependency is `mypy>=1.10.0` (unpinned).

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (pin + non-blocking)** | Pin `mypy==1.19.1` (actual installed version) in dev deps. Add a CI step `uv run mypy src/` with `continue-on-error: true`. Document the 48 errors as known noise. | CI stays green; mypy is loaded and monitored; no scope explosion fixing 48 errors. |
| **B (pin + blocking)** | Pin mypy. Fix all 48 + 50 errors. Add as a blocking CI gate. | Significant scope expansion (98 error fixes across 14 files); risk of touching production code; high effort. |
| **C (don't add)** | Leave mypy as a local-only tool. Document that it's intentionally not in CI. | Lowest effort; but the finding's core claim ("declared and never loaded") remains unaddressed. |
| **D (pin + selective scope)** | Add `files = ["src/mko_data_cleaner"]` to `[tool.mypy]`. Pin mypy. Add `continue-on-error`. Only type-check src/ (not tests/ — tests had 50 errors). | Tighter scope; fewer false positives; but 48 src errors remain. |

**Recommendation:** Option A (pin + non-blocking). Loads the gate, pins the version for
reproducibility, but doesn't block merges on 48 pre-existing errors. A follow-up phase
can incrementally fix errors and promote to blocking.

### OD-3 · TST-005: `drop_triggers` no-op — test-only or production fix?

**Finding:** `DBWorker.drop_triggers` uses `map(self.drop_trigger, tr_names)` (a lazy
iterator in Python 3 that is never consumed), so `drop_trigger` is never called.
Production code never calls `drop_triggers` (the call in `close()` is commented out).
The test finds that triggers exist but never fires them.

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (test-only)** | Leave `drop_triggers` as dead code. Add test that fires FTS triggers (INSERT/DELETE/UPDATE) and asserts the FTS table stays in sync. | Minimal scope; matches the finding's recommendation ("keep test-only unless explicitly accepted"). |
| **B (fix production)** | Change `map(self.drop_trigger, tr_names)` to `list(map(...))` or a `for` loop so triggers are actually dropped. | Behavior change to dead code (never called in production); minimal risk but also minimal value; scope expansion. |

**Recommendation:** Option A (test-only). The production code never calls
`drop_triggers`; fixing dead code is speculative. The trigger-firing test alone
closes the coverage gap. If the team later decides to use `drop_triggers` (e.g., in
`close()`), the fix can be applied then.

### OD-4 · TST-015: Cyrillic comment scope — tests-only or expand to production?

**Finding:** 51 Cyrillic comment lines drift up from 28 across `tests/conftest.py`,
`tests/test_dict_service.py`, and `tests/test_db_service.py`. Production code also has
Russian user-visible strings (e.g., `app.py` Rich console output, `app_service.py`
print statements, `db_service.py` debug logging).

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (tests-only)** | Translate all 51 Cyrillic lines in the test layer to English. Leave production Russian strings untouched. | Minimal scope per the finding's anchor (`conftest.py::sample_dictionary`). |
| **B (tests + production)** | Translate test comments AND production user-visible strings (console output, print statements, log messages). | Scope expansion into production code; user-visible output changes (Russian→English console messages in `run`/`run_report`); broader blast radius. |

**Recommendation:** Option A (tests-only). The finding's anchor is `conftest.py::sample_dictionary`
(test layer). Project rule 1 (English-only) covers all code, but production Russian
strings are a separate concern with a broader blast radius (user-facing CLI output).
Expanding scope should be a separate, deliberate initiative. Flag for Tech Lead: if
the team wants full i18n compliance, create a follow-up phase for production strings.

### OD-5 · TST-018: Shared E2E helper location — package or deliberate duplication?

**Finding:** `scripts/smoke_install.py` (lines 112-160) duplicates the e2e scenario from
`tests/test_app.py::test_process_data_end_to_end` (lines 429-473) — same synthetic CSV,
same dictionary, same assertions. Smoke runs in a fresh venv with ONLY the wheel
installed, so it cannot import from `tests/`.

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (package helper)** | Add `mko_data_cleaner.testing.e2e_fixture` module (or `mko_data_cleaner._testing.e2e_fixtures`) with the shared scenario builder + assertions. Both `tests/` and `scripts/smoke_install.py` import it. | Adds test infrastructure to the production package (ships in the wheel). Increases package surface. Must be excluded from `__all__`. |
| **B (conftest fixture + docstring)** | Extract the shared setup into a pytest fixture in `tests/conftest.py` for pytest-side de-duplication. Add a maintenance comment in `smoke_install.py` documenting the deliberate duplication and the drift risk. | No package pollution; duplication remains but is documented and acknowledged. Lowest blast radius. |
| **C (scripts module)** | Create `scripts/e2e_fixture.py` and have `smoke_install.py` import it via `sys.path` manipulation. | Hacky; `scripts/` is not a package; fragile. |

**Recommendation:** Option B (conftest fixture + documented duplication). The smoke
script runs in a fresh venv without the repo; importing from `tests/` is impossible
without shipping test code in the wheel. Putting test helpers in the production package
(via Option A) expands the package surface inappropriately. Documented duplication with
a clear maintenance comment is the minimal-scope approach. If the team later wants
full DRY, they can add a `mko_data_cleaner.testing` subpackage as a separate decision.

### OD-6 · TST-010: Merge-path column-name investigation (⚠ GATE)

**Finding:** `CSVWorker.get_merged_dictionary` calls `merged.sort("search_column_idx")`
(csv_service.py). The column name `"search_column_idx"` is documented in the README as
the production dictionary format column name (README.md line 153). However, the test
fixtures use `"search"` as the column name (conftest.py `sample_dictionary`,
test_app.py e2e fixture dictionary).

**Risk if unaddressed:** Removing the pre-created `merged_dictionary.csv` from the e2e
fixture (the core of TST-010) will trigger `get_merged_dictionary`, which will crash
with a Polars `ColumnNotFoundError` because the test data has column `"search"`, not
`"search_column_idx"`.

**Required investigation before B08 can proceed:**
1. Confirm whether `get_merged_dictionary`'s `sort("search_column_idx")` is correct for
   production data (README says yes — production dictionaries use `search_column_idx`).
2. Determine whether the test fixtures should use `search_column_idx` as the header
   name to match production, or whether the sort column should be parameterized from
   `DictColumnsIndexes`.
3. Assess the latent edge: empty dict directory → `pl.concat([])` → "cannot concat
   empty list" (confirmed in findings). Determine if this should be guarded.

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (fix production sort)** | Parameterize the sort column from `DictColumnsIndexes` or rename to match the actual column. | Production code change; broader fix; needs audit of what column name the raw CSV uses. |
| **B (fix test data)** | Use `search_column_idx` as the column header in test dictionary data. | Test-only; matches production format; lowest blast radius. |
| **C (defer merge-path test)** | Keep the pre-created `merged_dictionary.csv` but add a separate unit test for `get_merged_dictionary` with production-format data. | Avoids the e2e fixture change; still covers the merge path. |

**Recommendation:** Option C (defer e2e fixture change, use isolated unit test).
This avoids modifying the e2e fixture (which could break the smoke test) while still
exercising the merge path. **Gate:** Auditor must verify the production column name
and confirm whether `search_column_idx` is the correct sort column for the raw CSV
format, or whether the code has a latent bug.

### OD-7 · SR-03: polars 2.0 pin — lift or defer?

**Finding:** `pyproject.toml` pins `polars>=1.38.1,<2.0.0` (line 21). Code migration
(commit `4d5d35e`) already uses `scan_csv().collect_batches()` and the
`_strip_removed_reader_keys` validator strips the `rechunk` key. The code appears
ready for polars 2.0 but the pin is stale.

**Options:**
| Option | Approach | Impact |
|--------|----------|--------|
| **A (lift pin)** | Change to `polars>=1.38.1` (remove `<2.0.0` cap). | Potential API behavior differences in polars 2.0; must verify all 65 tests pass; interacts with SR-01 (strict column behavior may differ in 2.0). |
| **B (defer)** | Keep the pin. Document that code is 2.0-ready. Lift in a follow-up after SR-01 fix + TST-007 mypy gate is integrated. | Minimal scope; no risk; pin remains stale but harmless. |

**Recommendation:** Option B (defer). The pin is stale but not harmful — the code
runs correctly on polars 1.38.1. Lifting introduces a version-change risk surface
(version-specific API behavior, potential mypy regressions) that is out of scope for
this remediation phase. Defer to a dedicated dependency-update phase. **Depends on:**
B01 (SR-01 fix) and B14 (TST-007 mypy strategy) being resolved first.

---

## SR Include/Defer Decisions

| SR    | Finding                         | Decision | Rationale |
|-------|---------------------------------|----------|-----------|
| SR-01 | Spurious `strict` column        | **INCLUDE** in B01 | Verified (polars 1.38.1: `with_columns(strict=False)` creates a `[False, False, ...]` column). Fix is minimal (remove kwarg). Interacts with TST-002 assertion cleanliness. |
| SR-02 | `[-0:]` empty-slice bug         | **INCLUDE** in B02 | Verified (`list[-0:] == list[0:]` returns all elements). Real latent crash when `extra_cols=[]`. Fix is a guard clause. Prerequisite for TST-016/TST-006 correctness. |
| SR-03 | Polars 2.0 pin stale            | **DEFER** (see OD-7) | Code is 2.0-ready but lifting the pin is a version change with its own risk surface. Minimal scope: defer. |
| SR-04 | `scripts/` ungated by ruff/CI   | **INCLUDE** in B16, but verify E402 count | Primary finding valid (scripts/ excluded from CI scope). The "2× E402" claim could not be reproduced on HEAD `f6ac8e1` (`ruff check scripts/` passes clean). Action: add `scripts/` to CI ruff scope only (no E402 fixes needed if clean). |

---

## Per-Block Task Records

> Format follows `.ai/tasks/templates/task_template.yaml` — semantic code units only, never line numbers.

---

### B01 — SR-01: Fix spurious `strict` column in `MappingDict._build_query` (LOW RISK)

**Agents:** Implementor → Validator (regression test quality)

#### {task_description}

```yaml
id: b01_sr01_strict_column_fix

title: "B01/SR-01 — Remove misplaced strict=False kwarg from with_columns in MappingDict._build_query"

priority: high

depends_on: []

description: >
  MappingDict._build_query (dict_service.py) calls
  df.with_columns(pl.col(search_col).cast(pl.Int64), strict=False).
  In polars 1.38.1, DataFrame.with_columns() has no `strict` parameter, so
  `strict=False` is interpreted as a named expression — creating a spurious
  "strict" column with value False on every like_data DataFrame. This column
  is inert (no downstream consumer selects it, no test asserts its absence)
  but is a latent production defect. Remove `strict=False` from the with_columns
  call; the cast expression is the only intended argument.

goals:
  - remove the spurious "strict" Boolean column from like_data output
  - preserve the existing Int64 cast behavior for the search column
  - ensure all 65 existing tests still pass (the column was inert)
  - add a regression assertion that like_data has no "strict" column

files:
  - path: src/mko_data_cleaner/core/dict_service.py
    targets:
      - type: class
        name: MappingDict
      - type: method
        name: _build_query
    semantic_anchors:
      - "df.with_columns(pl.col(search_col).cast(pl.Int64), strict=False)"
    changes:
      - action: modify
        description: >
          Remove `strict=False` from the with_columns call in _build_query.
          The call becomes: df.with_columns(pl.col(search_col).cast(pl.Int64))
        code_hint: |
          data = df.with_columns(pl.col(search_col).cast(pl.Int64))

  - path: tests/test_dict_service.py
    targets:
      - type: test
        name: test_build_query_no_strict_column (new test)
    changes:
      - action: add_code
        description: >
          Add a test that builds a MappingDict with sample_dictionary + dict_indexes,
          calls build_mapping, and asserts "strict" is NOT in mapping.like_data.columns.
          This test fails against the pre-fix code and passes after the fix.
        code_hint: |
          def test_build_query_no_strict_column(sample_dictionary, dict_indexes):
              mapping = MappingDict(data=sample_dictionary, action_col_indexes=dict_indexes)
              mapping.build_mapping("adId", "researchDate", "programName", "channelName", "brand", extra_col_names=["brand_normalized"])
              assert "strict" not in mapping.like_data.columns

acceptance_criteria:
  - "mapping.like_data has no 'strict' column"
  - "regression test fails pre-fix, passes post-fix"
  - "uv run pytest tests/ passes (65 tests green)"
  - "uv run ruff check src/mko_data_cleaner/core/dict_service.py tests/test_dict_service.py clean"
  - "uv run mypy src/mko_data_cleaner/core/dict_service.py reports no new errors"
```

---

### B02 — SR-02: Fix `[-0:]` empty-slice bug in `DBWorker.set_data_tbl_columns` (LOW RISK)

**Agents:** Implementor → Validator (regression test)

#### {task_description}

```yaml
id: b02_sr02_empty_slice_fix

title: "B02/SR-02 — Guard against empty extra_cols slice in DBWorker.set_data_tbl_columns"

priority: medium

depends_on: []

description: >
  DBWorker.set_data_tbl_columns uses `self._extra_columns = self.data_tbl_columns[-len(extra_cols):]`
  (db_service.py). When extra_cols is an empty list, len([]) == 0, so `[-0:]` == `[0:]`
  returns ALL columns instead of []. This is a Python gotcha: -0 == 0. The result is
  that `extra_columns` is incorrectly set to all data columns, which would later cause
  build_mapping to raise a ValueError (length mismatch in the extra_col_names setter)
  or cause build_non_mapped to scan all columns for NULLs. Fix: guard the slice with
  a conditional — when extra_cols is empty, set _extra_columns to [].

goals:
  - set_data_tbl_columns with empty extra_cols sets extra_columns to []
  - set_data_tbl_columns with non-empty extra_cols preserves existing behavior
  - ensure all 65 existing tests still pass
  - add regression test for the empty-extra_cols case

files:
  - path: src/mko_data_cleaner/core/db_service.py
    targets:
      - type: class
        name: DBWorker
      - type: method
        name: set_data_tbl_columns
    semantic_anchors:
      - "self._extra_columns = self.data_tbl_columns[-len(extra_cols) :]"
    changes:
      - action: modify
        description: >
          Replace the slice assignment with a conditional that returns [] when
          extra_cols is empty:
          if extra_cols:
              self._extra_columns = self.data_tbl_columns[-len(extra_cols):]
          else:
              self._extra_columns = []

  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_set_data_tbl_columns_empty_extra_cols (new test)
    changes:
      - action: add_code
        description: >
          Add a test that calls set_data_tbl_columns with extra_cols=[] and
          asserts db_worker.extra_columns == []. This test fails against the
          pre-fix code (returns all columns) and passes after the fix.
        code_hint: |
          def test_set_data_tbl_columns_empty_extra_cols(db_worker):
              db_worker.set_data_tbl_columns("id", "name", "value", extra_cols=[])
              assert db_worker.extra_columns == []

acceptance_criteria:
  - "set_data_tbl_columns with extra_cols=[] sets extra_columns to []"
  - "regression test fails pre-fix, passes post-fix"
  - "uv run pytest tests/ passes (65 tests green)"
  - "uv run ruff check src/mko_data_cleaner/core/db_service.py tests/test_db_service.py clean"
```

---

### B03 — TST-001 + TST-002: Add FTS row to fixture + un-guard vacuous assertions (LOW RISK, test-only)

**Agents:** Implementor

> **Dependency note:** TST-002 un-guards assertions. Without the FTS row (TST-001),
> `fts_data` is an empty DataFrame and the un-guarded assertions would check an empty
> frame. Both findings must land in a single commit to keep the gate green. B01 (SR-01
> fix) is a soft prerequisite — the un-guarded assertions should not observe a spurious
> `strict` column.

#### {task_description}

```yaml
id: b03_tst001_tst002_fixture_contract

title: "B03/TST-001+002 — Add FTS row to sample_dictionary fixture and un-guard vacuous assertion guards"

priority: high

depends_on:
  - b01_sr01_strict_column_fix

description: >
  TST-001: sample_dictionary fixture (conftest.py) contains only f, p, s, d rows —
  no `match="fts"` row. TST-002: test_mapping_build, test_pattern_generation, and
  test_build_search_like_pattern_real_cases all wrap assertions in
  `if not df.is_empty():`, so when build_mapping is broken (empty output), every
  assertion is skipped and tests pass vacuously.

  Fix: (1) Add one FTS row to the sample_dictionary fixture so fts_data is non-empty
  after build_mapping. (2) Remove the `if not df.is_empty():` guards and assert
  frame height and column presence directly. Both changes land in one commit —
  TST-001 must land before or simultaneously with TST-002's un-guarding, or the
  un-guarded tests observe an empty fts_data frame.

goals:
  - sample_dictionary includes a row with match="fts" (search column indexes separated by "|")
  - test_mapping_build asserts like_data and fts_data heights > 0 directly (no is_empty guard)
  - test_pattern_generation asserts on patterns without is_empty guard
  - test_build_search_like_pattern_real_cases asserts without is_empty guard
  - all 3 tests pass with the new fts row in the fixture

files:
    - path: tests/conftest.py
      targets:
      - type: fixture
        name: sample_dictionary
      semantic_anchors:
      - "data = [["
      changes:
      - action: add_code
        description: >
          Add an fts row to the sample_dictionary data list. The row must have
          match="fts", search containing pipe-separated column indexes (e.g. "0|1"),
          and a term containing pipe-separated values (e.g. "Apple|Company").
          Example: ["a", "fts", "0|1", None, "Apple|Company", "fruit_tag"]
        code_hint: |
          ["a", "fts", "0|1", None, "Apple|Company", "fruit_tag"],
      - action: modify
        description: >
          ⚠ CORRECTION (Auditor): Change the `search` column schema from
          `pl.Int64` to `pl.String` in sample_dictionary's pl.DataFrame schema.
          The FTS row needs "0|1" (a string), which cannot be stored in an Int64
          column. In production, the dictionary is read from CSV (polars infers
          String for mixed "3"/"0|1" values), so pl.String matches production.
          Existing integer search values (3, 1, 5, 4) auto-cast to strings.
          `_build_query` casts like_data's search back to Int64 for the LIKE path,
          and `_build_fts_query` calls .split("|") on the string for the FTS path.

  - path: tests/test_dict_service.py
    targets:
      - type: test
        name: test_mapping_build
      - type: test
        name: test_pattern_generation
      - type: test
        name: test_build_search_like_pattern_real_cases
    semantic_anchors:
      - "if not df.is_empty():"
      - "if not mapping.like_data.is_empty():"
    changes:
      - action: modify
        description: >
          In test_mapping_build: remove both `if not df.is_empty():` guards
          (iterate over like_data and fts_data). Assert frame height > 0 for both.
          Keep the column-presence assertions.
        code_hint: |
          for df in (mapping.like_data, mapping.fts_data):
              assert not df.is_empty(), f"Expected non-empty {df} after build_mapping"
              _cols = set(df.columns)
              assert MappingColumns.mapping_index in _cols
              ...
      - action: modify
        description: >
          In test_pattern_generation: remove the `if not df.is_empty():` guard.
          Assert like_data is not empty, then check patterns.
      - action: modify
        description: >
          In test_build_search_like_pattern_real_cases: remove the `if not df.is_empty():`
          guard. Assert like_data is not empty first.

acceptance_criteria:
  - "sample_dictionary has a row with match='fts'"
  - "test_mapping_build: no is_empty guards; asserts both like_data and fts_data are non-empty"
  - "All 3 tests pass"
  - "uv run pytest tests/test_dict_service.py passes"
  - "uv run pytest tests/ passes (65+ tests green)"
```

---

### B04 — TST-016 + TST-006: Wire `extra_cols` fixture + multi-extra-column `build_non_mapped` test (LOW RISK, test-only)

**Agents:** Implementor

> **Dependency note:** TST-016 notes that the `extra_cols` fixture (conftest.py) has
> zero consumers. TST-006 needs a test with 2+ extra columns exercising
> `build_non_mapped`. SR-02 (B02) must be fixed first so that `set_data_tbl_columns`
> with the fixture's single extra column works correctly.

#### {task_description}

```yaml
id: b04_tst016_tst006_extra_cols_and_build_non_mapped

title: "B04/TST-016+006 — Wire extra_cols fixture into build_non_mapped test; add multi-extra-column coverage"

priority: medium

depends_on:
  - b03_tst001_tst002_fixture_contract
  - b02_sr02_empty_slice_fix

description: >
  TST-016: the extra_cols (conftest.py), sample_mapping_df, tmp_db_path, and
  db_worker_memory fixtures have zero consumers. TST-006: no test constructs a
  DBWorker with 2+ extra columns and calls build_non_mapped.

  Fix: (1) Wire the extra_cols fixture into test_build_non_mapped (new test that
  exercises build_non_mapped with the fixture's ["tag"] extra column). (2) Add a
  second test that constructs a DBWorker with 2+ extra columns (e.g. ["tag", "grp"])
  and asserts build_non_mapped correctly selects rows where any extra column is NULL.
  (3) Add a consumer comment to the remaining unused fixtures (sample_mapping_df,
  tmp_db_path, db_worker_memory) documenting intent or marking for removal.

goals:
  - build_non_mapped is tested with the extra_cols fixture (1 extra column)
  - build_non_mapped is tested with 2+ extra columns (multi-extra-column branch)
  - unused fixtures get a consumer comment or deprecation note
  - all existing tests remain green

files:
  - path: tests/conftest.py
    targets:
      - type: fixture
        name: extra_cols
    changes:
      - action: add_code
        description: >
          Add a docstring to each unused fixture (sample_mapping_df, tmp_db_path,
          db_worker_memory) documenting that it is reserved for integration tests
          or marking it for removal if no consumer is added within one cycle.
          No structural change to the fixtures.

  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_build_non_mapped_with_extra_cols (new test)
      - type: test
        name: test_build_non_mapped_multi_extra_cols (new test)
    changes:
      - action: add_code
        description: >
          test_build_non_mapped_with_extra_cols: use the db_worker fixture, create
          a data table with an extra column, populate rows with some NULL extras,
          call build_non_mapped(), and assert the non_mapped table contains rows
          where the extra column is NULL.
        code_hint: |
          def test_build_non_mapped_with_extra_cols(db_worker):
              db_worker.set_data_tbl_columns("id", "name", "tag", extra_cols=["tag"])
              db_worker.search_columns = ["name"]
              db_worker.create_table_with_index()
              # insert rows: one with tag=NULL, one with tag set
              ...
              db_worker.build_non_mapped()
              # assert non_mapped table has the NULL-tag row
      - action: add_code
        description: >
          test_build_non_mapped_multi_extra_cols: same setup but with 2+ extra
          columns. Asserts that rows where ANY extra column is NULL are selected
          (verifying the `|` operator behavior documented in TST-006's refutation).

acceptance_criteria:
  - "test_build_non_mapped_with_extra_cols passes"
  - "test_build_non_mapped_multi_extra_cols passes"
  - "uv run pytest tests/test_db_service.py passes"
  - "uv run pytest tests/ passes (all green)"
```

---

### B05 — TST-003: Add REPLACE action test for `_apply_replace` (LOW RISK, test-only)

**Agents:** Implementor

> **Pattern reference:** Follows `test_apply_delete_executes_delete_once` in
> `tests/test_db_service.py` (lines 302-351) — uses `mocker.spy` on `perform_query`,
> sets up target table rows + joined_matches_table, calls the private method, and
> asserts on the SQL and observable outcome.

#### {task_description}

```yaml
id: b05_tst003_replace_action_test

title: "B05/TST-003 — Add test_apply_replace_picks_highest_mapping_index"

priority: medium

depends_on: []

description: >
  TST-003: DBWorker._apply_replace (db_service.py) is called from apply_mapping,
  which is called only from app_service.py:234. No test exercises the REPLACE action.
  The e2e fixture dictionary contains only d and a rules — no r (REPLACE) rules.

  Fix: Add test_apply_replace_picks_highest_mapping_index following the pattern of
  test_apply_delete_executes_delete_once. The test must exercise overlapping REPLACE
  rules and assert that the highest mapping_index wins (the SQL uses
  ROW_NUMBER() OVER(PARTITION BY data_rowid ORDER BY mapping_index DESC, rowid DESC)).

goals:
  - set up target table with rows and a joined_matches_table referencing them
  - insert overlapping REPLACE rules with different mapping_index values
  - call _apply_replace and assert that the highest mapping_index rule is applied
  - assert exactly one UPDATE statement is executed (no double execution)

files:
  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_apply_replace_picks_highest_mapping_index (new test)
    changes:
      - action: add_code
        description: >
          New test modeled on test_apply_delete_executes_delete_once:
          1. Create data_tbl_name with adId/brand/sponsor columns.
          2. Insert 3 rows.
          3. Create TEMP joined_matches_table with 2 entries for the same rowid
             (same data_rowid) but different mapping_index values (e.g. 1 and 2).
          4. Set extra_columns to ["sponsor"].
          5. Spy on perform_query.
          6. Call _apply_replace(column_names=["sponsor"]).
          7. Assert: (a) perform_query call count == 1, (b) SQL contains UPDATE,
             (c) the target row for the matched rowid has the value from
             mapping_index=2 (the higher one).

acceptance_criteria:
  - "test_apply_replace_picks_highest_mapping_index passes"
  - "test asserts the highest mapping_index wins"
  - "test asserts exactly one UPDATE execution (no double execution)"
  - "uv run pytest tests/test_db_service.py passes"
  - "uv run pytest tests/ passes (all green)"
```

---

### B06 — TST-005: Extend FTS5 trigger test to fire + assert sync (LOW RISK, test-only)

**Agents:** Implementor

> **Scope note:** `drop_triggers` (db_service.py) uses `map(self.drop_trigger, tr_names)`
> — a lazy iterator never consumed. Production never calls `drop_triggers`. The
> production fix is deferred per OD-3 (test-only). The `DROP TRIGGER IF EXISTS`
> statements are idempotent, so the test can create triggers and verify they fire
> without needing `drop_triggers` to work.

#### {task_description}

```yaml
id: b06_tst005_fts5_trigger_firing_test

title: "B06/TST-005 — Extend FTS5 trigger test: INSERT/DELETE/UPDATE + assert sync"

priority: medium

depends_on: []

description: >
  TST-005: test_create_search_table only asserts the FTS table exists (assert
  fts_table in all_tables). No test fires the triggers or asserts sync after
  mutation. The triggers (create_triggers, db_service.py) keep the FTS virtual
  table synced to the data table on INSERT, DELETE, UPDATE.

  Fix: Extend the test (or add a new test) to:
  1. Create a data table + FTS table with triggers (reuse test_create_search_table setup).
  2. INSERT a row into the data table → assert the FTS table reflects it (MATCH query).
  3. UPDATE a searchable column → assert the FTS index is updated.
  4. DELETE a row → assert the FTS entry is removed.

goals:
  - FTS triggers are verified to fire on INSERT (content propagated to FTS table)
  - FTS triggers are verified to fire on UPDATE (content refreshed in FTS table)
  - FTS triggers are verified to fire on DELETE (content removed from FTS table)
  - the test uses the fts_table MATCH syntax to verify (not just table existence)

files:
  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_create_search_table (extend)
      - type: test
        name: test_fts5_triggers_sync_after_mutation (new test)
    changes:
      - action: add_code
        description: >
          New test test_fts5_triggers_sync_after_mutation that:
          1. Sets up a data table with search columns and an FTS5 table + triggers
             (reuse the existing create_table_with_index + link_search_table pattern).
          2. Inserts a row with a searchable value.
          3. Queries the FTS table via MATCH and asserts the row appears.
          4. Updates the searchable value.
          5. Queries via MATCH on the new value and the old value; asserts new matches,
             old does not.
          6. Deletes the row.
          7. Queries via MATCH and asserts no rows.

acceptance_criteria:
  - "test_fts5_triggers_sync_after_mutation passes"
  - "INSERT/UPDATE/DELETE all verified via FTS MATCH queries"
  - "uv run pytest tests/test_db_service.py passes"
  - "uv run pytest tests/ passes (all green)"

open_decisions:
  - OD-3: drop_triggers no-op fix is deferred (production-only, dead code, low value)
```

---

### B07 — TST-018: Extract shared E2E helper (LOW RISK, depends on OD-5)

**Agents:** Planner

> ⚠ GATE: OD-5 (TST-018: package helper vs deliberate duplication) must be resolved
> by Tech Lead before implementation. If "deliberate duplication" is chosen, this
> block becomes a one-line maintenance comment in smoke_install.py only.

#### {task_description}

```yaml
id: b07_tst018_shared_e2e_helper

title: "B07/TST-018 — Extract shared E2E scenario helper between tests/ and scripts/"

priority: medium

depends_on:
  - od_05_tst018_helper_location

description: >
  TST-018: scripts/smoke_install.py (lines 112-160) duplicates the e2e scenario
  and assertions from tests/test_app.py::test_process_data_end_to_end (lines 429-473).
  The smoke script runs in a fresh venv with ONLY the wheel installed — it cannot
  import from tests/. The two universes can drift with all gates still green.

  See OD-5 for the full decision tree. Two paths:

  Path A (package helper, if Tech Lead chooses): Add
  src/mko_data_cleaner/testing/e2e_fixtures.py with a function
  build_synthetic_report(tmp_path) -> ReportResult that creates the raw data,
  dictionary, and runs process_data. Both tests/ and scripts/smoke_install.py
  import it.

  Path B (documented duplication, if Tech Lead chooses): Keep the smoke script's
  copy. Add a maintenance comment at the top of the duplicated section in
  smoke_install.py referencing test_app.py::test_process_data_end_to_end by name
  and listing the shared assertions. Extract the pytest-side setup into a fixture
  in tests/conftest.py so future pytest tests reuse it.

  Default assumption for downstream blocks (B08, B09): Path B is chosen
  (minimal scope). If Path A is chosen, B08/B09 import the helper.

goals:
  (Path A): shared helper in package; both consumers import it
  (Path B): documented duplication comment; pytest-side fixture extraction

files:
  - path: scripts/smoke_install.py (Path B) or
    src/mko_data_cleaner/testing/e2e_fixtures.py (Path A)
  - path: tests/conftest.py (Path B: add e2e fixture)

acceptance_criteria:
  (Path A): "uv run pytest tests/ passes"; "smoke_install.py passes"
  (Path B): "maintenance comment added to smoke_install.py referencing test_app.py"; "e2e fixture extracted to tests/conftest.py for pytest reuse"
```

---

### B08 — TST-010: Remove pre-created merged_dictionary + add merge/dedup/sort test (MEDIUM RISK ⚠ GATE)

**Agents:** Auditor (investigation) → Implementor

> ⚠ GATE: OD-6 (column-name investigation) must be resolved by Tech Lead before
> implementation. `get_merged_dictionary` sorts by `"search_column_idx"` (csv_service.py),
> but test fixtures use `"search"` as the column header. Removing the pre-created
> `merged_dictionary.csv` will trigger the merge path and crash. The default
> recommendation (OD-6, Option C) is to **defer the e2e fixture change** and add an
> isolated unit test for `get_merged_dictionary` with production-format data.

#### {task_description}

```yaml
id: b08_tst010_merge_path_test

title: "B08/TST-010 — Add merge/dedup/sort coverage for get_merged_dictionary"

priority: medium

depends_on:
  - b07_tst018_shared_e2e_helper
  - od_06_tst010_column_investigation

description: >
  TST-010: CSVWorker.get_merged_dictionary (csv_service.py) is unreachable from
  tests — every test pre-creates merged_dictionary.csv, so get_dictionary reads it
  directly and never calls get_merged_dictionary. The unique() and sort() steps
  (which fix mapping_index order and thereby the REPLACE winner) are unobserved.

  Per OD-6 (default: Option C), do NOT remove the pre-created file from the e2e
  fixture. Instead, add an isolated unit test for get_merged_dictionary using
  production-format dictionary data (with "search_column_idx" column name, matching
  the README format).

  If Tech Lead chooses Option A or B from OD-6, the implementation scope changes:
  - Option A: parameterize the sort column in get_merged_dictionary, then modify the
    e2e fixture.
  - Option B: use "search_column_idx" as the column header in e2e test data, then
    remove the pre-created file.

goals:
  - unit test get_merged_dictionary with 2+ source fragment files
  - assert merged output: deduplication (unique), sort order (search_column_idx),
    and correct file output path (dict_path)
  - assert the empty-dict-directory edge: pl.concat([]) is guarded or the error is
    handled gracefully

files:
  - path: tests/test_csv_service.py
    targets:
      - type: test
        name: test_get_merged_dictionary_dedup_sort (new test)
      - type: test
        name: test_get_merged_dictionary_empty_dir (new test)
    changes:
      - action: add_code
        description: >
          test_get_merged_dictionary_dedup_sort: construct a CSVWorker with
          a dict_path pointing to a non-existent file, create 2 source fragment
          files in dict_path.parent (production format: action;match;search_column_idx;value;term;tag),
          call get_merged_dictionary(), and assert the merged file at dict_path
          contains deduplicated rows sorted by search_column_idx.
      - action: add_code
        description: >
          test_get_merged_dictionary_empty_dir: construct a CSVWorker with an
          empty dict directory (no source files). Assert the behavior is a handled
          error (not an unhandled pl.concat([]) crash). If production code doesn't
          guard this, the test documents the current behavior as a known limitation.

acceptance_criteria:
  - "test_get_merged_dictionary_dedup_sort passes"
  - "test_get_merged_dictionary_empty_dir passes (documents current behavior)"
  - "uv run pytest tests/test_csv_service.py passes"
  - "uv run pytest tests/ passes (all green)"
```

---

### B09 — TST-004: Add CSV export roundtrip content test (LOW RISK, test-only)

**Agents:** Implementor

> **Reuse:** The test reuses the e2e report fixture setup (either from conftest.py
> after B07 Path B, or from the package helper after B07 Path A). Must honor
> BOM, `decimal_comma`, and gzip compression on read-back.

#### {task_description}

```yaml
id: b09_tst004_csv_roundtrip_content_test

title: "B09/TST-004 — Add CSV export roundtrip content test (BOM + decimal_comma + gzip)"

priority: high

depends_on:
  - b08_tst010_merge_path_test

description: >
  TST-004: test_export_sql_to_csv_returns_row_count asserts only count == 3.
  test_export_sql_to_csv_returns_zero_for_empty_table asserts only count == 0.
  No test reads an exported file back and asserts cell content, separator, BOM,
  or per-chunk naming. The e2e test (test_app.py:469) asserts only file existence.

  Fix: Add test_export_sql_to_csv_roundtrip_content that:
  1. Creates a SQLite table with known rows (including non-ASCII and decimal values).
  2. Exports to CSV via CSVWorker.export_sql_to_csv with export_settings
     (compression=gzip, separator=';', include_bom=True, decimal_comma=True).
  3. Reads the exported .csv.gz file back with polars (honoring gzip, separator,
     decimal_comma).
  4. Asserts: content matches original rows, separator is preserved, BOM is present
     in the first file, compression produces .gz extension, per-chunk naming
     follows the {prefix}_{timestamp}_{index}.csv.gz pattern.

goals:
  - exported CSV content matches source data (row-by-row)
  - gzip compression verified (.csv.gz files, readable via pl.read_csv)
  - BOM present at start of file (UTF-8 BOM, \ufeff)
  - separator ';' preserved on read-back
  - decimal_comma handled (numeric values parse correctly)
  - per-chunk files follow naming pattern

files:
  - path: tests/test_csv_service.py
    targets:
      - type: test
        name: test_export_sql_to_csv_roundtrip_content (new test)
    changes:
      - action: add_code
        description: >
          New test that exports a known table, reads the .csv.gz back, and
          asserts content fidelity. Uses CSVWorker.__new__ pattern (like existing
          tests) with export_settings set manually.
        code_hint: |
          def test_export_sql_to_csv_roundtrip_content(tmp_path):
              """Roundtrip: export SQL table to gzip CSV and read content back."""
              con = sqlite3.connect(tmp_path / "test.db")
              con.execute("CREATE TABLE data_table (id INTEGER, name TEXT, price TEXT)")
              con.executemany("INSERT INTO data_table VALUES (?, ?, ?)",
                  [(1, "Alice", "10.50"), (2, "Bob", "20.00")])
              con.commit()

              worker = CSVWorker.__new__(CSVWorker)
              worker.export_settings = {"compression": "gzip", "chunk_size": 10000,
                "include_bom": True, "separator": ";", "decimal_comma": True,
                "include_header": True}

              count = worker.export_sql_to_csv(db_con=con, data_table="data_table",
                file_prefix="data_table", export_path=tmp_path / "export")

              # find and read back the exported file
              files = list((tmp_path / "export").glob("*.csv.gz"))
              assert len(files) == 1
              df = pl.read_csv(files[0], separator=";", decimal_comma=True)
              assert df.height == 2
              assert df["name"].to_list() == ["Alice", "Bob"]
              # verify BOM
              raw = gzip.decompress(files[0].read_bytes())
              assert raw[:3] == b'\xef\xbb\xbf'  # UTF-8 BOM
              con.close()

acceptance_criteria:
  - "test_export_sql_to_csv_roundtrip_content passes"
  - "content, separator, BOM, gzip compression all verified on read-back"
  - "uv run pytest tests/test_csv_service.py passes"
  - "uv run pytest tests/ passes (all green)"
```

---

### B10 — TST-009: Reference `DATE_REGEX` + assert both verdicts + auto-detection fallback (LOW RISK, test-only)

**Agents:** Implementor

#### {task_description}

```yaml
id: b10_tst009_date_column_detection

title: "B10/TST-009 — Reference DATE_REGEX, assert both verdicts, add auto-detection test"

priority: medium

depends_on: []

description: >
  TST-009: test_is_date_column (test_csv_service.py) passes the regex
  r"^\d{4}[-/.]\d{2}[-/.]\d{2}$" as a literal instead of referencing
  CSVWorker.DATE_REGEX (csv_service.py:class attribute). Asserts only `result is True`.
  No assertion for False verdicts (non-String dtype, all-null series, match ratio below
  threshold). The check_date_column auto-detection loop (csv_service.py) is never
  exercised — every run supplies a valid researchDate.

  ⚠ CORRECTION (Auditor): `check_date_column` return-type annotation is
  `-> tuple[int, str] | None` (csv_service.py L153) but it actually returns
  `str | None` via `_detect_date_column` (L124). The annotation is incorrect
  — it claims a tuple but returns a string. The test plan does not change
  production code; this is noted as a known type annotation bug.

  Fix:
  1. Reference CSVWorker.DATE_REGEX instead of restating the regex literal.
  2. Add False-assertion tests: non-String series returns False, all-null series
     returns False, sub-threshold match ratio returns False.
  3. Add a test for check_date_column auto-detection fallback (no exact column
     provided; the method scans all columns and returns the first date-like one).

goals:
  - test references CSVWorker.DATE_REGEX, not a literal regex
  - False verdicts tested: non-String dtype, all-null series, sub-threshold ratio
  - check_date_column auto-detection fallback exercised (DataFile.date_column is
    non-Optional — driver creates a CSV without a matching date column)

files:
  - path: tests/test_csv_service.py
    targets:
      - type: test
        name: test_is_date_column (modify)
      - type: test
        name: test_is_date_column_false_non_string (new)
      - type: test
        name: test_is_date_column_false_all_null (new)
      - type: test
        name: test_is_date_column_false_subthreshold (new)
      - type: test
        name: test_check_date_column_auto_detect (new)
    changes:
      - action: modify
        description: >
          In test_is_date_column: replace the literal regex with
          CSVWorker.DATE_REGEX. Add `assert result is True` (keep). Add a comment
          that the test now references the canonical regex.
      - action: add_code
        description: >
          test_is_date_column_false_non_string: pass a pl.Int64 series,
          assert result is False.
        code_hint: |
          def test_is_date_column_false_non_string():
              series = pl.Series([1, 2, 3], dtype=pl.Int64)
              result = CSVWorker._is_date_column(series, CSVWorker.DATE_REGEX)
              assert result is False
      - action: add_code
        description: >
          test_is_date_column_false_all_null: pass a String series of all nulls,
          assert result is False.
      - action: add_code
        description: >
          test_is_date_column_false_subthreshold: pass a String series where only
          1/3 values match the date regex (below 0.8 threshold), assert False.
      - action: add_code
        description: >
          test_check_date_column_auto_detect: construct a CSVWorker (via
          __new__ + manual field setting, like test_get_file_name) with reader_settings,
          write a CSV with a non-date column and a date column, call check_date_column()
          without arguments, assert it returns the date column name.

acceptance_criteria:
  - "test_is_date_column references CSVWorker.DATE_REGEX"
  - "4 new tests pass: non-String, all-null, sub-threshold, auto-detect"
  - "uv run pytest tests/test_csv_service.py passes"
  - "uv run pytest tests/ passes (all green)"
```

---

### B11 — TST-008: Fix `null_data_files` path attribution (MEDIUM RISK, production + test)

**Agents:** Implementor → Validator

#### {task_description}

```yaml
id: b11_tst008_null_data_files_attribution

title: "B11/TST-008 — Record null-data file paths at write time instead of directory glob"

priority: medium

depends_on: []

description: >
  TST-008: app_service.py::run_report (AppService.run_report) sets
  null_data_files = sorted(self.base_path.glob("null_data_*")) (line 252). This
  directory-scoped glob would include stale files from prior runs. The only assertion
  (test_app.py:470, isinstance(result.null_data_files, list)) runs on a pristine temp
  dir where the distinction cannot surface.

  Fix: Add an optional `output_files: list[Path] | None = None` parameter to
  CSVWorker.export_sql_to_csv. When provided, append each written file path to the
  list. In run_report, pass a list to collect the null-data file paths, and use that
  list (instead of the glob) for null_data_files. The glob is removed.

  This is backward-compatible: the two existing call sites in run_report do not
  change their return-value handling. The existing csv_service tests
  (test_export_sql_to_csv_returns_row_count, test_export_sql_to_csv_returns_zero_for_empty_table)
  don't pass output_files and are unaffected.

goals:
  - export_sql_to_csv accepts optional output_files list and populates it with written paths
  - run_report uses the collected paths instead of base_path.glob("null_data_*")
  - stale files from prior runs are NOT included in null_data_files
  - existing tests unaffected (backward-compatible parameter default)

files:
  - path: src/mko_data_cleaner/core/csv_service.py
    targets:
      - type: class
        name: CSVWorker
      - type: method
        name: export_sql_to_csv
    semantic_anchors:
      - "def export_sql_to_csv(self, db_con, data_table, ...)"
      - "file_path = export_path / file_name.format(file_index=file_index)"
      - "return total_rows"
    changes:
      - action: modify
        description: >
          Add `output_files: list[Path] | None = None` parameter to export_sql_to_csv.
          Inside the while loop, after file_path is computed, append it to
          output_files if the list is provided. Update the docstring to document
          the new parameter. The return type stays int.
      - action: add_code
        description: >
          Insert: if output_files is not None: output_files.append(file_path)
          after the file_path assignment, inside the while loop body.

  - path: src/mko_data_cleaner/core/app_service.py
    targets:
      - type: class
        name: AppService
      - type: method
        name: run_report
    semantic_anchors:
      - "null_data_files = sorted(self.base_path.glob(\"null_data_*\"))"
    changes:
      - action: modify
        description: >
          In the null-data export call (export_sql_to_csv for non_mapped_table):
          create null_data_file_paths = [] and pass output_files=null_data_file_paths.
          Replace the glob line with: null_data_files = null_data_file_paths.

  - path: tests/test_csv_service.py
    targets:
      - type: test
        name: test_export_sql_to_csv_populates_output_files (new test)
    changes:
      - action: add_code
        description: >
          Unit test: export a table with output_files=[] and assert the list
          contains the written file path(s) after the call.

  - path: tests/test_app.py
    targets:
      - type: test
        name: test_null_data_files_excludes_stale (new test)
    changes:
      - action: add_code
        description: >
          E2E test: pre-seed a stale null_data_*.csv.gz in base_path, run the
          report, and assert result.null_data_files contains ONLY the current run's
          file(s), not the stale one. Uses populated_settings + tmp_path.

acceptance_criteria:
  - "export_sql_to_csv populates output_files when provided"
  - "run_report uses collected paths, not glob"
  - "stale null_data files excluded from result.null_data_files"
  - "test_export_sql_to_csv_populates_output_files passes"
  - "test_null_data_files_excludes_stale passes"
  - "uv run pytest tests/ passes (all green)"
  - "uv run ruff check changed files clean"
  - "uv run mypy on changed files reports no new errors"

risks:
  - "R1: Changing export_sql_to_csv signature — mitigated by making output_files optional (default None), no behavior change for existing callers."
  - "R2: The stale-file regression test requires pre-seeding a file with a timestamp in the name; use a fixed timestamp string to avoid flaky matches."
```

---

### B12 — TST-011: `drop_table` contract (LOW RISK ⚠ GATE, production or test)

**Agents:** Planner (decision) → Implementor

> ⚠ GATE: OD-1 must be resolved by Tech Lead before implementation. Default
> recommendation: Option B (fix docstring + test; no production change).

#### {task_description}

```yaml
id: b12_tst011_drop_table_contract

title: "B12/TST-011 — Resolve drop_table True/False contract"

priority: low

depends_on:
  - od_01_tst011_drop_table_contract

description: >
  TST-011: DBWorker.drop_table returns bool(self.perform_query(query)).
  DROP TABLE IF EXISTS always produces a truthy cursor, so it always returns True.
  The docstring claims "True or False" but False is never returned for a missing table.
  test_drop_tables asserts result["non_existent"] is True.

  Per OD-1 (default: Option B — fix docstring + test), the change is:
  1. Update drop_table docstring to state: "Returns True when the DROP TABLE IF EXISTS
     statement executes successfully. Because IF EXISTS suppresses errors for
     non-existent tables, this effectively always returns True."
  2. Keep the test asserting True for non_existent (it already does).
  3. Add a comment in the test documenting WHY True is correct (IF EXISTS semantics).

  If Tech Lead chooses Option A (fix production): add tbl_exists() check before
  DROP, return False for missing tables. This changes the return contract and
  requires updating test_drop_tables to assert False for non_existent.

goals:
  (Option B): docstring accurate; test has explanatory comment; no production behavior change
  (Option A): drop_table returns False for missing tables; test asserts False

files:
  - path: src/mko_data_cleaner/core/db_service.py
    targets:
      - type: method
        name: drop_table
    (Option B only: update docstring)
    (Option A only: add tbl_exists check + return False)

  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_drop_tables
    changes:
      - action: modify
        description: >
          Add a comment explaining the IF EXISTS semantics. (Option B: keep
          `assert result["non_existent"] is True`. Option A: change to
          `assert result["non_existent"] is False`.)

acceptance_criteria:
  - "Docstring matches actual behavior (Option B) or behavior changed per decision (Option A)"
  - "test_drop_tables updated per the chosen option"
  - "uv run pytest tests/ passes (all green)"
```

---

### B13 — TST-012: `slow` marker — mark-real-use or delete (LOW RISK, config-only)

**Agents:** Implementor

#### {task_description}

```yaml
id: b13_tst012_slow_marker

title: "B13/TST-012 — Resolve inert slow marker in pyproject.toml"

priority: low

depends_on: []

description: >
  TST-012: pyproject.toml registers marker "slow: marks tests as slow" but
  `git grep -c "pytest.mark" -- tests/` returns zero matches. No test uses the
  marker. Its only effect is that --strict-markers accepts the name.

  Fix: Delete the marker registration. No test uses it; removing it has zero
  runtime impact (--strict-markers is already active, and no test uses the marker).
  This is the minimal change.

  Alternative (Option A, mark-real-use): if any genuinely slow test is identified,
  annotate it with @pytest.mark.slow and configure CI to optionally exclude it
  with `-m "not slow"`. Currently no slow test exists, so this is a no-op.

goals:
  - remove the inert "slow" marker from pyproject.toml [tool.pytest.ini_options] markers
  - verify --strict-markers still passes
  - no tests are affected

files:
  - path: pyproject.toml
    targets:
      - type: table
        name: tool.pytest.ini_options
    semantic_anchors:
      - 'markers = ["slow: marks tests as slow (use -m slow to run)"]'
    changes:
      - action: remove
        description: >
          Remove the `markers` key from [tool.pytest.ini_options]. If kept, set
          to an empty list or remove entirely. Recommended: remove entirely since
          no markers are used.

acceptance_criteria:
  - "pytest markers removed; uv run pytest passes (all green)"
  - "uv run ruff check pyproject.toml-equivalent clean (no lint impact)"
```

---

### B14 — TST-007: Add mypy to CI (HIGH RISK ⚠ GATE, config/CI)

**Agents:** Researcher → Planner → Implementor + Validator

> ⚠ GATE: OD-2 (mypy CI gate strategy) must be resolved by Tech Lead before
> implementation. Default recommendation: Option A (pin mypy version + add non-blocking
> CI step). This block is HIGH RISK because any CI change that turns red blocks all
> merges.

#### {task_description}

```yaml
id: b14_tst007_mypy_ci_gate

title: "B14/TST-007 — Add mypy to CI with pinned version (non-blocking)"

priority: high

depends_on:
  - od_02_tst007_mypy_strategy

description: >
  TST-007: pyproject.toml has [tool.mypy] configured but CI's test job
  (ci.yml) only runs ruff check + pytest — no mypy step. Running mypy manually
  reports 48 errors in src/ and 7 in tests/. The dev dependency is
  mypy>=1.10.0 (unpinned), and there's no `files` key in the config.

  Per OD-2 (default: Option A — pin + non-blocking), the changes are:
   1. Pin mypy version in pyproject.toml [dependency-groups] dev: change
      "mypy>=1.10.0" to "mypy==1.19.1" (actual installed version; 2.4.0 was
      cited in plan/finding text but does not resolve).
   2. Add [tool.mypy] `files = ["src/mko_data_cleaner"]` to scope the check
      to src/ only (tests/ had 50 errors, not 7 — OD-2 Option D rationale
      corrected per Auditor).
   3. Add a CI step in .github/workflows/ci.yml test job:
      - name: Type check (mypy)
        run: uv run mypy
        continue-on-error: true
      With a comment: "48 known src errors + 50 known test errors; will be
      promoted to blocking after incremental fixes (Phase TST-007+)."


  If Tech Lead chooses Option B (blocking): all 48+50 errors must be fixed before
  the CI step is added. This expands scope significantly and is not recommended
  for this phase.

goals:
  - mypy version pinned for reproducibility
  - mypy added to CI test job with continue-on-error=true
  - [tool.mypy] files key set to src/ only
  - CI test job stays green (non-blocking mypy step)
  - 48+7 known errors documented for future incremental fixing

files:
  - path: pyproject.toml
    targets:
      - type: table
        name: tool.mypy
      - type: list
        name: dependency-groups.dev
    changes:
      - action: modify
        description: >
          Change "mypy>=1.10.0" to "mypy==1.19.1" in dev dependencies (actual
          installed version; 2.4.0 does not resolve).
          Add `files = ["src/mko_data_cleaner"]` to [tool.mypy].
      - action: add_code
        description: >
          Add files key to [tool.mypy]:
          files = ["src/mko_data_cleaner"]

  - path: .github/workflows/ci.yml
    targets:
      - type: job
        name: test
    changes:
      - action: add_code
        description: >
          Add a mypy step after the ruff check step, before pytest:
          - name: Type check (mypy)
            run: uv run mypy
            continue-on-error: true
          With a comment noting 48 known src errors; non-blocking per OD-2 Option A.

acceptance_criteria:
  - "mypy version pinned in pyproject.toml (1.19.1)"
  - "[tool.mypy] has files = [\"src/mko_data_cleaner\"]"
  - "CI test job has a mypy step with continue-on-error: true"
  - "CI test job passes (ruff + pytest green; mypy non-blocking)"
  - "uv run mypy src/mko_data_cleaner reports same 48 errors as documented (no regression)"
  - "uv run mypy tests/ reports 50 errors (documented as known noise; tests/ excluded from CI scope)"
```

---

### B15 — SR-03: Lift polars 2.0 pin (MEDIUM RISK ⚠ GATE, config)

**Agents:** Researcher → Planner

> ⚠ GATE: OD-7 (polars pin strategy) must be resolved by Tech Lead. Default
> recommendation: DEFER. This block is only implemented if OD-7 is resolved as "lift."

#### {task_description}

```yaml
id: b15_sr03_polars_pin

title: "B15/SR-03 — Lift polars <2.0 pin (if Tech Lead approves)"

priority: medium

depends_on:
  - b01_sr01_strict_column_fix
  - b14_tst007_mypy_ci_gate
  - od_07_sr03_polars_pin

description: >
  SR-03: pyproject.toml pins "polars>=1.38.1,<2.0.0". The code migration (commit
  4d5d35e) already uses scan_csv().collect_batches() and the _strip_removed_reader_keys
  validator strips the polars-2.0-removed `rechunk` key. The code appears ready for
  polars 2.0 but the pin is stale.

  Per OD-7 (default: DEFER), this block is NOT implemented in the current phase.
  It is documented for a follow-up.

  If Tech Lead overrides to "lift":
  1. Change pyproject.toml: "polars>=1.38.1,<2.0.0" → "polars>=1.38.1".
  2. Run uv run pytest — all 65 tests must pass.
  3. Run uv run mypy src/mko_data_cleaner — no new errors vs. the 48 baseline.
  4. Verify SR-01 fix is compatible with polars 2.0 (with_columns signature).

  If deferred: no file changes. Document the rationale.

goals:
  (if lifted): "polars pin lifted; all 65 tests pass; mypy no new errors"
  (if deferred): "no changes; rationale documented"

files:
  - path: pyproject.toml
    (only if Tech Lead approves lifting)
    semantic_anchors:
      - '"polars>=1.38.1,<2.0.0"'
    changes:
      - action: modify
        description: >
          Change to "polars>=1.38.1" (remove the <2.0.0 upper bound).

acceptance_criteria:
  (if lifted): "uv run pytest passes"; "uv run mypy reports <= 48 errors"
  (if deferred): "no changes; documented in plan"
```

---

### B16 — SR-04: Add `scripts/` to ruff/CI scope (LOW RISK, config)

**Agents:** Implementor + Validator

#### {task_description}

```yaml
id: b16_sr04_scripts_ruff_scope

title: "B16/SR-04 — Add scripts/ to CI ruff scope; verify no E402 regressions"

priority: low

depends_on: []

description: >
  SR-04: scripts/ is excluded from CI ruff scope (CI runs `ruff check src/ tests/`)
  and from pytest (testpaths = ["tests"]). The finding claims 2× E402 violations,
  but verification on HEAD f6ac8e1 shows ruff check on scripts/ passes clean
  (no E402 or other violations). The primary finding (ungated scope) is valid.

  Fix: Add "scripts/" to the CI ruff check scope: `ruff check src/ tests/ scripts/`.
  If any violations appear, fix them. If clean (expected), no further changes needed.

goals:
  - scripts/ included in CI ruff scope
  - any lint violations in scripts/ fixed (if any appear)
  - CI test job stays green

files:
  - path: .github/workflows/ci.yml
    targets:
      - type: job
        name: test
    semantic_anchors:
      - "run: uv run ruff check src/ tests/"
    changes:
      - action: modify
        description: >
          Change `uv run ruff check src/ tests/` to
          `uv run ruff check src/ tests/ scripts/`.

  - path: scripts/smoke_install.py (only if ruff flags violations)
    (No changes expected; ruff passes clean currently)

acceptance_criteria:
  - "CI ruff check scope includes scripts/"
  - "uv run ruff check src/ tests/ scripts/ passes"
  - "CI test job stays green"
```

---

### B17 — TST-015: Translate Cyrillic comments (LOW RISK ⚠ GATE, test-only)

**Agents:** Planner (scope decision) → Implementor

> ⚠ GATE: OD-4 (TST-015 scope: tests-only vs expand to production) must be resolved
> by Tech Lead before implementation. Default recommendation: Option A (tests-only).

#### {task_description}

```yaml
id: b17_tst015_cyrillic_comments

title: "B17/TST-015 — Translate 51 Cyrillic comment lines in test layer to English"

priority: low

depends_on:
  - od_04_tst015_scope

description: >
  TST-015: 51 Cyrillic lines found across tests/conftest.py (6), tests/test_dict_service.py
  (20), and tests/test_db_service.py (25) — corrected from plan's original "65"
  claim per Auditor verification (actual count: 6+20+25=51). These violate
  project rule 1 (English-only comments). Production code also has Russian user-visible
  strings, but per OD-4 (default: Option A), only test-layer comments are translated
  in this phase.

  Fix: Translate all Cyrillic comments, docstrings, and assertion messages in the
  test files to English. No behavioral changes.

goals:
  - all comments in tests/conftest.py, tests/test_dict_service.py, tests/test_db_service.py
    are in English (51 lines translated: 6 conftest + 20 test_dict + 25 test_db)
  - all docstrings in those files are in English
  - assertion messages (the string after assert statements or in assert(..., msg))
    are in English
  - no behavioral changes to tests

files:
  - path: tests/conftest.py
    targets:
      - type: fixture
        name: sample_dictionary (docstring)
    changes:
      - action: modify
        description: Translate the sample_dictionary docstring and any inline comments.

  - path: tests/test_dict_service.py
    targets:
      - type: test
        name: test_mapping_build (docstring)
      - type: test
        name: test_pattern_generation (docstring + comments)
      - type: test
        name: test_build_search_like_pattern_real_cases (docstring)
    changes:
      - action: modify
        description: Translate all Cyrillic docstrings, comments, and assertion messages.

  - path: tests/test_db_service.py
    targets:
      - type: test
        name: test_create_table_with_index_realistic (docstring + comments)
      - type: test
        name: test_perform_query (docstring)
      - type: test
        name: test_tbl_exist (docstring)
      - type: test
        name: test_set_data_tbl_columns (docstring + comments)
      - type: test
        name: test_search_columns_setter (docstring)
      - type: test
        name: test_column_index_and_get_col_names (docstring)
      - type: test
        name: test_add_columns (docstring)
      - type: test
        name: test_drop_tables (docstring)
      - type: test
        name: test_get_table_columns (docstring)
      - type: test
        name: test_create_search_table (docstring + comments)
    changes:
      - action: modify
        description: Translate all Cyrillic docstrings, comments, and assertion messages.

  - path: tests/test_app.py (if affected — currently English-only, but check)
    changes:
      - action: modify (only if Cyrillic found)

acceptance_criteria:
  - "git grep for Cyrillic characters in tests/ returns zero matches"
  - "all test docstrings and comments are English"
  - "uv run pytest tests/ passes (all green)"
  - "uv run ruff check tests/ clean"
```

---

### B18 — TST-017: Load template values via `importlib.resources` (LOW RISK, test-only)

**Agents:** Implementor

> **Caution:** `DataFile.index_column` and `DataFile.date_column` are non-Optional
> (`str = None`). The template's `index_column: 'AdId'` and the fixture's
> `"index_column": "adId"` differ in casing. The smoke script uses `"ADID"`.
> Loading the template resolves this to a single source of truth.

#### {task_description}

```yaml
id: b18_tst017_template_loaded_values

title: "B18/TST-017 — Load app_config.yaml template values via importlib.resources in realistic_config"

priority: low

depends_on:
  - b17_tst015_cyrillic_comments

description: >
  TST-017: realistic_config (tests/conftest.py) hand-copies all template values
  as literals. The fixture's "index_column": "adId" differs from the template's
  'AdId' (camelCase mismatch). The smoke script uses "ADID" (third spelling).
  The fixture should load the shipped template via importlib.resources.files()
  and read the actual values, asserting keys are present.

  Fix:
  1. In conftest.py, import the template config:
     from importlib.resources import files as import_files
     _TEMPLATE = (import_files("mko_data_cleaner") / "settings" / "app_config.yaml").read_text()
     _template_config = yaml.safe_load(_TEMPLATE)
  2. Update realistic_config to derive values from _template_config instead of
     literals. Specifically: index_column from template, date_column from template,
     dict_file from template, etc.
  3. Assert key presence: assert "data_paths" in _template_config, etc.

  Note: conftest.py currently imports only `Path`, `pl`, `pytest`, `DBWorker`,
  `DictColumnsIndexes`. Adding `yaml` and `importlib.resources` imports is needed.

  Note on DataFile non-Optional fields: the template has `index_column: 'AdId'`
  (a string, not null). The model accepts it. If the template had `null` for
  date_column, the model would reject it (since DataFile.date_column is `str = None`,
  not `Optional[str]`). The template currently has `date_column: 'researchDate'`
  (a string), so this is fine.

goals:
  - realistic_config loads values from the shipped app_config.yaml template
  - fixture uses the template's index_column value ('AdId'), not the literal 'adId'
  - assertion verifies template keys are present
  - all tests using realistic_config still pass (index_column casing change must
    be compatible with DBWorker.make_valid)

files:
  - path: tests/conftest.py
    targets:
      - type: fixture
        name: realistic_config
    semantic_anchors:
      - "return {"
      - "from importlib.resources import files" (add)
    changes:
      - action: modify
        description: >
          Replace the literal dict in realistic_config with a function that loads
          the template via importlib.resources, reads app_config.yaml, and
          returns the parsed dict. Add yaml import. Add assertions for key presence.
        code_hint: |
          import yaml
          from importlib.resources import files as import_files

          @pytest.fixture
          def realistic_config():
              _template = (
                  import_files("mko_data_cleaner") / "settings" / "app_config.yaml"
              )
              _cfg = yaml.safe_load(_template.read_text(encoding="utf-8"))
              assert "data_paths" in _cfg
              assert "data_file_settings" in _cfg
              return _cfg

acceptance_criteria:
  - "realistic_config loads from importlib.resources template"
  - "index_column matches template ('AdId'), not fixture literal ('adId')"
  - "all tests using realistic_config pass (DBWorker.make_valid handles casing)"
  - "uv run pytest tests/ passes (all green)"
  - "uv run ruff check tests/conftest.py clean"
```

---

### B19 — TST-013: Fix notebook docstring gate references (LOW RISK, test-only)

**Agents:** Implementor

#### {task_description}

```yaml
id: b19_tst013_notebook_docstring_gates

title: "B19/TST-013 — Fix test_notebook.py docstring to reference real CI notebook job"

priority: low

depends_on: []

description: >
  TST-013: test_notebook.py module docstring (lines 1-8) cites "B5 verification
  commands (dev-venv nbclient run)" and "against-tag gate" — neither exists in
  .github/. The real CI job is the `notebook` job in ci.yml (lines 71-92) which
  executes the notebook via nbclient. The docstring also mentions "B4: minimal
  end-to-end pipeline" in test_app.py:427.

  Fix: Rewrite the module docstring to reference the actual CI notebook job:
  "Real execution is performed by the CI `notebook` job
  (.github/workflows/ci.yml) which runs the notebook via nbclient."
  Remove all "B5", "against-tag", and plan-phase references.

goals:
  - docstring references the real CI notebook job (ci.yml notebook job)
  - no references to non-existent "B5" or "against-tag" gates
  - nbformat validity and cell-id tests unchanged

files:
  - path: tests/test_notebook.py
    targets:
      - type: module
    semantic_anchors:
      - "the B5 verification commands"
    changes:
      - action: modify
        description: >
          Rewrite lines 1-8 module docstring to accurately describe what the tests
          do and reference the real CI notebook execution job.

acceptance_criteria:
  - "no 'B5' or 'against-tag' references in test_notebook.py docstring"
  - "docstring references ci.yml notebook job"
  - "uv run pytest tests/test_notebook.py passes"
  - "git grep 'B5\\|against-tag' -- .github/ returns no matches (verifies docstring no longer cites them)"
```

---

### B20 — TST-014: Replace source-text assertions with runtime/content checks (LOW RISK ⚠ GATE, test-only)

**Agents:** Implementor

> ⚠ GATE: TST-014 has two parts with different trade-offs (CI stability vs.
> assertion strength). OD-8 (below) captures this. The notebook install-pin
> cannot be executed in CI (the import guard makes it a no-op). The MANIFEST.in
> text assertion can be strengthened by testing sdist wheel contents.

#### {task_description}

```yaml
id: b20_tst014_source_text_assertions

title: "B20/TST-014 — Replace source-text assertions with runtime/content checks"

priority: low

depends_on:
  - od_08_tst014_source_text_tradeoff

description: >
  TST-014 has two assertions that inspect source text rather than runtime behavior:

  Part A (test_notebook.py::test_notebook_install_cell_pins_v0_1_0, line 45):
  Asserts the git-URL string is present in the notebook source. The install cell
  is wrapped in try/except ImportError and is a no-op in CI (package pre-installed
  via uv sync). The assertion cannot detect that @v0.1.0 is uninstallable from a
  clean kernel.

  Part B (test_packaging.py::test_manifest_in_includes_yaml_templates, line 59):
  Asserts "recursive-include", "*.yaml", "settings" appear as text in MANIFEST.in.
  This cannot detect that the sdist actually contains the YAML files (text presence
  ≠ sdist content).

  Fix per OD-8:
  Part A: Replace with a structural assertion that verifies the install cell
  contains the pinned URL pattern AND the try/except guard. Document the
  trade-off (cannot execute in CI). OR: remove the test entirely (advisory-only).
  Part B: Strengthen by asserting that the built wheel (if available) or sdist
  contains the YAML settings files. Since CI build job already asserts wheel
  contents (ci.yml:42-45), this test can verify MANIFEST.in is correct by
  building the sdist and checking for settings/*.yaml. However, building the
  sdist in a unit test is slow and may not be feasible. Minimal fix: keep the
  text assertion but add a comment noting it's a structural guard, not a content
  proof. The real content assertion is in CI's build job.

  Recommendation (OD-8): Part A — replace with structural assertion (verify
  pattern presence + try/except guard in same cell). Part B — add a comment
  noting the limitation, keep the assertion (CI build job is the real gate).

goals:
  - Part A: install-pin test verifies structural properties (URL pattern + guard)
  - Part B: MANIFEST.in test has a comment noting the limitation (CI build job
    is the real gate for sdist/wheel content)
  - both tests remain green in CI

files:
  - path: tests/test_notebook.py
    targets:
      - type: test
        name: test_notebook_install_cell_pins_v0_1_0
    changes:
      - action: modify
        description: >
          Replace the bare substring assertion with: (1) assert the pinned URL
          pattern is present, (2) assert the install cell contains a try/except
          ImportError guard (structural check that documents the no-op-in-CI behavior),
          (3) add a comment that @v0.1.0 installability cannot be verified in CI
          (the package is pre-installed).

  - path: tests/test_packaging.py
    targets:
      - type: test
        name: test_manifest_in_includes_yaml_templates
    changes:
      - action: modify
        description: >
          Add a comment to the existing assertion noting that it is a structural
          guard, not a content proof — the real assertion is in CI's build job
          (ci.yml:42-45 checks wheel contents). No behavioral change to the test.

acceptance_criteria:
  - "test_notebook_install_cell_pins_v0_1_0 verifies URL pattern + try/except guard"
  - "test_packaging YAML test has limitation comment"
  - "uv run pytest tests/ passes (all green)"
  - "uv run ruff check changed files clean"
```

---

## OD-8 — TST-014 trade-off (supplementary to Open Decisions)

| Option | Approach | Trade-off |
|--------|----------|-----------|
| **A (strengthen)** | Build sdist/wheel in test, assert YAML presence | Slow test; may not work in all CI environments; duplicates CI build job |
| **B (document)** | Keep text assertions, add explanatory comments | Fast; acknowledges limitation; CI build job remains the real gate |
| **C (remove)** | Delete the source-text assertions entirely | Loses even the structural guard |

**Recommendation:** Option B (document). The CI build job already asserts wheel
contents (ci.yml:42-45). Strengthening the test adds latency and fragility without
improving real coverage.

---

## Resolved Open Decisions (Tech Lead)

> **Status: RESOLVED.** All Open Decisions are technical (not business) and carry
> clear recommendations in the plan. Resolved by Tech Lead based on the Auditor's
> `{code_context}` verification. No user input required per workflow Step 2 (skip
> unless business uncertainty exists).

| OD | Resolution | Rationale |
|---|---|---|
| OD-1 (drop_table contract) | **Option B** — Fix docstring + add explanatory comment in test. No production behavior change. | `DROP TABLE IF EXISTS` makes no-op success intentional. Option A introduces behavioral change for zero user benefit. |
| OD-2 (mypy CI gate) | **Option A** — Pin `mypy==1.19.1` (actual installed version), add non-blocking CI step (`continue-on-error: true`), scope `files = ["src/mko_data_cleaner"]`. | Loads the gate, pins for reproducibility, 48+50 known errors documented as noise. Follow-up phase can fix incrementally. |
| OD-3 (drop_triggers no-op) | **Option A** — Test-only (FTS trigger firing test). Production `drop_triggers` left as dead code. | Never called in production; fixing dead code is speculative. Trigger-firing test closes the coverage gap. |
| OD-4 (Cyrillic scope) | **Option A** — Tests-only (51 lines, not 65). Production Russian strings deferred to separate initiative. | Finding anchor is in test layer. Production CLI output change has broader blast radius — separate initiative. |
| OD-5 (E2E helper location) | **Option B** — Documented duplication + conftest fixture extraction for pytest. | Smoke script runs in fresh venv without `tests/`; package helper pollutes wheel. Lowest blast radius. |
| OD-6 (column-name investigation) | **Option C** — Isolated unit test with production-format data (`search_column_idx` header). Do NOT modify e2e fixture. | Auditor confirmed: `get_merged_dictionary` sorts by `"search_column_idx"` (correct for production). Test fixtures use `"search"` (by index, header is irrelevant after `_initialize_mapping`). E2e fixture pre-creates merged file so merge path is never hit. |
| OD-7 (polars pin) | **DEFER** — Keep `polars>=1.38.1,<2.0.0`. | Pin is stale but harmless. Lifting introduces version-change risk surface out of scope for this phase. |
| OD-8 (source-text assertions) | **Option B** — Document limitations, strengthen Part A to structural assertion, keep Part B with limitation comment. | CI build job already asserts wheel contents. Strengthening adds latency without improving real coverage. |

### OD-6 Investigation Summary (Auditor verification)

**Column name flow in production:**
1. Source CSV fragment files have header `search_column_idx` (per README).
2. `get_merged_dictionary` reads fragments → `pl.concat` → `merged.sort("search_column_idx")` → writes `merged_dictionary.csv` with `search_column_idx` header.
3. `get_dictionary` reads merged file → DataFrame has `search_column_idx` column.
4. `MappingDict._initialize_mapping` selects columns **by index** (`self.data[:, action_idx]` where `action_idx = [0,1,2,4]`) and **renames** them via `self._data_actions.columns = action_names`. The original header name (`search_column_idx`) is discarded — the model field name `search` takes over.
5. `_build_query` casts `pl.col("search")` to Int64 — works because column was renamed to `search` by step 4.

**Test fixture flow:**
1. Test pre-creates `merged_dictionary.csv` with header `search`.
2. `get_dictionary` reads it → DataFrame has `search` column.
3. `_initialize_mapping` selects by index and renames to `search` — same result.

**Key insight:** The `search_column_idx` sort in `get_merged_dictionary` is correct for **production** fragment files. The test fixtures use `search` as the header but it doesn't matter because `_initialize_mapping` renames by position. **No latent bug** — this is an intentional design. The B08 isolated unit test should use production-format data with `search_column_idx` header.

---

## Green-Gate Ordering Rationale

The 20 blocks are ordered in 8 phases. After each block's commit, `pytest` (65 tests)
remains green:

| Phase | Blocks | Why green persists |
|-------|--------|--------------------|
| P1 — Production inert-bug fixes | B01, B02 | Both bugs are inert (no test observes the spurious column or the empty-slice path). Fixes are behavior-preserving for all 65 tests. Each fix includes a regression test that passes post-fix. |
| P2 — Fixture-contract cluster | B03, B04 | B03 lands TST-001+002 together (fts row added before assertions un-guarded → non-empty fts_data → assertions hold). B04 adds new tests (no existing test affected). B01/B02 already fixed, so data is clean. |
| P3 — db_service test coverage | B05, B06 | Both add new tests for previously-untested paths. No existing test modified. No production changes (B06 trigger test exercises triggers via INSERT/DELETE/UPDATE, not drop_triggers). |
| P4 — E2E cluster | B07, B08, B09 | B07 is a refactor/comment (no test behavior change). B08 (per OD-6 default Option C) adds isolated unit tests without modifying the e2e fixture — e2e stays green. B09 adds a new roundtrip test. |
| P5 — CSV service | B10 | Replaces a literal regex with `DATE_REGEX` (identical value) + adds False-verdict + auto-detect tests. Existing test still passes (same regex, same True result). |
| P6 — Production-defect fixes | B11, B12 | B11 adds optional `output_files` param (backward-compatible); existing tests don't pass it. B12 (OD-1 default Option B) only updates docstring + adds comment — no production behavior change. |
| P7 — Gate/config | B13, B14, B16 | B13 removes inert marker (no test uses it). B14 adds non-blocking mypy (continue-on-error). B16 adds scripts/ to ruff scope (clean). B15 (SR-03) is DEFERRED per OD-7. |
| P8 — Test hygiene | B17, B18, B19, B20 | All are test-only changes (translations, fixture improvements, docstring fixes, assertion strengthening). No production changes. |
| **Order constraint** | B11 before B08 | TST-008 (null_data_files path attribution) is a production change to `run_report`. It must land before B08/B09 E2E tests that may assert on null_data_files content. B11 is in Phase 6 but B08/B09 are in Phase 4 — **the phase ordering must be adjusted**: B11 (TST-008) should actually precede B08/B09. See revised ordering below. |

**Revised ordering (critical dependency correction):**

The green-gate rationale above assumed B11 (TST-008) could come after B08/B09.
However, B08 (TST-010) and B09 (TST-004) exercise the e2e pipeline which produces
`null_data_files`. If B11 changes how `null_data_files` is populated (from glob to
write-time paths), the e2e tests' assertions on `null_data_files` must be compatible.
Therefore **B11 must precede B08 and B09**. The revised phase order is:

1. **B01 (SR-01)** → **B02 (SR-02)** — production inert fixes
2. **B03 (TST-001+002)** → **B04 (TST-016+006)** → **B05 (TST-003)** → **B06 (TST-005)** — fixture + db_service tests
3. **B11 (TST-008)** — production fix (null_data_files) + regression test
4. **B07 (TST-018)** → **B08 (TST-010)** → **B09 (TST-004)** — E2E (B11's production fix is already in place)
5. **B10 (TST-009)** — CSV service test
6. **B12 (TST-011)** — drop_table contract
7. **B13 (TST-012)** → **B14 (TST-007)** → **B16 (SR-04)** — gate/config (B15 SR-03 deferred)
8. **B17 (TST-015)** → **B18 (TST-017)** → **B19 (TST-013)** → **B20 (TST-014)** — test hygiene

---

## Agent Requirements Summary

| Block ID | Implementor | Auditor | Researcher | Planner | Validator | Gate/Decision |
|----------|-------------|---------|------------|---------|-----------|---------------|
| B01 (SR-01) | ✓ | — | — | — | ✓ (regression) | — |
| B02 (SR-02) | ✓ | — | — | — | ✓ (regression) | — |
| B03 (TST-001+002) | ✓ | — | — | — | — | — |
| B04 (TST-016+006) | ✓ | — | — | — | — | — |
| B05 (TST-003) | ✓ | — | — | — | — | — |
| B06 (TST-005) | ✓ | — | — | — | — | OD-3 (deferred fix) |
| B07 (TST-018) | — | — | — | ✓ | — | **OD-5** (package vs duplicate) |
| B08 (TST-010) | ✓ | ✓ (investigate) | — | — | — | **OD-6** (column-name investigation) |
| B09 (TST-004) | ✓ | — | — | — | — | depends on B08 |
| B10 (TST-009) | ✓ | — | — | — | — | — |
| B11 (TST-008) | ✓ | — | — | — | ✓ (regression) | — |
| B12 (TST-011) | — | — | — | ✓ | — | **OD-1** (prod vs test) |
| B13 (TST-012) | ✓ | — | — | — | — | — |
| B14 (TST-007) | ✓ | — | ✓ | ✓ | ✓ | **OD-2** (gate strategy) |
| B15 (SR-03) | — | — | ✓ | ✓ | — | **OD-7** (lift vs defer) |
| B16 (SR-04) | ✓ | — | — | — | ✓ | — |
| B17 (TST-015) | — | — | — | ✓ | — | **OD-4** (scope) |
| B18 (TST-017) | ✓ | — | — | — | — | depends on B17 |
| B19 (TST-013) | ✓ | — | — | — | — | — |
| B20 (TST-014) | ✓ | — | — | — | — | **OD-8** (trade-off) |

---

## Revised Execution Order (Final)

```
B01 (SR-01, production) → B02 (SR-02, production) →
B03 (TST-001+002, test) → B04 (TST-016+006, test) →
B05 (TST-003, test) → B06 (TST-005, test) →
B11 (TST-008, production+test) →
B07 (TST-018, decision) → B08 (TST-010, test) → B09 (TST-004, test) →
B10 (TST-009, test) →
B12 (TST-011, decision) →
B13 (TST-012, config) → B14 (TST-007, config) → B16 (SR-04, config) →
B17 (TST-015, decision) → B18 (TST-017, test) → B19 (TST-013, test) → B20 (TST-014, decision)
```

Blocks B15 (SR-03), if Tech Lead overrides OD-7, inserts after B01 and B14.

---

## Block-to-Finding Coverage Matrix

| Finding | Block | Status |
|---------|-------|--------|
| TST-001 | B03 | Included |
| TST-002 | B03 | Included |
| TST-003 | B05 | Included |
| TST-004 | B09 | Included |
| TST-005 | B06 (+ optional drop_triggers fix per OD-3) | Included (test-only) |
| TST-006 | B04 | Included (test-only; production `|`→`OR` rewrite deferred — code is functionally correct per VAL-002) |
| TST-007 | B14 | Included |
| TST-008 | B11 | Included |
| TST-009 | B10 | Included |
| TST-010 | B08 | Included |
| TST-011 | B12 | Included |
| TST-012 | B13 | Included |
| TST-013 | B19 | Included |
| TST-014 | B20 | Included |
| TST-015 | B17 | Included |
| TST-016 | B04 | Included |
| TST-017 | B18 | Included |
| TST-018 | B07 | Included |
| SR-01 | B01 | Included |
| SR-02 | B02 | Included |
| SR-03 | B15 (deferred per OD-7) | Deferred |
| SR-04 | B16 | Included |

**Deferred (no block):**
- TST-006 production rewrite (`|` → `OR` in `build_non_mapped`): The code is functionally correct per VAL-002 (empirical SQLite test confirms identical results). The rewrite is a code-quality improvement, not a bug fix. Defer to a future phase.
- TST-005 `drop_triggers` no-op: Dead code, never called in production. Per OD-3, test-only approach chosen.
