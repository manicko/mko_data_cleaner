---
phase: "99"
phase_name: "validation"
id_prefix: "TST"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/11-audit-test-coverage.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Validation Report

## Summary

Six findings audited against the tree at `c0fd0e1`; all six are **confirmed** (kept as filed). The highest validated severity is CRITICAL. The broadest consequence is false assurance: the green suite (93 passed) proves nothing about the FTS matches-bridge (TST-001), the no-date branch boundary (TST-007), or non-force re-init config preservation (TST-010), and the type-safety gate is red but never blocks a merge (TST-002). No cross-phase conflicts; no namespace collisions; no validation-level findings.

## Scope

**Examined:** `src/mko_data_cleaner/` (`app_service`, `csv_service`, `dict_service`, `db_service`, `models`, `paths`, `utils`, `errors`, `init_service`), `app.py`, `tests/conftest.py`, `tests/test_app.py`, `tests/test_db_service.py`, `tests/test_csv_service.py`, `tests/test_dict_service.py`, `.github/workflows/ci.yml`, `pyproject.toml`, `scripts/smoke_install.py`.

**Tools:** `pytest --cov=mko_data_cleaner --cov-report=term-missing`, `mypy`, `basedpyright`, `ruff check`, `grep` (via Grep tool), `git ls-files`, `git rev-parse`.

**Not verified:** CI jobs at runtime (no GitHub Actions runner available in this sandbox); real user reports with FTS rules or date-less CSVs beyond the synthetic fixture.

## Dispositions

| ID | Severity | Anchor | Title | Disposition |
|---|---|---|---|---|
| TST-001 | CRITICAL | `db_service.py::DBWorker.insert_matches_from_fts` | FTS matches-bridge never exercised by the suite | confirmed |
| TST-002 | HIGH | `ci.yml::"Type check (non-blocking)"` | Type-safety gate is red and never blocks a merge | confirmed |
| TST-007 | MEDIUM | `db_service.py::DBWorker.update_index_from_data` | `date_column=None` boundary crossed only on the date-present side | confirmed |
| TST-008 | MEDIUM | `pyproject.toml::[tool.pytest.ini_options]` | No test-timeout guard; one hang stalls the run | confirmed |
| TST-009 | LOW | `smoke_install.py::"DUPLICATED — keep in sync with"` | Fixture-data sync comment names a deleted test | confirmed |
| TST-010 | MEDIUM | `init_service.py::_copy_templates` | `force=False` re-init skip-existing branch unobserved | confirmed |

**Tally:** confirmed: 6 | re-typed: 0 | re-graded: 0 | merged: 0 | not substantiated: 0 | unsettled: 0

---

#### TST-001 — [CRITICAL] FTS matches-bridge never exercised by the suite

**Disambiguator resolution:** `_full_matches_table` — exists at `db_service.py:93` (`self._full_matches_table: str = "full_matches_table"`). ✓

**Claim re-derived from the executing path:**
- Anchor `db_service.py::DBWorker.insert_matches_from_fts` resolves to `db_service.py:559`. The method body (lines 560–571) builds an `INSERT INTO fts_full_matches SELECT ... FROM fts_table JOIN mapping_table ... MATCH ...` SQL and calls `perform_query(sql)`. ✓
- Anchor `app_service.py::AppService.run_report` resolves to `app_service.py:110`. Line 171: `if not mapping_dict.fts_data.is_empty():` → line 172: `db_worker.link_search_table()`. Lines 181–194: the FTS branch calls `db_worker.insert_matches_from_fts("fts_temp_tbl")` at line 193. ✓
- **Mechanical resolution:** All line references in the input resolve. No `:<digits>` locators in the anchor fields. All paths appear in `git ls-files`.

**Support that is only a green control:** The green `pytest` gate (93 passed) does not observe the FTS matches-bridge. `grep insert_matches_from_fts tests/` → 0 hits. `synthetic_report`'s `dict_lines` (conftest.py:119–123) contains only `d;f;4;NULL;Brand2;` and `a;f;4;NULL;Brand1;added_by_brand` — no `fts` match row, so `mapping_dict.fts_data.is_empty()` is always `True` in every e2e test, and `run_report`'s FTS branch (lines 171–172, 181–194) is never entered.

**Coverage evidence reproduced:**
```text
src/mko_data_cleaner/core/app_service.py      148     11    93%   51-52, 62-64, 172, 182-194, 305, 344
src/mko_data_cleaner/core/db_service.py       318     37    88%   ... 560-571 ...
```

**Grade against Phase 11 rubric:** CRITICAL — "the suite's green is affirmatively false today: a decision the system acts on whose effect no assertion anywhere can observe, where the unobserved effect is one a user or an operator receives." The `if not mapping_dict.fts_data.is_empty()` decision at app_service.py:171 gates the sole path that makes shipped FTS rules fire, and no assertion anywhere observes its effect. Downgrading to HIGH ("boundary asserted from one side only") is rejected: the matches side is not one-sided, it has *zero* coverage (no direct unit test). ✓ No re-grade.

**Code vs. documentation:** Production code is load-bearing. The `insert_matches_from_fts` method and its caller in `run_report` are both live code paths. No documentation contradicts the executing path. Type unchanged.

**Recommendation carry-out:** The fix names concrete targets: add `test_insert_matches_from_fts_populates_matches` (db_service.py) and add an `fts` row to `synthetic_report`'s `dict_lines` (conftest.py). Both symbols exist and are stable. Rollout risk noted in the input: adding an `fts` row changes `rows_loaded`/`rows_exported` in `test_process_data_end_to_end` and `test_run_cli_success`, which must co-change. ✓

---

#### TST-002 — [HIGH] Type-safety gate is red and never blocks a merge

**Disambiguator resolution:** `continue-on-error` — exists at `ci.yml:28`. ✓

**Claim re-derived from the executing path:**
- Anchor `.github/workflows/ci.yml::"Type check (non-blocking)"` resolves to ci.yml:27–29. The step name is "Type check (non-blocking)", `continue-on-error: true` at line 28, `run: uv run mypy src/mko_data_cleaner` at line 29. ✓
- Anchor `pyproject.toml::[tool.mypy]` resolves — `[tool.mypy]` section at pyproject.toml:139–150 with `disallow_any_generics = true`, `strict_optional = true`, etc. ✓
- Anchor `pyproject.toml::[tool.basedpyright]` resolves — section at pyproject.toml:152–154 with `typeCheckingMode = "standard"`. ✓

**Type checks reproduced:**
```text
$ uv run mypy src/mko_data_cleaner
Found 45 errors in 6 files (checked 13 source files)

$ uv run basedpyright src/
39 errors, 0 warnings, 0 notes

$ uv run ruff check src/ tests/
All checks passed!

$ uv run pytest
93 passed in 3.55s
  (plugins: cov-7.1.0, mock-3.15.1)
```

**Support that is only a green control:** `ruff check` is green (passed) and `pytest` is green (93 passed). Neither substantiates the type-safety claim. The type gate is red (45 mypy errors, 39 basedpyright errors) and explicitly non-blocking (`continue-on-error: true`). `basedpyright` is not referenced anywhere in `ci.yml` — confirmed by grep of the CI file. ✓

**CI step scope verification:** The `test` job (ci.yml:12–30) has exactly four `run` steps: `ruff check` (blocking), `Type check` with `continue-on-error: true` (non-blocking), `pytest` (blocking). The type step is the sole `continue-on-error: true` step. No `timeout-minutes` is declared on the `test` job. ✓

**Grade against Phase 11 rubric:** HIGH — "a property the harness depends on that nothing holds it to" (project rule: Type Safety Everywhere; the project set `disallow_any_generics`, `strict_optional`, etc.) and "a gate declared and never loaded" — the gate is loaded but red and explicitly non-blocking. Downgrading to CRITICAL is rejected: no shipped assertion is false (CI green is accurate because the type step is explicitly allowed to fail). Downgrading to MEDIUM is rejected: the redness is not a sanctioned noise class. ✓ No re-grade.

**Code vs. documentation:** Load-bearing artefact is `ci.yml` (CI gate) with `pyproject.toml` (checker config) as secondary. The `.kilo/commands` documentation lists `uv run basedpyright src/` as the type check but it is not wired into CI — this is a documentation-vs-execution divergence but does not change the finding. Type unchanged.

**Recommendation carry-out:** The fix names a concrete target: remove `continue-on-error: true` from the mypy step in `ci.yml:28`. Dependencies are correctly stated (Phase 04 must fix the 45 diagnostics first). ✓

---

#### TST-007 — [MEDIUM] `date_column=None` boundary crossed only on the date-present side

**Disambiguator resolution:** `if self.date_column` — exists at `db_service.py:374` (`if self.date_column:`) in `_create_index_table` and at `db_service.py:405` (`if self.date_column:`) in `update_index_from_data`. ✓

**Claim re-derived from the executing path:**
- Anchor `db_service.py::DBWorker.update_index_from_data` resolves to `db_service.py:400–416`. Lines 401: `if self.index_column:`, line 405: `if self.date_column:`, lines 406–407: the date-present append (`insert_columns += f", {self.date_column}"`, `select_columns += f", MAX({self.date_column})"`). ✓
- Anchor `db_service.py::DBWorker._create_index_table` resolves to `db_service.py:368–381`. Line 374: `if self.date_column:` gates appending `self.date_column` to the index table columns. ✓
- Anchor `csv_service.py::CSVWorker._detect_date_column` resolves to `csv_service.py:124–151`. Line 151: `return None` when no date column is found. ✓

**Coverage evidence reproduced:**
```text
src/mko_data_cleaner/core/csv_service.py      162     16    90%   84-86, 145-146, 151, 182-184, 205, 226-228, 379-381
```

**Support that is only a green control:** `pytest` is green (93 passed) but the green suite never exercises the date-absent path. Verified via grep:
- `grep date_column=None tests/` → 0 matches. ✓
- `grep update_index_from_data tests/` → 0 direct calls. ✓

**Fixture contract verification:** The `db_worker` fixture (conftest.py:28–38) always sets `date_column=realistic_config["data_file_settings"]["date_column"]`. The `synthetic_report` fixture ships `researchDate` rows with valid dates. `run_report` (app_service.py:143–144) calls `check_date_column(date_column)` which returns the column name, never `None` at line 151. The `test_check_date_column_auto_detection` test (test_csv_service.py:68) calls `check_date_column()` with no column arg on a CSV that *has* a date column, so `_detect_date_column` returns "date_str" at line 150, never reaching `return None` at line 151. ✓

**Grade against Phase 11 rubric:** MEDIUM — "bounded and real, with a specific audience." By blast radius, this affects only reports with no date-like column. Downgrading to HIGH is rejected per the rubric's effect/blast-radius anchor: the untested side is a trivial `if self.date_column:` skip branch with a limited audience, not a one-sided boundary assertion on a core shipped feature. ✓ No re-grade.

**Code vs. documentation:** Production code is load-bearing. No documentation contradicts the executing path. Type unchanged.

**Recommendation carry-out:** The fix names `test_run_report_without_date_column_skips_max_date` as a new test targeting the date-absent path. Target symbol exists. ✓

---

#### TST-008 — [MEDIUM] No test-timeout guard; one hang stalls the run

**Disambiguator resolution:** `--strict-markers` — exists at `pyproject.toml:163` in `addopts`. ✓

**Claim re-derived from the executing path:**
- Anchor `pyproject.toml::[tool.pytest.ini_options]` resolves to pyproject.toml:156–168. `addopts` (lines 159–165): `--import-mode=importlib`, `-ra`, `-v`, `--strict-markers`. No `--timeout`. ✓

**Config evidence reproduced:**
```text
addopts = [
    "--import-mode=importlib",
    "-ra",
    "-v",
    "--strict-markers",
]
```

**Support that is only a green control:** `pytest` is green (93 passed) but provides no timeout protection. The harness's own plugin list is the authoritative evidence:

```text
plugins: cov-7.1.0, mock-3.15.1
```

No `pytest-timeout` plugin is loaded. ✓

**Additional verification:**
- `grep pytest-timeout pyproject.toml` → no match. ✓
- `grep pytest-timeout .github/workflows/ci.yml` → no match. ✓
- CI `test` job (ci.yml:12–30) declares no `timeout-minutes` key. ✓
- The only `timeout` token in `ci.yml` is `client = NotebookClient(nb, timeout=300, kernel_name='python3')` at line 92 — this is the notebook execution client's per-execution timeout, not a CI job-level `timeout-minutes` backstop. It does not guard the `test` job. ✓

**Grade against Phase 11 rubric:** MEDIUM — "a resource left unbounded so that one test can take the run down." No test currently hangs, so this is latent, not an active false-green. Downgrading to HIGH is rejected (latent, not active); downgrading to LOW is rejected (a hung run stalls the whole suite, not "no runtime consequence"). ✓ No re-grade.

**Recommendation carry-out:** The fix names concrete targets: add `pytest-timeout` to the `dev` group in `pyproject.toml`, add `--timeout=30 --timeout-method=thread` to `addopts`, and add `timeout-minutes:` to the CI `test` job. All targets exist and are stable. ✓

---

#### TST-009 — [LOW] Fixture-data sync comment names a deleted test

**Disambiguator resolution:** `test_null_data_files_excludes_stale` — exists at `smoke_install.py:116` (stale comment reference). The test itself is absent from `tests/`. ✓

**Claim re-derived from the executing path:**
- Anchor `scripts/smoke_install.py::"DUPLICATED — keep in sync with"` resolves to smoke_install.py:114–117. ✓
- Anchor `tests/conftest.py::synthetic_report docstring` resolves to conftest.py:90–127 (docstring at lines 92–103). ✓

**Static evidence reproduced:**
```text
# smoke_install.py:114-117
# DUPLICATED — keep in sync with:
#   - tests/test_app.py::test_process_data_end_to_end
#   - tests/test_app.py::test_null_data_files_excludes_stale
#   (both use tests/conftest.py::synthetic_report fixture)
```

- `grep test_null_data_files_excludes_stale tests/` → 0 matches. The test was deleted in a prior cycle (referenced in `.ai/plans/04-test-coverage-remediation.md` as T-DEL-04). ✓
- `grep test_null_data_files_excludes_stale tests/ tests/` → 0 matches in any test file. ✓
- The `synthetic_report` docstring (conftest.py:97–99) lists the correct, current trio: `scripts/smoke_install.py`, `tests/test_app.py::test_process_data_end_to_end`, `tests/test_app.py::test_run_cli_success`. All three exist. ✓

**Grade against Phase 11 rubric:** LOW — "self-description and hygiene with no runtime consequence today." The two copies of synthetic data currently agree; CI's `install-smoke` runs `smoke_install.py` green. Downgrading to MEDIUM is rejected: no behaviour gap yet, only a stale sync list. ✓ No re-grade.

**Code vs. documentation:** The load-bearing contract lives in the `conftest.py::synthetic_report` docstring (lines 97–99), which is authoritative and correct. The stale comment in `smoke_install.py:114-117` contradicts the actual sync list. Documentation is the divergent artefact; code is correct. Type unchanged.

**Recommendation carry-out:** The fix names `scripts/smoke_install.py:114-117` as the target, to match the conftest.py docstring. Target is stable. ✓

---

#### TST-010 — [MEDIUM] `force=False` re-init skip-existing branch unobserved

**Disambiguator resolution:** `if target_path.exists() and not force` — exists at `init_service.py:23`. ✓

**Claim re-derived from the executing path:**
- Anchor `src/mko_data_cleaner/core/init_service.py::_copy_templates` resolves to init_service.py:7. Lines 23–24:
  ```python
  if target_path.exists() and not force:
      continue
  ```
  ✓

**Coverage evidence reproduced:**
```text
src\mko_data_cleaner\core\init_service.py      25      4    84%   20-21, 24, 57
```

Line 24 (the `continue` in the non-force skip-existing branch) is uncovered. ✓

**Support that is only a green control:** `pytest` is green (93 passed) but the preservation contract is never asserted. Verified via grep:
- `grep force=False tests/` → 0 matches. ✓

**Fixture boundary verification:** Every init test in `test_app.py` exercises only one side of the `force` boundary:
- `test_init_cli_creates_settings_in_tmp_dir` (line 203): uses `--force`.
- `test_initialize_settings_force_overwrites` (line 331): uses `force=True`, pre-seeds "stale" content — but `force=True` skips the `not force` guard.
- `test_init_cli_with_settings_dir` (line 347): uses `--force`.
- `test_initialize_settings_threads_settings_dir` (line 322): calls `initialize_settings(settings_dir=user_dir)` with default `force=False`, but `user_dir` is a fresh non-existent path — no pre-existing `app_config.json5` to skip over.

No test pre-seeds an existing settings file with `force=False` and asserts non-overwrite. ✓

**Grade against Phase 11 rubric:** MEDIUM — "bounded and real, with a specific audience." Bounded to the re-init audience (operators running `mko-data-cleaner init` a second time without `--force`). Downgrading to HIGH is rejected: the untested side is a single guard statement with limited blast radius (the copy path is fully covered via `--force` tests). Downgrading to LOW is rejected: it carries real runtime consequence (config/data loss). ✓ No re-grade.

**Code vs. documentation:** Production code is load-bearing. No documentation contradicts the executing path. Type unchanged.

**Recommendation carry-out:** The fix names `test_initialize_settings_no_force_preserves_existing` targeting `init_service.py::_copy_templates` line 24. The target is existing and stable. ✓

---

## Cross-Phase Analysis

| Concern | Phase that owns it | Validation disposition |
|---|---|---|
| FTS SQL correctness | Phase 04 | Complementary — TST-001 owns coverage gap, Phase 04 owns SQL diagnostics. No conflict. |
| Type-check diagnostics | Phase 04 | Complementary — TST-002 owns the gate, Phase 04 owns the diagnostics. No conflict. |
| FTS rule path not executed | OQ-5 | Cross-reference — noted as "owner notes 'any FTS5 rule path' not executed." Aligns with TST-001. No conflict. |
| No-date path | OQ-5 | Cross-reference — aligns with TST-007. No conflict. |

**Sibling reports compared:** `.ai/audit/11-test-coverage/findings.md` (input, raw, draft). No pre-validated sibling reports exist. The input report's own verification table (Appendix A gate inventory) is methodology, not a verdict — not gated upon. ✓

## Namespace Integrity

The audited phase's prefix is `TST-` (Phase 11 — Test Coverage). Six identifiers minted: `TST-001`, `TST-002`, `TST-007`, `TST-008`, `TST-009`, `TST-010`. The `TST-003`–`TST-006` range is vacant (stale cross-references to a deleted test, documented in the input). Validation-level findings (`VAL-`) occupy a **separate namespace and section** — none were discovered during this validation pass. The audited (`TST-`) and validation (`VAL-`) namespaces do not share a table.

**Ruling:** The `TST-` prefix is reused per the phase file's declaration; no second namespace was minted. The stale `TST-003`–`TST-006` references are drift to be retired (not load-bearing provenance). The `code_ref: c0fd0e1` matches `git rev-parse HEAD` (`c0fd0e1bbc7e5fe823ea9b7ab14e293ae93c1959`). ✓

## Report Template Compliance

The input report follows the `audit-findings.md` template: front matter with all mandated fields (`phase`, `phase_name`, `id_prefix`, `mode`, `report_status`, `severity_taxonomy`, `code_ref`); Summary; Scope (Examined/Tools/Not verified); Findings table; per-finding sections with Anchor, Disambiguator, Checked, Why this band, Problem, Repro, Impact, Root cause, Fix, Verify, Related; Evidence fences; Fix Order; Appendices A and B. No field is renamed or omitted. ✓

## Does the Input Rest on Its Own Enumerated Checks?

Each finding's support is an independent observation, not solely a green control from the phase's own check list:

| Finding | Support source | Independent observation? |
|---|---|---|
| TST-001 | Coverage report + grep + fixture data inspection | Yes — coverage lines 560-571 uncovered, grep 0 hits |
| TST-002 | mypy/basedpyright output + ci.yml text inspection | Yes — mypy 45 errors, basedpyright 39 errors |
| TST-007 | Coverage report + grep + fixture boundary analysis | Yes — csv_service.py:151 uncovered, grep 0 hits |
| TST-008 | pyproject.toml addopts + pytest plugin list + ci.yml | Yes — no timeout plugin, no timeout-minutes |
| TST-009 | Grep of two sites, docstring comparison | Yes — stale reference in one, authoritative in the other |
| TST-010 | Coverage report + grep + test-by-test boundary analysis | Yes — init_service.py:24 uncovered, grep 0 hits |

Each finding additionally arises from an angle the phase's check list does not explicitly name — e.g., TST-001's distinction between "reachable" (FTS triggers/unit tests exist) and "exercised" (no test makes `insert_matches_from_fts` fire is a structural observation about the fixture-universe, not a pass/fail of any enumerated check). ✓

---

## Fix Order

- **Dependencies:** TST-002 Fix (make a type gate blocking) depends on Phase 04 fixing the 45 mypy / 39 basedpyright diagnostics first (Phase 04 owns the diagnostics; TST-002 owns the gate). **Unchanged from input.**
- **Merge candidates:** TST-001 and TST-007 share a root cause (synthetic_report exercises only a subset of run_report branches); their fixes are independent but both expand the e2e fixture. **Unchanged from input.**
- **Conflicting evidence:** None. **Unchanged.**
- **Cross-phase conflicts:** None. **Unchanged.**
- **Rollout risk:**
  - TST-001 Fix — adding an `fts` row to `synthetic_report` changes `rows_loaded`/`rows_exported` (3→3 with FTS, but match semantics may change row counts); `test_process_data_end_to_end` (`==3`/`==2`) and `test_run_cli_success` (`"3 loaded → 2 clean"`) must co-change or the suite breaks. **Confirmed.**
  - TST-002 Fix — removing `continue-on-error` makes CI red until mypy is 0, blocking merges until Phase 04 lands. **Confirmed.**
  - TST-007 Fix — new test with a date-less CSV, non-breaking. **Confirmed.**
  - TST-008 Fix — config-only (add `pytest-timeout` + `--timeout=30`), non-breaking. **Confirmed.**
  - TST-009 Fix — comment-only, non-breaking. **Confirmed.**
  - TST-010 Fix — adds a non-force re-init preservation test, independent and non-breaking. **Confirmed.**

## Recommendations Carry-Out Check

| Finding | Fix target resolved? | Applies the fix? |
|---|---|---|
| TST-001 | Yes — `db_service.py::insert_matches_from_fts`, `conftest.py::synthetic_report` | Yes — new test + fts row |
| TST-002 | Yes — `ci.yml` type step, `pyproject.toml` mypy config | Yes — remove `continue-on-error` |
| TST-007 | Yes — `csv_service.py::_detect_date_column`, `db_service.py::update_index_from_data` | Yes — new date-less fixture case |
| TST-008 | Yes — `pyproject.toml::[tool.pytest.ini_options]`, `ci.yml` test job | Yes — add timeout guard |
| TST-009 | Yes — `scripts/smoke_install.py:114-117` | Yes — sync comment to conftest docstring |
| TST-010 | Yes — `init_service.py::_copy_templates` line 24 | Yes — new preservation test |

All six recommendations target existing, stable symbols and apply fixes directly tied to the verified defect.

---

## Appendix A — Reproduced Evidence

### A1 — Coverage report (reproduces TST-001, TST-007, TST-010)

```text
Name                                        Stmts   Miss  Cover   Missing
-------------------------------------------------------------------------
src/mko_data_cleaner/core/app_service.py      148     11    93%   51-52, 62-64, 172, 182-194, 305, 344
src/mko_data_cleaner/core/csv_service.py      162     16    90%   84-86, 145-146, 151, 182-184, 205, 226-228, 379-381
src/mko_data_cleaner/core/db_service.py       318     37    88%   51-52, 128, 158, 198, 230-234, 259-262, 281-283, 294-301, 345-348, 560-571, 641, 645-646, 783-784, 792-793
src/mko_data_cleaner/core/init_service.py      25      4    84%   20-21, 24, 57
-------------------------------------------------------------------------
============================= 93 passed in 3.55s ==============================
```

### A2 — mypy (reproduces TST-002)

```text
Found 45 errors in 6 files (checked 13 source files)
```

### A3 — basedpyright (reproduces TST-002)

```text
39 errors, 0 warnings, 0 notes
```

### A4 — ruff (reproduces TST-002 gate status)

```text
All checks passed!
```

### A5 — CI type step (reproduces TST-002)

```yaml
      - name: Type check (non-blocking)
        continue-on-error: true
        run: uv run mypy src/mko_data_cleaner
```

### A6 — pytest plugins (reproduces TST-008)

```text
plugins: cov-7.1.0, mock-3.15.1
```

### A7 — pytest addopts (reproduces TST-008)

```toml
addopts = [
    "--import-mode=importlib",
    "-ra",
    "-v",
    "--strict-markers",
]
```

### A8 — smoke_install.py stale sync comment (reproduces TST-009)

```text
# DUPLICATED — keep in sync with:
#   - tests/test_app.py::test_process_data_end_to_end
#   - tests/test_app.py::test_null_data_files_excludes_stale
#   (both use tests/conftest.py::synthetic_report fixture)
```

### A9 — conftest.py synthetic_report docstring sync list (reproduces TST-009)

```text
    When either the data or the assertions change, update ALL copies:
      1. scripts/smoke_install.py (maintenance comment)
      2. tests/test_app.py::test_process_data_end_to_end
      3. tests/test_app.py::test_run_cli_success
```

### A10 — Unreproducible claims (out of validation scope)

The input report cites CI runtime behaviour (build, install-smoke, notebook kernel execution) that cannot be reproduced in this sandbox. The claims rest on `ci.yml` text inspection, not runtime execution. This is stated as a limit, not a finding.
