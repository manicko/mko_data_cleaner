---
name: audit-tests-full
description: audit-tests-full
agent: auditor
alwaysApply: false
---

# Test Quality Audit

## Step 0 — Ensure uv Environment is Ready

Run `uv sync --group dev` to install/sync all dependencies. Confirm the Python interpreter is available (`uv run python --version`) before proceeding. This is a standalone local uv project with no Docker or external services.

## Objective

Perform a complete audit of test coverage to:
- Identify and delete/rewrite tests that don't match the current architecture and code style
- Ensure tests verify **real system behavior**, not mocks or implementation details
- Verify sufficient coverage of all key system parts
- Guarantee high diagnostic value of tests
- Recommend missing test coverage for critical business flows

**Core principle:** Production code is the source of truth. Tests adapt to it, not the other way around. When tests and docs conflict with production code, update tests and docs — not the code.

## Recommendation Types

Label every finding:
- `[TEST-DELETE]` — test is harmful or worthless; delete it
- `[TEST-REWRITE]` — test intent is right but implementation is wrong; rewrite
- `[TEST-UPDATE]` — test needs minor updates to match current code
- `[BEST-PRACTICE]` — missing test coverage or quality improvement; advisory
- `[DOC-UPDATE]` — test reveals that docs/spec are wrong, not the code

## Research

Use `websearch` to verify current best practices for:
- pytest test isolation patterns (tmp_path, SQLite in-memory)
- Polars DataFrame testing patterns
- SQLite FTS5 trigger verification in tests
- Meaningful test coverage metrics vs coverage for its own sake

## Anti-Patterns (subject to deletion or complete rewrite)

### 1. Architecture / Contract Mismatch
- Sync/async mismatch 
- Use of deprecated methods, DTOs, data types, or contracts
- Testing removed or renamed functionality
- Forces keeping legacy code just to pass tests
- Tests against old data formats (e.g., pre-migration CSV schema)

### 2. Overmocking
- Mock completely replaces business logic
- Assertions check mock values, not real results
- Excessive `patch`/`mock` without verifying side effects
- Mocking Redis when `fakeredis` or real Redis would be more valuable
- Mocking SQLite database operations instead of using tmp_path file-backed DB

### 3. Tautological / Useless Tests
- `assert True`, missing `assert`, or trivial checks
- Verify obvious things ("object created", "status 200")
- Repeat implementation instead of verifying business contract

### 4. Wrong Abstraction Level
- Test private methods, internal details, specific SQL queries
- Verify call order instead of final result and business rules

### 5. Fragile Tests
- Break on refactoring without behavior change
- Depend on test execution order, `sleep`, or shared mutable state
- Shared mutable state in fixtures across test functions

### 6. Poor Business Coverage
- Ignore negative scenarios and boundary conditions
- Use primitive data instead of realistic data
- Don't verify DB state, logs, and side effects

## Additional Checks

- **Test Pyramid**: balance between unit, integration, and e2e tests (avoid over-reliance on mocked unit tests)
- Duplication, copy-paste tests and fixtures
- Giant fixtures, hidden dependencies, shared mutable state
- **Mutation resistance** — test must fail when business logic is broken

## 9.1 pytest Standards

- All tests use **pytest** (not `unittest.TestCase`)
- Fixtures extracted to `conftest.py`
- Mocking only via `unittest.mock` or `pytest-mock`
- Use `pytest.raises` for error scenarios
- Test file naming: `test_*.py`
- Test function naming: `test_*`

## 9.2 System Coverage

Verify tests exist for all key system parts:

### Required Coverage Areas

- **Settings / Configuration**
  - `WrongDataSettings` error raised for missing/invalid settings
  - Default settings resolution from `app_config.json5`
  - Settings directory validation (existing dir, nonexistent dir)
  - Path resolution (paths module, report/output dirs)

- **Dictionary Processing (dict_service)**
  - `MappingDict.build_mapping` produces correct `like_data` and `fts_data` frames
  - Mapping index ordering (highest `mapping_index` wins for REPLACE)
  - FTS row handling (pipe-separated search columns, pipe-separated terms)
  - Extra columns passed through correctly

- **CSV Service (csv_service)**
  - CSV/CSV.gz loading with correct headers and schema
  - Date column auto-detection (`DATE_REGEX`, `_detect_date_column`)
  - Merged dictionary creation (dedup, sort by `search_column_idx`)
  - CSV export roundtrip (BOM, separator, gzip compression, `decimal_comma`)
  - Empty dictionary directory edge case (graceful error, not `pl.concat([])` crash)

- **Database Service (db_service)**
  - `DBWorker` table creation with correct indexes
  - `check_date_column` verdicts (True/False paths)
  - `_apply_delete` executes exactly once (no double execution)
  - `_apply_replace` picks highest `mapping_index`
  - FTS5 trigger sync on INSERT/UPDATE/DELETE
  - `drop_table` contract (always True with `IF EXISTS`)

- **Application Service (app_service)**
  - End-to-end pipeline: CSV → dictionary merge → database import → export
  - `generate_rules_blocks` loop handles all action types (f, p, s, d, r)
  - Error propagation for invalid input (bad CSV, missing dictionary)
  - Clean row counts (input rows, output rows, null data files)

- **CLI Entry Point (app.py)**
  - `CliRunner` integration test for full pipeline invocation
  - Settings-not-initialized error path
  - Print output contains expected summary information

- **Packaging & Smoke**
  - Wheel builds successfully with correct metadata
  - Package is pip-installable in a fresh venv
  - `smoke_install.py` passes in a clean environment
  - Version consistency across `pyproject.toml` and `__init__.py`

### Special Attention
- Negative scenario and boundary condition coverage
- Integration tests with real SQLite (`db_worker` fixture, tmp_path)
- Critical path coverage (process_data → clean_data export)
- Test database isolation (each test gets its own tmp SQLite DB)
- Fixture structure matches `tests/conftest.py` canonical fixtures


## Audit Result Format

Create file: `.ai/audit/tests/audit_report_<number>.md` (next available number)

**Report Structure:**

1. **Statistics**
   - Total tests: N
   - Critical problems: M
   - Recommended for deletion: K
   - Coverage by module

2. **Problematic Tests Table**

| File | Test | Type | Category | Problem | Action | Priority |
|------|------|------|----------|---------|--------|----------|
| tests/test_db_service.py | test_drop_tables | [TEST-UPDATE] | Contract | Asserts `result["non_existent"] is True` — IF EXISTS makes this always True | Update test + docstring | LOW |
| tests/test_csv_service.py | test_uses_pandas | [TEST-DELETE] | Architecture | Imports pandas instead of polars for CSV loading | Delete | HIGH |
| tests/test_app.py | test_no_assert | [TEST-DELETE] | Quality | No assert statement | Delete | MEDIUM |
| tests/test_dict_service.py | — | [BEST-PRACTICE] | Coverage | No FTS trigger firing assertions | Add tests | HIGH |
| tests/test_csv_service.py | test_export_csv | [DOC-UPDATE] | Contract | Expects old cleanup behavior | Update test + spec | LOW |

3. **Coverage Assessment** — which important areas/scenarios are uncovered or weakly covered

4. **Key Findings** (with examples)

5. **Action Plan**
   - Delete Required (tests that force wrong production code changes)
   - Rewrite Required (tests with right intent, wrong implementation)
   - Improve (add negative cases, integration tests, etc.)
   - Doc Updates (tests that revealed spec/docs are wrong)

6. **Blocked Refactorings** (if any)

## Acceptance Criteria

- Clear separation: what to delete / rewrite / improve
- For each `ARCHITECTURE_CONFLICT` — link to production code
- Prioritization of problems
- Recommendations for improving test culture
- Critical coverage gaps identified

**Rule:** Better to delete a questionable test than to keep a test that misleads or slows down project development.
