# Test Quality Audit — Report Index

| Field | Value |
|---|---|
| **Project** | mko_data_cleaner |
| **Audit Phase** | Step 4 — Test Quality Synthesis |
| **Date** | 2026-10-18 |
| **Total Reports** | 1 |

## Reports

| Report | Date | Findings | Severity Range | Status |
|---|---|---|---|---|
| [audit_report_01.md](./audit_report_01.md) | 2026-10-18 | 37 | HIGH (28) · MEDIUM (7) · LOW (2) | Final |

## Scope Summary

The audit covers 9 test modules across 6 thematic blocks:

| Block | Test File | Findings | High | Medium | Low |
|---|---|---|---|---|---|
| Block 1 | `tests/conftest.py` | 7 | 7 | 0 | 0 |
| Block 2 | `tests/test_app.py` | 13 | 13 | 0 | 0 |
| Block 3 | `tests/test_db_service.py` | 2 | 2 | 0 | 0 |
| Block 4 | `tests/test_dict_service.py` | 8 | 4 | 4 | 0 |
| Block 5 | `tests/test_csv_service.py` | 4 | 1 | 2 | 1 |
| Block 6 | `tests/test_packaging.py`, `tests/test_metadata.py` | 3 | 1 | 1 | 1 |
| **Total** | | **37** | **28** | **7** | **2** |

## Classification Breakdown

| Classification | Count | Description |
|---|---|---|
| `[TEST-DELETE]` | 9 | Remove dead fixtures and redundant tests |
| `[TEST-REWRITE]` | 10 | Strengthen weak/tautological assertions |
| `[TEST-UPDATE]` | 12 | Extend coverage for missing branches and edge cases |
| `[TEST-WRITE]` | 1 | Write new tests for entirely untested production methods |
| `[BEST-PRACTICE]` | 3 | Architectural improvements and latent bug fixes |
| `[DOC-UPDATE]` | 2 | Documentation corrections (docstrings, line references) |

## Key Findings at a Glance

- **Highest priority:** `_apply_add` method (`db_service.py:690-745`) — 56 lines of production SQL with **zero** direct test coverage. Sibling methods `_apply_delete` and `_apply_replace` are both directly tested.
- **Latent bug:** `DBWorker.column_index` (`@cached_property` at `db_service.py:124`) is not invalidated when `set_data_tbl_columns` reassigns `self._data_tbl_columns` (`db_service.py:133`). Currently non-impactful but a future risk.
- **Error masking:** `test_process_data_past_gate_does_not_raise_settings_error` (`test_app.py:226-236`) catches all `Exception` with a bare `pass`, hiding operational errors behind a passing test.
- **Dead code:** 3 unused fixtures in `conftest.py` (`sample_mapping_df`, `tmp_db_path`, `db_worker_memory`) have zero consumers and are documented as "reserved for future" — but `db_worker` already covers the same use case.
- **Tautological test:** `test_null_data_files_excludes_stale` (`test_app.py:452-463`) asserts a stale file is not in `null_data_files`, but `null_data_files` is a fresh list that never contains the stale file — the assertion is always True.

## Effort Estimate (Aggregate)

| Effort Level | Count | Approximate Time |
|---|---|---|
| Trivial | 13 | < 1 hour |
| Small | 9 | 2-4 hours |
| Medium | 3 | 3-6 hours |
| **Total** | **25** | **~6-11 hours** |

*Excludes findings superseded by deletion (FINDING-05) and findings referencing tests not in the current tree (FINDING-27-30, FINDING-32-33, FINDING-37).*
