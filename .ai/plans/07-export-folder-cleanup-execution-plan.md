# Execution Plan — Export Folder Cleanup Flags (`clear_export_folder` / `force`)

**Source spec:** `.ai/problems/06_export_folder_cleanup_spec.md`
**Audit context:** `{code_context}` auditor result (2026-10-29)
**Status:** Ready for implementation

---

## 1. Overview

Implement two configurable boolean flags — `clear_export_folder` and `force` — that
control pre-run cleanup of the tool's output directory (`clean_data/`). Both flags
are configurable via three channels: `app_config.json5` (persistent defaults), CLI
(`mko-data-cleaner run <report> --clear-export-folder --force`), and Python API
(`process_data(report, clear_export_folder=True, force=True)`).

The four-scenario truth table governs all combinations of the two flags, with
interactive vs. non-interactive terminal detection applied uniformly across CLI and
Python API.

---

## 2. Block Decomposition

| Block | Title | Spec Tasks | Dependencies | Risk |
|-------|-------|------------|--------------|------|
| B1 | Model + config + error class + threading infra | T1, T2, T3, T5, T7 | — | Low |
| B2 | CLI options + core export folder logic | T4, T6 | B1 | High |
| B3 | Tests | T8 | B2 | Medium |
| B4 | Documentation | T9 | B2 | Low |

### Dependency chain

```
B1 (infrastructure) → B2 (behavior) → B3 (tests) → B4 (docs)
```

Blocks execute sequentially. One Implementor per block (never parallel).

---

## 3. Block Details

### BLOCK 1 — Infrastructure: Model fields, config template, error class, threading, resolution

**Spec tasks:** T1, T2, T3, T5, T7

**Files to modify:**
- `src/mko_data_cleaner/core/models.py` — add two `bool` fields to `DataSettings`
- `src/mko_data_cleaner/settings/app_config.json5` — add two top-level keys
- `src/mko_data_cleaner/core/errors.py` — add `ExportFolderConflictError`
- `src/mko_data_cleaner/app.py` — thread params through `process_data()` and `get_app_service()`
- `src/mko_data_cleaner/core/app_service.py` — thread params through `AppService.__init__()`, add resolution methods

**Acceptance criteria:**
1. `DataSettings` has `clear_export_folder: bool = False` and `force: bool = False`
2. `app_config.json5` template has `"clear_export_folder": false, "force": false` as top-level keys
3. `ExportFolderConflictError` exists in `errors.py`, inherits `Exception`
4. `process_data(report_path, settings_dir=None, dict_path=None, clear_export_folder=None, force=None)` threads both params to `get_app_service()`
5. `get_app_service(settings_dir=None, dict_path=None, clear_export_folder=None, force=None)` passes params to `AppService.__init__()`
6. `AppService.__init__()` accepts and stores `clear_export_folder` and `force` as `self._clear_export_folder` / `self._force`
7. `AppService._resolved_clear_export_folder()` returns `self._clear_export_folder if not None else self.app_config.clear_export_folder`
8. `AppService._resolved_force()` returns `self._force if not None else self.app_config.force`

**No behavior changes** — this block only sets up the infrastructure. Existing tests must pass unchanged (except the mock lambda, which is deferred to B3).

```yaml
# task_template.yaml — BLOCK 1
id: b1_infrastructure
title: Add model fields, config keys, error class, override threading, resolution methods
priority: high
depends_on: []
description: >
  Add clear_export_folder and force fields to DataSettings, config template,
  error class, and thread override parameters through the call chain.
goals:
  - DataSettings has both bool fields with False defaults
  - app_config.json5 template has both keys
  - ExportFolderConflictError added to errors.py
  - process_data, get_app_service, AppService.__init__ all accept and thread params
  - Resolution methods resolve None → config value
files:
  - path: src/mko_data_cleaner/core/models.py
    targets:
      - type: class
        name: DataSettings
  - path: src/mko_data_cleaner/settings/app_config.json5
  - path: src/mko_data_cleaner/core/errors.py
  - path: src/mko_data_cleaner/app.py
    targets:
      - type: function
        name: process_data
      - type: function
        name: get_app_service
  - path: src/mko_data_cleaner/core/app_service.py
    targets:
      - type: class
        name: AppService
acceptance_criteria:
  - DataSettings has clear_export_folder: bool = False and force: bool = False
  - app_config.json5 template has both top-level keys
  - ExportFolderConflictError exists in errors.py
  - process_data and get_app_service accept clear_export_folder/force params
  - AppService.__init__ stores self._clear_export_folder / self._force
  - _resolved_clear_export_folder() and _resolved_force() methods exist
  - Existing tests pass (mock lambda update is B3)
```

---

### BLOCK 2 — Behavior: CLI options + export folder handling logic

**Spec tasks:** T4, T6

**Files to modify:**
- `src/mko_data_cleaner/app.py` — add `--clear-export-folder/--no-clear-export-folder` and `--force/--no-force` Typer options to `run` command
- `src/mko_data_cleaner/core/app_service.py` — implement `_handle_export_folder()` on `AppService`, wire into `run_report()` between `ensure_dir(self.export_path)` (line 136) and CSVWorker creation (line 143)

**Acceptance criteria:**
1. CLI `run` command accepts `--clear-export-folder/--no-clear-export-folder` and `--force/--no-force`, passes to `get_app_service()`
2. `_handle_export_folder()` implements the four-scenario truth table from spec §5.2
3. Empty/missing folder → no warning, proceed (R9)
4. Same-suffix file detection uses `*<export_suffix>` where `<export_suffix>` = `CSVWorker.get_files_suffix(export_settings.to_csv.compression)` (R5, never hardcoded)
5. Clear pattern uses `<table_name>_*.<export_suffix>` where table_name = `database_settings.table_name` (R4, never hardcoded)
6. Interactive detection: `sys.stdout.isatty()` + `'ipykernel' in sys.modules` guard (§5.5)
7. Declining prompt raises `ExportFolderConflictError` (R8)
8. Non-interactive abort in scenario 1 raises `ExportFolderConflictError` with remediation message (R6)
9. All user-facing messages are English (house rule)

```yaml
# task_template.yaml — BLOCK 2
id: b2_behavior
title: Add CLI options and implement export folder handling truth table
priority: high
depends_on:
  - b1_infrastructure
description: >
  Add CLI Typer options for the two flags and implement the _handle_export_folder
  method that enacts the four-scenario truth table.
goals:
  - CLI run command accepts --clear-export-folder/--no-clear-export-folder and --force/--no-force
  - _handle_export_folder() implements the four-scenario truth table
  - File pattern detection derives suffix from CSVWorker.get_files_suffix()
  - Interactive detection via isatty() + ipykernel guard
  - Decline/abort raises ExportFolderConflictError
files:
  - path: src/mko_data_cleaner/app.py
    targets:
      - type: function
        name: run
  - path: src/mko_data_cleaner/core/app_service.py
    targets:
      - type: class
        name: AppService
      - type: method
        name: run_report
acceptance_criteria:
  - CLI accepts both flag pairs with None default (falls back to config)
  - _handle_export_folder() handles all 4 truth-table scenarios
  - Empty/missing folder → proceed silently
  - Suffix derived from CSVWorker.get_files_suffix(), never hardcoded
  - Clear pattern uses database_settings.table_name, never hardcoded
  - Interactive detection via isatty() + ipykernel
  - Decline/abort raises ExportFolderConflictError
  - All messages in English
```

---

### BLOCK 3 — Tests

**Spec task:** T8

**Files to modify:**
- `tests/test_app.py` — update mock lambda (line 311-313), add scenario tests

**Acceptance criteria:**
1. Mock lambda updated to accept `clear_export_folder=None, force=None`
2. Tests cover all 4 truth-table cells (CLI and Python API)
3. Tests cover override precedence (None → config, explicit → override)
4. Test non-interactive abort (scenario 1)
5. Test empty/missing folder (no warning)
6. Test table_name/compression variations
7. All tests pass: `uv run pytest`

```yaml
# task_template.yaml — BLOCK 3
id: b3_tests
title: Update mock and add tests for all scenarios
priority: high
depends_on:
  - b2_behavior
description: >
  Update the test mock lambda signature and add comprehensive tests covering
  all four truth-table scenarios, override precedence, and edge cases.
goals:
  - Mock lambda accepts clear_export_folder and force params
  - All 4 truth-table scenarios tested
  - Override precedence tested
  - Edge cases: empty/missing folder, non-interactive abort, table_name/compression variations
files:
  - path: tests/test_app.py
    targets:
      - type: function
        name: test_process_data_returns_report_result
acceptance_criteria:
  - Mock lambda updated with new params
  - All 4 scenarios covered (CLI + API)
  - Override precedence tested
  - Non-interactive abort tested
  - Empty/missing folder tested
  - table_name/compression variations tested
  - uv run pytest passes
```

---

### BLOCK 4 — Documentation

**Spec task:** T9

**Files to modify:**
- `docs/00-overview/current-behavior.md` — entry points table, config table, clean_data section
- `docs/00-overview/architecture.md` — error handling section, component responsibilities (correct stale D-8.1 reference)
- `README.md` — if flags documented there
- `docs/00-overview/open-questions.md` — mark D-6, D-8.1 as stale (auditor findings)

**Acceptance criteria:**
1. `current-behavior.md` documents `--clear-export-folder` / `--force` CLI flags and `process_data()` params
2. `architecture.md` updated with error handling entry, stale D-8.1 corrected
3. `open-questions.md` D-6/D-8.1 marked resolved/stale with evidence

```yaml
# task_template.yaml — BLOCK 4
id: b4_docs
title: Update documentation for export folder cleanup flags
priority: medium
depends_on:
  - b3_tests
description: >
  Update documentation to reflect the new flags, CLI options, and behavior.
  Correct stale auditor findings in open-questions.md.
goals:
  - Document flags in current-behavior.md and architecture.md
  - Correct stale D-8.1 and D-6 references
files:
  - path: docs/00-overview/current-behavior.md
  - path: docs/00-overview/architecture.md
  - path: docs/00-overview/open-questions.md
acceptance_criteria:
  - CLI flags documented in entry points table
  - process_data params documented
  - clean_data behavior section updated
  - Stale doc references corrected
```

---

## 4. Key Technical References (from audit)

- **Insertion point:** `run_report()` line 136 (`ensure_dir(export_path)`) → line 143 (`CSVWorker` creation). Lines 138–141 are local variable init (`rows_loaded`, `rows_exported`, `null_data_files`, `timings`).
- **Suffix source of truth:** `CSVWorker.get_files_suffix(export_settings.to_csv.compression)` returns `.csv.gz` for `"gzip"` (csv_service.py:267-288)
- **Threading pattern:** Follow spec 05 `dict_path` pattern — already implemented in current codebase (dict_path → `_dict_path` → property)
- **Existing cleanup pattern:** `DBWorker._delete_base_files()` uses `self.db_file.parent.glob(self.db_file.name + "*")` (db_service.py:827-828)
- **Existing Jupyter guard:** `hasattr(sys.stdout, "reconfigure")` at app.py:17-24 (not ipykernel check; new code uses `'ipykernel' in sys.modules`)
- **Mock lambda:** `tests/test_app.py:311-313` needs `clear_export_folder=None, force=None` params
- **`export_path` property:** `app_service.py:76-78` → `Path(self.base_path, self.app_config.data_paths.export_folder)`
