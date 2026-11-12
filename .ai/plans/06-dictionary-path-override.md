# Execution Plan: Dictionary Path Override via CLI Flag

- **Source plan:** `.ai/problems/05_dictionary_path_override_spec.md`
- **Auditor findings:** `{code_context}` (see §1.2 below)
- **Status:** ✅ Auditor complete. Approach B (factory parameter) confirmed. PO decisions Q1=(b), Q2=(a), Q3=(a), Q4=(b) confirmed 2026-10-28.
- **Date:** 2026-10-28

---

## 1. Code Context (from Auditor)

### 1.1 What is Already Implemented

**`app.py` (165 lines)** — Current state:
- CLI `run` command (lines 54–89): accepts `report` (Path, `exists=True`), `verbose` (bool), `settings_dir` (Path | None, `exists=False`). No `dict_path` parameter.
- `get_app_service(settings_dir)` call at line 77 — no `dict_path` threading.
- Error handler (lines 79–89): `try/except Exception as e:` prints `❌ Ошибка: {e}` and exits code 1.
- `process_data` API (lines 126–145): signature `process_data(report_path, settings_dir=None)`. Calls `get_app_service(settings_dir).run_report(path)`.
- `__all__` (lines 157–164) includes `process_data`, `get_app_service`.

**`app_service.py` (346 lines)** — Current state:
- `AppService.__init__` (line 36): `__init__(self, app_paths: AppPaths, resolver: PathResolver)`. Sets `self._base_path = None` at line 40. No `dict_path` parameter.
- `dict_path` property (lines 74–76): `return Path(self.base_path, self.app_config.data_paths.dict_file)` — always uses config default.
- `run_report` (line 110): signature `run_report(self, data_path)`. The `dict_path` property is consumed at **TWO** places:
  - Line 130: `dict_path=self.dict_path` → `CSVWorker` constructor
  - Line 141: `dict_path=self.dict_path` → `MappingDict` constructor
- `get_app_service` (line 329): signature `get_app_service(settings_dir=None)`. Body: `build_app_paths`, `_require_settings`, `AppService(app_paths=app_paths, resolver=...)`, `dictConfig`, return.

**`csv_service.py` (406 lines)** — Current state:
- `CSVWorker.__init__` (lines 32–49): **already accepts `dict_path`** (line 37) and stores `self.dict_path = Path(dict_path)` (line 45). **No changes needed.**
- `get_dictionary()` (lines 203–208): checks `is_file()`; if missing, calls `get_merged_dictionary()` (folder-merge). Also calls `_validate_dictionary_columns(df)` (line 207) — **NOT mentioned in plan's data-flow simplification but verified present**.
- `get_merged_dictionary()` (lines 233–256): scans `self.dict_path.parent` for fragments, merges via `pl.concat` + `unique()` + `sort("search_column_idx")`, **writes merged result back to `self.dict_path`** (lines 245–249).

**`models.py` (207 lines)** — Current state:
- `WorkingPaths.dict_file` = `"dict/merged_dictionary.csv"` (line 67).
- `ReportResult` (lines 182–207): 11 fields, no `dict_path`. Per Q4=(b), no changes.

**`paths.py`, `__init__.py`** — No changes needed. `__init__.py` re-exports `process_data` by reference; signature changes are transparent.

### 1.2 Discrecancies Found by Auditor (Corrected)

| # | Issue | Plan claim | Reality | Impact |
|---|---|---|---|---|
| D1 | `dict_path` property consumers | "one place" (line 130 → CSVWorker only) | **Two** consumers: CSVWorker (line 130) AND MappingDict (line 141) | Low — both go through the property, so the property-modification approach handles both transparently. MappingDict uses `dict_path` for error messages in `DictionaryStructureError`, which is desirable. |
| D2 | `get_dictionary()` data flow | Shows `return pl.read_csv(...)` only | Also calls `self._validate_dictionary_columns(df)` (line 207) | Low — override file will also be validated against `col_indexes`; correct behavior, plan should note. |
| D3 | Line numbers | `get_app_service` at 327, `AppService(...)` at 338 | Lines 329 and 340 (+2 offset) | Cosmetic — conceptual references correct. |
| D4 | `get_merged_dictionary` | Lines 208–231 | Lines 233–256 (+25 offset) | Cosmetic. |
| D5 | Stale doc: `__init__.py` empty | Architecture.md:165-167 | `__init__.py` is 24 lines, re-exports work | Not in scope; documentation debt. |
| D6 | Stale doc: `process_data` returns None | current-behavior.md:272-275 | `process_data` returns `ReportResult` | Not in scope; documentation debt. |
| D7 | Fixture location | Commands list `populated_settings` in conftest | Actually defined in `test_app.py` (lines 30–38) | Low — new tests go in `test_app.py`, fixtures accessible. |

### 1.3 Selected Approach: B (Factory Parameter)

Rationale (confirmed by auditor):
1. Matches the established `--settings-dir` pattern — `settings_dir` is threaded through `get_app_service(settings_dir)` at the same layer.
2. Preserves `run_report`'s public contract (signature stays `run_report(data_path)`).
3. Property remains single source of truth — all callers (CSVWorker + MappingDict) transparently get the correct path.

---

## 2. Execution Blocks

The plan's T1–T7 decompose into two implementation blocks. PO decisions are final; no separate Researcher or Planner phase is needed since the approach is already selected and the Auditor confirmed feasibility.

### Block 1: Core Implementation (T1, T2, T3, T4)

**Scope:** Thread `dict_path` parameter through `AppService.__init__` → `get_app_service` → `dict_path` property → CLI `run` command → `process_data` API + existence validation in `run_report`.

**Files:**
- `src/mko_data_cleaner/core/app_service.py` — `__init__`, `dict_path` property, `run_report` (existence check), `get_app_service` signature + AppService construction
- `src/mko_data_cleaner/app.py` — `run` command (add Typer Option), `process_data` (add `dict_path` param), thread `dict_path` to `get_app_service`

**Changes:**
1. `AppService.__init__` (line 36): add `dict_path: Path | None = None` parameter; store `self._dict_path = dict_path` after `self._base_path = None` (line 40).
2. `dict_path` property (lines 74–76): add guard before existing return:
   ```python
   if self._dict_path is not None:
       return Path(self.base_path, self._dict_path)
   ```
3. `run_report` (after line 117 `self.base_path = data_path`): add existence validation:
   ```python
   if self._dict_path is not None and not self.dict_path.is_file():
       raise FileNotFoundError(...)
   ```
4. `get_app_service` (line 329): add `dict_path: Path | None = None`; pass to `AppService` at line 340.
5. `app.py` `run` (line 55): add `dict_path` Typer Option (`exists=False`, `file_okay=True`, `dir_okay=False`).
6. `app.py` line 77: `get_app_service(settings_dir, dict_path=dict_path)`.
7. `app.py` line 126: add `dict_path: Path | None = None` to `process_data`.
8. `app.py` line 145: `get_app_service(settings_dir, dict_path=dict_path).run_report(path)`.

**Backward compatibility:** All new parameters are optional with `None` default. When `None`, the property falls through to the existing `Path(self.base_path, self.app_config.data_paths.dict_file)`.

**Required tests:** None — covered in Block 2.

### Block 2: Tests (T5, T6) + Test fixture (T6)

**Scope:** Update existing mock test, add new tests for all override behaviors, add `external_dict_file` fixture.

**Files:**
- `tests/test_app.py` — update mock lambda (line 310), add 5 new tests
- `tests/test_app.py` (or `conftest.py`) — add `external_dict_file` fixture

**Changes:**
1. Update mock at line 310: `lambda settings_dir=None, dict_path=None: fake_service`
2. Add `external_dict_file` fixture: creates `merged_dictionary.csv` in `tmp_path` outside the report directory.
3. Add tests:
   - `test_process_data_with_dict_path_override` — external dict file is used; correct rows cleaned
   - `test_process_data_without_dict_path_uses_default` — default behavior unchanged
   - `test_process_data_dict_path_missing_raises_filenotfound` — non-existent override raises `FileNotFoundError` with resolved path in message
   - `test_run_cli_with_dict_path` — CLI `--dict-path` end-to-end via `CliRunner`
   - `test_dict_path_relative_resolves_against_report_dir` — relative path resolves against report dir (Q1)

**Acceptance criteria:** All existing tests pass + 5 new tests pass.

### Block 3: Documentation (T7)

**Scope:** Update README, current-behavior.md, architecture.md.

**Files:**
- `README.md` — document `--dict-path` CLI flag and `dict_path` param on `process_data`
- `docs/00-overview/current-behavior.md` — add `--dict-path` to CLI entry point table and report directory layout section
- `docs/00-overview/architecture.md` — update sequence diagram to show `dict_path` threading

---

## 3. Task Description (for Implementor)

See `.ai/tasks/task_021_dictionary_path_override.yaml`.

---

## 4. Definition of Done

- [x] PO decisions confirmed (Q1=(b), Q2=(a), Q3=(a), Q4=(b))
- [x] Approach B selected (factory parameter)
- [x] Auditor confirms plan matches implementation + discrepancies documented
- [ ] Core implementation complete (app_service.py + app.py)
- [ ] Tests updated + new tests added (test_app.py + fixture)
- [ ] `uv run pytest` — all tests pass
- [ ] `uv run ruff check src/ tests/` — lint clean
- [ ] `uv run basedpyright src/` — no new type errors
- [ ] Documentation updated (README, current-behavior.md, architecture.md)
- [ ] Commit created with conventional commit format
- [ ] Final validation passed
