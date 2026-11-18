# Plan 08: Local Windows run scripts for `examples/notebooks/02-clean`

## 0. Current implementation / `{code_context}` (Auditor, read-only)

Source: Auditor task `ses_e2f61a2f2ffe5CWV0PsI2n6hFD` (Step 0). No code or audit files changed.

### Relevant facts
- **Notebook to mirror:** `examples/notebooks/02-clean/02-clean_data.ipynb`
  - Imports: `Path`, `process_data`, `ReportResult`, `mko_data_cleaner.__version__`.
  - Defaults: `WORKSPACE_ROOT = Path.cwd()`, `SETTINGS_WORKSPACE = WORKSPACE_ROOT`, `REPORT_PATH = WORKSPACE_ROOT.parent / "theiss"`, `DICT_PATH = REPORT_PATH / "merged_dictionary.csv"`.
  - Required paths check: `SETTINGS_WORKSPACE/settings/app_config.json5`, `SETTINGS_WORKSPACE/settings/log_config.json5`, `REPORT_PATH`, `DICT_PATH`.
  - Processing: iterate `REPORT_PATH.iterdir()`; for each directory print folder name, call `process_data(report_dir, settings_dir=SETTINGS_WORKSPACE, dict_path=DICT_PATH)`, print result; on exception print `RUN STATUS: FAILED - no success claim.` and `f"{type(exc).__name__}: {exc}"` then continue.
  - Install cell and result-preview/display cells (`polars.read_csv`, `display`) are out of scope for scripts.
- **Settings present:** `examples/notebooks/02-clean/settings/app_config.json5`, `examples/notebooks/02-clean/settings/log_config.json5`.
- **API/behavior:** `mko_data_cleaner.process_data(report_path, settings_dir=None, dict_path=None, ...) -> ReportResult` (`src/mko_data_cleaner/app.py`). `settings_dir` resolves to `<settings_dir>/settings` for config files (`core/paths.py`, `core/app_service.py`). `dict_path` override: relative paths are resolved against the processed report directory, absolute paths preserved—pass shared dict as absolute `Path` to match notebook intent. `ReportResult` fields include `rows_loaded`, `rows_exported`, `timings`, `export_dir`, `null_data_files`, `report_dir`, `db_path`, etc. (`core/models.py`).
- **Runtime notes/risks:** core emits stdout banners/progress and logging may emit console records; script should add required CLI status/fields without `display()` previews. `REPORT_PATH`/dict/data may be absent outside this repo layout—scripts must print missing-path checks and clear per-folder/error status. Windows launchers must keep console open on success/failure and set CWD to script directory; Python fallback chain `py -3` -> `python` -> `uv run python` is required. No existing `run_report*` files in repo. Export-folder conflicts may prompt/raise depending on TTY/config (existing behavior).

## 1. User clarifications (authoritative)

- **1A:** Create scripts in `examples/notebooks/02-clean/` (next to `02-clean_data.ipynb` and `settings/`).
- **2A:** Script directory is `WORKSPACE_ROOT` / settings workspace; settings loaded from `<script_dir>/settings/`.
- **3A:** Keep notebook default behavior: `REPORT_PATH = WORKSPACE_ROOT.parent / "theiss"`, `DICT_PATH = REPORT_PATH / "merged_dictionary.csv"`, iterate each subdirectory in `REPORT_PATH` and run `process_data(...)` per folder; no install/inspection/display cells.
- **4A:** CLI-only output: resolved paths, missing-path checks, per-folder run status, key `ReportResult` fields (`rows_loaded`, `rows_exported`, `timings`, `export_dir`, `null_data_files`), and errors. No DataFrame previews/display.

## 2. Decomposition and execution order

| Block | Scope | Depends on | Risk | Required agents |
|---|---|---|---|---|
| B1 | Add `run_report.py`, `run_report.bat`, `run_report.ps1` in `examples/notebooks/02-clean/` implementing clarified behavior | Step 0 context (done) | Low–medium (Windows launcher edge cases, path/data assumptions, output clarity) | **Implementor** (single). Auditor/Researcher/Planner/Validator(plan) not required: solution path is specified, API/constraints known, scope is small and localized. |
| B2 | Documentation updates affected by implementation (if any), per `docs/00-overview/doc-maintenance-rules.md` | B1 | Low | **Doc-specialist** (conditional/minimal review+update only if affected) |
| B3 | Final validation of completed implementation | B1, B2 | Low | **Validator** |

No parallel implementation. Single Implementor at a time.

## 3. Block B1 — Implementation task

### Goal
Provide local Windows scripts in `examples/notebooks/02-clean/` to run the notebook’s 02-clean processing workflow from that folder without installation steps, with visible CLI output and a console window that remains open on success or failure.

### Out of scope
- Installation/update logic, notebook edits, core library behavior changes, tests for example scripts (unless required by existing gates), unrelated documentation, speculative redesign.

### Files / semantic units
- **Add** `examples/notebooks/02-clean/run_report.py` (module-level script; `main()` entrypoint and `if __name__ == "__main__"` guard recommended).
- **Add** `examples/notebooks/02-clean/run_report.bat`.
- **Add** `examples/notebooks/02-clean/run_report.ps1`.

### Implementation requirements

#### `run_report.py`
- Use script directory as root: `WORKSPACE_ROOT = Path(__file__).resolve().parent`; `SETTINGS_WORKSPACE = WORKSPACE_ROOT`.
- Derive defaults exactly: `REPORT_PATH = WORKSPACE_ROOT.parent / "theiss"`; `DICT_PATH = REPORT_PATH / "merged_dictionary.csv"` (match notebook; pass `dict_path` as absolute `Path`).
- Print resolved paths (`WORKSPACE_ROOT`, `SETTINGS_WORKSPACE`, `REPORT_PATH`, `DICT_PATH`).
- Perform and print missing-path checks for: `SETTINGS_WORKSPACE/settings/app_config.json5`, `SETTINGS_WORKSPACE/settings/log_config.json5`, `REPORT_PATH`, `DICT_PATH` (clear `none` or list of missing paths).
- If required paths are missing, print a clear CLI error/status and exit with non-zero code without traceback noise where practical; do not claim success.
- If `REPORT_PATH` exists, iterate subdirectories in deterministic order (e.g. sorted by name) and for each:
  - print folder/run status header (folder name).
  - call `process_data(report_dir, settings_dir=SETTINGS_WORKSPACE, dict_path=DICT_PATH)`.
  - on success print key `ReportResult` fields: `rows_loaded`, `rows_exported`, `timings`, `export_dir`, `null_data_files` (and `report_dir` as useful context is acceptable).
  - on failure print `RUN STATUS: FAILED - no success claim.` and `f"{type(exc).__name__}: {exc}"`, then continue to next folder.
- Track overall outcome: non-zero exit if any folder fails or required setup/data missing; zero only when processing completed without failures (define and print final summary status).
- CLI-only: no `display()`, no DataFrame/CSV previews, no install steps. Keep logic simple/readable (English-only messages/comments if added).
- Imports: minimal (`pathlib.Path`, `sys`, `mko_data_cleaner` API/types as needed).

#### `run_report.bat`
- Set CWD to script directory (`cd /d "%~dp0"`).
- Locate and invoke Python with fallback order: `py -3` -> `python` -> `uv run python`, running `run_report.py` from the same directory.
- Keep console output visible. Do not close window automatically on success or failure (e.g. final `pause`/equivalent executed in all paths).
- Propagate/reflect failure reasonably (non-zero exit status when run fails), while still pausing.

#### `run_report.ps1`
- Set CWD to script directory (`Set-Location -LiteralPath $PSScriptRoot` or equivalent).
- Invoke Python with fallback order: `py -3` -> `python` -> `uv run python`, running `run_report.py`.
- Keep console output visible. Do not close automatically on success or failure (e.g. `Read-Host`/pause equivalent in a `finally`/all-paths manner).
- Preserve/report exit code when possible while still keeping window open.

### Acceptance criteria
- All three files exist in `examples/notebooks/02-clean/`.
- `run_report.py` implements 2A/3A/4A behavior, no install/display/preview logic, and prints required path checks/status/result fields/errors.
- Batch and PowerShell launchers set CWD to script directory, try Python fallbacks in required order, show output, and keep window open on success/failure.
- No unrelated files changed; no production code changes required.

## 4. Block B2 — Documentation (conditional)

- Review whether implementation affects docs covered by `docs/00-overview/doc-maintenance-rules.md` (notably `examples/README.md`, which currently documents the notebook flow). If affected, make minimal, accurate documentation updates for the new scripts only; otherwise record “no documentation changes required”.
- Doc-specialist to handle; commit documentation separately if changed.

## 5. Block B3 — Final validation

Validator checks: plan completeness, cross-block integration (scripts + docs), architectural/convention consistency (example-only, no core changes), regressions/risk review, relevant quality gates feasible for changed artifacts, documentation accuracy, and absence of unrelated changes.

## 6. Risks / watch items

- `../theiss` report workspace and dictionary may not exist in local environment; scripts must fail clearly without success claims.
- Library stdout/progress/log output may interleave with script output (acceptable if required info is present and no previews/display added).
- `uv run python` fallback behavior depends on environment; launchers should still attempt it last and pause on failure.
- Export-folder conflicts and per-report data validity are runtime/data concerns, not script-install concerns.
