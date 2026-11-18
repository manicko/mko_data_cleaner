# `examples/02-clean-data` — bootstrap and cleaning scripts

PowerShell workflow for preparing a Windows environment and running `mko_data_cleaner` cleaning jobs from local raw data and a dictionary file. The scripts are example-only helpers and do not modify `src/mko_data_cleaner/**`, package behavior, or existing tests.

## Prerequisites

- Windows 11 (scripts target Windows/NTFS behavior).
- PowerShell 5.1+ or PowerShell 7+.
- Internet access for Git/package installation and updates.
- For first-time bootstrap: `winget` (Windows Package Manager / App Installer) is required when Git must be installed or updated by Script 1.
- A usable package toolchain is required for Script 1 package installation: `uv`, or `pipx`, or `py`/`python` with `pip` and Python >= 3.12 (detected in that priority order).
- Run commands from this folder (`examples/02-clean-data`) so relative script paths and the local state/staging files resolve as expected.

## Script 1 — `01-init-environment.ps1`

Bootstraps the local environment in three idempotent steps.

### What it does

1. **Git (detect/install/update via `winget`)**
   - Detects `git` in the current session.
   - If Git is missing, attempts installation via `winget` (`--id Git.Git -e --source winget`, silent/non-interactive, agreements accepted).
   - If Git is already installed, attempts a `winget upgrade` for `Git.Git` (best effort; benign/no-update states are tolerated when `git` remains usable).
   - Refreshes the process `PATH` from Machine/User environment and re-probes before deciding outcome.

2. **`mko_data_cleaner` install/update**
   - Requires `git` to be usable in the current session.
   - Detects a fully satisfiable toolchain in priority order: `uv` -> `pipx` -> `py`/`python` with `pip` and Python >= 3.12. If none is available, the script fails with an actionable error.
   - Installs/updates **latest default-branch HEAD** from `https://github.com/manicko/mko_data_cleaner.git` on each run using a forced-refresh strategy appropriate to the selected tool:
     - `uv`: `uv pip install --python <target> --upgrade --force-reinstall --no-cache-dir git+https://github.com/manicko/mko_data_cleaner.git`
     - `pipx`: `pipx install --force --python <target> git+https://github.com/manicko/mko_data_cleaner.git`
     - `pip`: `<python> -m pip install --upgrade --force-reinstall --no-cache-dir git+https://github.com/manicko/mko_data_cleaner.git`
   - Reports selected toolchain, install source/ref policy, target/verified interpreter, installed version when discoverable, and source commit when discoverable from environment metadata (best effort).

3. **Default user settings initialization**
   - Initializes default platformdirs user settings for `mko_data_cleaner` **without overwrite** by calling `initialize_settings(force=False)` via the resolved target Python interpreter.
   - Reports the actual settings path and whether `app_config.json5` / `log_config.json5` were created or already present.

### First-time Git restart behavior

- If Git is newly installed but `git` is not available in the current session after install (expected `PATH` refresh case), Script 1 **stops further steps**, does **not** claim full success, and exits with dedicated code **`301`** (restart required).
- Action: close the terminal, open a new terminal, and rerun `.\01-init-environment.ps1`.

### Exit codes and diagnostics

- `0` — success (all required steps completed).
- `301` — restart required after first-time Git install (not full success).
- `1` — failure (actionable error reported with failed step/cause and next action).

Diagnostics reported (best effort): Git detection/version and winget outcomes, selected toolchain and target interpreter, package install source/ref policy, installed package version and source commit when discoverable, and actual default settings path/status. Script 1 always pauses before closing. It does **not** create or update `last_run_state.txt`.

## Script 2 — `02-run-cleaning.ps1`

Interactive cleaning workflow with saved-repeat support.

### Environment and settings preflight

- Resolves a Python interpreter/environment capable of running `mko_data_cleaner` (prefers a valid saved interpreter on repeat; otherwise probes candidates such as `py -3.12/-3`, `python`, `python3` for `sys.executable` and package importability/version).
- If no usable environment is found, fails with an actionable error suggesting running Script 1.
- Verifies default platformdirs settings via the helper driver; if missing/incomplete, performs **safe auto-initialization** (`initialize_settings`, no overwrite/force) and reports the action and actual settings path. If settings remain unusable, fails with an actionable error.

### State detection and run flows

- State file: `last_run_state.txt` in this folder (fixed name).
- **Valid saved state found:** menu offers `(1) Repeat previous cleaning` or `(2) New cleaning`.
- **No or invalid state:** explains the condition (if applicable) and proceeds to the new-cleaning flow.

### New-cleaning flow

1. Prompts for:
   - raw data location (report folder containing `raw_data`, or folder with raw-data contents),
   - dictionary file location.
2. Validates inputs (dictionary must be an existing file).
3. Normalizes layout (auto-detected):
   - **Report-dir mode:** if the selected raw-data path contains a `raw_data\` subfolder, the selection is treated as the resolved report directory.
   - **Raw-data-contents mode:** if `raw_data\` is not present, Script 2 uses staging report directory `.staging\default\` and creates/updates an NTFS junction `.staging\default\raw_data` pointing to the selected contents folder. **No raw-data copy is performed by default.**
4. **Junction safety:** if `.staging\default\raw_data` already exists as a reparse point, the link is removed and recreated safely; if it exists as a non-reparse directory/file, Script 2 refuses to replace it automatically and fails with an actionable error. Junction creation/removal failures are reported explicitly (NTFS/permissions/lock issues).
5. Runs config/input preflight (best effort, via helper driver): reads effective settings/config and checks that the configured import folder contains files matching the configured data extension; obvious incompatibilities fail clearly.
6. Prompts for explicit export/interaction policy and persists the choices for repeat:
   - `Clear export folder? (y/n)` -> `ClearExportFolder`
   - `Force/non-interactive confirmations? (y/n)` -> `Force`
7. Executes cleaning through the helper driver (`02-run-cleaning.driver.py`) using the resolved absolute Python interpreter and `mko_data_cleaner.app.process_data` API with explicit booleans and absolute dictionary path. The resolved deterministic launch command (module form, absolute interpreter and paths, explicit `--clear-export-folder/--no-clear-export-folder` and `--force/--no-force`) is displayed for transparency/copy.
8. On success, shows a summary (resolved report/import/export/db/null-data outputs and available counts/timings/`ReportResult` fields, plus package version when available). Then asks: `Save this run for repeat? (y/n)`.

### Repeat-previous-cleaning flow

- Loads and revalidates structured state fields (schema/mode/booleans/paths/interpreter usability). If required values are invalid or missing, Script 2 reports the issue and falls back to the new-cleaning flow.
- Displays saved paths/flags and saved launch command.
- Re-resolves the interpreter (preferring the saved one), rechecks settings (safe auto-init if needed), revalidates dictionary and raw-data selection/mode, refreshes staging/junction deterministically for raw-data-contents mode, reruns input preflight, and executes using persisted explicit flags.
- Does not rewrite state on repeat completion except as designed by script behavior (no silent state rewrite on successful repeat).

### Helper driver

`02-run-cleaning.driver.py` is a minimal colocated helper external to the package/module. Script 2 uses it for environment/settings/config preflight, safe settings initialization, and API-based cleaning execution with structured JSON result/error capture (including `ReportResult` fields and typed error details when available).

### Exit behavior

- `0` on success, `1` on failure.
- Console remains open on success and failure (pause before close).
- Errors are reported as explicit, actionable English messages (failed step/cause/next action). Core `mko_data_cleaner` errors/messages may appear interleaved and may be non-English.

## `last_run_state.txt`

- Fixed file name in this folder: `last_run_state.txt`.
- Created or updated **only** after a user-confirmed save following a successful run.
- UTF-8 key-value text, `SchemaVersion=1`.
- Structured fields are authoritative for repeat; `LaunchCommand` is stored for transparency/copy.

| Field | Meaning |
|---|---|
| `SchemaVersion` | State schema version (`1`). |
| `SavedAt` | Save timestamp (ISO-8601). |
| `RawDataSelection` | Absolute path selected by the user for raw data/report input. |
| `RawDataMode` | `report_dir` or `raw_data_contents` (layout mode used). |
| `ResolvedReportDir` | Absolute report/staging directory passed to processing. |
| `DictPath` | Absolute dictionary file path. |
| `ClearExportFolder` | Persisted explicit choice (`true`/`false`). |
| `Force` | Persisted explicit choice (`true`/`false`). |
| `SettingsMode` | Settings mode used (`default_platformdirs`). |
| `PythonExecutable` | Absolute Python interpreter path resolved for execution. |
| `LaunchCommand` | Exact resolved one-line command recorded for transparency/copy. |

Unknown keys may be ignored for forward compatibility. Repeat flow validates required values, paths, mode, and booleans and falls back to a new run when saved state is invalid or required values are missing.

## "Latest" install/update policy (D1)

Script 1 installs/updates `mko_data_cleaner` from `git+https://github.com/manicko/mko_data_cleaner.git` tracking the repository **default branch HEAD**. Each run uses a forced-refresh strategy to pick up moving refs even when package version metadata is unchanged. Script output reports the configured source/ref policy and, when discoverable, installed version and source commit.

## Limitations and notes

- Example-only workflow; **no changes to `mko_data_cleaner` module code**.
- Windows-focused; raw-data-contents staging relies on NTFS directory junction behavior under `.staging\default\`.
- Depends on local environment/tooling/network/permissions (winget, Git, Python/toolchain, filesystem access, GitHub connectivity).
- Script messages are English, but core `mko_data_cleaner` output/errors may be non-English and can appear interleaved.
- Repeat execution depends on saved absolute interpreter and path validity; if they become invalid, Script 2 reports the issue and/or falls back to a new run as applicable.
- Effective processing behavior (import folder, file extension, export/db paths, etc.) is config-driven by `mko_data_cleaner` settings; scripts perform best-effort preflight and surface core errors clearly.
- `last_run_state.txt` is not created by Script 1 and is written only on user-confirmed save after a successful Script 2 run.

## Troubleshooting

| Symptom / category | What to check / do |
|---|---|
| `winget` missing/unavailable | Install/update App Installer (Windows Package Manager) or install Git manually, then rerun Script 1. |
| No usable Python/toolchain (`uv`/`pipx`/`py`+pip, Python >= 3.12) | Install a supported toolchain and/or Python 3.12+, ensure it is available in PATH, then rerun Script 1. |
| Git installed but not usable / exit code `301` | Close the terminal, open a new terminal, and rerun Script 1 (PATH refresh required). |
| Package install/update fails | Verify internet/GitHub access, Git usability, and toolchain health; review installer output shown by Script 1 and rerun. |
| Settings missing/invalid or initialization fails | Check permissions for the user config directory; rerun Script 1 (or let Script 2 perform safe auto-init) and review reported settings path. |
| Junction creation/removal fails or staging path refused | Ensure NTFS junction support and write permissions in `examples/02-clean-data`; if `.staging\default\raw_data` exists as a real directory/file, remove or rename it manually only when safe, then rerun. |
| Missing/invalid raw-data path, dictionary path, or input files | Confirm paths exist, dictionary is a file, and input files match the configured import folder/extension; adjust selection or settings as needed. |
| Export/state issues | Review persisted `ClearExportFolder`/`Force` choices and export conflicts reported by core; re-run as a new cleaning to update saved choices. State save failures do not mark a run as saved. |

