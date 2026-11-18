# 07 — PowerShell Bootstrap and Cleaning Scripts (`examples/02-clean-data`)

- **Status:** Complete analytical specification (PO decisions confirmed). Ready for implementation planning/tasking.
- **Date:** 2026-11-18
- **PO confirmation:** 2026-11-18 — **Console/UI language: English. All other recommendations (Q1–Q8) confirmed as proposed.**
- **Source request:** User request (2026-11-18, Russian) — add two PowerShell scripts in `examples/02-clean-data` without changing current module code.
- **Hard constraint:** **No changes to current module code** (`src/mko_data_cleaner/**`, package metadata/runtime behavior, existing tests). Scripts must work with the existing CLI/API contract as-is.

---

## 1. Problem Statement

End users on Windows need a simple, repeatable way to (1) prepare a machine to run `mko_data_cleaner` and (2) execute data-cleaning runs against local raw data and a dictionary without using a notebook or editing code. Today this requires manual steps (install Git/Python tooling, install/update the package, initialize settings, understand the report-directory layout, and craft CLI/API calls). `examples/02-clean-data/` is currently empty and there is no user-facing bootstrap/cleaning workflow there.

**Requested outcome:** two PowerShell scripts in `examples/02-clean-data/`:

1. **Script 1 — Initialization (`01-init-environment.ps1`):** install/update Git, install/update the latest `mko_data_cleaner`, and initialize/copy default settings without overwriting existing user settings. If Git is installed for the first time and unavailable in the current session (PATH not refreshed), stop and instruct the user to restart the script in a new terminal/session.
2. **Script 2 — Cleaning/export run (`02-run-cleaning.ps1`):** interactively collect raw-data and dictionary paths on first run, run cleaning, on success offer to save paths and persist a reusable launch string/state to a neighboring text file in the same folder; on repeat runs offer to repeat the previous cleaning (saved data/dictionary) or start a new cleaning.

**Cross-cutting UX:** both scripts must keep the console open after execution, clearly report success, and explicitly report user-facing errors when any step fails.

---

## 2. Business Goal, Scope, Constraints, Assumptions

### 2.1 Business goal

- Reduce setup and operational friction for non-developer Windows 11 users.
- Make cleaning runs reproducible/repeatable from saved parameters.
- Provide clear, actionable failure feedback without requiring users to inspect source code or module internals.

### 2.2 Scope (in)

| Area | In scope |
|---|---|
| Primary deliverables | `examples/02-clean-data/01-init-environment.ps1` and `examples/02-clean-data/02-run-cleaning.ps1` (English filenames, English console messages). |
| Allowed colocated helpers (minimal, only if necessary) | Small helper assets in `examples/02-clean-data/` required for reliable operation/documentation (e.g. a Python driver invoked by Script 2, and/or a short `README.md`). **No `.bat` wrappers required** unless later requested. **No changes to module code.** |
| Platform | Windows 11, PowerShell (Windows PowerShell 5.1 and/or PowerShell 7+ where feasible; implementation should prefer broadly compatible patterns). |
| Script 1 | Preflight; Git detect/install/update with first-install restart-stop gate; `mko_data_cleaner` install/update (latest per D1); default user-settings initialization without overwrite; visible success/failure; no auto-close. |
| Script 2 | First-run prompts (raw-data location, dictionary location), path/layout normalization, settings preflight, deterministic cleaning execution, post-success save prompt and persistence of launch string + structured state, repeat-run menu (repeat previous / new), visible success/failure; no auto-close. |
| Compatibility target | Current module behavior: `mko_data_cleaner` v0.2.0 API/CLI (`process_data`, `initialize_settings`, `mko-data-cleaner init/run`), default platformdirs user settings, report-dir + `data_paths` layout, export conflict semantics. |

### 2.3 Out of scope

- Any modification to `mko_data_cleaner` module source, public API, CLI behavior, defaults, packaging metadata, or existing tests.
- Building/publishing releases, tag/PyPI management, or changing upstream package behavior.
- Folder-merge dictionary fragments (single dictionary file path only).
- Multi-profile run management UI beyond the confirmed single saved state.
- Automated modification of user data, dictionary contents, or customized settings files.
- Non-Windows platforms, GUI, notebook pipeline authoring.
- Offline bootstrap beyond clear prerequisite errors/remediation.

### 2.4 Binding constraints

- **Module code immutable:** no edits under `src/**`.
- **Existing CLI/API contract authoritative:** scripts adapt to current behavior (report dir must exist and contain `<import_folder>` data per config; `dict_path` override supported; settings must be initialized; export handling governed by current flags/interaction rules).
- **No hidden destructive behavior:** deletions/overwrites (e.g. staging junction recreation inside staging area, export clearing when explicitly chosen/persisted) must be explicit, explainable, and aligned with current tool semantics.
- **English:** repository artifacts and script console messages are English (D9).

### 2.5 Confirmed assumptions

| ID | Assumption | Basis | Impact/mitigation |
|---|---|---|---|
| A1 | Target users are non-developers on Windows 11 with internet access but may lack Git/Python/uv/pipx. | Request + R1. | Script 1 must provide explicit, actionable prerequisite errors (D2/D3). |
| A2 | "Latest version" = latest commit on repository default branch from `https://github.com/manicko/mko_data_cleaner.git`, refreshed on update even when version metadata is unchanged. | D1, R1. | Document source/ref and show installed version/commit when feasible (OQ5 resolved). |
| A3 | Default user settings = platformdirs user config (`user_config_dir('mko_data_cleaner')/settings`; typically `%LOCALAPPDATA%\mko_data_cleaner\mko_data_cleaner\settings` on Windows). Script 1 initializes there without overwrite; Script 2 uses it by default. | D5, R1/R2, `core/paths.py`. | Script 2 may auto-run safe init if missing (OQ3 resolved). |
| A4 | Minimal colocated helper files (e.g. Python driver, README) in `examples/02-clean-data/` are allowed if necessary for reliability; module code remains unchanged. | Confirmed scope clarification. | Keep helpers minimal and documented. |
| A5 | "Neighboring text file in the same folder" = one fixed-name state file in `examples/02-clean-data/`. | D6. | Repeat-run discovery is deterministic. |

---

## 3. Current-State Facts (code-verified, authoritative)

| Fact | Evidence | Implication |
|---|---|---|
| `process_data(report_path, ...)` requires existing directory; raw files read from `<report>/<data_paths.import_folder>` (default `raw_data`), non-recursive, extension from config (template default `gz`). | `app.py:177-179`, `app_service.py:206-208,278`, `csv_service.py`; `settings/app_config.json5` | Script 2 must provide a compliant report layout or fail clearly. |
| Dictionary: default `<report>/<data_paths.dict_file>`; override `dict_path/--dict-path`; relative resolved against report dir, absolute used as-is; missing override raises detailed `FileNotFoundError`. | `app_service.py:214-217,259-267`, `app.py:70-80,155` | Persist/pass absolute dictionary paths. |
| Settings gate: requires `app_config.json5` and `log_config.json5`; missing raises `SettingsNotInitializedError`. `initialize_settings(force=False)`/`init` copies templates without overwriting existing files. | `app_service.py:475-516`, `init_service.py`, `errors.py`, `app.py:34-52,188-190` | Script 1 initializes safely; Script 2 preflights and may safe-init. |
| Export conflicts depend on `clear_export_folder`/`force` and interactivity; non-interactive + no clear + no force can abort (`ExportFolderConflictError`); prompts via `typer.confirm` in CLI. | `app_service.py:56-179` | Repeat runs must use explicit, persisted flags. |
| Console script `mko-data-cleaner = mko_data_cleaner.app:app`; no package `__main__`. `process_data` returns `ReportResult`. | `pyproject.toml:44-45`, `app.py:152-205`, `models.py:184-209` | Reliable invocation via resolved interpreter/module or absolute script path; API enables rich success reporting. |
| Existing examples keep window open and propagate exit codes. | `examples/notebooks/02-clean/run_report.ps1`, `run_report.bat` | Follow same UX convention. |

---

## 4. Product Owner Decisions (confirmed)

| ID | Decision | Resolution |
|---|---|---|
| **D1 (Q1)** | "Latest" `mko_data_cleaner` version source/ref | **Confirmed (a):** install/update from `git+https://github.com/manicko/mko_data_cleaner.git` **default branch HEAD** each run, with an update strategy that refreshes moving refs even if package version metadata is unchanged. |
| **D2 (Q2)** | Missing Python (3.12+)/package toolchain | **Confirmed (a):** detect supported toolchains in priority order (e.g. `uv` → `pipx` → `py -3`/`python` with pip, Python >= 3.12) and use first available; if none found, **fail with explicit, actionable error** and stop (no silent partial success). |
| **D3 (Q3)** | `winget`/App Installer unavailable for Git | **Confirmed (a):** fail fast with clear, actionable error/remediation (install/update App Installer or install Git manually) and stop; no silent fallback. |
| **D4 (Q4)** | Meaning of "folder with raw_data" | **Confirmed (c):** auto-detect/support both: if selection contains `raw_data/` subfolder, treat as report folder; otherwise treat selection as raw-data contents and create/use a staging report dir (preferred no-copy method, e.g. NTFS directory junction) so `process_data` sees `<report>/raw_data/...`; clearly report chosen mode. |
| **D5 (Q5)** | Run scope and settings source | **Confirmed (a/a):** run **one selected target per execution**; use **default platformdirs user settings** initialized by Script 1 (no custom settings-dir prompt). |
| **D6 (Q6)** | Saved state file location/cardinality | **Confirmed (a):** **one** fixed-name state file in `examples/02-clean-data/` (same folder as scripts), containing saved paths and launch string/state. |
| **D7 (Q7)** | Saved "launch string" content and repeat execution | **Confirmed (a):** save both a human-readable, re-executable launch string (quoted paths/flags) and structured key-value fields in the same text file; repeat using validated saved structured values (launch string retained for visibility/copy). |
| **D8 (Q8)** | Deterministic export/interaction behavior for repeat runs | **Confirmed (a):** on **new** runs collect/persist explicit export/interaction choices (e.g. `clear_export_folder` yes/no, `force` yes/no as applicable) and reuse them on **repeat** for deterministic behavior. |
| **D9 (Q9)** | Filenames and console message language | **Confirmed:** `01-init-environment.ps1`, `02-run-cleaning.ps1`; **English** console messages and repository text artifacts. |

### 4.1 Resolved implementation defaults (derived from confirmed decisions)

| ID | Topic | Resolution | Rationale |
|---|---|---|---|
| **OQ2** | Staging directory location/reuse (for D4 junction mode) | Create staging under `examples\02-clean-data\.staging\default\` (fixed, inspectable, inside scripts folder). Inside it maintain the report layout required by config (`raw_data` junction pointing to selected raw-data contents, plus tool-created `clean_data`/`data_base` as applicable). Reuse between runs; (re)create/update the `raw_data` junction safely when the selected source changes. Do not delete user source data; do not copy raw data by default. If junction creation fails (permissions/FS), fail with explicit, actionable error. | Predictable, non-scattering, compatible with D6 single profile; avoids large copies. |
| **OQ3** | Script 2 behavior when default settings are missing | Script 2 performs settings preflight; if missing, it **automatically runs safe initialization** (`initialize_settings()` / equivalent, no `--force`) to default platformdirs and reports this action. If initialization fails, stop with actionable error. | Safe (no overwrite), aligns with D5/A3, reduces friction. |
| **OQ4** | Launch string / invocation form | Persist the **resolved, absolute invocation actually used**. Preferred form: `& "<absolute path to python.exe>" -m mko_data_cleaner.app run "<resolved_report_dir>" --dict-path "<absolute_dict_path>" [--clear-export-folder/--no-clear-export-folder] [--force/--no-force]` (or absolute console-script path if reliably resolved). Structured fields are the source of truth for repeat execution; launch string is stored for transparency/copy. Avoid dependence on PATH. | Deterministic, repeatable in the same environment (R2). |
| **OQ5** | Version/commit in success output | Script 1 and Script 2 should display installed `mko_data_cleaner` version when discoverable (e.g. `mko-data-cleaner --version` / `importlib.metadata.version` in selected interpreter); include source ref/commit in output when easily discoverable from installer metadata/environment, otherwise at least report the configured install source/ref policy (D1). | Diagnostics and "latest" traceability. |
| **OQ6** | `.bat` wrappers | Not included (not requested). Scripts are `.ps1` (+ minimal helpers/readme only if necessary). | Scope control. |

---

## 5. Gray Areas — Resolution Status

| ID | Gray area | Resolution |
|---|---|---|
| G1 | Meaning of "folder with raw_data" | **Resolved by D4/OQ2:** auto-detect report dir vs raw-data contents; staging+junction in `.staging\default` when needed. |
| G2 | Location/identity of neighboring state file | **Resolved by D6:** one fixed-name file in `examples/02-clean-data/` (implementation: `last_run_state.txt`, defined in §7). |
| G3 | Content/semantics of saved launch string | **Resolved by D7/OQ4:** structured fields + resolved re-executable launch string; repeat from validated fields. |
| G4 | "Latest version" source/ref | **Resolved by D1:** default branch HEAD from git with forced refresh. |
| G5 | Missing winget/Python toolchain policy | **Resolved by D2/D3:** explicit actionable failures, no silent fallback/bootstrap. |
| G6 | Script 2 run scope and settings source | **Resolved by D5/OQ3:** single target, default platformdirs settings, safe auto-init if missing. |
| G7 | Export conflicts/prompts determinism | **Resolved by D8:** persist explicit flag choices and reuse on repeat. |
| G8 | Dictionary input/validation expectations | **Resolved:** prompt for dictionary **file path**; validate existence and pass as absolute `dict_path/--dict-path`; on failure surface explicit actionable errors (including core-reported path details). |
| G9 | Filenames and console message language | **Resolved by D9:** English filenames/messages. |
| G10 | Helper files alongside `.ps1` | **Resolved by A4:** minimal helpers allowed only if necessary; module code unchanged. |

---

## 6. Research Summary (sufficient and applied)

### R1 — Git install/update and package bootstrap (Windows 11)
- **Git via winget:** `--id Git.Git -e --source winget` with `--silent --disable-interactivity --accept-package-agreements --accept-source-agreements`. First-time install does not refresh PATH in current session → mandatory **stop + restart-script instruction** when `git` was absent/unresolved after install. Winget/App Installer may be unavailable or require elevation → explicit error (D3).
- **"Latest" from git (D1):** VCS install without ref tracks default-branch HEAD; moving same-version commits require forced refresh (tool-appropriate `--force-reinstall` / reinstall semantics).
- **Package install/update (D2):** detect `uv` → `pipx` → `py -3`/`python`+pip (Python >= 3.12); install/update from `git+https://github.com/manicko/mko_data_cleaner.git`; if none available, fail with actionable remediation.
- **Settings init:** `initialize_settings()` / `mko-data-cleaner init` without `--force` creates missing defaults in platformdirs without overwrite.

**Selected approach (Script 1):** winget-first Git flow + mandatory restart gate + tool-detected package install (Approach A).

### R2 — Mapping Script 2 to current module behavior
- **Layout:** `process_data` requires `<report_dir>/<import_folder>` (default `raw_data`). D4 resolves via auto-detect: report dir if `raw_data/` exists, else staging report dir with NTFS junction `raw_data -> selected contents` under `.staging\default` (no copy).
- **Dictionary:** pass absolute path via `dict_path/--dict-path`.
- **Invocation:** resolve absolute interpreter/module (or console script) and avoid PATH dependence; prefer API-capable driver/helper or `python -m mko_data_cleaner.app` for reliability; capture `ReportResult` for success reporting when practical.
- **State:** no built-in profiles; script owns fixed-name state file with structured fields + launch string (D6/D7).
- **Constraints:** settings preflight/safe-init, config-driven extensions/non-recursive discovery, deterministic export flags (D8), window stays open.

**Selected approach (Script 2):** PowerShell wrapper (+ minimal helper only if necessary) with auto-detect layout, absolute paths, deterministic persisted flags, and explicit UX.

**Research adequacy:** R1/R2 are sufficient for the confirmed options; no additional research required.

---

## 7. Design Contract (implementation-ready)

### 7.1 File and artifact layout

- `examples/02-clean-data/01-init-environment.ps1` — **Script 1** (required).
- `examples/02-clean-data/02-run-cleaning.ps1` — **Script 2** (required).
- `examples/02-clean-data/last_run_state.txt` — saved state file (created/updated by Script 2 on user-confirmed save; read on repeat).
- `examples/02-clean-data/.staging/default/` — staging report workspace used only when raw-data-contents mode is selected (created/managed by Script 2 as needed).
- Optional (only if necessary): `examples/02-clean-data/02-run-cleaning.driver.py` (or similarly named helper) and/or `examples/02-clean-data/README.md`.

### 7.2 Script 1 — `01-init-environment.ps1` behavior contract

1. **Preflight & UI:** clear step headers; English messages; track overall success/failure; always pause before exit and return non-zero on failure.
2. **Git step:**
   - Detect `git` in current session (`Get-Command git` / equivalent).
   - If `winget` unavailable when install/update is required: **fail fast** with actionable remediation (D3).
   - If Git **not installed**: attempt install via winget (`Git.Git`, silent/non-interactive/accepted agreements). After install, if `git` is not available in current session (expected first-time PATH case), **stop further steps**, clearly instruct user to close and reopen terminal and run Script 1 again, and exit with a dedicated non-zero "restart required" status (do **not** claim full success).
   - If Git **installed**: attempt upgrade to latest available via winget (best effort per winget semantics), report outcome; continue when `git` remains usable.
3. **Package step (`mko_data_cleaner`):**
   - Require Git usable in session for VCS install; if not, fail with actionable error.
   - Detect toolchain in priority order `uv` → `pipx` → `py -3`/`python` with pip and Python >= 3.12 (D2). If none: fail with explicit prerequisites/remediation.
   - Install/update latest default-branch HEAD from `https://github.com/manicko/mko_data_cleaner.git` using a forced-refresh strategy appropriate to chosen tool so moving refs update even when version metadata unchanged (D1).
   - Report chosen toolchain, install source/ref policy, and installed version when discoverable.
4. **Settings step:**
   - Initialize default platformdirs user settings without overwrite (`initialize_settings()` / `mko-data-cleaner init` without `--force`) (D1–D5 context).
   - Report settings path and whether files were created or already present.
5. **Success:** print clear summary (Git status, package status/version, settings path) and wait before close (exit 0).

**Idempotency:** reruns must be safe (no settings overwrite; Git/package steps converge to installed/updated state where possible).

### 7.3 Script 2 — `02-run-cleaning.ps1` behavior contract

1. **Preflight & UI:** English messages; always pause before exit; explicit actionable errors with failed step/cause/next action; non-zero exit on failure.
2. **Environment/settings preflight:**
   - Resolve a working Python/interpreter environment capable of running `mko_data_cleaner` (consistent with installed toolchains); if unavailable, fail with actionable error (suggest running Script 1).
   - Verify default platformdirs settings; if missing, run safe auto-init without overwrite and report (OQ3). If still missing/unusable, fail.
3. **State detection (D6):** look for `examples\02-clean-data\last_run_state.txt`.
   - **If valid state exists:** offer menu: `(1) Repeat previous cleaning` / `(2) New cleaning`.
   - **If no/invalid state:** proceed to new-cleaning flow (and explain invalid state if applicable).
4. **New cleaning flow:**
   - Prompt for **raw data location** and **dictionary file location**.
   - Validate paths exist (dictionary must be a file).
   - **Layout normalization (D4, OQ2):**
     - If selected raw-data path contains `raw_data\` subfolder → treat selected path as `report_dir`.
     - Else treat selected path as raw-data contents: ensure `.staging\default\` report layout exists and set/update `raw_data` junction to selected path; use `.staging\default` as `report_dir`. On junction failure, fail with explicit actionable error (no copy fallback).
   - Detect/validate input data presence against configured `import_folder` and `data_file_settings.extension` when feasible and warn/fail clearly if obviously incompatible.
   - Prompt for explicit export/interaction policy for this run and persist for repeat (D8): at minimum collect `Clear export folder? (y/n)` and `Force/non-interactive confirmations? (y/n)` (or equivalent) mapped to `clear_export_folder` and `force`.
   - Execute cleaning via resolved absolute invocation (OQ4), passing absolute `dict_path` and explicit `clear_export_folder/force` values.
   - On success, display summary (resolved report/import/export/db/null-data outputs and counts/timings when available, plus version when discoverable). Then ask: `Save this run for repeat? (y/n)`.
     - If yes: write `last_run_state.txt` with structured fields and launch string (D7, §7.4).
5. **Repeat previous cleaning flow:**
   - Load and validate structured fields from state; if invalid/missing required values, report and fall back to new cleaning.
   - Display saved data/report and dictionary paths and saved launch string/flags.
   - Re-run using validated structured values and persisted explicit flags (deterministic). Report outcome as in new flow; do not silently rewrite state except optionally updating a "last run timestamp/result" field if included (non-breaking).
6. **Failure handling:** on any failure (validation, junction, settings, execution, state write), print explicit actionable error, keep state consistent (do not claim successful save), wait before close, exit non-zero.

### 7.4 State file format — `last_run_state.txt` (D6/D7)

Plain UTF-8 text, human-readable, key-value lines plus a launch string. Required structured fields (minimum):

- `SchemaVersion=1`
- `SavedAt=<ISO-8601 local or UTC timestamp>`
- `RawDataSelection=<absolute path selected by user>`
- `RawDataMode=<report_dir|raw_data_contents>`
- `ResolvedReportDir=<absolute report/staging directory passed to processing>`
- `DictPath=<absolute dictionary file path>`
- `ClearExportFolder=<true|false>`
- `Force=<true|false>`
- `SettingsMode=default_platformdirs`
- `PythonExecutable=<absolute path>` (or `ConsoleScript=<absolute path>`)
- `LaunchCommand=<exact resolved one-line command>`

Rules: quote/escape paths in `LaunchCommand`; treat structured fields as authoritative for repeat; ignore unknown keys for forward compatibility; validate paths/mode/booleans before repeat and fail clearly or fall back to new run.

### 7.5 Deterministic execution and constraints

- **Single target per run** (D5).
- **No PATH dependence:** resolve and persist absolute interpreter/module invocation (OQ4).
- **No raw-data copy by default:** staging uses junction (OQ2).
- **Config awareness:** preflight/report likely issues from current config (e.g. expected data extension, import folder) when detectable; surface core errors clearly.
- **No module changes:** all adaptation/wrapping is external.

---

## 8. Conceptual Development Tasks (confirmed)

| ID | Purpose | Expected outcome | Dependencies | Owner |
|---|---|---|---|---|
| T1 | Implement Script 1 (`01-init-environment.ps1`) | Git install/update with restart-stop gate, toolchain-detected package install/update (D1/D2/D3), default settings init without overwrite, diagnostics, no-close UX. | §7.2, R1. | Implementor |
| T2 | Implement Script 2 (`02-run-cleaning.ps1`, + helper if necessary) | First/repeat flows, D4 layout normalization + staging, settings preflight/safe-init, deterministic run with persisted flags, result reporting, no-close UX. | §7.3–§7.5, R2, D4–D8. | Implementor |
| T3 | Implement state persistence and validation | `last_run_state.txt` read/write/validate/fallback per §7.4 and D6/D7. | T2. | Implementor |
| T4 | UX/error-handling consistency and diagnostics | English actionable errors, step outcomes, exit codes (incl. restart-required), version/source reporting. | T1, T2. | Implementor |
| T5 | Example documentation | `examples/02-clean-data/README.md` (or equivalent minimal doc) covering prerequisites, usage, restart behavior, staging/state behavior, limitations, and D1 "latest" policy. | T1–T4. | Doc-specialist / Implementor |
| T6 | Validation and acceptance review | Manual matrix executed/documented: first-time Git stop/restart path, idempotent rerun, missing winget/toolchain errors, report-dir mode, raw-data+junction mode, invalid paths/dict, export conflict policies, repeat parity, no-close/exit codes. No module code/test changes. | T1–T5. | Implementor / Tech Lead |

**Sequencing:** T1 and T3/state contract scaffolding can proceed in parallel; T2 integrates T3 and D4/OQ2 behavior; T4/T5 finalize; T6 verifies acceptance.

---

## 9. Risks and Mitigations

| Type | Item | Mitigation |
|---|---|---|
| Risk | PATH not refreshed after first-time Git install | Mandatory stop/restart gate (D3/R1, AC1.2). |
| Risk | `winget` unavailable/blocked or elevation issues | Explicit actionable failure (D3). |
| Risk | Python 3.12+/toolchain missing | Priority detection + explicit remediation error (D2). |
| Risk | Console script not on PATH after install | Resolve/persist absolute interpreter/module invocation (OQ4). |
| Risk | Staging junction failure (policy/FS) | NTFS junction preferred; explicit actionable error, no silent copy (OQ2/D4). |
| Risk | Ambiguous raw-data selection/layout or config/extension mismatch | Auto-detect with explicit mode reporting, validation, clear errors (D4, §7.3). |
| Risk | Export conflicts or unintended deletion | Explicit, persisted `clear_export_folder/force` choices (D8). |
| Risk | "Latest from git" instability | Document D1 policy; show version/commit/source when feasible (OQ5). |
| Risk | Non-interactive prompt/abort differences in core | Use deterministic flags and preflight; report core errors clearly. |

---

## 10. Definition of Ready (DoR) — Status

| Criterion | Status |
|---|---|
| PO decisions Q1–Q8 confirmed (Q9 noted/confirmed) | **Satisfied** |
| Scope locked (filenames, helper allowance, state file name/location/content) | **Satisfied** (§7.1, §7.4) |
| Behavior contracts finalized (layout/staging, export/force, launch/invocation) | **Satisfied** (§7.2–§7.5) |
| Prerequisite policy and error/remediation expectations finalized | **Satisfied** (D2/D3, §7.2) |
| Acceptance criteria enumerated | **Satisfied** (§11) |
| Validation approach agreed (no module changes) | **Satisfied** (T6, §11) |
| No blocking research gaps for confirmed options | **Satisfied** (R1/R2) |

**Overall: READY for implementation planning and execution tasking.**

---

## 11. Acceptance Criteria (confirmed)

### Script 1 — `01-init-environment.ps1`
- **AC1.1:** Performs Git check/install/update, `mko_data_cleaner` install/update (D1), and default settings initialization in a clear stepwise flow; reruns are idempotent.
- **AC1.2:** If Git is newly installed and unavailable in the current session, script stops further steps, clearly instructs the user to restart in a new terminal/session and rerun Script 1, and does **not** claim full success (dedicated non-zero exit status).
- **AC1.3:** Settings initialization targets the default platformdirs user settings location and does **not** overwrite existing settings files.
- **AC1.4:** On success, prints a clear English success summary (including relevant paths/status and version when discoverable) and waits for user input before closing (exit 0).
- **AC1.5:** On any step failure (including missing winget/toolchain per D2/D3), prints an explicit, actionable English error identifying the failed step/cause and next action, waits before closing, and exits non-zero.

### Script 2 — `02-run-cleaning.ps1`
- **AC2.1:** On first/new run (no valid saved state), prompts for raw-data location and dictionary location, validates inputs, normalizes layout per D4, runs cleaning, and reports outcome clearly.
- **AC2.2:** On successful cleaning, asks whether to save the run; if confirmed, writes `last_run_state.txt` in `examples/02-clean-data/` containing structured fields and the resolved launch string per D6/D7.
- **AC2.3:** On repeat run (valid saved state present), offers: (1) repeat previous cleaning using saved validated values/flags or (2) new cleaning.
- **AC2.4:** Does not close after execution in success or failure paths and returns appropriate exit status.
- **AC2.5:** Respects current module constraints without module changes: correct report/layout handling (including staging mode), absolute dictionary handling, settings preflight/safe-init (OQ3), and deterministic explicit export/interaction flags per D8.

### Cross-cutting
- **ACX.1:** Required scripts reside in `examples/02-clean-data/` with confirmed names; any helpers are minimal and colocated.
- **ACX.2:** No files under `src/mko_data_cleaner/**` are modified.
- **ACX.3:** User-facing errors are specific and actionable in English (not silent failures or raw tracebacks only).

---

## 12. Traceability to Request

| Request element | Spec coverage |
|---|---|
| Script 1: install/update Git; if first-time install requires restart, stop and instruct restart | D3, §7.2, AC1.2 |
| Script 1: install/update latest `mko_data_cleaner` | D1/D2, §7.2, AC1.1 |
| Script 1: initialize/copy settings without overwrite in default app folder | D5, §7.2, AC1.3 |
| Script 1: no close, show success, explicit user error on failure | §7.2, AC1.4/AC1.5 |
| Script 2: first run asks for raw_data folder and dictionary, runs cleaning | D4/D5, §7.3, AC2.1 |
| Script 2: on success ask to save paths and save launch string to neighboring text file in same folder | D6/D7, §7.3–§7.4, AC2.2 |
| Script 2: repeat run offers repeat previous (saved paths) or new cleaning | D6/D7, §7.3, AC2.3 |
| Script 2: no close, show success/failure, explicit user error on failure | §7.3, AC2.4/AC2.5, ACX.3 |
| Scripts in `C:\py_exp\mko_data_cleaner\examples\02-clean-data`; module code unchanged | §1, §2.2–§2.4, §7.1, ACX.1/ACX.2 |
| English language confirmed | D9, §2.4, §7, AC1.4/AC1.5 |
