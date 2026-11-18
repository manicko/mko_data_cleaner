# Execution Plan 09: PowerShell Bootstrap and Cleaning Scripts (`examples/02-clean-data`)

## 1. References and status

| Item | Value |
|---|---|
| **Source spec** | `.ai/problems/07_powershell_bootstrap_and_clean_scripts_spec.md` — "07 — PowerShell Bootstrap and Cleaning Scripts (`examples/02-clean-data`)" |
| **Spec status / readiness** | Complete analytical specification; PO decisions **D1–D9 confirmed** (2026-11-18); DoR **satisfied**; **READY for implementation planning and execution tasking** (spec §4, §10). |
| **This plan** | `.ai/plans/09-powershell-bootstrap-and-clean-scripts.md` (Step 1 decomposition / execution plan) |
| **Step 0** | Auditor `{code_context}` provided (read-only). No code or existing audit/plan files changed in this step. |
| **Step 2 (PO clarification)** | **Skipped** — no concrete remaining business uncertainty identified (see §3). |
| **Orchestration** | **Tech Lead orchestrator-only.** No module code changes. Single-owner execution per block unless explicitly parallelized by dependency-safe handoff; preserve `<original_prompt>` intent in every downstream task prompt. |

## 2. Step 0 `{code_context}` summary (incorporated, critical facts and discrepancies)

### 2.1 Target and reference state
- **Target:** `examples/02-clean-data/` exists and is **empty** (no scripts, no `last_run_state.txt`, no `.staging/`, no helpers/README). All required artifacts are net-new.
- **Reference UX/behavior pattern:** `examples/notebooks/02-clean/` (`run_report.ps1`, `.bat`, `.py`, `README.md`) demonstrates script-dir-relative execution, explicit path checks, actionable failure messages with no false success claims, per-target iteration, launcher pause/no-auto-close, and exit-code propagation. Reference `run_report.py` workflow differs from Script 2 contract (spec §7.3) and must not be copied as-is.

### 2.2 Module/API/CLI contract (authoritative, immutable)
- **Public API:** `initialize_settings(force=False, settings_dir=None) -> Path`; `process_data(report_path, settings_dir=None, dict_path=None, clear_export_folder=None, force=None) -> ReportResult`; exports include `ReportResult`, `SettingsNotInitializedError`, `__version__` (`src/mko_data_cleaner/app.py`, `__init__.py`).
- **CLI (Typer):** `mko-data-cleaner init/run/list-settings` (`mko-data-cleaner = mko_data_cleaner.app:app`, `pyproject.toml`); `run` accepts report dir, `--settings-dir`, `--dict-path`, `--clear-export-folder/--force` flags and reports `rows_loaded`, `rows_exported`, `duration`, `export_dir` on success.
- **Settings/templates:** package templates at `src/mko_data_cleaner/settings/app_config.json5`, `log_config.json5` (not `core/settings/templates`). `init_project` copies recursively without overwrite unless `force=True`; target is `settings_dir/"settings"` or default platformdirs user settings (`AppPaths`/`core/paths.py`).
- **Settings gate/runtime:** `get_app_service` requires `app_config.json5` and `log_config.json5` in user settings dir else `SettingsNotInitializedError`; logging configured lazily at service creation; log handler paths resolved/prepared by service.
- **Report/layout/path resolution:** report path must exist as directory; import data read from `<report>/<data_paths.import_folder>` (default `raw_data`), export/db under configured paths; `dict_path` override relative to report when not absolute, absolute preserved; missing override raises detailed `FileNotFoundError`.
- **Config defaults relevant to scripts:** `import_folder="raw_data"`, `export_folder="clean_data"`, `dict_file="dict/merged_dictionary.csv"`, `db_file="data_base/db_example.db"`, `data_file_settings.extension="gz"`, `clear_export_folder=false`, `force=false`.
- **Export conflict handling:** matches existing export-suffix pattern; TTY/ipykernel-aware prompts (`typer.confirm`) possible; non-interactive/no-clear/no-force cases can abort (e.g. `ExportFolderConflictError`) — Script 2 must enforce deterministic explicit flags (spec D8/AC2.5).
- **Result model:** `ReportResult` carries `report_dir, import_dir, export_dir, db_path, null_data_files, started_at, finished_at, duration, timings, rows_loaded, rows_exported`.

### 2.3 Constraints, dependencies, and discrepancies/risks surfaced by context
- **Immutable core:** no edits under `src/**`, package metadata/runtime behavior, or existing tests (spec §2.4, ACX.2).
- **Environment/toolchain:** Python >= 3.12; runtime deps include `typer`, `rich`, `platformdirs`, `pydantic`, `json5`, `polars`, `pyarrow`, `adbc-driver-sqlite`. Console script may not be on PATH after install; interpreter/environment selection must be explicit and reliable.
- **Discrepancies to preserve in planning:** (a) spec A3 gives a typical Windows platformdirs path example, but actual path is `user_config_dir('mko_data_cleaner')/settings` per implementation — scripts must resolve/report actual path, not assume example; (b) core/CLI emit some Russian messages, so transcripts may interleave with English script messages; (c) reference `examples/notebooks/02-clean/run_report.py` is not the Script 2 contract; (d) target dir is empty (all deliverables absent).
- **Key technical risks from context:** toolchain/interpreter resolution and VCS refresh semantics (D1/D2), winget/elevation/PATH restart gate (D3/AC1.2), NTFS junction creation/update/FS permissions for staging (D4/OQ2), config-dependent extension/import-folder preflight (JSON5 parsing feasibility in PowerShell), settings path mapping `settings_dir -> <base>/settings`, logging side effects at service init, export prompt behavior if flags inconsistent, and reliable structured result capture (CLI text vs Python API/helper).

- **Research-selected notes (2026-11-18, B1 {context_r}):** probe-and-converge flow with tracked exit codes (recommend `301` = restart required, `0` success, `1` failure); require a "fully satisfiable" D2 toolchain that yields a verified Python >=3.12 absolute target interpreter; prefer `uv pip install --python <targetPy>` when uv+targetPy are available, else `pipx install --force --python <targetPy>`, else `py -3`/`python -m pip` with `--upgrade --force-reinstall --no-cache-dir`; perform in-process Machine/User PATH refresh and re-probe before declaring restart-required; initialize/report settings and diagnostics via the resolved target interpreter (actual `APP_PATHS.user_settings_dir`, pre/post file presence, `importlib.metadata` version, best-effort `direct_url.json` commit) and always report D1 source/ref policy. Winget benign exit codes vary by version/locale - rely on post-conditions plus allowlisted codes, not exit code alone. VCS refresh for same-version moving HEAD is tool/cache-sensitive - use strongest supported reinstall+refresh+no-cache flags and verify by runtime probe. Script 1 must not create `last_run_state.txt` (Script 2 responsibility).
- **Research-selected notes (2026-11-18, B2 {context_r}):** recommended PowerShell orchestrator + minimal colocated Python driver (e.g. `02-run-cleaning.driver.py`) invoked via absolute Python for settings/config preflight, safe init, and `process_data` API execution with structured `ReportResult` capture and typed English actionable errors; helper narrow/external to `src/**`. UTF-8 (no BOM) safe-write for state; repeat-time revalidation (no silent misrun). Junction safety: detect reparse points; do not use `Remove-Item` for junction removal in PS 5.1 - prefer `cmd /c rmdir` for link removal and `New-Item -ItemType Junction`/`cmd /c mklink /J` for creation; refuse/confine if link path exists as non-reparse dir/file. Probe interpreters (e.g. `py -3.12`, `py -3`, `python`, `python3`) for absolute `sys.executable` and `mko_data_cleaner` importability/version; if none usable fail with actionable guidance to run Script 1 (note pipx/uv-managed env discoverability uncertainty). Config preflight best-effort via helper/package APIs when available; always pass explicit `--clear-export-folder/--no-clear-export-folder` and `--force/--no-force` (or explicit API bools).

## 3. PO/business uncertainty and Step 2 disposition

- **Confirmed PO decisions:** D1–D9 are confirmed; implementation defaults OQ2–OQ6 are recorded in spec §4.1; gray areas G1–G10 are resolved (spec §5); DoR/ACs are finalized (spec §10–§11).
- **Remaining concrete business uncertainty:** **None identified.** Scope, filenames, state file location/format minimum, language, platform, prerequisite policy, layout/staging behavior, launch/invocation intent, and deterministic export/force policy are business/product-resolved in the spec.
- **Step 2 outcome:** **Skip Step 2.** Any remaining ambiguity is **technical/implementation uncertainty** (external tooling behavior, filesystem/junction mechanics, invocation/config-preflight strategy, reliable result capture) and must be handled via research, planning, and explicit alternatives below — **not** by guessing product intent and **not** by changing module behavior.

## 4. Constraints and reminders (binding for all downstream tasks)

1. **Module code immutable:** do not modify `src/mko_data_cleaner/**`, package metadata/runtime behavior, or existing tests. All adaptation is external to the module (ACX.2).
2. **Tech Lead role:** orchestrator-only for this workflow; delegate implementation, documentation, and validation to appropriate agents per block; single Implementor ownership per implementation slice where practical.
3. **Deliverable location and names:** required scripts in `examples/02-clean-data/` as `01-init-environment.ps1` and `02-run-cleaning.ps1`; fixed-name state file `last_run_state.txt` in same folder; staging under `examples\02-clean-data\.staging\default\` when needed; minimal colocated helpers/README only if necessary (spec §7.1, ACX.1).
4. **Follow spec contracts and ACs:** implement per spec §7 (contracts, state format, deterministic execution) and satisfy §11 (AC1.1–AC1.5, AC2.1–AC2.5, ACX.1–ACX.3). Treat spec as the source of behavioral truth for scripts.
5. **Preserve `<original_prompt>` intent downstream:** every delegated task must retain the user/original intent — decompose safely, identify dependencies/order, assess implementation/rollout/regression/compatibility risks, identify viable alternatives when relevant, **do not choose a final implementation approach when technical uncertainty exists**, keep scope minimal, avoid speculative redesign, and never change module code or existing audit-phase artifacts.
6. **No speculative redesign / minimal scope:** no GUI, no non-Windows support, no multi-profile state, no folder-merge dictionaries, no module refactors, no `.bat` wrappers unless later requested. Documentation limited to what is necessary and accurate for the new example workflow (and minimal existing-doc touch only if directly affected).
7. **UX/safety invariants:** English console/repo text artifacts (D9), console stays open on success/failure with correct exit codes (including restart-required non-success path for Script 1), explicit actionable errors (no silent failure/raw-traceback-only UX), no hidden destructive behavior, no raw-data copy by default (junction/staging per D4/OQ2), no settings overwrite unless explicitly specified by contract (Script 1/Script 2 safe init without force).

## 5. Execution overview, dependencies, and sequencing

| Block | Workflow step / scope | Depends on | Parallelization | Exit condition |
|---|---|---|---|---|
| **B1** | Implementation: Script 1 (`01-init-environment.ps1`) bootstrap, package/settings init, diagnostics, UX per §7.2 | None (uses Step 0 context + spec) | May run in parallel with B2 design/prep artifacts **only** if interface assumptions are frozen to spec; integration/value realized independently | Script 1 meets contract/AC1.x intent, idempotent/safe behaviors encoded, English UX/exit semantics implemented; no module changes |
| **B2** | Implementation: Script 2 (`02-run-cleaning.ps1`) + state persistence/validation + D4 layout normalization/staging + deterministic resolved invocation/run + UX per §7.3–§7.5 (+ minimal helper only if necessary) | Spec §7.3–§7.5 and stable state contract §7.4; no hard runtime dependency on B1 artifact, but must handle "suggest running Script 1" preflight cases | May proceed after/alongside B1 with care; avoid conflicting assumptions about interpreter discovery/reporting | Script 2 meets contract/AC2.x intent, deterministic repeat behavior, safe staging/state handling, explicit actionable failures; no module changes |
| **B3** | **Step 4 — Documentation:** `examples/02-clean-data/README.md` (and minimal existing example/doc updates only if affected) | B1, B2 (final behavior must be known) | After implementation stabilizes | Accurate, minimal, English documentation covering prerequisites, usage, restart behavior, staging/state behavior, limitations, D1 "latest" policy; no unrelated doc churn |
| **B4** | **Step 5 — Final Validation:** Tech Lead quality gates + independent Validator review and acceptance/risk assessment | B1, B2, B3 | Final | Plan/spec/AC traceability satisfied, no `src/**` changes, relevant checks/gates executed or explicitly justified, risks/regressions/compatibility reviewed, no unrelated changes |

**Dependency DAG (logical):** `B1 -> B3`, `B2 -> B3`, `B3 -> B4` (with B1/B2 implementation streams independent at artifact level but converging for documentation and final validation).

## 6. Block details

### B1 — Script 1 bootstrap: `01-init-environment.ps1`

#### Scope
- Create `examples/02-clean-data/01-init-environment.ps1` implementing spec §7.2: preflight/UI scaffolding, Git detect/install/update via winget with first-install **restart-stop gate** (AC1.2), actionable failure if winget/App Installer unavailable when required (D3), package install/update of `mko_data_cleaner` latest default-branch HEAD from configured git source with moving-ref refresh strategy appropriate to selected toolchain (D1), toolchain detection in priority order `uv` → `pipx` → `py -3`/`python` with pip and Python >= 3.12 with explicit failure if none (D2), default platformdirs settings initialization without overwrite and path/status reporting (D5/AC1.3), diagnostics/version/source reporting when feasible (OQ5), and always-pause/no-close UX with correct exit codes (AC1.4/AC1.5).
- Keep logic idempotent and non-destructive to user settings (no `--force`).

#### In scope / out of scope
- **In:** PowerShell implementation, English step/outcome/error messaging, deterministic exit semantics including dedicated non-zero "restart required" state, best-effort/convergent reruns.
- **Out:** module code changes, Script 2 run flow/state, non-Windows support, automatic elevation bypass hacks, silent fallback installers, modifying existing tests.

#### Implementation, rollout, regression, and compatibility risks
| Type | Risks |
|---|---|
| **Implementation** | Correctly detecting post-install PATH availability in current session; distinguishing "restart required" from generic failure; mapping winget exit codes/no-upgrade cases; selecting and invoking install commands per tool (`uv`/`pipx`/pip) with forced VCS refresh; resolving installed version/commit reliably across tools; ensuring Python >= 3.12 check is accurate for `py -3`/`python`. |
| **Rollout** | Winget/App Installer absent, disabled, or requires elevation/agreements in restricted environments; corporate proxy/firewall blocks git/winget/PyPI; first-time Git install commonly unusable until new session (expected, must not be misreported as success). |
| **Regression** | Minimal repo regression risk (new example file only), but script reruns could be non-idempotent if implemented poorly (e.g. accidental settings overwrite, repeated noisy failures, or misleading status). Must not affect module behavior. |
| **Compatibility** | Windows PowerShell 5.1 vs PowerShell 7+ syntax/cmdlet differences; command resolution (`py` launcher vs `python`), PATH/session scoping, console encoding for mixed English script + Russian core output in downstream runs, varying winget versions/behavior. |

#### Viable implementation alternatives (relevant; **do not choose final approach while technical uncertainty exists**)
- **Git install/update path:** (A) `winget`-only flow per spec with explicit restart gate and best-effort upgrade reporting; (B) `winget` with richer preflight/probing (App Installer/winget source health) before action for clearer diagnostics. *Uncertainty:* reliable restart detection and upgrade/no-op semantics across hosts.
- **Package install/update and refresh (D1):** (A) tool-native commands with forced VCS reinstall/refresh semantics per detected tool; (B) install into an isolated environment managed/selected by the chosen tool and record its interpreter path for Script 2; (C) prefer making console script discoverable but also record module-run fallback. *Uncertainty:* which per-tool command/flags guarantee default-branch HEAD refresh when version metadata is unchanged and how to stably locate the resulting runnable interpreter/script afterward.
- **Toolchain/interpreter discovery (D2):** (A) detect executables in fixed priority and probe capabilities/version inline; (B) resolve candidate Python explicitly (`py -3.12`, `py -3`, `python`) and validate `>=3.12` before selecting pip path. *Uncertainty:* best cross-environment probe strategy without false positives.
- **Version/source diagnostics (OQ5):** (A) query selected interpreter/package metadata (`importlib.metadata`/module `__version__`); (B) query console script `--version` if/when available; (C) report configured source/ref policy plus discovered version, include commit only when cheaply discoverable. *Uncertainty:* cheapest reliable cross-tool way to expose commit/ref metadata for VCS installs.
- **Error/exit contract implementation:** (A) structured step tracking with explicit exit codes (including restart-required) in `try/finally`-style flow; (B) centralized failure helper ensuring pause-before-exit and non-zero propagation. *Uncertainty:* PowerShell-native pattern with best readability/robustness across 5.1/7+.

#### Required agents and rationale
- **Risk level:** **High** (external system tooling, install/refresh semantics, PATH/restart gate, environment variability) — apply heightened agent coverage.
- **Researcher — required:** modern/practical Windows 11 + PowerShell 5.1/7+ best practices for winget Git install/upgrade detection, PATH/session restart signaling, and per-tool (`uv`, `pipx`, pip) VCS install/forced-refresh behavior and interpreter resolution implications.
- **Planner — required:** detailed pre-implementation design for step sequencing/state transitions (especially restart-required non-success), idempotency, exit-code contract, diagnostics payload, and handoff points for Script 2 environment discovery assumptions.
- **Validator (independent plan/review) — required:** implementation risk is high; independent review should verify AC1.x traceability, no-overwrite safety, restart semantics, and absence of hidden fallbacks/destructive actions.
- **Auditor — conditionally required / recommended:** deeper investigation if implementation uncovers ambiguity in CLI init behavior, settings-path mapping, or version reporting surfaces beyond Step 0 `{code_context}`; otherwise Step 0 context is sufficient. Treat as available if uncertainty emerges.

### B2 — Script 2 cleaning workflow, state, staging/layout, deterministic run: `02-run-cleaning.ps1` (+ minimal helper only if necessary)

#### Scope
- Create `examples/02-clean-data/02-run-cleaning.ps1` implementing spec §7.3–§7.5 and §7.4 state contract: preflight/UI, environment/interpreter resolution and settings preflight with safe auto-init if missing (OQ3), state detection/menu and invalid-state fallback (D6), new-run prompts/validation for raw data and dictionary, D4 layout normalization (report-dir auto-detect vs raw-data-contents staging with NTFS junction under `.staging\default`, no raw-data copy) (OQ2), config/extension/import presence preflight when feasible, explicit export/interaction policy capture and persistence (`clear_export_folder`, `force`) (D8), deterministic resolved execution with absolute dictionary path and persisted flags (OQ4/D7), success summary/reporting (including `ReportResult` fields when available) and save prompt, repeat-run load/validate/re-execute from structured fields, consistent failure handling/no-close/exit codes (AC2.1–AC2.5).
- Implement `examples/02-clean-data/last_run_state.txt` read/write/validate/fallback per §7.4 (SchemaVersion 1, required keys, authoritative structured fields, quoted `LaunchCommand`, forward-compatible unknown-key handling).
- Add a **minimal colocated helper** (e.g. Python driver) **only if necessary** for reliable config preflight, API invocation/result capture, or version/path resolution; keep helper narrow, documented, and external to `src/**` (A4/G10).

#### In scope / out of scope
- **In:** PowerShell workflow/state/staging logic, English UX, deterministic flags and absolute-path handling, actionable validation and runtime errors, safe junction management within staging area, optional minimal helper and/or README hook points only as required by B3.
- **Out:** module code changes, multi-profile management, folder-merge dictionaries, GUI, automatic raw-data copying, changing core export/config semantics, non-Windows support.

#### Implementation, rollout, regression, and compatibility risks
| Type | Risks |
|---|---|
| **Implementation** | Correct auto-detection between "contains `raw_data/`" (report dir) vs raw-data contents mode; safe create/update of `raw_data` junction in `.staging\default` without touching user source data; robust state parsing/validation/escaping for paths and booleans; resolving and persisting an invocation form that is repeatable in the same environment; capturing structured run outcome (`ReportResult`) vs parsing CLI output; config preflight when settings/config are JSON5 and extension/import folder are config-driven. |
| **Rollout** | Junction creation may fail due to filesystem policy/permissions/non-NTFS targets; user-selected paths may be UNC/relative/removable/network drives with different semantics; saved absolute interpreter paths may become invalid after environment changes (must fail clearly on repeat, not silently misrun); existing export artifacts may trigger conflicts if flags misapplied. |
| **Regression** | New example artifacts only (low repo-code regression), but incorrect staging/state logic can mislead users (wrong target, stale junction, false "saved/repeat" claims) or delete wrong files if junction handling is unsafe — must constrain mutations to staging-managed paths and explicit export behavior per current tool flags. |
| **Compatibility** | PowerShell 5.1/7+ differences (`New-Item -ItemType Junction`, path APIs, error handling), NTFS junction behavior vs symlink policies, console interactivity/TTY differences affecting core prompts if flags omitted (must be prevented by D8), mixed-language core output, `gz` default extension and non-recursive discovery expectations from config. |

#### Viable implementation alternatives (relevant; **do not choose final approach while technical uncertainty exists**)
- **Invocation and result capture (OQ4, AC2.1 reporting):** (A) call Python API via minimal helper/driver to run `initialize_settings`/`process_data` and emit/return structured fields; (B) invoke `python -m mko_data_cleaner.app run ...` (or absolute console script) and parse/relay CLI output; (C) hybrid where structured fields come from helper but `LaunchCommand` stores resolved CLI/module command for transparency. *Uncertainty:* best balance of reliability, minimal helper footprint, and faithful `ReportResult`/version reporting without module changes.
- **Interpreter/environment discovery for Script 2:** (A) reuse persisted `PythonExecutable`/tool hints from state when valid, else probe installed toolchains/interpreters; (B) independently detect runnable `mko_data_cleaner` environment by probing module import/CLI availability in priority order; (C) require Script 1-produced environment and fail with actionable guidance when absent. *Uncertainty:* consistent strategy across `uv`/`pipx`/pip installs and PATH variability.
- **Staging junction management (D4/OQ2):** (A) manage `raw_data` link with PowerShell junction cmdlets/APIs; (B) manage via `cmd /c mklink /J` for broader 5.1-era behavior; (C) detect existing real directory/file at junction path and fail safely with remediation instead of destructive replacement. *Uncertainty:* most robust create/update/remove behavior across PowerShell versions and locked/invalid states.
- **Config preflight for `import_folder`/extension (when feasible):** (A) lightweight Python one-liner/helper using project/runtime JSON5/Pydantic-capable environment to read effective config; (B) PowerShell-side heuristic checks (existence/count by common patterns) with clear "best-effort" messaging; (C) skip deep preflight and rely on core errors, but surface them actionably. *Uncertainty:* feasibility without adding module code or heavy dependencies and desired strictness vs simplicity.
- **State file implementation (§7.4):** (A) strict ordered key-value writer and tolerant reader/validator with explicit required-key checks; (B) include optional non-breaking metadata fields (e.g. last result/timestamp) while preserving required schema; (C) store paths as raw values and centralize quoting/escaping only for `LaunchCommand`. *Uncertainty:* safest parsing/writing approach in PowerShell without ambiguity and with forward compatibility.
- **Repeat-run trust model (D7):** (A) always revalidate paths/mode/booleans and interpreter availability before reuse; (B) on invalid/missing values fall back to new flow immediately with explanation; (C) allow re-prompt for missing non-critical fields while keeping structured fields authoritative. *Uncertainty:* exact fallback boundaries, but spec requires validation and clear fallback on invalid required values.

#### Required agents and rationale
- **Risk level:** **High** (filesystem/junction behavior, state correctness, deterministic invocation across environments, interaction with core export/prompt semantics) — apply heightened agent coverage.
- **Researcher — required:** best practices and pitfalls for NTFS junction create/update in PowerShell 5.1/7+, safe staging patterns, robust key-value state handling/escaping, and reliable ways to invoke/capture results from Python packages/CLIs from PowerShell on Windows.
- **Planner — required:** detailed design for control flow (new/repeat/invalid-state), staging lifecycle, state schema validation/fallback, invocation resolution contract (including what is persisted vs recomputed), config-preflight strategy, and test/validation matrix mapping to AC2.x/ACX.x.
- **Validator (independent plan/review) — required:** high implementation risk around deterministic flags, no hidden destructive behavior, repeat parity, and correct adaptation to immutable core contract; independent review must check D4/D6/D7/D8/OQ2–OQ4 traceability.
- **Auditor — conditionally required / recommended:** invoke if deeper inspection is needed for export conflict edge cases, logging side effects, path resolution semantics, or `ReportResult`/CLI behavior details not fully resolved by Step 0 context during implementation design.

### B3 — Step 4: Documentation (example-local and minimal affected docs)

#### Scope
- Create `examples/02-clean-data/README.md` (or equivalent minimal colocated documentation if naming must differ, but prefer README) per spec T5: prerequisites (Windows 11, PowerShell, internet/tool availability expectations), Script 1/Script 2 usage, first-time Git restart behavior, D1 "latest" install/update policy and source, staging/junction behavior and `.staging/default` purpose, state file (`last_run_state.txt`) behavior and repeat flow, deterministic export/force choices, limitations, and actionable troubleshooting guidance aligned with ACX.3.
- Review and, **only if directly affected/necessary**, make minimal accurate updates to existing example documentation (e.g. `examples/README.md`) to reference the new `examples/02-clean-data` workflow; do not rewrite unrelated sections.
- Follow `docs/00-overview/doc-maintenance-rules.md` and project documentation conventions (English, factual, minimal).

#### Dependencies
- **Depends on B1 and B2:** documentation must reflect actual implemented behavior, messages/flags as applicable, and known limitations discovered during implementation.

#### Implementation, rollout, regression, and compatibility risks
| Type | Risks |
|---|---|
| **Implementation** | Documenting behavior that diverges from final scripts; omitting critical restart/staging/state semantics; over-documenting speculative helper internals. |
| **Rollout** | Users may misapply workflow if prerequisites, restart gate, or staging mode are unclear; unclear troubleshooting reduces value of actionable script errors. |
| **Regression** | Minimal (docs only). Risk of stale or inconsistent example docs if updates are partial or too broad. |
| **Compatibility** | Doc instructions must match supported PowerShell/Windows assumptions and avoid implying module behavior changes or unsupported platforms. |

#### Viable implementation alternatives (relevant; **do not choose final approach while technical uncertainty exists**)
- **Doc placement/detail level:** (A) single comprehensive `examples/02-clean-data/README.md` plus minimal cross-link from `examples/README.md`; (B) keep `examples/02-clean-data/README.md` focused and avoid touching existing docs unless clearly required by implementation outcome. *Uncertainty:* extent of necessary updates to existing example docs depends on final B1/B2 behavior and repository doc expectations.
- **Troubleshooting depth:** (A) concise symptom/action table aligned to script error categories; (B) short narrative sections per script. *Uncertainty:* best format for non-developer users given final script messages.

#### Required agents and rationale
- **Risk level:** **Low–Medium** (user-facing accuracy/operational clarity; no runtime code risk).
- **Doc-specialist — required:** primary owner for minimal, accurate, convention-aligned documentation updates.
- **Validator — recommended/required in final review context:** verify documentation accuracy against implementation and spec (captured in B4).
- **Researcher / Planner / Auditor — not required by default:** no external research or architecture investigation expected beyond implemented behavior; escalate only if implementation reveals unexpected constraints needing explanation.

### B4 — Step 5: Final validation (quality gates and independent review)

#### Scope
- **Tech Lead / orchestrator validation:** confirm all planned blocks completed, artifacts exist in correct locations/names, no files under `src/mko_data_cleaner/**` modified, no unrelated changes introduced, and downstream task outputs preserved `<original_prompt>` intent and spec traceability.
- **Quality gates (as applicable to changed artifacts):** run relevant repository checks for touched/example files where feasible in this environment (at minimum inspectability and, if practical, PowerShell parse/syntax checks for `.ps1`; run project lint/format/type/test gates only as appropriate to confirm no regressions and to comply with repo workflow — do not "fix" by changing module code). Record any environment-limited checks explicitly.
- **Independent Validator review:** assess completeness vs spec §7/§11 and plan B1–B3, cross-block integration, architectural/convention consistency (example-only, external adaptation), implementation/rollout/regression/compatibility risks, deterministic behavior and safety (restart gate, no overwrite, junction confinement, explicit export flags, state validation), documentation accuracy, and evidence for acceptance matrix (manual/runtime matrix items documented; note what could/could not be executed in-session on Windows 11 without side effects or missing prerequisites).

#### Dependencies
- **Depends on B1, B2, B3.**

#### Implementation, rollout, regression, and compatibility risks
| Type | Risks |
|---|---|
| **Implementation** | False confidence if validation is limited to static checks while key behaviors (winget install, PATH restart, VCS refresh, junctions, real `process_data` runs) are environment-dependent. |
| **Rollout** | Undetected edge cases in user environments (permissions, missing toolchains, non-NTFS paths, console differences) may remain; validation must clearly state coverage limits. |
| **Regression** | Residual risk is low if `src/**` and existing tests remain untouched; still verify no accidental edits and that repo quality gates remain coherent. |
| **Compatibility** | Behavior may differ between Windows PowerShell 5.1 and PowerShell 7+; validation should note which shell(s)/conditions were exercised or reasoned about. |

#### Viable validation alternatives (relevant; **do not force a single path when environment uncertainty exists**)
- **Execution depth:** (A) static + parse/syntax checks plus dry reasoning against AC matrix; (B) targeted local/manual runs for safe scenarios (e.g. state parsing/layout logic with temp paths) and documented simulation for privileged/install flows; (C) full manual matrix on a prepared Windows 11 environment. *Uncertainty:* feasible depth depends on available tools, permissions, network access, and presence of sample data/dictionary in validation environment.
- **Evidence format:** (A) checklist mapped to AC1.x/AC2.x/ACX.x with observed results/limitations; (B) concise validation report with residual risks. *Uncertainty:* prefer format consistent with repository validation artifacts if established later, but keep minimal now.

#### Required agents and rationale
- **Risk level:** **Medium–High for release confidence** (high environment dependence) though code-surface change is isolated to examples/docs.
- **Validator — required:** independent final review is mandatory for high-risk implementation blocks and overall acceptance/risk closure.
- **Tech Lead (orchestrator) — required:** gate execution, artifact/integrity checks, and consolidation of residual risks.
- **Auditor / Researcher / Planner — not required as separate execution blocks** unless validation uncovers blocking ambiguity; escalate targeted follow-up rather than speculative rework.

## 7. Agent requirement summary by block

| Block | Auditor | Researcher | Planner | Validator | Notes |
|---|---|---|---|---|---|
| **B1 (high risk)** | Conditional/recommended on newly discovered contract ambiguity | **Required** | **Required** | **Required** | External tooling, restart/PATH semantics, VCS refresh, idempotency/exit contract |
| **B2 (high risk)** | Conditional/recommended on newly discovered core behavior edge cases | **Required** | **Required** | **Required** | Staging/junction FS behavior, state determinism, invocation/result capture, config/export interaction |
| **B3 (low–medium)** | Not required by default | Not required by default | Not required by default | Included in B4 (and recommended for doc accuracy) | Doc-specialist primary; escalate only on unexpected constraints |
| **B4 (final)** | Escalation only | Escalation only | Escalation only | **Required** | Independent acceptance/risk/regression review + Tech Lead gates |

**High-risk rule application:** B1 and B2 carry high implementation/rollout/compatibility risk and material technical uncertainty; therefore they require **Researcher, Planner, and independent Validator** involvement, with **Auditor** engaged when deeper code/architecture or contract investigation becomes necessary beyond Step 0 context.

## 8. Expected result

Upon completion of this plan's execution (B1–B4), the repository will contain:

- `examples/02-clean-data/01-init-environment.ps1` and `examples/02-clean-data/02-run-cleaning.ps1` implementing the confirmed spec §7 contracts and satisfying AC1.1–AC1.5, AC2.1–AC2.5, and ACX.1/ACX.3 as applicable.
- `examples/02-clean-data/last_run_state.txt` support (created/updated only on confirmed save) and `.staging/default/` usage only when required by D4 raw-data-contents mode, with safe, explicit behavior.
- Minimal colocated helper(s) and/or `examples/02-clean-data/README.md` only as necessary, with accurate English documentation of prerequisites, restart/staging/state behavior, limitations, and D1 policy.
- **No modifications** to `src/mko_data_cleaner/**`, package metadata/runtime behavior, or existing tests; no unrelated file changes.
- A documented validation outcome (including executed checks, manual-matrix coverage/limitations, and residual risks) confirming traceability to spec D1–D9, §7, and §11, with technical uncertainties resolved through evidence-based implementation choices recorded during execution (not guessed in advance).
