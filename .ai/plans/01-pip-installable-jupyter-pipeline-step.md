# Execution Plan 01 — pip-installable Jupyter pipeline step (`mko_data_cleaner`)

- **Status:** In progress (Audit phase complete 2026-10-14)
- **Created:** 2026-10-14 by Tech Lead (multi-agent workflow)
- **Source spec:** `.ai/problems/01_pip_installable_jupyter_pipeline_step_spec.md` (all F1–F12 verified TRUE at HEAD `6723c86`; PO decisions D1–D11 confirmed)
- **Business request:** `.ai/plans/Problem_01.md`
- **Code context (Audit):** `.ai/tmp/code_context.md` (written by Auditor, read-only phase)
- **Baseline HEAD:** `6723c86` · remote `origin https://github.com/manicko/mko_data_cleaner.git` · 0 tags · no notebooks · no `.github/`

## Global constraints (from Audit + spec §8)

- C1. Keep setuptools + src-layout + `pyproject.toml`; no build-backend change; no `setup.py`.
- C2. Python `>=3.12`; dev env Windows 11 + uv (Python 3.14.7). This repo is a **standalone uv project** — no Django/Postgres/Docker. Tests run locally via `uv run pytest` (pyproject: `testpaths=["tests"]`, `pythonpath=["src"]`).
- C3. `build` / `check-wheel-contents` are not in the dev group; usable via `uv tool run ...`. CI will canonicalize builds.
- C4. Order: T4(API/factory groundwork in T1) → T1 → T2. Packaging alone does not fix the import crash.
- C6. `list_settings` is **repointed** to the user settings directory (D5) — not removed.
- C7. `initialize_settings` propagates `init_project`'s existing return value (user settings dir `Path`); `settings_dir` param added per D3.
- C8. `process_data` stays synchronous — docstring fix only for F7.
- C9. App-layer tests must point `APP_PATHS`/`USER_DIR` at a temp dir (construction has filesystem side effects).
- C10. Keep the 43 MB `data/example` sample out of package data and out of the notebook.
- C11. README must stop recommending `mko_data_cleaner.core.app_service.app_service`.
- Do not introduce unrelated documentation changes; `mko_bazuna/docs/00-overview/doc-maintenance-rules.md` does **not exist** in this repo — README.md is the documentation surface (project rule 14 applies).

## Block decomposition (dependency order)

| Block | Spec task | Summary | Depends on | Risk | Agents |
|---|---|---|---|---|---|
| B1 | T1 | Remove import-time init: lazy factory + `SettingsNotInitializedError` + logging at run time | — | **High** | Researcher → Planner → Implementor |
| B2 | T2 | Ship YAML templates in wheel/sdist (`package-data`, `MANIFEST.in`) | — | Medium | Planner → Implementor |
| B3 | T3 | Metadata/hygiene: SPDX license + copyright, `rich` declared, `__version__`, `list_settings` repoint, docstring fix, stale names/loggers | — | Low | Planner → Implementor |
| B4 | T4 | Public API contract: top-level re-exports, `process_data` result object (paths + timings + clean row counts, D8), `settings_dir` param | B1 | **High** | Researcher → Planner → Implementor |
| B5 | T5 | Example notebook `examples/notebooks/*.ipynb` + tiny synthetic sample (D9) | B1–B4 | Medium | Planner → Implementor |
| B6 | T6 | Smoke tests (import/init/error/e2e/wheel-content) + GitHub Actions verification workflow (D10) | B1–B4 | Medium-High | Planner → Implementor |
| B7 | T7 | Version `0.1.0`, git tag `v0.1.0`, notebook pin, GitHub install verification (D1/D11) | B1–B6 | Low-Medium | Planner → Implementor |

After B7: Documentation (Doc-specialist: README per R10 + notebook docs), then Final Validation (Validator).

## Key facts from Audit (for all agents)

- Import crash chain (F1): `app.py` imports singleton `app_service` (`core/app_service.py`) → `AppService.__init__` calls `prepare_log_paths()` → `log_config` cached_property → `LoggingSettings(**yaml_to_dict(...))`; `yaml_to_dict` returns implicit `None` on missing file → `TypeError`. `init` CLI is unreachable (F2). Only importer of the singleton is `app.py` (plus README).
- `APP_PATHS` importers: `app_service.py`, `init_service.py` — keep `APP_PATHS` working.
- Row counts available but discarded: `_import_data` accumulates loaded `rows_count` (logged); `CSVWorker.export_sql_to_csv` computes `total_rows`/`row_counter` and returns `None`; `DBWorker.data_chunk_to_sql` returns per-chunk counts (unused).
- `AppPaths` has no `reports` property (F8); `list_settings` must list the user settings dir.
- `LoggingSettings` requires `formatters/handlers/loggers/root` — the settings-presence gate must raise the dedicated error, never leak a pydantic `ValidationError`.
- `settings/` is not a package (no `__init__.py`) — `include-package-data = true` alone cannot ship it; explicit `[tool.setuptools.package-data]` required.
- `pydantic-settings` (runtime) and `pytest-django` (dev) are unused residue — out of scope; do not touch.
- Spec discrepancy (immaterial): wheel entry count is 18, not 19.

## Per-block task records

(Updated by Researcher/Planner agents as blocks execute. Format follows `.ai/tasks/templates/task_template.yaml` — semantic code units only, never line numbers.)

---

## B1 — T1: Remove import-time initialization (HIGH RISK)

**Agents:** Researcher (approach) → Planner (task) → Implementor (code).
**Return artifacts:** `{context_r}` → `{task_description}` appended below.

---

### {context_r} — Research findings (B1/T1)

**Confidence:** HIGH — all claims verified statically against HEAD `6723c86` and dynamically via reproduction on this machine (Windows 11, Python 3.14.7).

#### 0. Verified crash chain (F1)

`import mko_data_cleaner.app` (or `from mko_data_cleaner.app import process_data`) executes:

1. `app.py:12` → `from mko_data_cleaner.core.app_service import app_service`
2. `core/app_service.py:248` (module level) → `app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.user_dir))`
3. `AppService.__init__` (line 23) → `self.prepare_log_paths()`
4. `prepare_log_paths` (line 68) → reads `self.log_config` (cached_property, line 26-28)
5. `log_config` → `LoggingSettings(**utils.yaml_to_dict(self.app_paths.log_config))`
6. `utils.yaml_to_dict` (line 131-147): the target file is `user_settings_dir / "log_config.yaml"` (the **user** config dir, NOT the packaged template). In a fresh env that file does not exist → `FileNotFoundError` branch (line 146-147) logs and **falls through with no return** → implicit `None`.
7. `LoggingSettings(**None)` → `TypeError: argument after ** must be a mapping, not NoneType`.
8. Additionally `core/app_service.py:250` → `logging.config.dictConfig(...)` runs at import as a second side effect.

Reproduction on this machine: `PYTHONPATH=src py -c "import mko_data_cleaner.app"` reaches the crash (first failing on `unidecode` only because system Python lacks the project deps; once installed via `uv`/pip it proceeds to the documented `TypeError`). `paths.py` import was separately confirmed side-effect-free: `user_dir`/`user_settings_dir` resolve to path strings only — no filesystem mutation.

#### 1. Importer census (regression scope)

| Symbol | Importer(s) in active source | Notes |
|---|---|---|
| `app_service` singleton (`app_service.py:248`) | **`app.py:12`** only | Grep confirms zero other active importers. The `.ai/tmp/scratch/` copy is a build artifact, not source. |
| `APP_PATHS` (`paths.py:54`) | `app_service.py:13`, `init_service.py:4` | Both must keep working. `init_service` does **not** need the `AppService` singleton. |
| `AppService` class | `app_service.py:19` (def), `app.py` (via singleton) | No other importers. |
| `yaml_to_dict` | `app_service.py:28` (`log_config`), `app_service.py:32` (`app_config`) | Only two call sites. |
| `__main__` block (`app_service.py:252-253`) | `app_service.run_report(r"data\mvideo")` | References the singleton — must be rewritten. |
| Tests | none import `app`/`app_service`/`process_data`/`initialize_settings` | `tests/conftest.py` + `test_csv_service.py`/`test_db_service.py`/`test_dict_service.py` only use `DBWorker`, `CSVWorker`, `MappingDict`, and model fixtures. Removing the singleton breaks zero tests. |

README.md (L70-76) documents `from mko_data_cleaner.core.app_service import app_service` — a documentation consumer, to be migrated per D6/R10 (T3).

#### 2. Candidate alternatives — evaluation

| Candidate | Mechanism | Verdict |
|---|---|---|
| **(a) Explicit factory function** in `core/app_service.py` | `get_app_service(settings_dir=None) -> AppService`: builds `AppPaths`, gates on settings existence (raises `SettingsNotInitializedError`), constructs `AppService`, applies `dictConfig`, returns. | **CHOSEN.** Matches spec T1 ("factory that validates settings presence"). Explicit, trivially testable, supports `settings_dir` (D3), no module-level state. Smallest diff. Aligns with project rule "avoid overengineering" + "single responsibility" (factory = validation orchestration; `AppService` = processing state). |
| (b) PEP 562 module-level `__getattr__` lazy singleton | `from mko_data_cleaner.core.app_service import app_service` would resolve lazily on first attribute access. | **REJECTED.** Spec D6 explicitly forbids a backwards-compat shim for `core.app_service.app_service` ("the internal path disappears"). This candidate preserves exactly that path. |
| (c) Lazy `cached_property`/`__getattr__` on a holder class | A holder object exposes `app_service` lazily, constructing on first access. | **REJECTED.** Same D6 conflict — it keeps the `app_service` internal name alive as a public-ish accessor. Also adds an indirection class for no benefit over (a). |
| (d) Keep singleton, defer construction | A module-level variable stays `None` until first use, then constructs. | **REJECTED by D6.** "No shim" + "internal path disappears" means the module-level `app_service` name must not exist. A deferred singleton still reifies that name. (a) supersedes this with a named factory. |

**Net:** (a) is the only candidate consistent with D6. It also makes the "only when a command/function actually runs" requirement literal: nothing constructs or configures logging until `get_app_service()` is called.

#### 3. Logging-configuration timing (R1 side-effect rule)

Current side effects at import time:
- `core/app_service.py:248-250` — singleton construction + `dictConfig` (the F1 crash vector and a global side effect).
- `app.py:16` — `sys.stdout.reconfigure(line_buffering=True)` (mutates stdout; a side effect).
- `app.py:18` — `console = Console()` (instantiates `rich.console.Console`, which probes the terminal; borderline side effect).

**Design decision:**
- Move `logging.config.dictConfig(...)` **out** of module scope entirely. Apply it inside `get_app_service()` **after** `AppService(...)` construction (the factory is the single runtime entry point). This keeps `AppService.__init__` free of the *global* logging side effect while still running it only at run time.
- `prepare_log_paths()` **stays** in `AppService.__init__` (line 23). It is no longer import-time because `__init__` is only invoked by the factory, and the factory's settings-existence gate runs *first* — so `prepare_log_paths()` (which calls `self.log_config` → `yaml_to_dict`) is never reached when settings are missing. Its `resolver.ensure_file_parent(...)` dir creation therefore occurs only when a service actually runs. ✓
- In `app.py`: move `sys.stdout.reconfigure(...)` into the CLI entry (a `main()` wrapper or typer `@app.callback`), and make `console` a local/lazy object inside the command functions (or a small module-level lazy accessor). Both are T3-hygiene details but are noted here because R1 ("side-effect-free import") technically covers `stdout.reconfigure`. The *critical* T1 fix is the singleton removal; `stdout.reconfigure`/`Console()` do not crash and are not in the crash chain, but should be deferred for purity.
- Update `core/app_service.py:252-253` `__main__` block to `get_app_service().run_report(...)`.

#### 4. `yaml_to_dict` semantics + minimal guard location

**Decision — two layers (defense in depth):**

1. **Primary guard (factory):** `get_app_service()` checks that both `app_config.yaml` and `log_config.yaml` exist under `user_settings_dir` (via `AppPaths.app_config` / `AppPaths.log_config`). If any are absent → raise `SettingsNotInitializedError`. This runs *before* `AppService(...)` is constructed, so `prepare_log_paths()`/`log_config` cached_property never execute on the missing path. This fully satisfies R3/R1 (dedicated error, no pydantic `ValidationError` leak on the settings-missing path).

2. **Secondary guard (defense in depth):** fix `yaml_to_dict` in `core/utils.py` so both `except` branches return `{}` instead of falling through to implicit `None`. This matches the function's existing docstring ("or an empty dict if the file does not exist or is invalid") and eliminates the `TypeError` if the gate is ever bypassed. It does **not** by itself satisfy R3 (because `LoggingSettings(**{})` still raises a `ValidationError` for missing required `formatters/handlers/loggers/root`) — hence the explicit factory gate is mandatory, per risk R1.

**Minimal guard location = factory (`get_app_service`), NOT `AppService.__init__`.** Rationale:
- `AppService.__init__` is a constructor; mixing existence-validation policy into it couples "am I properly initialized?" to every direct construction (including tests that may construct with a temp config dir). The factory is the single production chokepoint.
- Keeping `__init__` "dumb" (just state setup + `prepare_log_paths`) means B6 tests can construct `AppService` with a temp user dir without triggering the "settings missing" error (the files exist in temp), while production always goes through the gate.

`yaml_to_dict` callers (`app_service.py:28`, `:32`) are unchanged in their call form; they just benefit from the `{}` return.

#### 5. `SettingsNotInitializedError` — definition

**Placement:** `src/mko_data_cleaner/core/errors.py` (alongside the existing `ConfigError`, `DataValidationError`, etc.).

**Base class:** `FileNotFoundError` — per spec D2/R3 (`SettingsNotInitializedError` "subclassing `FileNotFoundError`" — D2). This makes it catchable both as `SettingsNotInitializedError` and as `FileNotFoundError` by generic "missing config" handlers, while remaining distinct from `pydantic.ValidationError` (which is `ValueError`-based, not `FileNotFoundError`).

**Naming of missing files:** the two user-settings files checked are `app_config.yaml` and `log_config.yaml`, located under `AppPaths.user_settings_dir` (`platformdirs` user config dir / "settings" by default, or `settings_dir / "settings"` when overridden). The gate enumerates whichever of the two are missing.

**Remediation text:** the message must name `initialize_settings()` (D2). Proposed message template:
```
User settings are not initialized in <user_settings_dir>.
Missing file(s): app_config.yaml, log_config.yaml.
Run mko_data_cleaner.app.initialize_settings() to generate default settings.
```
This is English (C1), self-remediating (R3), and names the missing file(s) + the function (D2).

#### 6. Per-command / per-function wiring (after change)

`app.py` replaces the singleton import (line 12) with `from mko_data_cleaner.core.app_service import get_app_service`.

| Surface | How it obtains the service | Gate behavior |
|---|---|---|
| `init` CLI (`app.py:26-34`) | **Does not need `AppService`.** Calls `init_project(force=force)` directly (already independent of the singleton — it uses `APP_PATHS` via `init_service.py`). Becomes reachable once line 12 import no longer crashes. `settings_dir` param threaded in T4. | N/A (init *creates* settings, doesn't require them). |
| `run` CLI (`app.py:37-55`) | `service = get_app_service(settings_dir)` → `service.run_report(report)`. | Raises `SettingsNotInitializedError` if settings absent; CLI catches generic `Exception` (line 53) → prints + `typer.Exit(1)`. Optionally special-case the new error for a friendlier message (T3 nicety). |
| `list_settings` CLI (`app.py:58-69`) | **Does not need `AppService`.** Lists `APP_PATHS.user_settings_dir` (or the `settings_dir`-overridden dir). Repoints away from the non-existent `app_service.app_paths.reports` (F8 fix, D5). Gracefully handles a missing directory (prints "no settings found" rather than raising). | N/A (listing is read-only; must work even before `init`). |
| `process_data(report_path, settings_dir=None)` | `service = get_app_service(settings_dir)` → `service.run_report(Path(report_path))`. | Raises `SettingsNotInitializedError` if settings absent (R3/D2). Docstring fixed (F7: synchronous, not async). Signature gains `settings_dir` (D3). |
| `initialize_settings(force=False, settings_dir=None)` | Calls `init_project(force=force, settings_dir=settings_dir)` (returns the user settings dir `Path` — C7/D5). | N/A (creates settings). |
| `__main__` (app.py:88-89, typer entry) | Unchanged — `app()`. Entry point `mko-data-cleaner = "mko_data_cleaner.app:app"` stays (or repoints to `main()` if `stdout.reconfigure` is centralized — T3). | — |

**No circular imports:** `app.py` imports `get_app_service` from `core/app_service.py`; `core/app_service.py` imports only from sibling `core.*` modules (paths, models, errors, utils, csv_service, db_service, dict_service) — none import `app.py`. The dependency direction is strictly `app.py → core.*`, never reverse. ✓

#### 7. Fresh-venv import acceptance — achievability

**YES, achievable.** After removing `app_service.py:248-250` (singleton + `dictConfig`) and changing `app.py:12` to import `get_app_service` (a function, not a constructed object):

- `import mko_data_cleaner.app` executes only the module-level definitions (imports of `typer`, `rich.console.Console`, `get_app_service`, `init_project`, `list_files_in_directory`; `sys.stdout.reconfigure`; `console = Console()`; `app = typer.Typer(...)`; function/command defs). None of these construct `AppService`, read the user config dir, create directories, or call `dictConfig`. ✓
- `from mko_data_cleaner.app import process_data` works: `process_data` is a plain function (not called at import). ✓
- The settings gate is not triggered at import (the factory is never called). ✓
- `paths.py`'s `APP_PATHS = AppPaths(...)` and `USER_DIR = Path(user_config_dir(...))` are confirmed side-effect-free (path-string computation only; no `mkdir`, no file open, no `dictConfig`). ✓
- `init` CLI becomes reachable: importing `app.py` no longer hits the singleton, so `mko-data-cleaner init` runs `init_project` and copies the packaged YAML templates (once T2 ships them). ✓

Residual import-time statements to defer for full R1 purity (ranked): `sys.stdout.reconfigure` (move to `main()`/callback); `console = Console()` (make lazy). Neither causes a crash or reads/writes user config; both are noted as T3 hygiene.

#### 8. Exact changes in scope for B1 (semantic names, no line numbers)

- **`src/mko_data_cleaner/core/app_service.py`**
  - Add `get_app_service(settings_dir: Path | None = None) -> AppService` factory (builds `AppPaths`, gates via `_require_settings`, constructs, calls `dictConfig`, returns).
  - Add `_require_settings(app_paths: AppPaths) -> None` helper that raises `SettingsNotInitializedError` (missing file list + remediation message).
  - Remove module-level `app_service = AppService(...)` (singleton).
  - Remove module-level `logging.config.dictConfig(...)`.
  - Update `if __name__ == "__main__"` block to use `get_app_service().run_report(...)`.
  - Add `build_app_paths(settings_dir=None) -> AppPaths` helper (reused by factory and, in T4, by `init_project`).
  - `AppService.__init__`: keep `prepare_log_paths()` (now gate-safe); do **not** add `dictConfig` here (factory owns it).
- **`src/mko_data_cleaner/core/errors.py`**
  - Add `SettingsNotInitializedError(FileNotFoundError)`.
- **`src/mko_data_cleaner/core/utils.py`**
  - `yaml_to_dict`: add `return {}` to both `except` branches (defense in depth; matches existing docstring).
- **`src/mko_data_cleaner/app.py`**
  - L12: replace singleton import with `from mko_data_cleaner.core.app_service import get_app_service`.
  - `run`/`process_data`: call `get_app_service(settings_dir)` then `.run_report(...)`.
  - `list_settings`: rewrite to list `APP_PATHS.user_settings_dir` contents (no service, no `.reports` attr); handle missing dir.
  - Defer `sys.stdout.reconfigure` and `console = Console()` (T3 hygiene — out of strict B1, noted).
- **`src/mko_data_cleaner/core/init_service.py`** — no change required for B1 (it already uses `APP_PATHS` independently and returns `user_settings_dir`). `settings_dir` threading into `init_project` is a T4 item; note the shared `build_app_paths` helper will be used.
- **`src/mko_data_cleaner/core/paths.py`** — no change required for B1 (`APP_PATHS` stays as-is; `user_dir` computation is side-effect-free).

#### 9. Risks and open items for the Planner

1. **`settings_dir` semantic (D3/B4 handoff).** B1 designs `get_app_service(settings_dir=None)` with default `None` = platformdirs default. The exact meaning of `settings_dir` (is it the `user_dir` base, or the `.../settings` folder itself?) must be finalized in B4 alongside `initialize_settings`/`init_project` and the `build_app_paths` helper. Recommendation: treat `settings_dir` as the `user_dir` base (so YAMLs live at `settings_dir / "settings"`), mirroring the existing `USER_DIR` → `user_settings_dir` = `user_dir / "settings"` structure — this requires no `AppPaths` model change. Flag for B4 confirmation.
2. **`dictConfig` placement (factory vs `__init__`)** — chosen as factory-owned (§3). If B4's result-object refactor needs `AppService` constructed outside the factory (e.g., a direct constructor in tests), those call sites must also call `dictConfig` or accept unconfigured logging. Flag for B4/test design.
3. **`list_settings` before `init`** — must not raise when the user settings dir does not exist. Use `Path.exists()` guard; if absent, print an informational message. This deliberately does **not** raise `SettingsNotInitializedError` (listing is diagnostic, not a run). Confirm this asymmetry is acceptable.
4. **`yaml_to_dict` shared helper** — fixing it to return `{}` is a behavior change for any *future* caller that might rely on `None` as a sentinel. Currently only two call sites (both in `app_service.py`, both gated). Low risk; flag for awareness.
5. **Entry-point + `stdout.reconfigure` centralization** — if `stdout.reconfigure` moves to a `main()` wrapper, the `[project.scripts]` entry point (`mko_data_cleaner.app:app`) may need repointing to `mko_data_cleaner.app:main`. This is a T3 metadata change; coordinate so the CLI still launches.
6. **Version bump to `0.1.0`** — B1's import fix is independent of version, but the fresh-venv import acceptance test should run against the *installed* package. Version `0.1.0` (D7/D11) is a T3/T7 change; B1 design does not depend on it but the test harness will.

---

### {task_description} — Implementation Task (B1/T1)

```yaml
id: b1_t1_remove_import_time_init

title: "B1/T1 — Remove import-time initialization: lazy factory + SettingsNotInitializedError + runtime logging"

priority: high

depends_on: []

description: >
  Break the first-run import crash (F1/F2) so that `import mko_data_cleaner.app`
  and `from mko_data_cleaner.app import process_data, initialize_settings` succeed
  with no user settings present, create no directories, and configure no logging.

  A lazy factory `get_app_service(settings_dir=None)` replaces the module-level singleton:
  it validates that both `app_config.yaml` and `log_config.yaml` exist in the user
  settings directory before constructing `AppService`, raising `SettingsNotInitializedError`
  (subclass of `FileNotFoundError`) when absent. The `dictConfig` call moves from
  module import time into the factory, after construction. `AppService.__init__`
  keeps calling `prepare_log_paths()` (now safe because the gate runs first).

  Per spec T1, only. Out of scope: packaging/MANIFEST, license/rich/version/list_settings
  metadata hygiene beyond the repoint, result object, README, notebook, CI, tag (B3-B7).

goals:
  - "Import `mko_data_cleaner.app` succeeds in a fresh environment with no user settings"
  - "No filesystem side effects (no directory creation, no logging config) occur at import"
  - "`process_data` and CLI `run` raise `SettingsNotInitializedError` (catchable as `FileNotFoundError`) when settings are missing"
  - "`init` CLI becomes reachable and works without prior settings"
  - "`list_settings` lists the user settings directory contents without raising when the directory is missing"
  - "Existing dict/db/csv service tests still pass"
  - "`yaml_to_dict` returns `{}` in both except branches (defense in depth)"
  - "`initialize_settings` and `process_data` accept an optional `settings_dir` parameter"

files:
  - path: src/mko_data_cleaner/core/errors.py
    targets:
      - type: module
    semantic_anchors:
      - After the existing `ConfigError` class, before end-of-file
    changes:
      - action: add_code
        description: >
          Add `SettingsNotInitializedError(FileNotFoundError)` with an English message
          that names the missing settings file path(s) and the remediation
          `initialize_settings()`.
        code_hint: |
          class SettingsNotInitializedError(FileNotFoundError):
              """Raised when user settings (app_config.yaml / log_config.yaml) are missing."""

              def __init__(self, user_settings_dir: Path, missing_files: list[str]):
                  missing = ", ".join(missing_files)
                  super().__init__(
                      f"User settings are not initialized in {user_settings_dir}.\n"
                      f"Missing file(s): {missing}.\n"
                      f"Run mko_data_cleaner.app.initialize_settings() "
                      f"to generate default settings."
                  )

  - path: src/mko_data_cleaner/core/utils.py
    targets:
      - type: function
        name: yaml_to_dict
    semantic_anchors:
      - Both `except yaml.YAMLError` and `except FileNotFoundError` branches in `yaml_to_dict`
    changes:
      - action: modify
        description: >
          Add `return {}` to both except branches of `yaml_to_dict` so the function
          never returns implicit `None` on missing/invalid YAML. Matches the existing
          docstring ("or an empty dict if the file does not exist or is invalid").
          No behavior change in the success branch.

  - path: src/mko_data_cleaner/core/app_service.py
    targets:
      - type: class
        name: AppService
    semantic_anchors:
      - `AppService.__init__` (keep `prepare_log_paths()` call, no `dictConfig` here)
      - `log_config` / `app_config` cached_properties (unchanged)
      - Module-level code at the bottom: `app_service = AppService(...)` singleton and
        `logging.config.dictConfig(...)` (REMOVE both)
      - `if __name__ == "__main__"` block (rewrite to use factory)
    changes:
      - action: add_code
        description: >
          Add `build_app_paths(settings_dir: Path | None = None) -> AppPaths` helper
          that returns `APP_PATHS` when `settings_dir is None`, or constructs an
          `AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=settings_dir)`
          when provided.
        code_hint: |
          def build_app_paths(settings_dir: Path | None = None) -> AppPaths:
              if settings_dir is None:
                  return APP_PATHS
              return AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir))
      - action: add_code
        description: >
          Add `_require_settings(app_paths: AppPaths) -> None` helper that checks
          both `app_paths.app_config` and `app_paths.log_config` exist; collects
          the missing file names and raises `SettingsNotInitializedError` listing
          them.
        code_hint: |
          def _require_settings(app_paths: AppPaths) -> None:
              missing: list[str] = []
              for label, path in (
                  ("app_config.yaml", app_paths.app_config),
                  ("log_config.yaml", app_paths.log_config),
              ):
                  if not path.exists():
                      missing.append(label)
              if missing:
                  raise SettingsNotInitializedError(
                      user_settings_dir=app_paths.user_settings_dir,
                      missing_files=missing,
                  )
      - action: add_code
        description: >
          Add factory `get_app_service(settings_dir: Path | None = None) -> AppService`
          that: builds `AppPaths` via `build_app_paths`, calls `_require_settings`
          (gate — raises before construction), constructs `AppService(app_paths, resolver)`,
          then applies `logging.config.dictConfig(service.log_config.model_dump())`
          and returns the service.
        code_hint: |
          def get_app_service(settings_dir: Path | None = None) -> AppService:
              app_paths = build_app_paths(settings_dir)
              _require_settings(app_paths)
              service = AppService(
                  app_paths=app_paths,
                  resolver=PathResolver(app_paths.user_dir),
              )
              logging.config.dictConfig(service.log_config.model_dump())
              return service
      - action: remove
        description: >
          Remove the module-level singleton `app_service = AppService(...)` and the
          module-level `logging.config.dictConfig(app_service.log_config.model_dump())`.
      - action: modify
        description: >
          Update `if __name__ == "__main__"` block to use `get_app_service()` then
          `.run_report(...)`.

  - path: src/mko_data_cleaner/app.py
    targets:
      - type: module
      - type: function
        name: process_data
      - type: function
        name: initialize_settings
      - type: function
        name: run
      - type: function
        name: list_settings
    semantic_anchors:
      - `from mko_data_cleaner.core.app_service import app_service` (REPLACE with `get_app_service`)
      - `run` command body: `app_service.run_report(report)` → `get_app_service(settings_dir).run_report(report)`
      - `list_settings` body: `app_service.app_paths.reports` → list `APP_PATHS.user_settings_dir`
      - `process_data` signature and body: add `settings_dir` param, call `get_app_service(settings_dir).run_report(path)`
      - `initialize_settings` signature and body: add `settings_dir=None` param
      - `__all__` list (no change to contents — just the import symbol changes)
    changes:
      - action: modify
        description: >
          Replace `from mko_data_cleaner.core.app_service import app_service` with
          `from mko_data_cleaner.core.app_service import get_app_service`.
        code_hint: |
          from mko_data_cleaner.core.app_service import get_app_service
      - action: modify
        description: >
          Add `settings_dir` parameter to `process_data` (signature
          `process_data(report_path, settings_dir=None)`) and route through
          `get_app_service(settings_dir)`.
        code_hint: |
          def process_data(report_path: str | Path, settings_dir: Path | None = None) -> None:
              service = get_app_service(settings_dir)
              path = Path(report_path)
              if not path.exists():
                  raise FileNotFoundError(f"File not found: {path}")
              service.run_report(path)
      - action: modify
        description: >
          Add `settings_dir` parameter to `initialize_settings` (signature
          `initialize_settings(force=False, settings_dir=None)`). Per the init_project
          finding (below), `init_project` does NOT accept `settings_dir` in B1, so
          the parameter is accepted-and-forwarded as a no-op when `None`; B4 wires
          full threading.
        code_hint: |
          def initialize_settings(force: bool = False, settings_dir: Path | None = None) -> Path:
              """Wrapper for Jupyter/Airflow"""
              # settings_dir threading into init_project is wired in B4/T4.
              # In B1, init_project uses APP_PATHS (platformdirs default).
              return init_project(force=force)
      - action: modify
        description: >
          Rewire `run` command to obtain the service via `get_app_service(settings_dir)`
          where `settings_dir` comes from a new `--settings-dir` typer Option (default `None`).
          The existing `except Exception` handler stays; it will catch and surface
          `SettingsNotInitializedError` naturally.
        code_hint: |
          @app.command()
          def run(
              report: Annotated[Path, typer.Argument(...)] = ...,
              verbose: bool = typer.Option(True, "--verbose", "-v"),
              settings_dir: Path | None = typer.Option(
                  None, "--settings-dir", "-s", exists=True, file_okay=False,
                  help="User settings directory override",
              ),
          ):
              ...
              service = get_app_service(settings_dir)
              service.run_report(report)
              ...
      - action: modify
        description: >
          Rewire `list_settings` to list `APP_PATHS.user_settings_dir` (or `settings_dir`
          when provided) contents. Handle a missing directory gracefully (print a
          diagnostic message, do not raise). Remove the broken `app_service.app_paths.reports`
          reference (F8).
        code_hint: |
          @app.command()
          def list_settings(
              settings_dir: Path | None = typer.Option(
                  None, "--settings-dir", "-s", exists=True, file_okay=False,
                  help="User settings directory override",
              ),
          ):
              """List files in the user settings directory"""
              target = AppPaths... or APP_PATHS.user_settings_dir
              if not target.exists():
                  console.print("[yellow]Settings not found. Run 'mko-data-cleaner init' first.[/yellow]")
                  return
              files = list_files_in_directory(target, extensions=("yaml",), include_subfolders=True)
              for f in sorted(files):
                  console.print(f"• {f}")
      - action: modify
        description: >
          Import `APP_PATHS` and `AppPaths` from `mko_data_cleaner.core.paths` for
          `list_settings` directory resolution.

out_of_scope:
  - "B3: docstring F7 (beyond the async->sync wording already corrected in process_data), rich F4, stale name F10, __version__ F6"
  - "B4: top-level __init__.py re-exports with __all__, result object, full settings_dir threading into init_service.py"
  - "B2: packaging/MANIFEST/package-data, license text, wheel shipping"
  - "B5-B7: notebook, CI, version bump, tag"

init_project_settings_dir_finding: >
  READING core/init_service.py confirms: `init_project(force=False) -> Path` does
  NOT accept a `settings_dir` parameter. It hardcodes `APP_PATHS.user_settings_dir`
  (platformdirs user config dir / "settings") and returns that Path. The private
  `_copy_templates(destination, force)` likewise uses `APP_PATHS.app_settings_dir`
  as its source. Therefore in B1 the `settings_dir` parameter is accepted on
  `initialize_settings` and `process_data` but only forwarded when `None` (no-op).
  Full threading of `settings_dir` into `init_project(_copy_templates)` requires
  `AppPaths` to accept a `user_dir` override (it already does in its constructor)
  AND `init_project`/`_copy_templates` to accept and use it — that is a B4/T4 task.
  In B1, `get_app_service(settings_dir=tmp)` constructs an `AppPaths` with
  `user_dir=settings_dir`, so a test that pre-populates `tmp / "settings"` with the
  YAML templates (copied from `APP_PATHS.app_settings_dir`) will pass the gate.

acceptance_criteria:
  - "import mko_data_cleaner.app succeeds with no user settings present (fresh-platformdirs dir)"
  - "import does not create the user settings directory (assert dir absent before and after import)"
  - "import does not call logging.config.dictConfig (no logging handlers configured)"
  - "process_data(<tmp_report>) without settings raises SettingsNotInitializedError"
  - "SettingsNotInitializedError message names initialize_settings() and the missing file(s)"
  - "SettingsNotInitializedError is catchable as FileNotFoundError"
  - "SettingsNotInitializedError is NOT a pydantic ValidationError (not a subclass of ValueError)"
  - "CLI `run` without settings raises SettingsNotInitializedError (typer CliRunner)"
  - "CLI `list_settings` lists user settings dir files when they exist"
  - "CLI `list_settings` does not raise when user settings dir is missing (prints diagnostic)"
  - "CLI `init` works without prior settings (calls init_project, copies templates)"
  - "After initializing settings to a tmp dir, get_app_service(settings_dir=tmp) constructs; service.log_config and service.app_config are populated"
  - "process_data past the gate does NOT raise SettingsNotInitializedError (may fail later on missing report data — assert error is not SettingsNotInitializedError)"
  - "yaml_to_dict returns {} for missing file and {} for invalid YAML (both except branches)"
  - "Existing tests (test_csv_service, test_db_service, test_dict_service) still pass"

tests_to_create:
  - path: tests/test_app.py
    tests:
      - test_import_app_does_not_create_settings_dir
      - test_process_data_without_settings_raises_settings_error
      - test_process_data_error_is_file_not_found
      - test_process_data_error_names_initialize_settings
      - test_run_cli_without_settings_raises_settings_error
      - test_list_settings_lists_when_dir_exists
      - test_list_settings_does_not_raise_when_dir_missing
      - test_init_cli_creates_settings_in_tmp_dir
      - test_get_app_service_constructs_after_initialization
      - test_yaml_to_dict_returns_empty_dict_on_missing_file
      - test_yaml_to_dict_returns_empty_dict_on_invalid_yaml

verification_commands:
  - "uv run pytest tests/ -k test_app"
  - "uv run pytest"
  - "uv run ruff check src/ tests/"

risks:
  - "R1: SettingsNotInitializedError must NOT leak a pydantic ValidationError on the settings-missing path — the factory gate runs before AppService construction, so yaml_to_dict/LoggingSettings never execute on a missing file."
  - "R2: Removing the module-level app_service singleton is safe — app.py is the only active importer (grep-verified)."
  - "R3: build_app_paths must produce the same AppPaths semantics for settings_dir=None as APP_PATHS (app_dir, app_name, user_dir must match)."
  - "R4: stdout.reconfigure and Console() remain at module level in app.py — these do not crash and are out of B1 scope (T3 hygiene). Tests must not assert against them."

# Extra context (verbatim from plan §8, B1 design decisions)
extra_context:
  factory_placement: "Factory get_app_service lives in core/app_service.py; AppService __init__ stays 'dumb' (validation in the factory gate, not the constructor)."
  dictconfig_placement: "dictConfig moves into get_app_service after AppService construction — single runtime entry point."
  settings_dir_semantics: "settings_dir is the user_dir base (YAMLs live at settings_dir / 'settings'), mirroring USER_DIR -> user_settings_dir structure. AppPaths.user_settings_dir = user_dir / 'settings'."
  init_project: "init_project uses APP_PATHS.user_settings_dir (platformdirs default). B1 accepts settings_dir on initialize_settings but does not thread it into init_project — B4 wires that."
  console: "console = Console() stays module-level in app.py (B3 hygiene, not B1)."
```

---

## B2 — T2: Ship package templates in wheel and sdist

**Agents:** Planner → Implementor.

---

### {context_r} — Research findings (B2/T2)

**Confidence:** HIGH — all claims verified statically against HEAD `6723c86` and dynamically via empirical `uv build` reproduction on this machine (Windows 11, Python 3.14.7).

#### 0. Verified packaging facts

- **`pyproject.toml`**: `[tool.setuptools]` (L47-50) has `include-package-data = true` but **no** `[tool.setuptools.package-data]` table. `[tool.setuptools.packages.find]` (L52-56) has `where=["src"]`, `include=["mko_data_cleaner","mko_data_cleaner.*"]`, `namespaces=false`. No `package-data` directive exists.
- **`settings/` directory**: `src/mko_data_cleaner/settings/` contains only `app_config.yaml` and `log_config.yaml` — **no `__init__.py`**, so it is not a package; `include-package-data` cannot discover it, an explicit `package-data` table is required.
- **`MANIFEST.in`** (9 lines, read confirmed): `include README.md`; `include LICENSE.txt`; `include requirements.txt` (non-existent → build warning); `recursive-include mko_get_mediascope_data *.json *.yaml` (stale project name → no matches → warning); blank line; `exclude tests`; `exclude mko_get_medioscope_data/settings/connections/mediascope.json` (stale → no matches); `exclude *.git`; `global-exclude *.pyc *.o`.
- **Empirical build at HEAD**: wheel has **18 entries, zero `settings/*.yaml`**; sdist likewise omits both YAMLs; `uv build` emits "no files found matching" warnings for `requirements.txt` and `mko_get_mediascope_data`.
- **`.gitignore` coverage (verified, no finding)**: `build/` (L11), `dist/` (L13, L221), `*.egg-info/` (L24), `sdist/` (L20) — all build artifacts are properly ignored for commit.
- **`data/` exclusion (verified, no finding)**: `data/example/raw_data/example_data.csv` is VCS-force-tracked (43 MB, Cyrillic header) but lives at the repo root outside any package directory. The sdist does not include it (confirmed by `uv build` copy list). The `recursive-include` rule added here targets only `src/mko_data_cleaner/settings/*.yaml`, so `data/` is not introduced into any distribution.

#### 1. Fix mechanism (verified by audit)

Adding `[tool.setuptools.package-data]` with `mko_data_cleaner = ["settings/*.yaml"]` puts both YAMLs in the wheel (package-data globs within the `mko_data_cleaner` package directory, matching `src/mko_data_cleaner/settings/*.yaml`). Adding `recursive-include src/mko_data_cleaner/settings *.yaml` to MANIFEST.in puts them in the sdist. Both changes are independent and complementary: `package-data` controls wheel inclusion; MANIFEST.in controls sdist inclusion. The audit confirmed this exact combination yields 21 wheel entries with both YAMLs present and no build warnings.

#### 2. Risks

- **R1**: `include-package-data = true` coexists with the new `package-data` table — both are additive and the audit verified the combination works with no conflict. No change needed to `include-package-data`.
- **R2**: MANIFEST.in `recursive-include` path uses the `src/` prefix (consistent with `packages.find` `where=["src"]`); the glob matches from the repo root, not from inside `src/`.
- **R3**: `settings/` must NOT gain an `__init__.py` — `package-data` glob inclusion is the mechanism per block instructions. Adding `__init__.py` would change the package surface and is explicitly forbidden here.

---

### {task_description} — Implementation Task (B2/T2)

```yaml
id: b2_t2_ship_yaml_templates

title: "B2/T2 — Ship YAML settings templates in wheel and sdist"

priority: medium

depends_on: []

description: >
  Fix wheel and sdist packaging so that every built distribution contains
  `mko_data_cleaner/settings/app_config.yaml` and `mko_data_cleaner/settings/log_config.yaml`.
  These are the templates that `initialize_settings()` copies to the user config
  directory after pip install (per `init_service._copy_templates` source =
  `APP_PATHS.app_settings_dir`). At HEAD the wheel has 18 entries and no YAMLs,
  the sdist has none, and `uv build` emits "no files found matching" warnings from
  stale MANIFEST.in rules. Two minimal changes fix it: (1) add an explicit
  `[tool.setuptools.package-data]` entry so setuptools ships the non-package
  `settings/` data directory in the wheel; (2) correct MANIFEST.in to drop the
  stale `mko_get_mediascope_data` rules + non-existent `requirements.txt` and add
  a `recursive-include` rule for the sdist. No code, no tests, no runtime changes.
  B2 is independent of B1; both are required before release (T7).

goals:
  - "Wheel contains mko_data_cleaner/settings/app_config.yaml"
  - "Wheel contains mko_data_cleaner/settings/log_config.yaml"
  - "Sdist contains src/mko_data_cleaner/settings/app_config.yaml"
  - "Sdist contains src/mko_data_cleaner/settings/log_config.yaml"
  - "Build emits no 'no files found matching' warnings"
  - "43 MB data/ sample is NOT included in wheel or sdist"
  - "Runtime behavior unchanged (uv run pytest stays green)"
  - "Build artifacts cleaned up; dist/ and build/ stay gitignored"

files:
  - path: pyproject.toml
    targets:
      - type: table
        name: tool.setuptools.package-data
    semantic_anchors:
      - "After the [tool.setuptools.packages.find] section (after the line namespaces = false)"
      - "No other pyproject.toml field is modified in this block"
    changes:
      - action: add_code
        description: >
          Add a new [tool.setuptools.package-data] table immediately after the
          [tool.setuptools.packages.find] section. The key mko_data_cleaner maps
          the glob ["settings/*.yaml"] so that both YAML template files inside the
          non-package settings/ directory are included in the built wheel. This is
          the only pyproject.toml change in this block.
        code_hint: |
          [tool.setuptools.package-data]
          mko_data_cleaner = ["settings/*.yaml"]

  - path: MANIFEST.in
    targets:
      - type: file
    semantic_anchors:
      - "include requirements.txt (non-existent file -> build warning)"
      - "recursive-include mko_get_mediascope_data *.json *.yaml (stale project name)"
      - "exclude mko_get_mediascope_data/settings/connections/mediascope.json (stale)"
    changes:
      - action: remove
        description: "Remove the line include requirements.txt — the file does not exist at the repo root and produces a 'no files found matching' build warning."
      - action: remove
        description: "Remove the line recursive-include mko_get_mediascope_data *.json *.yaml — stale project name, matches zero files, produces a 'no files found matching' warning."
      - action: remove
        description: "Remove the line exclude mko_get_mediascope_data/settings/connections/mediascope.json — stale project name, no matching files exist."
      - action: add_code
        description: >
          Add recursive-include src/mko_data_cleaner/settings *.yaml so the
          packaged YAML templates are included in the sdist tarball. Keep the
          existing directives: include README.md, include LICENSE.txt,
          exclude tests, exclude *.git, global-exclude *.pyc *.o. Do NOT add
          license-files (belongs to B3/T3). The resulting MANIFEST.in is:
        code_hint: |
          include README.md
          include LICENSE.txt
          recursive-include src/mko_data_cleaner/settings *.yaml

          exclude tests
          exclude *.git
          global-exclude *.pyc *.o

acceptance_criteria:
  - "uv build succeeds with no errors"
  - "Wheel (dist/*.whl) namelist contains mko_data_cleaner/settings/app_config.yaml"
  - "Wheel (dist/*.whl) namelist contains mko_data_cleaner/settings/log_config.yaml"
  - "Sdist (dist/*.tar.gz) contains src/mko_data_cleaner/settings/app_config.yaml"
  - "Sdist (dist/*.tar.gz) contains src/mko_data_cleaner/settings/log_config.yaml"
  - "Build output contains no 'no files found matching' substring"
  - "Wheel contains NO path under data/ or mko_data_cleaner/data/"
  - "Sdist contains NO path under data/"
  - "dist/, build/, and *.egg-info removed after verification"
  - "git status shows only pyproject.toml and MANIFEST.in as modified (build artifacts gitignored)"
  - "uv run pytest passes (packaging-only change, no runtime impact)"

verification_commands:
  - "uv build"
  - "uv run python -c \"import zipfile,glob; z=zipfile.ZipFile(glob.glob('dist/*.whl')[0]); names=z.namelist(); assert 'mko_data_cleaner/settings/app_config.yaml' in names, 'app_config.yaml MISSING from wheel'; assert 'mko_data_cleaner/settings/log_config.yaml' in names, 'log_config.yaml MISSING from wheel'; print(f'Wheel OK: both YAMLs present, {len(names)} entries')\""
  - "uv run python -c \"import tarfile,glob; t=tarfile.open(glob.glob('dist/*.tar.gz')[0]); names=[m.name for m in t.getmembers()]; assert any(n.endswith('settings/app_config.yaml') for n in names), 'app_config.yaml MISSING from sdist'; assert any(n.endswith('settings/log_config.yaml') for n in names), 'log_config.yaml MISSING from sdist'; print('Sdist OK: both YAMLs present')\""
  - "uv run python -c \"import zipfile,glob; z=zipfile.ZipFile(glob.glob('dist/*.whl')[0]); leaked=[n for n in z.namelist() if 'data/' in n]; assert not leaked, 'data/ leaked into wheel: '+str(leaked); print('No data/ in wheel')\""
  - "uv run python -c \"import tarfile,glob; t=tarfile.open(glob.glob('dist/*.tar.gz')[0]); leaked=[m.name for m in t.getmembers() if '/data/' in m.name.replace(chr(92),'/')]; assert not leaked, 'data/ leaked into sdist: '+str(leaked); print('No data/ in sdist')\""
  - "uv run python -c \"import shutil,glob; shutil.rmtree('dist',ignore_errors=True); shutil.rmtree('build',ignore_errors=True); [shutil.rmtree(d,ignore_errors=True) for d in glob.glob('**/*.egg-info',recursive=True)]; print('Cleaned dist/, build/, *.egg-info')\""
  - "uv run pytest"

out_of_scope:
  - "B1/T1: import-time singleton removal, SettingsNotInitializedError, yaml_to_dict None-safety"
  - "B3/T3: SPDX license, rich dependency, __version__, list_settings/loggers cleanup, stale project-name references, docstring fixes, version bump"
  - "B6/T6: wheel-content assertion test in tests/ (CI gate — not this block)"
  - "B7/T7: version 0.1.0 bump, git tag, publish"
  - "Do NOT add settings/__init__.py (keep settings/ as non-package data; that is the simplest correct mechanism)"

risks:
  - "R1: include-package-data = true coexists with the new package-data table — both are additive; the audit verified this exact combination includes both YAMLs in the wheel with no conflict."
  - "R2: MANIFEST.in recursive-include uses the src/ prefix (consistent with src-layout); the glob matches from the repo root."
  - "R3: settings/ must NOT gain an __init__.py — package-data glob inclusion is the mechanism per block instructions."
```

---

## B3 — T3: Metadata, dependency and hygiene fixes

**Agents:** Planner → Implementor.

---

### {context_r} — Verified facts (B3/T3)

**Confidence:** HIGH — all claims verified statically against current HEAD and
dynamically via `uv run` on this machine (Windows 11, Python 3.14.7, uv 0.9.26).

#### 0. `pyproject.toml` — current state (B3-relevant keys only)

- `[build-system]` line: `requires = ["setuptools>=61.0", "wheel>=0.37.1"]`.
- `[project]` — NO `license` or `license-files` fields. The `classifiers` list
  includes `"License :: OSI Approved :: MIT License"` (deprecated, and wrong —
  LICENSE.txt is BSD-3-Clause). Remaining classifiers: Python 3.12, 3.13, 3.14,
  OS Independent.
- `[project].dependencies` — 13 entries, does NOT include `rich` (grep-verified:
  zero hits for `rich` in the dependencies array). `rich` IS imported at
  `app.py` and used at 8 `console.print()` call sites.
- `[tool.setuptools.package-data]` — already present (B2 done):
  `mko_data_cleaner = ["settings/*.yaml"]`.
- `version = "0.0.1"` — stays as-is (B7 bumps to 0.1.0).
- `[dependency-groups] dev` — has `pytest-mock>=3.15.1`, NOT `pytest-django`.

#### 1. `LICENSE.txt` — copyright

- Line 1: `Copyright (c) Audrey Roy Greenfeld and individual contributors.`
- Line 13: `Neither the name of Audrey Roy Greenfeld nor the names of its contributors`
- Text is BSD-3-Clause, entirely in English. Project author (pyproject `authors`):
  `mko <manicko@yandex.ru>`.

#### 2. `src/mko_data_cleaner/__init__.py` — empty

- 0 bytes. `uv run python -c "import mko_data_cleaner; print(getattr(mko_data_cleaner, '__version__', 'NOT SET'))"` → `NOT SET`.

#### 3. Stale `mko_get_mediascope_data` references in active files

| File | Line | Text |
|---|---|---|
| `src/mko_data_cleaner/app.py` | 2 | `mko_get_mediascope_data — основной публичный интерфейс` |
| `.gitignore` | 224 | `src/mko_get_mediascope_data/settings/connections/*` |
| `.gitignore` | 225 | `!src/mko_get_mediascope_data/settings/connections/mediascope_RENAME_.yaml` |
| `.kilo/commands/audit/phases/01-audit-entry-architecture.md` | 17 | `python -m mko_get_mediascope_data` |
| `.kilo/commands/audit/phases/02-audit-config-secrets.md` | 16 | `~/.config/mko_get_mediascope_data/` |

The `src/mko_get_mediascope_data/` directory does NOT exist — `src/mko_data_cleaner/`
settings/ contains only `app_config.yaml` + `log_config.yaml` (confirmed by listing
the directory). `.gitignore` lines 222-223 (`data/*` / `!data/output/.gitkeep`) are
NOT stale and stay.

#### 4. Stale `tests` / `telebot` loggers in `log_config.yaml`

- `"loggers"` object has `"tests"` (lines 51-55) and `"telebot"` (lines 56-60).
- Grep for `getLogger("tests")` / `getLogger("telebot")` in `*.py` → **zero matches**
  (confirmed). These are leftovers from the old Mediascope-telebot integration.
- Keep `"__main__"` logger + `root` handler + all formatters/handlers intact.
  Preserve the JSON-brace YAML format. File ships as package data and is copied by
  `init`; the fix propagates to new user configs. Existing copied configs are
  unaffected (template-only change).

#### 5. Setuptools version decision (PEP 639 support) — **YES, bump required**

**Decision:** Bump `build-system.requires` from `["setuptools>=61.0", ...]` to
`["setuptools>=77.0", "wheel>=0.37.1"]`.

**Justification:**
- PEP 639 SPDX `license` expression (string form) + `license-files` field were
  introduced in setuptools v77.0.0. Verified via setuptools docs: *"Support for
  project.license-files and SPDX license expressions in project.license (PEP 639)
  were introduced in version 77.0.0."* The current `setuptools>=61.0` constraint
  allows builds with setuptools 61-76, which cannot parse `license = "BSD-3-Clause"`
  as an SPDX string or recognize `license-files`.
- License classifiers are deprecated starting from setuptools 77.0.0 and fully
  deprecated in 82+. The current MIT classifier would emit
  `SetuptoolsDeprecationWarning: License classifiers are deprecated` during `uv build`.
- The bump is a **minimal edit** (only the version number in `build-system.requires`
  changes). `wheel>=0.37.1` stays unchanged. No runtime impact; it is a build-time
  requirement independent of `requires-python >= 3.12`. Build frontends (uv, pip)
  install setuptools in an isolated build environment, so declaring the minimum is
  sufficient.
- Without this bump, B3 cannot add PEP 639 license fields without risking build
  failures or warning emissions on older setuptools.

#### 6. Already DONE in earlier blocks (do NOT redo)

- Factory `get_app_service` / `build_app_paths` in `core/app_service.py` — `app.py`
  imports `get_app_service`, not the singleton.
- `SettingsNotInitializedError` exists in `core/errors.py`.
- `process_data` docstring already reads "Synchronous entry point for non-CLI
  contexts (Jupyter / Airflow)." (F7 fixed in B1).
- `list_settings` already repointed to `user_settings_dir` via `build_app_paths` (F8
  fixed in B1).
- `[tool.setuptools.package-data]` already present in `pyproject.toml`.
- `MANIFEST.in` already corrected (no stale refs).

#### 7. Unused dependencies — out of scope, leave untouched

- `pydantic-settings>=2.12.0`: declared in `[project].dependencies` but **no
  `pydantic_settings` import** found anywhere in `src/` (grep-verified). Known
  residue. Do not remove (spec does not ask; scope is minimal).
- `pytest-django`: audit (`code_context.md`) flagged it as unused dev dependency,
  but it is **NOT present** in the current `[dependency-groups] dev` list (which
  has `pytest-mock`). The `pytest-django` mention in `.kilo/rules/commands.md:68`
  is for the mko-bazuna-test Docker/Django project, not this repo. No action needed.

---

### {task_description} — Implementation Task (B3/T3)

```yaml
id: b3_t3_metadata_hygiene

title: "B3/T3 — Metadata, dependency and hygiene fixes (SPDX license, rich, __version__, stale refs, stale loggers)"

priority: low

depends_on: []

description: >
  Make the project publishable and remove the last metadata/dependency/hygiene
  residue (F4, F5, F6, F10). Five changes:

  1. Replace the deprecated MIT license classifier with a PEP 639 SPDX expression
  (license = "BSD-3-Clause") + license-files = ["LICENSE.txt"], and bump
  build-system.requires to setuptools>=77.0 (SPDX support). Keep Programming
  Language and OS classifiers.

  2. Correct LICENSE.txt copyright holder from Audrey Roy Greenfeld and
  individual contributors to mko (matching pyproject.toml authors), including
  clause 3.

  3. Declare rich>=13.0.0 in [project].dependencies (imported at app.py, used at
  8 call sites, currently transitive-only via typer). Place alphabetically after
  PyYAML.

  4. Expose __version__ in src/mko_data_cleaner/__init__.py via
  importlib.metadata.version("mko_data_cleaner") with a PackageNotFoundError
  fallback to "0.0.1" (matching pyproject version). Tests run from source
  (pythonpath=src, not installed), so the guard path is exercised.

  5. Remove stale mko_get_mediascope_data references in app.py docstring,
  .gitignore (2 lines), and two .kilo audit-phase handbooks (minimal textual fix
  only). Remove dead tests/telebot logger entries from log_config.yaml.

  Add tests/test_metadata.py with two tests. Do NOT touch: version number
  (stays 0.0.1; B7 bumps), process_data docstring (already B1-fixed),
  list_settings (already B1-fixed), MANIFEST.in (already B2-fixed),
  [tool.setuptools.package-data] (already B2-added), README (B4), CI (B6),
  core/**, or B4 __init__.py re-exports.

goals:
  - "pyproject.toml [project] has license = \"BSD-3-Clause\" and license-files = [\"LICENSE.txt\"]"
  - "pyproject.toml classifiers has no License classifier entry"
  - "pyproject.toml [build-system].requires = [\"setuptools>=77.0\", \"wheel>=0.37.1\"]"
  - "rich>=13.0.0 is in [project].dependencies (after PyYAML entry)"
  - "LICENSE.txt first line is 'Copyright (c) mko'"
  - "LICENSE.txt clause 3 reads 'Neither the name of mko nor the names of its contributors'"
  - "src/mko_data_cleaner/__init__.py defines __version__ (non-empty str)"
  - "uv run python -c import mko_data_cleaner; print(mko_data_cleaner.__version__) prints '0.0.1' (source-mode guard path)"
  - "app.py module docstring uses mko_data_cleaner, not mko_get_mediascope_data"
  - ".gitignore has no mko_get_mediascope_data lines"
  - "log_config.yaml loggers object has only __main__ (no tests, no telebot)"
  - ".kilo audit-phase handbooks have no mko_get_mediascope_data references"
  - "No mko_get_mediascope_data hits remain in src/ .gitignore .kilo/commands/audit/phases/"
  - "uv build produces no SetuptoolsDeprecationWarning about license classifiers"
  - "uv run twine check dist/* passes (exit 0, no warnings)"
  - "uv run pytest full suite is green"
  - "uv run ruff check src/ tests/ is clean"
  - "tests/test_metadata.py has test_version_is_non_empty_string and test_version_matches_installed_metadata_when_available"

files:
  - path: pyproject.toml
    targets:
      - type: table
        name: build-system
      - type: table
        name: project
    semantic_anchors:
      - "The requires key inside [build-system] (currently [\"setuptools>=61.0\", \"wheel>=0.37.1\"])"
      - "The [project] table — after requires-python, before dependencies"
      - "The dependencies array entry for PyYAML>=6.0.1"
      - "The classifiers list entry License :: OSI Approved :: MIT License"
    changes:
      - action: modify
        description: >
          Bump requires from [\"setuptools>=61.0\", \"wheel>=0.37.1\"] to
          [\"setuptools>=77.0\", \"wheel>=0.37.1\"]. PEP 639 SPDX license expression
          and license-files require setuptools >= 77.0.0. Keep wheel unchanged.
        code_hint: |
          requires = ["setuptools>=77.0", "wheel>=0.37.1"]
      - action: add_code
        description: >
          Add PEP 639 license fields to [project], after requires-python and
          before dependencies. license = SPDX expression; license-files declares
          LICENSE.txt to ship in wheel .dist-info/licenses/.
        code_hint: |
          license = "BSD-3-Clause"
          license-files = ["LICENSE.txt"]
      - action: add_code
        description: >
          Add rich>=13.0.0 to dependencies after PyYAML>=6.0.1 and before the
          commented-out sqlalchemy line. Case-insensitive alphabetical:
          PyYAML < rich < sqlalchemy < typer. Locked rich version is 14.3.3.
        code_hint: |
          "rich>=13.0.0",
      - action: remove
        description: >
          Remove the "License :: OSI Approved :: MIT License" classifier.
          LICENSE.txt is BSD-3-Clause, not MIT; the classifier is both incorrect
          and deprecated (setuptools >=77 prints a deprecation warning). Keep
          the Programming Language and Operating System classifiers.

  - path: LICENSE.txt
    targets:
      - type: file
    semantic_anchors:
      - "The copyright line (first line of file)"
      - "Clause 3: Neither the name of Audrey Roy Greenfeld nor the names of its contributors"
    changes:
      - action: modify
        description: >
          Change copyright holder from Audrey Roy Greenfeld and individual
          contributors (cookiecutter template) to mko (matching pyproject.toml
          authors = [{ name = "mko", ... }]). Spec D4.
        code_hint: |
          Copyright (c) mko
      - action: modify
        description: >
          Update clause 3 name reference from Audrey Roy Greenfeld to mko for
          consistency. BSD-3-Clause terms otherwise unchanged.
        code_hint: |
          Neither the name of mko nor the names of its contributors

  - path: src/mko_data_cleaner/__init__.py
    targets:
      - type: module
    semantic_anchors:
      - "The file is currently empty (0 bytes) — add __version__ as the sole content"
    changes:
      - action: add_code
        description: >
          Add __version__ only (no __all__, no re-exports — those are B4/T4).
          Single source of truth = pyproject.toml version. Use
          importlib.metadata.version("mko_data_cleaner") with a
          PackageNotFoundError guard returning the documented fallback "0.0.1"
          (matching the current pyproject version). No pyproject parsing at
          runtime. The fallback will be exercised in tests because they run from
          source via pythonpath=["src"], NOT installed.
        code_hint: |
          """Package-level version, sourced from installed package metadata."""
          from importlib.metadata import PackageNotFoundError, version

          try:
              __version__ = version("mko_data_cleaner")
          except PackageNotFoundError:
              # Fallback for source checkout / pythonpath=src (not pip-installed).
              __version__ = "0.0.1"

  - path: src/mko_data_cleaner/app.py
    targets:
      - type: module
    semantic_anchors:
      - "The module docstring (first triple-quoted string at file top)"
    changes:
      - action: modify
        description: >
          Fix the stale project-name mko_get_mediascope_data in the module
          docstring to the current package name mko_data_cleaner. Keep the
          Russian description text (основной публичный интерфейс) untouched —
          only the stale name token is corrected (do not rewrite other Russian
          strings in this file).
        code_hint: |
          """
          mko_data_cleaner — основной публичный интерфейс
          """

  - path: .gitignore
    targets:
      - type: file
    semantic_anchors:
      - "The two src/mko_get_mediascope_data/settings/connections/ lines at the end of the file"
    changes:
      - action: remove
        description: >
          Remove the two stale .gitignore lines:
          src/mko_get_mediascope_data/settings/connections/* and
          !src/mko_get_mediascope_data/settings/connections/mediascope_RENAME_.yaml.
          The src/mko_get_mediascope_data/ directory does not exist (the package is
          src/mko_data_cleaner/ and settings/ has only app_config.yaml and
          log_config.yaml — no connections/ subdirectory). Keep the preceding
          data/* and !data/output/.gitkeep lines unchanged.

  - path: src/mko_data_cleaner/settings/log_config.yaml
    targets:
      - type: file
    semantic_anchors:
      - "The tests logger entry inside the loggers object"
      - "The telebot logger entry inside the loggers object"
    changes:
      - action: remove
        description: >
          Remove the stale tests and telebot logger entries from the loggers
          object. Grep-verified: no Python source calls getLogger("tests") or
          getLogger("telebot"). Keep __main__ logger and root handler intact;
          preserve the JSON-brace YAML format. File ships as package data and is
          copied by init; the fix propagates to new user configs.
        code_hint: |
            "tests": {
              "level": "INFO",
              "handlers": [ "rotating_file" ],
              "propagate": False
            },
            "telebot": {
              "level": "INFO",
              "handlers": [ "console", "rotating_file" ],
              "propagate": False
            },

  - path: .kilo/commands/audit/phases/01-audit-entry-architecture.md
    targets:
      - type: file
    semantic_anchors:
      - "The python -m mko_get_mediascope_data reference in the Purpose section"
    changes:
      - action: modify
        description: >
          Minimal textual fix only (no restructuring): replace stale project name
          mko_get_mediascope_data with mko_data_cleaner in the descriptive Purpose
          text. This is an audit handbook (AI-workflow doc), not a runtime file;
          the reference is not load-bearing for any tooling (YAML frontmatter and
          audit-block structure are untouched). Included per T3 scope (F10).
        code_hint: |
          python -m mko_data_cleaner

  - path: .kilo/commands/audit/phases/02-audit-config-secrets.md
    targets:
      - type: file
    semantic_anchors:
      - "The ~/.config/mko_get_mediascope_data/ reference in the Purpose section"
    changes:
      - action: modify
        description: >
          Minimal textual fix only (no restructuring): replace stale project name
          mko_get_mediascope_data with mko_data_cleaner in the descriptive Purpose
          text. Not load-bearing for tooling. Included per T3 scope (F10).
        code_hint: |
          ~/.config/mko_data_cleaner/

tests_to_create:
  - path: tests/test_metadata.py
    rationale: >
      Package metadata concerns (__version__) are separate from the app-layer
      initialization tests in test_app.py (B1). A dedicated test_metadata.py keeps
      tests focused and coherent with the existing test layout. No fixtures needed.
    tests:
      - test_version_is_non_empty_string
      - test_version_matches_installed_metadata_when_available
    code_hint: |
      """Tests for package-level metadata (__version__)."""

      from importlib.metadata import PackageNotFoundError, version

      import pytest

      import mko_data_cleaner


      def test_version_is_non_empty_string():
          """__version__ resolves to a non-empty string in all execution modes."""
          assert isinstance(mko_data_cleaner.__version__, str)
          assert len(mko_data_cleaner.__version__) > 0


      def test_version_matches_installed_metadata_when_available():
          """When installed, __version__ must agree with importlib.metadata.version."""
          try:
              installed = version("mko_data_cleaner")
          except PackageNotFoundError:
              pytest.skip("Running from source; package metadata not installed")
          assert mko_data_cleaner.__version__ == installed

acceptance_criteria:
  - "pyproject.toml [project] has license = BSD-3-Clause and license-files = [LICENSE.txt]"
  - "pyproject.toml classifiers has no License classifier entry"
  - "pyproject.toml [build-system].requires includes setuptools>=77.0"
  - "rich>=13.0.0 is in [project].dependencies"
  - "LICENSE.txt first line is Copyright (c) mko"
  - "LICENSE.txt clause 3 reads Neither the name of mko nor the names of its contributors"
  - "src/mko_data_cleaner/__init__.py defines __version__ as a non-empty str"
  - "uv run python -c import mko_data_cleaner prints 0.0.1 (source-mode guard path)"
  - "app.py module docstring contains mko_data_cleaner not mko_get_mediascope_data"
  - ".gitignore has no mko_get_mediascope_data lines"
  - "log_config.yaml loggers object has only __main__ (no tests, no telebot)"
  - "01-audit-entry-architecture.md has no mko_get_mediascope_data references"
  - "02-audit-config-secrets.md has no mko_get_mediascope_data references"
  - "uv build produces no SetuptoolsDeprecationWarning about license classifiers"
  - "uv run twine check dist/* passes (exit 0, no warnings)"
  - "uv run pytest full suite is green"
  - "uv run ruff check src/ tests/ is clean"
  - "tests/test_metadata.py has both test functions"

verification_commands:
  - "uv run ruff check src/ tests/"
  - "uv run pytest"
  - "uv build"
  - "uv run twine check dist/*"
  - 'uv run python -c "import mko_data_cleaner; print(mko_data_cleaner.__version__)"'
  - "grep -r mko_get_mediascope_data src/ .gitignore .kilo/commands/audit/phases/ || echo PASS no stale name in active files"
  - "rm -rf dist/ build/ *.egg-info src/mko_data_cleaner.egg-info"
  - "git status --short"

out_of_scope:
  - "process_data docstring (already B1-fixed: reads Synchronous entry point)"
  - "list_settings repoint (already B1-fixed: uses build_app_paths + user_settings_dir)"
  - "MANIFEST.in stale rules (already B2-fixed)"
  - "[tool.setuptools.package-data] (already B2-added: mko_data_cleaner = [settings/*.yaml])"
  - "Version number: stays 0.0.1 (B7/T7 bumps to 0.1.0 — do NOT change now)"
  - "Top-level __init__.py re-exports with __all__ (B4/T4 scope)"
  - "README.md changes (B4/Doc-specialist scope)"
  - ".github/ workflow creation (B6/T6 scope)"
  - "src/mko_data_cleaner/core/** — no changes in this block"
  - "pydantic-settings>=2.12.0: declared but never imported — out-of-scope residue, leave untouched"
  - "pytest-django: not in current dev group (audit discrepancy); .kilo/rules/commands.md reference is for mko-bazuna-test — leave as-is"
  - "settings/ __init__.py: must NOT be added (package-data glob is the mechanism)"

risks:
  - "R1: PEP 639 fields (license, license-files) require setuptools>=77.0; bumping build-system.requires ensures the build env provides PEP 639 support. Build isolation installs the declared minimum automatically."
  - "R2: Removing the MIT classifier and adding BSD-3-Clause is a correction (LICENSE.txt IS BSD-3-Clause), not a license change. twine check must confirm no metadata warnings."
  - "R3: The __version__ fallback constant 0.0.1 must be manually kept in sync with pyproject.toml version. If B7 bumps to 0.1.0, the Implementor must also update the fallback. Documented in code comment."
  - "R4: rich>=13.0.0 must be compatible with typer>=0.24.0 transitive rich requirement. Typer 0.24.1 depends on rich; >=13.0.0 satisfies it. Locked rich is 14.3.3."
  - "R5: Removing tests/telebot loggers from log_config.yaml is safe — grep verified no Python code references these logger names. Existing user configs with stale entries are unaffected (template-only change)."
  - "R6: The .kilo audit-phase handbooks are not load-bearing — mko_get_mediascope_data appears in descriptive Purpose text only, not in YAML frontmatter. Minimal textual fix only; no restructuring."
```

---

## B4 — T4: Public API contract for pipeline use (HIGH RISK)

**Agents:** Researcher (approach) → Planner → Implementor.

### {context_r} — Research Report (Researcher)

**Scope:** Design the public API contract for Jupyter/pipeline callers. READ-ONLY.
Verified against committed B1-B3 state at HEAD `37140aa`.

---

### 1. Chosen implementation path (summary)

| Concern | Decision | Rationale |
|---|---|---|
| Result model name | `ReportResult` | Matches `run_report` method; concise and domain-aligned |
| Model module | `core/models.py` | All Pydantic models live there; lightweight import (pydantic only) |
| Result assembly | Inside `AppService.run_report` → returns `ReportResult` | `run_report` already orchestrates the full pipeline; has direct access to all intermediate counts and paths. Keeps `AppService` responsibility coherent as pipeline coordinator |
| `process_data` signature | `process_data(report_path, settings_dir=None) -> ReportResult` | Passes through `run_report`'s return; thin wrapper |
| Count mechanism | **Hybrid (c)**: `_import_data` returns `rows_loaded`; `export_sql_to_csv` returns `total_rows` | Both values are *already computed internally*; minimal 1-line change each |
| `settings_dir` in `init_project` | `init_project(force, settings_dir=None)` — inline `AppPaths` construction | Avoids moving `build_app_paths`; preserves monkeypatchable `APP_PATHS` for tests |
| Top-level re-exports | Eager imports in `__init__.py`; `__all__` lists 5 names | Simplest; no import cycles; spec says "keep minimal" |
| CLI `run` behavior | Print summary from returned `ReportResult` | Gives user feedback; replaces the generic "Отчёт успешно завершён" |

---

### 2. Rejected alternatives (with reasons)

#### Counts

- **(a) Add dedicated `SELECT COUNT(*)` queries after import and after sync**
  Rejected. `export_sql_to_csv` *already* runs `SELECT COUNT(*) FROM {data_table}` internally (csv_service.py, count_cursor at L328-330) and `_import_data` *already* accumulates `rows_count` (app_service.py L86-99). Adding separate queries duplicates work, adds latency, and requires the DB connection to be alive at the query point. The DBWorker context manager closes+deletes the SQLite file on `__exit__`, so post-`with` queries are impossible.

- **(b) Read counts from the CSV files on disk after export**
  Rejected. `null_data_*` and `clean_data_*` files are compressed `.csv.gz`; counting rows would require re-reading them (Polars round-trip). The DB count is authoritative and already available. Also, file-level row count ≠ table row count if compression or chunk boundaries cause issues.

- **(chosen) (c) Hybrid: service returns already-computed internal counts**
  - `_import_data` → `return rows_count` (1 line added; variable already accumulated; verified polars `write_database` returns `int`)
  - `export_sql_to_csv` → `return total_rows` (changed from implicit `None`; early-return at L334 becomes `return 0`; end-of-function adds `return total_rows`)
  - `rows_loaded` = return of `_import_data` (rows inserted into `data_table` before any mapping rules)
  - `rows_exported` = return of the *final* `export_sql_to_csv` call (exports `data_table` to `clean_data/` *after* DELETE/REPLACE/ADD rules + sync)
  - This is a 2-line change per internal method, zero new DB queries, zero new file reads

#### Result assembly point

- **(a) Assemble in `app.py` `process_data`** — rejected. `process_data` calls `service.run_report(path)` but has no access to intermediate values (loaded count, per-stage timings, null_data file list). The DB file is deleted by `DBWorker.__exit__` inside the `with` block in `run_report`, so `process_data` cannot query the DB after the fact. Would require re-architecting `run_report` to expose internals.
- **(b) New `run_report_with_result` service method** — rejected. Unnecessary indirection; `run_report` already owns the pipeline.

#### `settings_dir` threading

- **(a) Move `build_app_paths` to `paths.py`** — considered, but rejected for B4. `paths.py` is imported by `init_service.py` already; moving the factory there is architecturally cleaner but changes the import surface established by B1 (tests import `build_app_paths` from `core.app_service`). Moving it now creates churn without resolving a real problem. **Recommendation for later refactor:** move `build_app_paths` to `paths.py` so both `app_service` and `init_service` can import it without pulling in polars/adbc. See Risks §10.
- **(b) `init_service.py` imports `build_app_paths` from `app_service`** — rejected. Creates dependency `init_service → app_service`, which transitively imports `csv_service` (polars), `db_service` (adbc), `dict_service` (polars). This makes `init_service` a heavy import. `init_service` is currently lightweight (only `paths.py`).
- **(chosen) Inline `AppPaths` construction in `init_project`** — `init_project` imports `AppPaths`, `APP_DIR`, `APP_NAME` directly from `paths.py` (all already available). When `settings_dir is None`, uses the module-level `APP_PATHS` (preserving the existing test monkeypatch pattern). When `settings_dir` is provided, constructs `AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir))` inline — exactly mirroring `build_app_paths`'s logic. `_copy_templates` source stays `APP_PATHS.app_settings_dir` (always the packaged templates); only the destination changes.

#### Top-level re-exports

- **(a) Lazy `__getattr__` (PEP 562)** — considered for avoiding heavy imports when only `__version__` or `SettingsNotInitializedError` is needed. Rejected as premature optimization; the public API contract is "Jupyter/pipeline users call `initialize_settings` and `process_data`", both of which require the full pipeline. Eager imports keep the contract simple and testable. Can be added later without breaking the API.
- **(chosen) Eager imports** — `from mko_data_cleaner.app import initialize_settings, process_data` pulls in `app_service` (polars, adbc) transitively. Acceptable because B1 already verified no import-time side effects (no dir creation, no logging config, no crash).

---

### 3. Result object design

**Model name:** `ReportResult`
**Module:** `src/mko_data_cleaner/core/models.py` (all Pydantic models live here; import is lightweight — pydantic + pathlib + datetime only)
**Base:** `pydantic.BaseModel` (matches existing models; no `extra="allow"` needed — output is a fixed schema)

```python
class ReportResult(BaseModel):
    """Structured result of a data-cleaning report run.

    Returned by ``process_data`` / ``AppService.run_report`` so that
    Jupyter/pipeline callers can inspect outputs and metrics without
    parsing CLI strings or globbing the filesystem.
    """

    # --- output paths ---
    report_dir: Path
    import_dir: Path
    export_dir: Path
    db_path: Path
    null_data_files: list[Path]

    # --- timing ---
    started_at: datetime
    finished_at: datetime
    duration: float               # total seconds (wall clock)

    # --- per-stage timings (seconds) ---
    timings: dict[str, float]

    # --- row counts ---
    rows_loaded: int
    rows_exported: int
```

**Field sources (all verified from source):**

| Field | Source in `run_report` / `app_service.py` |
|---|---|
| `report_dir` | `self.base_path` (set from `data_path` arg, L109) |
| `import_dir` | `self.import_path` → `Path(self.base_path, self.app_config.data_paths.import_folder)` (L62) |
| `export_dir` | `self.export_path` → `Path(self.base_path, self.app_config.data_paths.export_folder)` (L66) |
| `db_path` | `self.db_path` → `Path(self.base_path, self.app_config.data_paths.db_file)` (L74) |
| `null_data_files` | glob `null_data_*` in `self.base_path` after null-data export (L228-233 writes there) |
| `started_at` | `datetime.now()` at start of `run_report` (replaces/morphs existing `start_time` at L104) |
| `finished_at` | `datetime.now()` at end of `run_report` (L250) |
| `duration` | `time.perf_counter()` delta from start to end of `run_report` |
| `timings` | `time.perf_counter()` deltas per stage: `{"import": …, "mapping": …, "export": …}` |
| `rows_loaded` | return value of `self._import_data(db_worker, csv_worker)` (L162) |
| `rows_exported` | return value of the final `csv_worker.export_sql_to_csv(...)` exporting `data_tbl_name` (L244-248) |

**Stages for timing:**
1. `"import"` — the `self._import_data(db_worker, csv_worker)` call (L162)
2. `"mapping"` — from FTS/LIKE data import through the `apply_mapping` rule loop (L164-223)
3. `"export"` — `build_non_mapped` → null_data export → `sync_with_data_table` → clean_data export (L226-248)

**Rejected field alternatives:**
- `timedelta` instead of `float` for `duration` — rejected. `timedelta` is awkward for JSON serialization and the spec prefers `float`/`int`. `float` seconds is machine-friendly.
- Per-rule statistics — rejected. D8 explicitly excludes "Per-rule statistics and non-mapped row counts" from the object.
- `rows_deleted`, `rows_replaced`, `rows_tagged` — rejected. Not in the spec; derivable from input/output count diff if needed.
- `StrEnum` for timing stage keys — rejected as overengineering. Stage names are internal pipeline labels, not configuration constants. A `dict[str, float]` is minimal and flexible.

---

### 4. Counts approach (detail)

**Loaded count (`rows_loaded`):**
- `_import_data` (app_service.py L84-101) already accumulates `rows_count += chunk.write_database(...)` — verified that polars 1.38.1 `DataFrame.write_database()` returns `int` (rows written).
- Change: add `return rows_count` at end of `_import_data`.
- Capture in `run_report`: `rows_loaded = self._import_data(db_worker, csv_worker)` (L162).
- This count is taken *after* all chunks are inserted and the index is updated (L99-101), but *before* any DELETE/REPLACE/ADD rules. This is the correct "loaded into database" count.

**Exported count (`rows_exported`):**
- `CSVWorker.export_sql_to_csv` (csv_service.py L269-372) already computes `total_rows` via `SELECT COUNT(*) FROM {data_table}` (L329-330) and accumulates `row_counter` (L313, L346).
- Change: 
  - L334 early `return` → `return 0`
  - End of function (after L368 logger.debug) → `return total_rows`
- The *final* `export_sql_to_csv` call in `run_report` (L244-248) exports `db_worker.data_tbl_name` to `self.export_path` (clean_data/), *after* all mapping rules and sync. Its return value is `rows_exported`.
- The *first* `export_sql_to_csv` call (L228-233, exports `non_mapped_table` to null_data) also returns a count, but per D8 this is intentionally not included in the result object. The return value is simply discarded.

**Why not query `perform_query("SELECT COUNT(*) ...")`:** `DBWorker.perform_query` exists (db_service.py L195) and returns a cursor. But `total_rows` is already computed inside `export_sql_to_csv` — querying again is redundant work. The hybrid approach reuses existing computation.

---

### 5. Result assembly point

**Inside `AppService.run_report`** — `run_report` returns `ReportResult`.

**Justification:**
- `run_report` already owns the full pipeline orchestration (L103-255). It has direct references to `self.base_path`, `self.import_path`, `self.export_path`, `self.db_path`, `csv_worker`, `db_worker`.
- The DB is created and destroyed *inside* the `with DBWorker(...)` block (L133-248); `DBWorker.__exit__` → `close()` → `_delete_base_files()` deletes the SQLite file (L762-781). No code outside the `with` block can query the DB.
- Capturing counts requires access to `_import_data`'s return and `export_sql_to_csv`'s return — both inside the `with` block.
- Assembling outside (`app.py` `process_data`) is impossible — the result object is built from data only available inside `run_report`.

**CLI `run` command (app.py L37-68):** Currently calls `service.run_report(report)` and prints a generic success message. Change to capture the return value and print a one-line summary:
```
result = service.run_report(report)
console.print(f"[green]✅ Report complete:[/green] "
              f"{result.rows_exported:,} clean rows → {result.export_dir}")
```

---

### 6. `settings_dir` threading design (D3, C7)

**`AppPaths` confirmation (verified, paths.py L32-39):**
```python
class AppPaths(BaseModel):
    app_dir: Path
    app_name: str
    user_dir: Path
    # user_settings_dir = user_dir / "settings"
    # app_config = user_settings_dir / "app_config.yaml"
    # log_config = user_settings_dir / "log_config.yaml"
```
`build_app_paths` (app_service.py L258-269) constructs `AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir))` when `settings_dir` is provided. This is the correct pattern and is reused by this design.

**Threading chain:**

1. **`init_service.init_project(force=False, settings_dir=None) -> Path`** (changed):
   ```python
   def init_project(force: bool = False, settings_dir: Path | None = None) -> Path:
       if settings_dir is None:
           user_settings_dir = APP_PATHS.user_settings_dir  # module-level APP_PATHS
       else:
           app_paths = AppPaths(
               app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir)
           )
           user_settings_dir = app_paths.user_settings_dir
       _copy_templates(user_settings_dir, force)
       return user_settings_dir
   ```
   - When `settings_dir is None`: uses `init_service`'s module-level `APP_PATHS` (monkeypatchable — preserves existing test `test_init_cli_creates_settings_in_tmp_dir`).
   - When `settings_dir` is provided: constructs `AppPaths` inline (mirrors `build_app_paths` in app_service.py). No dependency on `app_service` module.
   - `_copy_templates` source stays `APP_PATHS.app_settings_dir` (always the packaged templates — never changes with `settings_dir`).

2. **`app.initialize_settings(force=False, settings_dir=None) -> Path`** (changed):
   ```python
   def initialize_settings(force: bool = False, settings_dir: Path | None = None) -> Path:
       return init_project(force=force, settings_dir=settings_dir)
   ```
   - Removes the B1-era NOTE comment (it's now implemented).
   - Forwards `settings_dir` to `init_project`.

3. **CLI `init` command (app.py L26-34):** Add `--settings-dir` option mirroring `run` and `list_settings`:
   ```python
   def init(force: bool = ..., settings_dir: Path | None = ...):
       path = init_project(force=force, settings_dir=settings_dir)
       console.print(f"[green]✅ Settings initialized:[/green] {path}")
   ```

4. **`process_data` / `get_app_service` / `list_settings`** (app.py + app_service.py):
   - Already thread `settings_dir` correctly (B1 done). No changes needed.
   - `get_app_service(settings_dir)` → `build_app_paths(settings_dir)` → `_require_settings(app_paths)` → `AppService(app_paths=app_paths, ...)`
   - `list_settings(settings_dir)` → `build_app_paths(settings_dir)` → `app_paths.user_settings_dir` ✓

**Consistency matrix:**

| Entry point | `settings_dir` role | Resolves to |
|---|---|---|
| `initialize_settings(settings_dir=...)` | user base dir | `settings_dir / "settings"` |
| `init_project(force, settings_dir=...)` | user base dir | `settings_dir / "settings"` |
| `process_data(report_path, settings_dir=...)` | user base dir | `settings_dir / "settings"` |
| CLI `init --settings-dir X` | user base dir | `X / "settings"` |
| CLI `run --settings-dir X` | user base dir | `X / "settings"` |
| CLI `list-settings --settings-dir X` | user base dir | `X / "settings"` |

When `settings_dir is None`, all paths resolve to platformdirs default (`USER_DIR` = `~/.config/mko_data_cleaner/`), so `user_settings_dir` = `~/.config/mko_data_cleaner/settings/`.

---

### 7. `__init__.py` re-export design (R10, R11)

**Target file:** `src/mko_data_cleaner/__init__.py` (currently has only `__version__` via importlib.metadata)

**Exact re-exported names:**

```python
from mko_data_cleaner.app import initialize_settings, process_data
from mko_data_cleaner.core.errors import SettingsNotInitializedError
from mko_data_cleaner.core.models import ReportResult

__all__ = [
    "ReportResult",
    "SettingsNotInitializedError",
    "__version__",
    "initialize_settings",
    "process_data",
]
```

**Rationale for each name:**
- `process_data` — primary pipeline entry point (spec T4: "Jupyter/pipeline users call exactly `initialize_settings()` and `process_data`")
- `initialize_settings` — primary setup entry point (spec T4)
- `ReportResult` — return type of `process_data` (spec D8/R5: "users import the error and result type from the top level")
- `SettingsNotInitializedError` — the error users catch when settings are missing (spec: "users import the error ... from the top level"; already subclass of `FileNotFoundError`)
- `__version__` — already present (B3); kept in `__all__` for discoverability

**Names NOT re-exported:**
- `get_app_service` — internal factory; B1 already removed the singleton. Spec R10 says "the internal `core.app_service` import path disappears". Users use `process_data`/`initialize_settings`.

### 8. Import-cycle analysis

```
mko_data_cleaner/__init__.py
    │  imports (eager)
    ├── from mko_data_cleaner.app import initialize_settings, process_data
    ├── from mko_data_cleaner.core.errors import SettingsNotInitializedError  (lightweight)
    └── from mko_data_cleaner.core.models import ReportResult               (lightweight)

mko_data_cleaner.app
    │  imports
    ├── from mko_data_cleaner.core.app_service import build_app_paths, get_app_service
    ├── from mko_data_cleaner.core.init_service import init_project
    └── from mko_data_cleaner.core.utils import list_files_in_directory

core.app_service
    ├── from mko_data_cleaner.core.csv_service import CSVWorker   (polars)
    ├── from mko_data_cleaner.core.db_service import DBWorker     (adbc)
    ├── from mko_data_cleaner.core.dict_service import MappingDict  (polars)
    ├── from mko_data_cleaner.core.errors import ...
    ├── from mko_data_cleaner.core.models import DataSettings, LoggingSettings
    └── from mko_data_cleaner.core.paths import ...

core.init_service
    └── from mko_data_cleaner.core.paths import APP_PATHS, APP_DIR, APP_NAME, AppPaths  (lightweight)
```

**No circular imports.** The dependency graph is a clean DAG:
- `__init__` → `app` → `core.*` → (none back to `__init__`)
- `core.init_service` → `core.paths` (lightweight: platformdirs + pydantic only)
- No `core.*` module imports `mko_data_cleaner` (the top-level package `__init__`)

**Heavy-import note:** `__init__.py` → `app` → `core.app_service` → polars + adbc. This is a regression for `import mko_data_cleaner` if users only want `__version__`. Mitigated by:
- B1 confirmed no import-time side effects (no dir creation, no logging config, no crash)
- The spec R4 says the public API is `initialize_settings` + `process_data` — both require the full pipeline
- **PEP 562 `__getattr__`** can be added later if lazy loading is needed (non-breaking)

**Existing test compatibility:** `test_app.py` imports `from mko_data_cleaner.app import app, process_data` and `from mko_data_cleaner.core.app_service import get_app_service`. These paths are unchanged. New `__init__` re-exports don't break existing imports.

---

### 9. Exact files / modules / classes / functions to change

| File | Symbol | Change |
|---|---|---|
| `core/models.py` | **NEW class** `ReportResult` | Add Pydantic BaseModel at end of file, in a new `# ---Report Result` section |
| `core/app_service.py` | `AppService._import_data` | Add `-> int` return annotation; add `return rows_count` at end |
| `core/app_service.py` | `AppService.run_report` | Change return annotation to `-> "ReportResult"`; add `import time`; wrap stages in `time.perf_counter()`; capture `rows_loaded` from `_import_data`; capture `rows_exported` from final `export_sql_to_csv`; collect `null_data_files` glob; construct and return `ReportResult` |
| `core/app_service.py` | module-level imports | Add `from time import perf_counter` (or `import time`); add `from mko_data_cleaner.core.models import … ReportResult` (extend existing import) |
| `core/csv_service.py` | `CSVWorker.export_sql_to_csv` | Change return annotation to `-> int`; change early `return` (L334) to `return 0`; add `return total_rows` at end (after L368) |
| `core/init_service.py` | `init_project` | Add `settings_dir: Path | None = None` param; inline `AppPaths` construction (or `build_app_paths` if moved); pass `settings_dir`-aware destination to `_copy_templates`; return `user_settings_dir` |
| `core/init_service.py` | module imports | Add `APP_DIR, APP_NAME, AppPaths` to the import from `core.paths` (currently only `APP_PATHS`) |
| `app.py` | `process_data` | Change return annotation from `-> None` to `-> ReportResult`; add `return` before `get_app_service(...).run_report(path)` |
| `app.py` | `initialize_settings` | Remove NOTE comment block; forward `settings_dir=settings_dir` to `init_project` |
| `app.py` | `init` CLI command | Add `settings_dir: Annotated[Path | None, …] = None` option; pass to `init_project` |
| `app.py` | `run` CLI command | Capture `result = service.run_report(report)`; print summary from result |
| `app.py` | `process_data` docstring | Update return-type mention (add "Returns ReportResult") |
| `__init__.py` | module-level | Add re-export imports + `__all__`; update docstring |
| `pyproject.toml` | (no change) | Result model needs no new deps (pydantic already declared) |

**Symbols NOT changed:**
- `core/paths.py` — no change (verified: `AppPaths` fields are `app_dir`, `app_name`, `user_dir`; `build_app_paths` stays in `app_service` for B4)
- `core/errors.py` — no change (`SettingsNotInitializedError` already exists)
- `core/db_service.py` — no change (counts obtained via csv_service + _import_data returns)
- `core/utils.py` — no change

---

### 10. Required tests

**A. Model-level test** (`tests/test_app.py` or `tests/test_result.py`):
- Instantiate `ReportResult` with sample values (paths, datetimes, float, dict, ints)
- Assert all 10 fields exist and have correct types
- Assert `model_dump()` produces serializable output

**B. `process_data` return-type test** (`tests/test_app.py`):
- Mock `get_app_service` to return a fake service whose `run_report` returns a pre-built `ReportResult`
- Call `process_data(some_report_dir, settings_dir=populated_settings)`
- Assert the return value is a `ReportResult` with expected fields
- Assert `SettingsNotInitializedError` is still raised when settings are missing (existing B1 tests already cover this — no change needed)

**C. `settings_dir` threading test** (`tests/test_app.py`):
- `initialize_settings(settings_dir=tmp_path)` writes `app_config.yaml` and `log_config.yaml` to `tmp_path / "settings" /`
- `init_project(force=True, settings_dir=tmp_path)` returns `tmp_path / "settings"`
- `init_project(force=True, settings_dir=None)` returns the default platformdirs path (or uses the monkeypatched `APP_PATHS` as in existing `test_init_cli_creates_settings_in_tmp_dir`)
- CLI `init --settings-dir X --force` writes to `X / "settings"`

**D. Top-level re-export test** (`tests/test_app.py` or `tests/test_metadata.py`):
- `import mko_data_cleaner`
- Assert `mko_data_cleaner.__all__` contains exactly: `"ReportResult"`, `"SettingsNotInitializedError"`, `"__version__"`, `"initialize_settings"`, `"process_data"`
- Assert `mko_data_cleaner.process_data`, `mko_data_cleaner.initialize_settings`, `mko_data_cleaner.ReportResult`, `mko_data_cleaner.SettingsNotInitializedError` are accessible
- Assert `mko_data_cleaner.process_data` is the same object as `mko_data_cleaner.app.process_data`

**E. Minimal end-to-end test** (bridge to B5):
- Create a tiny report directory: `raw_data/sample.csv` (3-5 rows, uppercase headers matching the README format: `YEAR;MONTH;ADVERTISERS;BRANDS;SUBBRANDS;ARTICLE LIST3;ARTICLE LIST4;ADID;MEDIA;COST RUB;QUANTITY`) with separator `;`
- Create `dict/merged_dictionary.csv` (2-3 rules: one DELETE, one REPLACE, one ADD)
- Use `populated_settings` fixture (from `test_app.py`) for settings
- Call `process_data(report_dir, settings_dir=settings_dir)`
- Assert `result.rows_loaded == N`, `result.rows_exported <= N` (some rows may be deleted), `result.export_dir` contains `.csv.gz` files, `result.null_data_files` is a list
- **Caveat:** The `app_config.yaml` sets `data_file_settings.extension: gz` — CSV files must be `.csv.gz` OR the config must be overridden. For a minimal test, either (a) create `.csv.gz` files, or (b) use a custom `app_config.yaml` with `extension: csv`. The existing `test_app.py` `realistic_config` fixture in `conftest.py` uses `extension: gz` — so the test fixture should use gzipped CSVs or override the setting.

**Existing tests that remain valid (no changes needed):**
- `test_import_app_does_not_create_settings_dir` — still passes (B1 behavior preserved)
- `test_process_data_without_settings_raises_settings_error` — still passes
- `test_process_data_past_gate_does_not_raise_settings_error` — uses `empty_report_dir` which has no data; the pipeline will fail operationally (no CSV files), but the test catches `Exception` broadly. With B4, `process_data` now returns `ReportResult` — the test's `except Exception` still catches the operational failure. **Note:** this test should be updated to use a populated report dir after B5 creates the sample; for B4 it can remain as-is (asserts the settings gate passes).
- `test_init_cli_creates_settings_in_tmp_dir` — still passes (monkeypatches `init_service.APP_PATHS`, which is still used when `settings_dir is None`)

---

### 11. Risks and open items for the Planner

**R1 — `write_database` return value assumption.**
Verified: polars 1.38.1 `DataFrame.write_database()` returns `int`. But the codebase declares `polars>=1.38.1` (pyproject L21) — older versions in that range might return `None`. If a user has polars 1.38.0, `_import_data` would already crash (not a B4 regression, but the `return rows_count` change depends on it). **Confidence: HIGH** — the installed version returns `int`, and the existing code already relies on this.

**R2 — DB deletion timing.**
`DBWorker.__exit__` calls `close()` → `_delete_base_files()` which deletes `db_file` + WAL/SHM variants (L762-767). The `result` object captures `db_path` (the path string), `rows_exported` (from `export_sql_to_csv`'s COUNT query, captured *inside* the `with` block), and `null_data_files` (globbed *inside* the `with` block). If `null_data_files` globbing is deferred to *after* the `with` block, the DB is gone but the CSV files in `base_path` survive (they're written to the report directory, not the DB file). **Recommendation:** collect `null_data_files` inside the `with` block to be safe. **Confidence: HIGH** — verified from `DBWorker.__exit__` (L786-787).

**R3 — `null_data_files` glob pattern.**
`CSVWorker.get_file_name` (csv_service.py L237-240) generates filenames as `{prefix}_{timestamp}_{suffix}{ext}` where `ext` comes from `get_files_suffix(compression)`. For gzip, `ext = ".csv.gz"`. So globbing `self.base_path.glob("null_data_*")` would match `null_data_20240101-120000_1.csv.gz`. But if compression changes (e.g., `None` → `.csv`, `zip` → `.csv.zip`), the glob still matches because it uses the `null_data_*` prefix only. **Confidence: HIGH.**

**R4 — `_import_data` is a `@staticmethod`.**
It currently takes `(db_worker, csv_worker)` with no `self`. Changing the return type is trivial. But if future work needs `self` (e.g., for logging configuration), the static method pattern would need revisiting. Out of scope for B4. **Confidence: HIGH.**

**R5 — `export_sql_to_csv` return value consistency.**
The function has two exit paths: the early `return` when `total_rows == 0` (L334), and the implicit fall-through at the end (L372). Both must return an `int`. The early return becomes `return 0`; the end becomes `return total_rows`. **No existing tests** assert `export_sql_to_csv` returns `None` — verified by reading `tests/test_csv_service.py` (exists, 5 test methods — need to confirm none assert `None` return). **Recommendation:** scan `test_csv_service.py` for `export_sql_to_csv` assertions before implementing. **Confidence: MEDIUM** — needs verification of existing csv_service tests.

**R6 — `timings` dict keys.**
Using string keys `"import"`, `"mapping"`, `"export"` is simple but untyped. If the team prefers an `Enum` or `StrEnum` (per project rule #10), a `StageName(StrEnum)` could be defined in `models.py`. **Recommendation:** defer enum; keep `dict[str, float]` for B4 minimalism. **Confidence: MEDIUM.**

**R7 — `AppPaths` construction duplication.**
`init_project` inlines `AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir))` — this duplicates `build_app_paths` logic in `app_service.py` (L269). If `build_app_paths` is later moved to `paths.py`, `init_service` can import it. **Open item:** decide in B4 review whether to move `build_app_paths` to `paths.py` now (architectural cleanup) or defer. Moving it would require updating `app_service.py` imports and the `__init__.py` re-export of `build_app_paths` is not needed (it's only used internally). **Confidence: HIGH** — clear architectural recommendation.

**R8 — README documentation coupling (R10).**
The following README claims become stale after B4 and must be updated by the Doc-specialist step:
- L65: `from mko_data_cleaner.app import process_data` → should become `from mko_data_cleaner import process_data, initialize_settings`
- L67: `process_data(r"data\mvideo")` → should show return value usage (`result = process_data(r"data\mvideo")`)
- L64-68: The "Рекомендуемый способ" section should mention `initialize_settings` as the first step and the `ReportResult` return
- L38-43: `mko-data-cleaner init` should document the `--settings-dir` flag
- No README currently documents `settings_dir` parameter on `process_data` — needs adding per D3
- No mention of `SettingsNotInitializedError` or `ReportResult` in README — Doc-specialist should add a "Python API" section
- **Confidence: HIGH** — all claims verified against current README content.

**R9 — `DataSettings` missing `read_settings`/`export_settings` defaults are not the concern.**
The `DataSettings` model (models.py L157-162) requires `read_settings: ReadCSV` and `export_settings: WriteCSV` but the `realistic_config` fixture in `tests/conftest.py` does NOT include them. This means `DataSettings(**realistic_config)` would fail validation. This is a pre-existing issue, not a B4 concern. The app_config.yaml template *does* include them. **Confidence: HIGH** — verified by reading both `models.py` and `conftest.py`.

**R10 — `process_data_past_gate_does_not_raise_settings_error` test behavior change.**
This test (test_app.py L191-201) calls `process_data(empty_report_dir, ...)` with an empty report dir. The pipeline will fail (no CSV files in `raw_data/`). Currently the test catches `Exception` broadly. After B4, `process_data` returns `ReportResult` on success — but this test won't reach success (empty dir). No conflict, but the test should be enhanced post-B5 with a real report dir to verify the full return path. **Confidence: HIGH.**

**R11 — `duration` as `float` vs `datetime.timedelta`.**
The existing `run_report` prints `end_time - start_time` (a `timedelta`) at L252-253. The result object uses `float` for `duration` (seconds). The CLI summary doesn't need to print the `timedelta` anymore — it prints the `ReportResult` summary. **Confidence: HIGH.**

**Open item — `list_settings` as a Python-level function (D5):**
`list_settings` exists only as a CLI command in `app.py` (L71-101). The spec D5 says "list_settings is a stable part of the contract." Currently there is no Python-level `list_settings()` function — only the CLI command. **Recommendation for B4 review:** Either (a) promote `list_settings` to a module-level function in `app.py` (e.g., `def list_settings(settings_dir=None) -> list[Path]`), or (b) keep it CLI-only and document it as such. The spec R10/T4 mentions only `initialize_settings` and `process_data` as the stable contract; `list_settings` is a CLI convenience. **Confidence: MEDIUM** — depends on spec interpretation.

**Open item — `__init__.py` docstring:**
Currently `"""Package-level version, sourced from installed package metadata."""`. After B4, it should become something like `"""Public API: initialize_settings, process_data, and result types."""`. Minor but should be consistent.

---

### 12. Summary of semantic changes (no line numbers)

**New code:**
- `ReportResult` Pydantic model in `core/models.py` (10 fields: 5 Path, 5 Path-list, 2 datetime, 1 float, 1 dict, 2 int)

**Modified functions (signatures):**
- `AppService._import_data(db_worker, csv_worker) -> int` (was `-> None`)
- `AppService.run_report(data_path) -> ReportResult` (was `-> None`)
- `CSVWorker.export_sql_to_csv(...) -> int` (was `-> None`)
- `init_project(force, settings_dir=None) -> Path` (adds `settings_dir`)
- `process_data(report_path, settings_dir=None) -> ReportResult` (was `-> None`)
- `initialize_settings(force, settings_dir=None) -> Path` (forwards `settings_dir`)
- CLI `init` command (adds `--settings-dir`)
- CLI `run` command (captures result, prints summary)

**New re-exports in `__init__.py`:**
- `initialize_settings`, `process_data`, `ReportResult`, `SettingsNotInitializedError`, `__version__` with `__all__`

---

### 13. Verification plan for Implementor

1. `uv run ruff check src/ tests/` — clean (no new lint violations)
2. `uv run ruff check --fix src/` — import sorting passes
3. `uv run pytest tests/test_app.py tests/test_metadata.py` — all green
4. `uv run python -c "import mko_data_cleaner; print(mko_data_cleaner.__all__)"` — shows 5 names
5. `uv run python -c "from mko_data_cleaner import ReportResult, SettingsNotInitializedError, process_data, initialize_settings, __version__"` — succeeds
6. `uv build && uv run twine check dist/*` — passes (no new metadata issues)
7. Confirm `null_data_*` files persist after `run_report` (DB file is deleted, but CSVs in report dir survive)

---

### {task_description} — Implementation Task (B4/T4)

**Preamble — verified current state of symbols (HEAD `37140aa`, B1-B3 committed, 37 tests green, ruff clean):**

- `core/models.py`: Has `DataSettings`, `DataDict`, `DataFile`, `Database`, `LoggingSettings`, `WorkingPaths`, `ActionType`, `MatchType`, `MappingColumns`, `DictColumnsIndexes`, `ReadCSV`/`PolarsReadCSV`, `WriteCSV`/`PolarsWriteCSV`. **No `ReportResult` model.** Imports include `from __future__ import annotations`, stdlib (`codecs`, `enum.StrEnum`, `pathlib.Path`, `typing`), and Pydantic. **No `datetime` import.**
- `core/app_service.py`: `AppService` class. `_import_data(db_worker, csv_worker)` — no return annotation, no return statement, ends with `db_worker.update_index_from_data()`. `run_report(self, data_path: str | Path)` — no return annotation, no `time`/`perf_counter` import, no timing, returns `None` implicitly. Imports: `from mko_data_cleaner.core.models import DataSettings, LoggingSettings, MappingColumns` (no `ReportResult`). **`build_app_paths` already exists (L258-269) and threads `settings_dir` correctly** — reuse its pattern in `init_project`.
- `core/csv_service.py`: `CSVWorker.export_sql_to_csv(self, db_con, data_table, file_prefix=None, export_path=None)` — no return annotation, early `return` at L334 (when `total_rows == 0`), implicit `None` return at end. Docstring says `Returns: None`.
- `core/init_service.py`: `init_project(force: bool = False) -> Path` — no `settings_dir` parameter; uses module-level `APP_PATHS.user_settings_dir`. Import: `from mko_data_cleaner.core.paths import APP_PATHS` — missing `APP_DIR, APP_NAME, AppPaths`.
- `core/db_service.py`: `DBWorker` class with `perform_query` returning a cursor (L195-206). **`export_sql_to_csv` uses `db_con.cursor()` (raw sqlite3) directly, not `DBWorker.perform_query`**. Counts come from `polars.DataFrame.write_database()` return value (`_import_data`) and `SELECT COUNT(*)` inside `export_sql_to_csv` — both already computed internally. **No change needed.**
- `core/errors.py`: `SettingsNotInitializedError(FileNotFoundError)` with `__init__(self, user_settings_dir: Path, missing_files: list[str])` — already exists (B1).
- `core/utils.py`: `yaml_to_dict` returns `dict[str, Any] | None` but returns `{}` in both except branches — already fixed (B1).
- `core/paths.py`: `AppPaths(BaseModel)` with `app_dir: Path`, `app_name: str`, `user_dir: Path`; properties `user_settings_dir`, `app_settings_dir`, `app_config`, `log_config`. `APP_PATHS = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=USER_DIR)`. **No change needed.**
- `app.py` (142 lines): Module docstring `"""mko_data_cleaner — основной публичный интерфейс"""` (B3-fixed). `import sys`, `from pathlib import Path`, `from typing import Annotated`, `import typer`, `from rich.console import Console`. `sys.stdout.reconfigure(line_buffering=True)` at L16 — module-level (B1 kept; B4 does NOT touch). `console = Console()`, `app = typer.Typer(...)`. `__all__ = ["process_data", "initialize_settings", "init", "run", "list_settings", "app", "get_app_service"]`. `process_data(report_path: str | Path, settings_dir: Path | None = None) -> None` — calls `get_app_service(settings_dir).run_report(path)` WITHOUT `return`. `initialize_settings(force=False, settings_dir=None) -> Path` — has NOTE comment, does NOT forward `settings_dir`. `init` CLI — no `settings_dir` option. `run` CLI — has `settings_dir` option (B1), calls `service.run_report(report)` WITHOUT capturing return value, prints `[green]✅ Отчёт успешно завершён[/green]`.
- `__init__.py` (10 lines): Docstring `"""Package-level version, sourced from installed package metadata."""`. `__version__` via `importlib.metadata.version("mko_data_cleaner")` with `PackageNotFoundError` fallback to `"0.0.1"`. **No `__all__`, no re-exports.**
- `tests/test_app.py` (216 lines): Has B1 tests (12 test functions). Imports: `from mko_data_cleaner.app import app, process_data`; `from mko_data_cleaner.core.app_service import get_app_service`; `from mko_data_cleaner.core.errors import SettingsNotInitializedError`; `from mko_data_cleaner.core.paths import APP_DIR, APP_NAME, AppPaths`; `from mko_data_cleaner.core.utils import yaml_to_dict`. Fixtures: `populated_settings` (copies template `app_config.yaml`+`log_config.yaml` into `tmp_path/userdir/settings/`), `empty_settings_dir`, `empty_report_dir`, `_isolate_logging` (autouse). **Missing:** `initialize_settings`, `ReportResult`, `init_project`, `datetime`, `SimpleNamespace`, `gzip` imports.
- `tests/test_csv_service.py` (29 lines): 3 tests — `test_get_files_suffix`, `test_is_date_column`, `test_get_file_name`. **No tests for `export_sql_to_csv`** (no return-value assertions to fix).
- `tests/conftest.py` (107 lines): Fixtures `realistic_config` (extension=`gz`, index_column=`AdId`, date_column=`researchDate`, col_indexes action=0/match=1/search=2/term=4), `db_worker`, `dict_indexes`, `sample_dictionary` (5 rows: 1 `a`, 2 `r`, 1 `d`), `tmp_db_path`, `db_worker_memory`.
- `app_config.yaml` template: `data_paths: import_folder=raw_data, export_folder=clean_data, dict_file=dict/merged_dictionary.csv, db_file=data_base/db_example.db`; `data_file_settings: extension=gz, index_column=AdId, date_column=researchDate`; `dict_file_settings.col_indexes: action=0, match=1, search=2, term=4`; `database_settings.table_name=data_table`; `read_settings.from_csv: separator=;`; `export_settings.to_csv: compression=gzip`.
- `README.md` (201 lines): L65 `from mko_data_cleaner.app import process_data`; L67 `process_data(r"data\mvideo")` (no return capture); L39-43 `mko-data-cleaner init` (no `--settings-dir`); L72-73 `mko-data-cleaner run path/to/...` (no `--settings-dir`).

**Test-file placement decision:**
Extend `tests/test_app.py` (not a new module). Rationale: (a) it already owns the `populated_settings`/`empty_report_dir`/`empty_settings_dir` fixtures that B4 tests require; (b) the `_isolate_logging` autouse fixture prevents `logging.config.dictConfig` leakage from `get_app_service`; (c) B1 tests live there, so B4 contract tests are naturally co-located; (d) the research section 10 already recommends `test_app.py` for tests A-D. Only the `export_sql_to_csv` return-value test goes in `tests/test_csv_service.py` — that is where `CSVWorker` tests already live. No new test files are created.

**Stale README claims (Doc-specialist must fix; not in scope for B4 implementation):**
1. L65: `from mko_data_cleaner.app import process_data` → should be `from mko_data_cleaner import process_data, initialize_settings`
2. L67: `process_data(r"data\mvideo")` discards return; should show `result = process_data(...)` and reference `ReportResult` fields
3. L62-68 ("Рекомендуемый способ"): omits `initialize_settings()` prerequisite; omits `ReportResult` return documentation
4. L39-43: `mko-data-cleaner init` / `init --force` — no `--settings-dir` flag documented
5. L72-73: `mko-data-cleaner run path/to/...` — no `--settings-dir` flag documented
6. No mention of `ReportResult`, `SettingsNotInitializedError`, or `settings_dir` parameter on `process_data`
7. No "Python API" reference section for Jupyter/pipeline usage

```yaml
id: b4_t4_public_api_contract

title: "B4/T4 — Public API contract: ReportResult, settings_dir threading, top-level re-exports"

priority: high

depends_on:
  - b1_t1_remove_import_time_init
  - b2_t2_ship_yaml_templates
  - b3_t3_metadata_hygiene

# ── Plan/Audit report source ──
# source_reference: .ai\plans\01-pip-installable-jupyter-pipeline-step.md
# source_section: B4 — T4: Public API contract for pipeline use (HIGH RISK)
# source_blocks:
#   - "{context_r} — Research Report (Researcher)"
#   - "9. Exact files / modules / classes / functions to change"
#   - "10. Required tests"
#   - "11. Risks and open items for the Planner"
#   - "12. Summary of semantic changes"
#   - "13. Verification plan for Implementor"

description: >
  Implement the public API contract for Jupyter/pipeline callers by adding a
  structured result object (ReportResult), threading the settings_dir parameter
  end-to-end, and exposing a clean top-level package interface. All changes are
  additive or minimal-signature — no pipeline logic in run_report is restructured
  beyond count capture and result assembly. Eight files are touched (6 source,
  2 test); core/db_service.py is verified NO-CHANGE. Backward compatibility is
  preserved: settings_dir=None behaves identically to before (monkeypatchable
  APP_PATHS still used), existing imports still work, and no import cycles are
  introduced.

goals:
  - "ReportResult Pydantic model in core/models.py with 10 typed fields"
  - "_import_data returns rows_loaded as int"
  - "run_report captures rows_loaded, rows_exported, null_data_files, timings, duration and returns ReportResult"
  - "export_sql_to_csv returns total_rows (int) — 0 when table is empty"
  - "init_project(force, settings_dir=None) forwards to a settings_dir-aware AppPaths; None path byte-identical to today"
  - "initialize_settings forwards settings_dir to init_project (NOTE comment removed)"
  - "CLI init gains --settings-dir flag; CLI run captures result and prints summary"
  - "__init__.py eagerly re-exports 5 names with __all__; no import cycle; no import-time side effects"
  - "process_data returns ReportResult; docstring updated"
  - "All existing tests stay green; new tests cover model, return contract, settings_dir threading, re-exports, and end-to-end pipeline"

files:
  - path: src/mko_data_cleaner/core/models.py

    targets:
      - type: class
        name: ReportResult
      - type: module_import

    semantic_anchors:
      - "The import block at the top of models.py (after `from __future__ import annotations` and before the pydantic import block)"
      - "The end of the file (after the DataSettings class, which is the last class defined)"

    changes:
      - action: add_code
        description: >
          Add `from datetime import datetime` to the stdlib import section,
          alphabetically before `from pathlib import Path` (current L5).
          Required because ReportResult declares `datetime`-typed fields and
          Pydantic v2 needs the type resolvable at model-build time.
        code_hint: |
          from datetime import datetime

      - action: add_code
        description: >
          Append the ReportResult Pydantic BaseModel at the end of the file,
          in a new `# --- Report Result` section, after DataSettings. Fields
          match the research spec exactly (5 Path, list[Path], 2 datetime,
          float duration, dict[str, float] timings, 2 int). Follows existing
          model style: BaseModel, type hints, English docstring.
        code_hint: |
          # --- Report Result


          class ReportResult(BaseModel):
              """Structured result of a data-cleaning report run.

              Returned by ``process_data`` / ``AppService.run_report``
              so that Jupyter/pipeline callers can inspect outputs and
              metrics without parsing CLI strings or globbing the filesystem.
              """

              # --- output paths ---
              report_dir: Path
              import_dir: Path
              export_dir: Path
              db_path: Path
              null_data_files: list[Path]

              # --- timing ---
              started_at: datetime
              finished_at: datetime
              duration: float  # total seconds (wall clock)

              # --- per-stage timings (seconds) ---
              timings: dict[str, float]

              # --- row counts ---
              rows_loaded: int
              rows_exported: int

  - path: src/mko_data_cleaner/core/app_service.py

    targets:
      - type: method
        name: AppService._import_data
      - type: method
        name: AppService.run_report
      - type: module_import

    semantic_anchors:
      - "The stdlib import block (after `from pathlib import Path`, before the blank line that separates stdlib from first-party)"
      - "The existing `from mko_data_cleaner.core.models import DataSettings, LoggingSettings, MappingColumns` line"
      - "The `AppService._import_data` static method definition (currently `def _import_data(db_worker, csv_worker):`)"
      - "The line `self._import_data(db_worker, csv_worker)` inside `run_report`'s `with DBWorker(...)` block"
      - "The `csv_worker.export_sql_to_csv(...)` call inside `run_report` that exports to `self.export_path` (the final clean_data export)"
      - "The `print(...)` block at the end of `run_report` (the 'Обработка завершена' message)"

    changes:
      - action: add_code
        description: >
          Add `from time import perf_counter` to the stdlib import section
          (after `from pathlib import Path`, alphabetical).
        code_hint: |
          from time import perf_counter

      - action: modify
        description: >
          Extend the existing models import to include ReportResult:
          `from mko_data_cleaner.core.models import DataSettings, LoggingSettings, MappingColumns, ReportResult`
        code_hint: |
          from mko_data_cleaner.core.models import (
              DataSettings,
              LoggingSettings,
              MappingColumns,
              ReportResult,
          )

      - action: modify
        description: >
          `_import_data`: add `-> int` return annotation.
        code_hint: |
          @staticmethod
          def _import_data(db_worker, csv_worker) -> int:

      - action: add_code
        description: >
          `_import_data`: after the final `db_worker.update_index_from_data()`
          call (the last statement in the method), add `return rows_count`.
          The `rows_count` variable is already accumulated inside the method;
          this is a one-line addition.
        code_hint: |
          db_worker.update_index_from_data()
          return rows_count

      - action: modify
        description: >
          `run_report`: change return annotation from `-> None` (implicit)
          to `-> ReportResult`.
        code_hint: |
          def run_report(self, data_path: str | Path) -> ReportResult:

      - action: modify
        description: >
          `run_report`: after `start_time = ...` and the start `print(...)`,
          add `t_start = perf_counter()` plus initialize result-collection
          locals: `rows_loaded = 0`, `rows_exported = 0`,
          `null_data_files: list[Path] = []`, `timings: dict[str, float] = {}`.
        code_hint: |
          start_time = datetime.now().replace(microsecond=0)
          t_start = perf_counter()
          # ... existing print(...) ...
          rows_loaded = 0
          rows_exported = 0
          null_data_files: list[Path] = []
          timings: dict[str, float] = {}

      - action: modify
        description: >
          Inside the `with DBWorker(...)` block, replace
          `self._import_data(db_worker, csv_worker)` (currently L162)
          with a timing-wrapped capture: `t_import = perf_counter()` /
          `rows_loaded = self._import_data(db_worker, csv_worker)` /
          `timings["import"] = perf_counter() - t_import`.
        code_hint: |
          t_import = perf_counter()
          rows_loaded = self._import_data(db_worker, csv_worker)
          timings["import"] = perf_counter() - t_import

      - action: modify
        description: >
          Wrap the mapping stage (FTS/LIKE rules import + apply_mapping loop,
          currently L164-L223) in timing: insert `t_mapping = perf_counter()`
          before the `if not mapping_dict.fts_data.is_empty():` block
          and `timings["mapping"] = perf_counter() - t_mapping` after the
          `for action, data in ...generate_rules_blocks():` loop.
        code_hint: |
          t_mapping = perf_counter()
          # ... existing FTS/LIKE import + rules loop ...
          timings["mapping"] = perf_counter() - t_mapping

      - action: modify
        description: >
          Wrap the export stage (build_non_mapped through final
          export_sql_to_csv, currently L226-L248) in timing: insert
          `t_export = perf_counter()` before `db_worker.build_non_mapped()`
          and `timings["export"] = perf_counter() - t_export` after the
          final `export_sql_to_csv` return value is captured.
        code_hint: |
          t_export = perf_counter()
          # ... existing build_non_mapped, null_data export, sync, clean_data export ...
          timings["export"] = perf_counter() - t_export

      - action: modify
        description: >
          After the null_data `export_sql_to_csv` call (exports
          `db_worker.non_mapped_table` to `self.base_path`), capture the
          null_data file list inside the `with` block (per R2 — DB file is
          deleted on `__exit__`).
        code_hint: |
          null_data_files = sorted(self.base_path.glob("null_data_*"))

      - action: modify
        description: >
          Capture the return value of the final (clean_data)
          `export_sql_to_csv` call into `rows_exported`.
        code_hint: |
          rows_exported = csv_worker.export_sql_to_csv(
              db_con=db_worker.db_con,
              data_table=db_worker.data_tbl_name,
              export_path=self.export_path,
          )

      - action: add_code
        description: >
          After the existing end-of-`run_report` print block (the
          "Обработка завершена" message), before the implicit `return None`,
          construct and return a ReportResult from the captured locals.
          `self.base_path`, `self.import_path`, `self.export_path`,
          `self.db_path` are all accessible here (properties backed by
          `self.app_config` cached_property).
        code_hint: |
          return ReportResult(
              report_dir=self.base_path,
              import_dir=self.import_path,
              export_dir=self.export_path,
              db_path=self.db_path,
              null_data_files=null_data_files,
              started_at=start_time,
              finished_at=end_time,
              duration=perf_counter() - t_start,
              timings=timings,
              rows_loaded=rows_loaded,
              rows_exported=rows_exported,
          )

  - path: src/mko_data_cleaner/core/csv_service.py

    targets:
      - type: method
        name: CSVWorker.export_sql_to_csv

    semantic_anchors:
      - "The method signature `def export_sql_to_csv(self, db_con, data_table, ...)`"
      - "The `if total_rows == 0:` early-return branch inside the try block"
      - "The final `logger.debug(f"Successfully exported ...")` call before the `except Exception` block"

    changes:
      - action: modify
        description: >
          Change return annotation from implicit `None` to `-> int`.
        code_hint: |
          def export_sql_to_csv(
              self,
              db_con: sqlite3.Connection,
              data_table: str,
              file_prefix: str | None = None,
              export_path: Path | str | None = None,
          ) -> int:

      - action: modify
        description: >
          Update the docstring Returns section from "None" to "int".
        code_hint: |
          Returns
          -------
          int
              Number of rows exported (`total_rows` from `SELECT COUNT(*)`)
              or `0` when the table is empty.

      - action: modify
        description: >
          Change the early `return` (when `total_rows == 0`) to `return 0`.
        code_hint: |
          if total_rows == 0:
              logger.debug(f"Table {data_table} is empty.")
              return 0

      - action: add_code
        description: >
          After the final `logger.debug("Successfully exported ...")` call
          (still inside the `try` block, before the `except`), add
          `return total_rows`. This is the total row count from
          `SELECT COUNT(*)` — the same value used internally.
        code_hint: |
          logger.debug(
              f"Successfully exported {row_counter:,} rows from table '{data_table}'"
          )
          return total_rows

  - path: src/mko_data_cleaner/core/init_service.py

    targets:
      - type: function
        name: init_project
      - type: module_import

    semantic_anchors:
      - "The import line `from mko_data_cleaner.core.paths import APP_PATHS`"
      - "The `init_project` function definition and its docstring"
      - "The `_copy_templates(APP_PATHS.user_settings_dir, force)` call inside `init_project`"

    changes:
      - action: modify
        description: >
          Extend the paths import to include APP_DIR, APP_NAME, and AppPaths
          (mirroring what app_service.build_app_paths already imports).
        code_hint: |
          from mko_data_cleaner.core.paths import APP_DIR, APP_NAME, APP_PATHS, AppPaths

      - action: modify
        description: >
          Add `settings_dir: Path | None = None` parameter to `init_project`.
          When provided, construct AppPaths inline (mirroring build_app_paths
          in app_service.py) and use `app_paths.user_settings_dir` as
          destination; when None, use the module-level APP_PATHS (preserving
          the existing monkeypatch-tested behavior). _copy_templates source
          stays APP_PATHS.app_settings_dir (always packaged templates).
        code_hint: |
          def init_project(force: bool = False, settings_dir: Path | None = None) -> Path:
              """
              Initialize configuration directory with default templates.

              Args:
                  force: Overwrite existing files.
                  settings_dir: Optional user base directory. When provided,
                      settings are written to ``settings_dir / "settings"``
                      instead of the platformdirs-default location.

              Returns:
                  Path to the directory where settings were written.
              """
              if settings_dir is None:
                  user_settings_dir = APP_PATHS.user_settings_dir
              else:
                  app_paths = AppPaths(
                      app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir)
                  )
                  user_settings_dir = app_paths.user_settings_dir

              _copy_templates(user_settings_dir, force)
              return user_settings_dir

  - path: src/mko_data_cleaner/app.py

    targets:
      - type: function
        name: process_data
      - type: function
        name: initialize_settings
      - type: function
        name: init
      - type: function
        name: run
      - type: module_import

    semantic_anchors:
      - "The first-party import block (after `from rich.console import Console`)"
      - "The `process_data` function signature and docstring"
      - "The `get_app_service(settings_dir).run_report(path)` call inside `process_data`"
      - "The `initialize_settings` function body (the NOTE comment block)"
      - "The `init` CLI command decorator and signature"
      - "The `init_project(force=force)` call inside the `init` command"
      - "The `service.run_report(report)` call inside the `run` CLI command"
      - "The `console.print("[green]✅ Отчёт успешно завершён[/green]")` line in `run`"

    changes:
      - action: add_code
        description: >
          Add `from mko_data_cleaner.core.models import ReportResult` to the
          first-party import block (alphabetically between `core.init_service`
          and `core.utils`).
        code_hint: |
          from mko_data_cleaner.core.models import ReportResult

      - action: modify
        description: >
          `process_data`: change return type from `-> None` to `-> ReportResult`.
        code_hint: |
          def process_data(
              report_path: str | Path, settings_dir: Path | None = None
          ) -> ReportResult:

      - action: modify
        description: >
          `process_data`: add `return` before `get_app_service(settings_dir).run_report(path)`.
        code_hint: |
          return get_app_service(settings_dir).run_report(path)

      - action: modify
        description: >
          `process_data`: add a `Returns:` section to the docstring documenting
          the ReportResult return value.
        code_hint: |
          Returns:
              :class:`~mko_data_cleaner.core.models.ReportResult` with output
              paths, timings, and row counts.

      - action: modify
        description: >
          `initialize_settings`: remove the NOTE comment block (4 lines) and
          forward `settings_dir=settings_dir` to `init_project`.
        code_hint: |
          def initialize_settings(force: bool = False, settings_dir: Path | None = None) -> Path:
              """Initialize user settings with default templates (Jupyter/Airflow entry point)."""
              return init_project(force=force, settings_dir=settings_dir)

      - action: modify
        description: >
          `init` CLI command: add a `settings_dir` parameter using the same
          Annotated[Path | None, typer.Option(...)] pattern as `run` and
          `list_settings`, and pass it to `init_project`.
        code_hint: |
          def init(
              force: bool = typer.Option(
                  False, "--force", "-f", help="Перезаписать существующие файлы"
              ),
              settings_dir: Annotated[
                  Path | None,
                  typer.Option(
                      exists=False,
                      file_okay=False,
                      dir_okay=True,
                      help="User settings directory (default: system config dir)",
                  ),
              ] = None,
          ):
              """Инициализировать пользовательские настройки"""
              path = init_project(force=force, settings_dir=settings_dir)
              console.print(f"[green]✅ Настройки инициализированы:[/green] {path}")

      - action: modify
        description: >
          `run` CLI command: capture the return value of `service.run_report(report)`
          into `result`, and replace the generic success message with an
          English summary line showing loaded/exported counts, duration, and
          export path. Keep the existing Russian error message and style
          (per B3, new messages are English).
        code_hint: |
          try:
              result = service.run_report(report)
              console.print(
                  f"[green]✅ Report complete:[/green] "
                  f"{result.rows_loaded:,} loaded → {result.rows_exported:,} clean "
                  f"({result.duration:.1f}s) ↠ {result.export_dir}"
              )
          except Exception as e:
              console.print(f"[red]❌ Ошибка:[/red] {e}")
              raise typer.Exit(1) from e

  - path: src/mko_data_cleaner/__init__.py

    targets:
      - type: module

    semantic_anchors:
      - "The module docstring (first line of file)"
      - "The `try/except` block that sets `__version__`"
      - "The end of file (after the __version__ fallback)"

    changes:
      - action: modify
        description: >
          Update the module docstring to reflect the broader public API.
        code_hint: |
          """Public API for mko_data_cleaner: initialize_settings, process_data, and result types."""

      - action: add_code
        description: >
          Add three eager re-export imports after the existing
          `from importlib.metadata import ...` line (before the try/except
          block). Import chain is a clean DAG (no cycles — verified by
          reading all import chains; core/* never imports the top-level
          package __init__).
        code_hint: |
          from mko_data_cleaner.app import initialize_settings, process_data
          from mko_data_cleaner.core.errors import SettingsNotInitializedError
          from mko_data_cleaner.core.models import ReportResult

      - action: add_code
        description: >
          Add `__all__` list after the `__version__` try/except block.
          Must contain exactly these 5 names.
        code_hint: |
          __all__ = [
              "ReportResult",
              "SettingsNotInitializedError",
              "__version__",
              "initialize_settings",
              "process_data",
          ]

  - path: src/mko_data_cleaner/core/db_service.py

    targets: []

    changes:
      - action: none
        description: >
          NO CHANGE. Verified: both row counts are obtained without
          db_service.py modifications. `rows_loaded` comes from
          `polars.DataFrame.write_database()` return value in
          `AppService._import_data`. `rows_exported` comes from
          `SELECT COUNT(*)` already executed inside
          `CSVWorker.export_sql_to_csv` (csv_service.py L329-330).
          `DBWorker.perform_query` exists but is not needed for B4.

  - path: tests/test_app.py

    targets:
      - type: module_import
      - type: test_function

    semantic_anchors:
      - "The import block at the top of the file"
      - "The end of the file (after `test_yaml_to_dict_returns_empty_dict_on_invalid_yaml`)"

    changes:
      - action: modify
        description: >
          Add imports needed by new tests: `gzip`, `datetime`,
          `SimpleNamespace`; extend the `mko_data_cleaner.app` import to
          include `initialize_settings`; add `init_project`,
          `ReportResult` imports.
        code_hint: |
          import gzip
          import shutil
          import subprocess
          import sys
          from datetime import datetime
          from pathlib import Path
          from types import SimpleNamespace

          import pytest
          from pydantic import ValidationError
          from typer.testing import CliRunner

          from mko_data_cleaner.app import app, initialize_settings, process_data
          from mko_data_cleaner.core.app_service import get_app_service
          from mko_data_cleaner.core.errors import SettingsNotInitializedError
          from mko_data_cleaner.core.init_service import init_project
          from mko_data_cleaner.core.models import ReportResult
          from mko_data_cleaner.core.paths import APP_DIR, APP_NAME, AppPaths
          from mko_data_cleaner.core.utils import yaml_to_dict

      - action: add_code
        description: >
          Append new test functions at the end of the file. Tests cover:
          (A) ReportResult model construction + serialization;
          (B) process_data returns ReportResult (mocked get_app_service);
          (C) initialize_settings/init_project/init CLI with settings_dir;
          (D) top-level __init__ re-exports;
          (E) minimal end-to-end pipeline run.
        code_hint: |
          # ------------------------------------------------------------------- #
          # B4: ReportResult model contract
          # ------------------------------------------------------------------- #
          def test_report_result_model_construction_and_serialization():
              """ReportResult accepts all 10 fields and serializes via model_dump()."""
              result = ReportResult(
                  report_dir=Path("/tmp/report"),
                  import_dir=Path("/tmp/report/raw_data"),
                  export_dir=Path("/tmp/report/clean_data"),
                  db_path=Path("/tmp/report/data_base/db.db"),
                  null_data_files=[],
                  started_at=datetime(2024, 1, 1, 12, 0, 0),
                  finished_at=datetime(2024, 1, 1, 12, 0, 5),
                  duration=5.0,
                  timings={"import": 1.0, "mapping": 3.0, "export": 1.0},
                  rows_loaded=100,
                  rows_exported=95,
              )
              assert isinstance(result.report_dir, Path)
              assert isinstance(result.null_data_files, list)
              assert isinstance(result.timings, dict)
              assert result.rows_loaded == 100
              assert result.rows_exported == 95
              assert result.duration == 5.0
              dumped = result.model_dump()
              assert isinstance(dumped["started_at"], datetime)
              assert dumped["rows_loaded"] == 100


          # ------------------------------------------------------------------- #
          # B4: process_data returns ReportResult (mock get_app_service)
          # ------------------------------------------------------------------- #
          def test_process_data_returns_report_result(
              monkeypatch, populated_settings: Path, empty_report_dir: Path
          ):
              """process_data returns the ReportResult from run_report."""
              fake_result = ReportResult(
                  report_dir=empty_report_dir,
                  import_dir=empty_report_dir / "raw_data",
                  export_dir=empty_report_dir / "clean_data",
                  db_path=empty_report_dir / "data_base/db_example.db",
                  null_data_files=[],
                  started_at=datetime.now(),
                  finished_at=datetime.now(),
                  duration=0.5,
                  timings={"import": 0.1, "mapping": 0.2, "export": 0.2},
                  rows_loaded=42,
                  rows_exported=40,
              )
              fake_service = SimpleNamespace(run_report=lambda path: fake_result)
              monkeypatch.setattr(
                  "mko_data_cleaner.app.get_app_service",
                  lambda settings_dir=None: fake_service,
              )
              result = process_data(empty_report_dir, settings_dir=populated_settings)
              assert isinstance(result, ReportResult)
              assert result.rows_loaded == 42
              assert result.rows_exported == 40
              assert result.report_dir == empty_report_dir


          # ------------------------------------------------------------------- #
          # B4: settings_dir threading — initialize_settings
          # ------------------------------------------------------------------- #
          def test_initialize_settings_threads_settings_dir(tmp_path: Path):
              """initialize_settings(settings_dir=...) writes to settings_dir / settings."""
              user_dir = tmp_path / "myapp"
              result = initialize_settings(settings_dir=user_dir)
              assert result == user_dir / "settings"
              assert (result / "app_config.yaml").exists()
              assert (result / "log_config.yaml").exists()


          def test_initialize_settings_force_overwrites(tmp_path: Path):
              """initialize_settings(force=True, settings_dir=...) overwrites existing files."""
              user_dir = tmp_path / "myapp"
              dest = user_dir / "settings"
              dest.mkdir(parents=True)
              (dest / "app_config.yaml").write_text("stale")

              result = initialize_settings(force=True, settings_dir=user_dir)
              assert (result / "app_config.yaml").exists()
              # The template should have overwritten the stale file
              assert "data_paths" in (result / "app_config.yaml").read_text()


          # ------------------------------------------------------------------- #
          # B4: settings_dir threading — init_project
          # ------------------------------------------------------------------- #
          def test_init_project_with_settings_dir(tmp_path: Path):
              """init_project(force=True, settings_dir=...) writes to settings_dir / settings."""
              user_dir = tmp_path / "myapp"
              result = init_project(force=True, settings_dir=user_dir)
              assert result == user_dir / "settings"
              assert (result / "app_config.yaml").exists()
              assert (result / "log_config.yaml").exists()


          # ------------------------------------------------------------------- #
          # B4: settings_dir threading — CLI init --settings-dir
          # ------------------------------------------------------------------- #
          def test_init_cli_with_settings_dir(tmp_path: Path):
              """CLI `init --settings-dir X --force` writes to X / settings."""
              user_dir = tmp_path / "myapp"
              runner = CliRunner()
              result = runner.invoke(
                  app,
                  ["init", "--settings-dir", str(user_dir), "--force"],
              )
              assert result.exit_code == 0, result.output
              dest = user_dir / "settings"
              assert (dest / "app_config.yaml").exists()
              assert (dest / "log_config.yaml").exists()


          # ------------------------------------------------------------------- #
          # B4: settings_dir threading — init_project with None uses APP_PATHS
          # ------------------------------------------------------------------- #
          def test_init_project_none_uses_app_paths(tmp_path: Path, monkeypatch):
              """init_project(force, settings_dir=None) uses monkeypatchable APP_PATHS."""
              tmp_app_paths = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=tmp_path)
              monkeypatch.setattr("mko_data_cleaner.core.init_service.APP_PATHS", tmp_app_paths)
              result = init_project(force=True, settings_dir=None)
              assert result == tmp_path / "settings"
              assert (result / "app_config.yaml").exists()


          # ------------------------------------------------------------------- #
          # B4: top-level re-exports
          # ------------------------------------------------------------------- #
          def test_top_level_exports():
              """import mko_data_cleaner exposes exactly the 5 public names."""
              import mko_data_cleaner

              expected = {
                  "ReportResult",
                  "SettingsNotInitializedError",
                  "__version__",
                  "initialize_settings",
                  "process_data",
              }
              assert set(mko_data_cleaner.__all__) == expected
              assert isinstance(mko_data_cleaner.__version__, str)
              assert len(mko_data_cleaner.__version__) > 0
              assert mko_data_cleaner.process_data is process_data
              assert mko_data_cleaner.initialize_settings is initialize_settings
              assert mko_data_cleaner.ReportResult is ReportResult
              assert mko_data_cleaner.SettingsNotInitializedError is SettingsNotInitializedError


          def test_top_level_no_import_side_effects(tmp_path: Path):
              """importing the top-level package must not create a settings dir."""
              target = str(tmp_path)
              script = (
                  "import platformdirs, os, sys\n"
                  "platformdirs.user_config_dir = lambda *a, **k: sys.argv[1]\n"
                  "import mko_data_cleaner\n"
                  "print(mko_data_cleaner.__version__)\n"
                  "if os.path.isdir(os.path.join(sys.argv[1], 'settings')):\n"
                  "    print('FAIL: settings dir created', file=sys.stderr)\n"
                  "    sys.exit(1)\n"
                  "print('OK')\n"
                  "sys.exit(0)\n"
              )
              result = subprocess.run(
                  [sys.executable, "-c", script, target],
                  capture_output=True, text=True,
              )
              assert result.returncode == 0, result.stderr


          # ------------------------------------------------------------------- #
          # B4: minimal end-to-end pipeline (bridge to B5)
          # ------------------------------------------------------------------- #
          def test_process_data_end_to_end(populated_settings: Path, tmp_path: Path):
              """Run a tiny synthetic report and verify the ReportResult fields."""
              report_dir = tmp_path / "report"
              raw_data_dir = report_dir / "raw_data"
              dict_dir = report_dir / "dict"
              raw_data_dir.mkdir(parents=True)
              dict_dir.mkdir(parents=True)

              # Data CSV matching app_config.yaml: index_column=AdId, date_column=researchDate
              # Extension is 'gz' (from template config), so files must be .csv.gz
              csv_lines = [
                  "AdId;researchDate;YEAR;MONTH;BRAND",
                  "id1;2024-01-01;2024;01;Brand1",
                  "id2;2024-01-15;2024;01;Brand2",
                  "id3;2024-02-01;2024;02;Brand1",
              ]
              with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
                  f.write("\n".join(csv_lines) + "\n")

              # Dictionary CSV — columns: action(0) match(1) search(2) value(3) term(4) tag(5)
              # search=4 means "search in BRAND column" (data table column index 4:
              #   AdId=0, researchDate=1, YEAR=2, MONTH=3, BRAND=4, tag=5)
              dict_lines = [
                  "action;match;search;value;term;tag",
                  "d;f;4;NULL;Brand2;",
                  "a;f;4;NULL;Brand1;added_by_brand",
              ]
              with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
                  f.write("\n".join(dict_lines) + "\n")

              result = process_data(report_dir, settings_dir=populated_settings)

              assert isinstance(result, ReportResult)
              assert result.rows_loaded == 3
              assert result.rows_exported == 2  # Brand2 row deleted
              assert result.rows_exported <= result.rows_loaded
              assert result.export_dir == report_dir / "clean_data"
              assert result.import_dir == report_dir / "raw_data"
              assert result.report_dir == report_dir
              assert (result.export_dir).exists()
              assert len(list(result.export_dir.glob("*.gz"))) > 0
              assert isinstance(result.null_data_files, list)
              assert result.duration > 0.0
              assert set(result.timings.keys()) == {"import", "mapping", "export"}
              assert all(v >= 0.0 for v in result.timings.values())

  - path: tests/test_csv_service.py

    targets:
      - type: test_function

    semantic_anchors:
      - "The end of the file (after `test_get_file_name`)"
      - "The import block at the top of the file"

    changes:
      - action: modify
        description: >
          Add `import sqlite3` to the top of the file (stdlib import, before
          the `import polars as pl` line).
        code_hint: |
          import sqlite3

          import polars as pl

      - action: add_code
        description: >
          Append two tests verifying that export_sql_to_csv returns an int
          row count — both for a populated table (returns N) and an empty
          table (returns 0). Uses the same CSVWorker.__new__ pattern as the
          existing test_get_file_name test (bypass __init__, set export_settings).
        code_hint: |
          def test_export_sql_to_csv_returns_row_count(tmp_path):
              """export_sql_to_csv returns the row count as int."""
              db_path = tmp_path / "test.db"
              con = sqlite3.connect(db_path)
              con.execute("CREATE TABLE data_table (id INTEGER, name TEXT, value TEXT)")
              con.executemany(
                  "INSERT INTO data_table VALUES (?, ?, ?)",
                  [(1, "Alice", "tag1"), (2, "Bob", "tag2"), (3, "Charlie", None)],
              )
              con.commit()

              worker = CSVWorker.__new__(CSVWorker)
              worker.export_settings = {"compression": "gzip", "chunk_size": 10000}

              count = worker.export_sql_to_csv(
                  db_con=con,
                  data_table="data_table",
                  export_path=tmp_path / "export",
              )

              assert isinstance(count, int)
              assert count == 3
              con.close()


          def test_export_sql_to_csv_returns_zero_for_empty_table(tmp_path):
              """export_sql_to_csv returns 0 when the table has no rows."""
              db_path = tmp_path / "test.db"
              con = sqlite3.connect(db_path)
              con.execute("CREATE TABLE empty_table (id INTEGER)")
              con.commit()

              worker = CSVWorker.__new__(CSVWorker)
              worker.export_settings = {"compression": "gzip"}

              count = worker.export_sql_to_csv(
                  db_con=con,
                  data_table="empty_table",
                  export_path=tmp_path / "export",
              )

              assert isinstance(count, int)
              assert count == 0
              con.close()

acceptance_criteria:
  - "ReportResult Pydantic model exists in core/models.py with all 10 fields; `from mko_data_cleaner.core.models import ReportResult` succeeds"
  - "_import_data(db_worker, csv_worker) -> int returns rows_count"
  - "run_report(data_path) -> ReportResult captures rows_loaded from _import_data and rows_exported from the final export_sql_to_csv call"
  - "run_report collects null_data_files via glob inside the with DBWorker block"
  - "run_report returns a ReportResult with timing dict keys {import, mapping, export}"
  - "export_sql_to_csv returns total_rows (int); returns 0 when table is empty"
  - "init_project(force, settings_dir=None) -> Path forwards settings_dir to inline AppPaths construction; None path byte-identical to existing behavior"
  - "initialize_settings(force, settings_dir=None) forwards settings_dir to init_project; NOTE comment removed"
  - "CLI init command has --settings-dir flag"
  - "CLI run command captures result and prints summary with counts + duration + export path"
  - "process_data returns ReportResult from run_report; docstring updated with Returns section"
  - "__init__.py eagerly imports initialize_settings, process_data, ReportResult, SettingsNotInitializedError; __all__ has exactly 5 names"
  - "import mko_data_cleaner has no import-time side effects (no settings dir creation)"
  - "No circular imports introduced (__init__ → app → core.* → none back to __init__)"
  - "core/db_service.py is unchanged (no change needed)"
  - "All 37 existing tests still pass"
  - "New tests: test_report_result_model_construction_and_serialization, test_process_data_returns_report_result, test_initialize_settings_threads_settings_dir, test_initialize_settings_force_overwrites, test_init_project_with_settings_dir, test_init_cli_with_settings_dir, test_init_project_none_uses_app_paths, test_top_level_exports, test_top_level_no_import_side_effects, test_process_data_end_to_end, test_export_sql_to_csv_returns_row_count, test_export_sql_to_csv_returns_zero_for_empty_table"

tests_to_create:
  - path: tests/test_app.py
    rationale: >
      Extends the existing test_app.py which already owns the populated_settings,
      empty_report_dir, and _isolate_logging fixtures plus the CliRunner/app
      infrastructure. Co-locating B4 contract tests here matches the B1 pattern
      and the research section 10 recommendation. No new test files created.
    tests:
      - test_report_result_model_construction_and_serialization
        description: >
          Construct ReportResult with all 10 fields (sample paths, datetimes,
          float, dict, ints). Assert field types, values, and that
          model_dump() produces serializable output.
      - test_process_data_returns_report_result
        description: >
          Mock get_app_service to return a SimpleNamespace service whose
          run_report returns a pre-built ReportResult. Call
          process_data(empty_report_dir, settings_dir=populated_settings).
          Assert the return value is ReportResult with expected fields.
      - test_initialize_settings_threads_settings_dir
        description: >
          Call initialize_settings(settings_dir=tmp_path / "myapp").
          Assert return is tmp_path/app_settings and files exist.
      - test_initialize_settings_force_overwrites
        description: >
          Pre-seed a stale app_config.yaml in the destination, call
          initialize_settings(force=True, settings_dir=...). Assert the file
          is overwritten with template content.
      - test_init_project_with_settings_dir
        description: >
          Call init_project(force=True, settings_dir=tmp_path / "myapp").
          Assert return is tmp_path/settings and files exist.
      - test_init_cli_with_settings_dir
        description: >
          Invoke `init --settings-dir X --force` via CliRunner. Assert
          exit_code 0 and files created at X/settings/.
      - test_init_project_none_uses_app_paths
        description: >
          Monkeypatch init_service.APP_PATHS to a tmp-based AppPaths.
          Call init_project(force=True, settings_dir=None). Assert it uses
          the monkeypatched APP_PATHS.user_settings_dir (preservation of
          existing test pattern).
      - test_top_level_exports
        description: >
          import mko_data_cleaner. Assert __all__ equals the exact 5-name set.
          Assert process_data, initialize_settings, ReportResult,
          SettingsNotInitializedError are accessible and are the same objects
          as their source-module counterparts.
      - test_top_level_no_import_side_effects
        description: >
          Subprocess: mock platformdirs.user_config_dir to a tmp path,
          import mko_data_cleaner (top-level, not just app). Assert no
          settings/ directory is created. (Extends the existing
          test_import_app_does_not_create_settings_dir pattern to the new
          eager-import __init__.py.)
      - test_process_data_end_to_end
        description: >
          Create a minimal report: raw_data/sample.csv.gz (3 rows, columns
          AdId;researchDate;YEAR;MONTH;BRAND), dict/merged_dictionary.csv
          (2 rules: DELETE Brand2, ADD tag for Brand1). Call
          process_data(report_dir, settings_dir=populated_settings).
          Assert ReportResult: rows_loaded==3, rows_exported==2, export_dir
          exists with .gz files, null_data_files is a list, duration > 0,
          timings has exactly {import, mapping, export} keys all >= 0.
        notes: >
          Data file uses .csv.gz extension per template config (extension: gz).
          The index_column is 'AdId' (from config) so CSV headers must use
          'AdId' (not uppercase 'ADID'). The search index 4 in the dictionary
          refers to the BRAND column (0-based data-table column position after
          clean_names: AdId=0, researchDate=1, YEAR=2, MONTH=3, BRAND=4, tag=5).
          If the pipeline fails due to environment-specific Polars/SQLite
          behavior, the test can be temporarily skipped; the mock-based
          test_process_data_returns_report_result still validates the
          return-type contract independently.

  - path: tests/test_csv_service.py
    rationale: >
      The new return contract for export_sql_to_csv (returns int) is a CSVWorker
      concern. test_csv_service.py already tests CSVWorker methods using the
      CSVWorker.__new__ bypass pattern; adding return-value tests here is the
      natural home. No existing assertions assert None — verified by reading
      the 3 existing tests (test_get_files_suffix, test_is_date_column,
      test_get_file_name).
    tests:
      - test_export_sql_to_csv_returns_row_count
        description: >
          Create a 3-row SQLite table, call export_sql_to_csv via
          CSVWorker.__new__ (set export_settings only). Assert return is int == 3
          and .csv.gz files are created.
      - test_export_sql_to_csv_returns_zero_for_empty_table
        description: >
          Create an empty SQLite table, call export_sql_to_csv. Assert return
          is int == 0 (the early-return path). No file should be written.

verification_commands:
  - "uv run ruff check src/ tests/"
  - "uv run ruff check --fix src/"
  - "uv run pytest"
  - "uv run python -c \"from mko_data_cleaner import initialize_settings, process_data, ReportResult, SettingsNotInitializedError, __version__; print('imports OK')\""
  - "uv run python -c \"import mko_data_cleaner; print(mko_data_cleaner.__all__)\""
  - "uv run python -c \"from mko_data_cleaner.core.models import ReportResult; r = ReportResult(report_dir=__import__('pathlib').Path('.'), import_dir=__import__('pathlib').Path('.'), export_dir=__import__('pathlib').Path('.'), db_path=__import__('pathlib').Path('.'), null_data_files=[], started_at=__import__('datetime').datetime.now(), finished_at=__import__('datetime').datetime.now(), duration=0.1, timings={'import': 0.1}, rows_loaded=1, rows_exported=1); print('ReportResult OK', r.model_dump())\""

out_of_scope:
  - "README.md changes (Doc-specialist step — see stale claims in preamble)"
  - "B5 notebook / synthetic sample data"
  - "B6 CI workflow"
  - "B7 version bump / git tag / publish"
  - "Packaging changes (B2/B3 already done)"
  - "Refactoring run_report core pipeline logic beyond count capture + result assembly"
  - "Moving build_app_paths to paths.py (deferred per R7)"
  - "PEP 562 lazy __getattr__ in __init__.py (deferred — eager imports are the chosen minimal approach)"
  - "StrEnum for timing stage keys (deferred per R6 — dict[str, float] is minimal)"
  - "Per-rule statistics, rows_deleted/rows_replaced/rows_tagged fields (excluded per D8)"
  - "core/utils.py yaml_to_dict (already fixed in B1 — returns {} on both except branches)"
  - "core/paths.py (no change — AppPaths fields verified correct)"
  - "core/errors.py (no change — SettingsNotInitializedError already exists)"
  - "core/dict_service.py (no change)"
  - "pyproject.toml (no new dependencies needed — pydantic already declared)"

risks:
  - "R1: write_database return value — polars>=1.38.1 declared in pyproject; write_database() returns int in all versions in that range (verified empirically via existing code that already relies on this). If a user has polars<1.38.1, _import_data would already crash before the return change — not a B4 regression."
  - "R2: DB deletion timing — DBWorker.__exit__ → close() → _delete_base_files() deletes the SQLite file. null_data_files glob is collected INSIDE the with block (after null_data export, before sync/export). All count/path fields are captured inside the with block or reference stable properties (self.base_path, self.import_path, etc.) — no DB access after __exit__."
  - "R3: null_data_files glob pattern — base_path.glob('null_data_*') matches all null_data_*.csv.gz files regardless of compression type (prefix-only match). Empty non_mapped table → early return 0 in export_sql_to_csv → no files → empty list. Verified from CSVWorker.get_file_name format."
  - "R4: _import_data is a @staticmethod — adding -> int return annotation is trivial; callers capture the return: `rows_loaded = self._import_data(db_worker, csv_worker)`."
  - "R5: export_sql_to_csv return consistency — early return changed from `return` to `return 0`; end-of-function adds `return total_rows`. No existing tests assert None (verified: test_csv_service.py has 3 tests, none reference export_sql_to_csv)."
  - "R6: timings dict keys — using plain string keys ('import', 'mapping', 'export'). Project rule #10 (StrEnum) is not applied to internal pipeline labels — a dict[str, float] is the minimal contract per research."
  - "R7: AppPaths construction duplication — init_project inlines AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir)). This duplicates build_app_paths in app_service.py. Acceptable for B4 (init_service must stay lightweight — importing build_app_paths from app_service would pull in polars/adbc). Recommendation: move build_app_paths to paths.py in a future refactor."
  - "R8: README stale claims — listed in preamble. The Doc-specialist step must update: L65 (top-level import), L67 (return value usage), L62-68 (initialize_settings prerequisite), L39-43 (init --settings-dir), L72-73 (run --settings-dir), and add Python API section for ReportResult and SettingsNotInitializedError."
  - "R9: DataSettings requires read_settings and export_settings — the test conftest.py realistic_config fixture omits them, so DataSettings(**realistic_config) would fail. This is pre-existing and NOT touched by B4. The app_config.yaml template includes them."
  - "R10: test_process_data_past_gate_does_not_raise_settings_error — calls process_data with empty_report_dir (no data files); pipeline fails with WrongDataSettings, caught by broad `except Exception`. Test still passes. Post-B5 enhancement recommended: use a populated report dir."
  - "R11: Heavy import regression — import mko_data_cleaner now triggers polars + adbc via __init__ → app → app_service. No import-time side effects (B1 verified). Acceptable per spec R4 (public API is initialize_settings + process_data, both need the pipeline)."
  - "R12: end-to-end test fragility — depends on polars write_database ADBC adapter, SQLite FTS5, and LIKE pattern matching with COLLATE NOCASE. The test data is designed to avoid FTS (LIKE-only rules) and uses columns matching app_config.yaml. If environment-specific behavior causes failure, the mock-based test_process_data_returns_report_result still validates the return contract independently."

```

---

## B5 — T5: Example notebook with synthetic sample data

**Agents:** Planner → Implementor.

### {context_r} — Research findings (B5/T5)

**Confidence:** HIGH — all claims verified statically against committed B1–B4 state and dynamically via reproduction on this machine (Windows 11, Python 3.14.7, uv 0.9.26). B5 is sequenced strictly after B1–B4 (the public API + `ReportResult` + `settings_dir` must exist before notebook cells 3 and 7 are real).

#### 0. Verified current state (post B1–B4, HEAD)

- Public API surface (verified at `app.py`, `__init__.py`, `core/app_service.py`):
  - `from mko_data_cleaner import initialize_settings, process_data, ReportResult, SettingsNotInitializedError, __version__`
  - `initialize_settings(force=False, settings_dir=None) -> Path` — writes `app_config.yaml` + `log_config.yaml` into `settings_dir / "settings"`.
  - `process_data(report_path, settings_dir=None) -> ReportResult`.
  - `ReportResult` fields (verified `core/models.py`): `report_dir`, `import_dir`, `export_dir`, `db_path`, `null_data_files: list[Path]`, `started_at`, `finished_at`, `datetime`, `duration: float`, `timings: dict[str, float]`, `rows_loaded: int`, `rows_exported: int`.
- `examples/` directory: **MISSING** (verified). Notebook path will be `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb`.

#### 1. Template `app_config.yaml` expectations (FACT — read `src/mko_data_cleaner/settings/app_config.yaml`)

- `data_paths`: `import_folder=raw_data`, `export_folder=clean_data`, `dict_file=dict/merged_dictionary.csv`, `db_file=data_base/db_example.db`.
- `data_file_settings`: `extension=gz`, `index_column=AdId`, `date_column=researchDate`.
- `dict_file_settings.col_indexes`: `action=0, match=1, search=2, term=4`.
- `read_settings.from_csv`: `separator=";"` (plus encoding/decimal_comma/etc. polars kwargs).
- `export_settings.to_csv`: `compression=gzip`, `chunk_size=100000`, `separator=";"`, `include_header=True`.
- **Inconsistency (D9/R7/A5 vs README):** the shipped template hardcodes `index_column: AdId` (camelCase), but the README's documented input shape (L83) uses uppercase `ADID`. `make_valid("AdId")="AdId"` ≠ `clean_names("ADID")="ADID"` as *strings*, though SQLite column-name resolution is case-insensitive. To avoid relying on implicit case-folding, the notebook uses the README's uppercase column family **with an explicit `settings_dir`-local config override** (`index_column="ADID"`) so execution is deterministic. The simpler template-consistent path (`AdId;researchDate;...` headers, no override) is the proven B4 `test_process_data_end_to_end` sample and is noted as an alternative.

#### 2. Export naming from `csv_service.py` (FACT — read `core/csv_service.py`)

- `CSVWorker.get_file_name(name_prefix, name_suffix)` → `f"{name_prefix}_{time_str}_{name_suffix}{ext}"`; `time_str=strftime("%Y%m%d-%H%M%S")`; `ext=get_files_suffix(compression)`.
- `get_files_suffix("gzip")`/`.gz` → `.csv.gz`; `None`/`"infer"` → `.csv`; `"zip"`→`.csv.zip`, `"zstd"`→`.csv.zst`, `"bz2"`→`.csv.bz2`, `"xz"`→`.csv.xz`.
- Null-data export: `app_service.run_report` calls `export_sql_to_csv(file_prefix="null_data", export_path=self.base_path, ...)` → files named `null_data_{timestamp}_{idx}.csv.gz` in **report root**; captured as `null_data_files = sorted(self.base_path.glob("null_data_*"))` **inside** the `with DBWorker` block (R2-verified: DB file deleted on `__exit__`, but CSVs in report dir survive).
- Clean-data export: `export_sql_to_csv(data_table=db_worker.data_tbl_name, export_path=self.export_path)` (no `file_prefix`) → prefix defaults to table name `data_table` → files `data_table_{timestamp}_{idx}.csv.gz` in **`clean_data/`**.
- `export_sql_to_csv` now returns `total_rows` (int) per B4; `0` when the table is empty (early-return path at the `if total_rows == 0:` branch).

#### 3. e2e fixture structure from `tests/test_app.py` (FACT — read `tests/test_app.py` + `tests/conftest.py`)

- `populated_settings(tmp_path)` fixture: builds `tmp_path/userdir/settings/`, copies `app_config.yaml` + `log_config.yaml` from `TEMPLATE_SETTINGS_DIR = APP_DIR / "settings"` (`src/mko_data_cleaner/core/paths.py` → `APP_DIR / "settings"`). Returns `user_dir` (the **base** dir, not the `settings/` subdir). `process_data(report, settings_dir=user_dir)` resolves `user_dir/settings/`.
- `empty_report_dir(tmp_path)` fixture: `tmp_path/report` (exists, empty).
- Verified `test_process_data_end_to_end` sample shape:
  - Header: `AdId;researchDate;YEAR;MONTH;BRAND` (5 cols; clean_names leaves valid identifiers unchanged).
  - Column map after `clean_names`: `AdId=0, researchDate=1, YEAR=2, MONTH=3, BRAND=4, tag=5` (tag is the ADD-extra column from the dict's column 5).
  - Dict header: `action;match;search;value;term;tag`; rules `d;f;4;NULL;Brand2;` and `a;f;4;NULL;Brand1;added_by_brand` (search=4 → BRAND column). `value` (col 3) is unused (README-confirmed).
  - Data file written as `.csv.gz` (gzip); dict file written as **plain** `merged_dictionary.csv` (read directly by `get_dictionary`, `extension` only governs merge-globs — verified: no crash).
  - Expected: `rows_loaded==3`, `rows_exported==2`, `export_dir/*.gz` non-empty, `null_data_files` is a list, `timings` keys `{import,mapping,export}`.
- `_isolate_logging` autouse fixture restores root logger state after `dictConfig`; notebook execution does NOT need this (fresh kernel), but the committed pytest-based notebook test reuses the dev venv (package already installed) so `dictConfig` runs in-process.

#### 4. Tooling for notebook validation/execution in THIS env (FACT — probed)

- `pyproject.toml [dependency-groups] dev` = `black, isort, pytest, pytest-mock, ruff, mypy, twine`. **None** of `jupyter`, `nbformat`, `nbclient`, `ipykernel`, `nbconvert` present. Confirmed: `uv run python -c "import nbformat"` → `ModuleNotFoundError`; site-packages scan empty.
- `python -m build` is also absent from dev (spec F11/R6/D10 — belongs to T6).
- uv 0.9.26, Python 3.14.7.
- **Decision:** B5 adds to `[dependency-groups] dev`: `nbformat>=5.10` (structural validation + cell-id schema), `nbclient>=0.10` (clean-kernel execution via `NotebookClient`), `ipykernel>=6` (kernel). Execution is programmatic via `nbclient` (no full `jupyter` install needed). `nbformat.validate` enforces nbformat 4.5+ cell-id requirements.
- **Execution gating:** the install cell pins `@v0.1.0` (D1/D11). That tag is cut in T7/B7, *after* B5. In B5 the tag does **not** exist, so the install cell cannot succeed in a clean kernel. Verification therefore (a) validates structure unconditionally (`nbformat.validate`), and (b) executes the notebook in the **dev venv** with the install cell made skippable via an `import`-guard (`try: import mko_data_cleaner / except ImportError: %pip install ...`) so the already-installed dev package satisfies downstream cells. Full clean-kernel **against-tag** execution is the T7 gate (deferred; not a B5 blocker).

#### 5. Notebook vs. committed test boundary

- `tests/test_notebook.py` (committed in B5): **structure-only** (fast, always green) — asserts `nbformat.validate` passes, cell ids are unique/non-empty, the install cell pins `@v0.1.0`, and the public-API import cell references `mko_data_cleaner`. No kernel execution in the committed test (avoid env fragility flagged in B4 R12); execution is exercised via the B5 verification_commands manually + the T7 against-tag gate.

#### 6. Synthetic sample design (verified sound)

Header (README uppercase Mediascope family): `YEAR;MONTH;ADVERTISERS;BRANDS;ADID;MEDIA;COST_RUB;QUANTITY`.
After `clean_names` (all valid identifiers, unchanged): `YEAR=0, MONTH=1, ADVERTISERS=2, BRANDS=3, ADID=4, MEDIA=5, COST_RUB=6, QUANTITY=7`; extra column `tag=8`.
Custom config override (cell 6): `data_file_settings.index_column = "ADID"` (→ `make_valid("ADID")="ADID"` matches header); `date_column = None` (data has no date column → `check_date_column` returns None → `update_index_from_data` skips date; no crash).

Dictionary header: `action;match;search;value;term;tag`; `col_indexes` from template = action0/match1/search2/term4; extra col = `tag`(5). Rules target `search=3` (BRANDS column):
- `d;f;3;NULL;Brand2;`  → DELETE BRANDS=="Brand2"
- `r;f;3;NULL;Apple;fruit` → REPLACE: write `fruit` into `tag` where BRANDS=="Apple" (replacement value lives in the extra `tag` column; `value`/col3 unused — README-confirmed)
- `a;f;3;NULL;Brand1;added_by_brand` → ADD: union `added_by_brand` into `tag` where BRANDS=="Brand1"

Data (7 rows):
```
YEAR;MONTH;ADVERTISERS;BRANDS;ADID;MEDIA;COST_RUB;QUANTITY
2024;01;EXAMPLE;Apple;id1;TV;1500000;25
2024;01;EXAMPLE;Brand2;id2;TV;3200000;12
2024;02;FAKE;Brand1;id3;TV;450000;8
2024;02;FAKE;Orange;id4;TV;100000;5
2024;03;EXAMPLE;Apple;id5;TV;200000;10
2024;03;FAKE;Pear;id6;TV;150000;3
2024;03;EXAMPLE;Brand2;id7;TV;500000;7
```
Expected: `rows_loaded=7`; DELETE removes 2 `Brand2` rows; `rows_exported=3` (Apple×2 → tag=fruit, Brand1×1 → tag=added_by_brand); `null_data_*` holds 2 unmatched rows (Orange, Pear → tag NULL) in one gzipped file (chunk_size 100000); `import`/`mapping`/`export` timings all >=0.

---

### {task_description} — Implementation Task (B5/T5)

```yaml
id: b5_t5_example_notebook

title: "B5/T5 — Example notebook with synthetic sample data + examples/README.md"

priority: medium

depends_on:
  - b1_t1_remove_import_time_init
  - b2_t2_ship_yaml_templates
  - b3_t3_metadata_hygiene
  - b4_t4_public_api_contract

description: >
  Provide a ready, valid (nbformat 4.5+), executable example notebook that
  demonstrates mko_data_cleaner as one step in a Jupyter pipeline: install
  (pinned to the v0.1.0 tag per D1/D11) → import + version →
  initialize_settings → expected input layout → create a tiny synthetic
  report (UPPERCASE Mediascope-family CSV + dictionary exercising
  DELETE/REPLACE/ADD) → process_data → consume the ReportResult and read
  the generated clean_data_*.csv.gz and null_data_*.csv.gz outputs. The
  notebook generates its synthetic sample in-cell (no 43 MB file; C5/A7) into
  a temporary directory and ships a short examples/README.md that documents
  prerequisites, the cell flow, expected outputs, and troubleshooting. Notebook
  tooling (nbformat + nbclient + ipykernel) is added to the dev dependency group
  since the project currently ships none (verified). The install cell pins
  @v0.1.0; against-tag clean-kernel execution is the T7 gate, so B5 verifies
  structure (nbformat.validate) and dev-venv execution with the install cell
  guarded to skip when the package is already importable.

goals:
  - "examples/notebooks/mko_data_cleaner_pipeline_example.ipynb exists (nbformat 4.5+, valid cell ids)"
  - "Notebook cell flow: install → import+version → initialize_settings → layout → synthetic sample → process_data → consume result"
  - "Install cell pins %pip install git+...@v0.1.0 (D1/D11)"
  - "Synthetic sample mirrors README uppercase input shape (YEAR;MONTH;ADVERTISERS;BRANDS;ADID;MEDIA;COST_RUB;QUANTITY) with DELETE/REPLACE/ADD dictionary rules"
  - "3 dictionary rules: DELETE Brand2, REPLACE Apple→fruit, ADD Brand1 tag"
  - "7 data rows; expected rows_loaded=7, rows_exported=3, 2 unmatched → null_data"
  - "Custom config override sets index_column=ADID (deterministic; avoids README/template header-case mismatch)"
  - "process_data returns ReportResult with rows_loaded, rows_exported, timings {import,mapping,export}, export_dir, null_data_files"
  - "Consume cell reads clean_data_*.csv.gz and null_data_*.csv.gz via polars and prints shapes/heads"
  - "examples/README.md documents prerequisites, cell flow, sample, expected outputs, troubleshooting (C1: English only)"
  - "nbformat + nbclient + ipykernel added to [dependency-groups] dev"
  - "tests/test_notebook.py committed: nbformat.validate + unique/non-empty cell ids + install pin + API import (structure-only, fast)"

files:
  - path: pyproject.toml
    targets:
      - type: table
        name: dependency-groups.dev
    semantic_anchors:
      - "The existing dev list ending with twine>=5.0.0 (after mypy>=1.10.0)"
    changes:
      - action: add_code
        description: >
          Append notebook tooling to [dependency-groups] dev, alphabetically
          ordered where possible. These are required only to author,
          validate and execute the example notebook (T5). `build` (python -m
          build) and check-wheel-contents are T6 and NOT added here.
        code_hint: |
          "ipykernel>=6.29.0",
          "nbclient>=0.10.0",
          "nbformat>=5.10.0",

  - path: examples/notebooks/mko_data_cleaner_pipeline_example.ipynb
    targets:
      - type: file
    semantic_anchors:
      - "New file — nbformat 4.5 (nbformat >= 4.5 with unique cell ids)"
    changes:
      - action: add_code
        description: >
          Create the notebook with the cell-by-cell content below. Kernel spec
          name "python3". The synthetic report is generated in-cell into a
          tempfile.mkdtemp() directory (no committed data artifacts; C5/A7).
          The install cell uses a guard so it is skippable when the package is
          already importable (enables dev-venv execution before v0.1.0 is
          tagged). Output cells are left empty (no committed outputs — C5);
          they are populated at execution time.
        cell_spec: |
          # Cell 1 — markdown. Title + one-paragraph purpose + prerequisites note.
          # Cell 2 (metadata.tags=["install"]) — code:
          #   import sys
          #   try:
          #       import mko_data_cleaner  # noqa: F401
          #   except ImportError:
          #       get_ipython().run_line_magic("pip", 'install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"')  # noqa
          #   Note: on a clean kernel this installs the pinned tag (T7). Locally
          #   (dev venv, already installed) it is a no-op so downstream cells run.
          # Cell 3 — markdown. "Import the library and public API".
          # Cell 4 — code:
          #   import tempfile
          #   from pathlib import Path
          #   import polars as pl
          #   from mko_data_cleaner import initialize_settings, process_data, ReportResult
          #   import mko_data_cleaner
          #   print("mko_data_cleaner", mko_data_cleaner.__version__)
          #   print("process_data ->", process_data.__doc__.strip().splitlines()[0])
          # Cell 5 — markdown. "Initialize settings" — explain writes app_config.yaml + log_config.yaml into settings/ under a user dir.
          # Cell 6 — code:
          #   settings_dir = Path(tempfile.mkdtemp())
          #   initialized = initialize_settings(settings_dir=settings_dir)
          #   print("settings written to:", initialized)
          #   for p in sorted(initialized.iterdir()):
          #       print(" -", p.name)
          # Cell 6b — code (config override so uppercase ADID header matches index_column):
          #   import yaml
          #   cfg = yaml.safe_load((initialized / "app_config.yaml").read_text())
          #   cfg["data_file_settings"]["index_column"] = "ADID"
          #   cfg["data_file_settings"]["date_column"] = None
          #   (initialized / "app_config.yaml").write_text(yaml.safe_dump(cfg))
          #   print("index_column set to ADID to match the uppercase sample header")
          # Cell 7 — markdown. "Expected report directory layout".
          # Cell 8 — code (print the layout tree):
          #   for p in ("raw_data", "dict", "clean_data", "data_base"):
          #       print(f"  {p}/")
          #   print("  null_data_*.csv.gz  (created after the run)")
          # Cell 9 — markdown. "Create a tiny synthetic report (DELETE / REPLACE / ADD)".
          # Cell 10 — code (write the synthetic sample):
          #   report_dir = Path(tempfile.mkdtemp())
          #   raw = report_dir / "raw_data"; raw.mkdir(parents=True)
          #   ddir = report_dir / "dict"; ddir.mkdir(parents=True)
          #   header = "YEAR;MONTH;ADVERTISERS;BRANDS;ADID;MEDIA;COST_RUB;QUANTITY"
          #   rows = [
          #       "2024;01;EXAMPLE;Apple;id1;TV;1500000;25",
          #       "2024;01;EXAMPLE;Brand2;id2;TV;3200000;12",
          #       "2024;02;FAKE;Brand1;id3;TV;450000;8",
          #       "2024;02;FAKE;Orange;id4;TV;100000;5",
          #       "2024;03;EXAMPLE;Apple;id5;TV;200000;10",
          #       "2024;03;FAKE;Pear;id6;TV;150000;3",
          #       "2024;03;EXAMPLE;Brand2;id7;TV;500000;7",
          #   ]
          #   import gzip
          #   with gzip.open(raw / "sample.csv.gz", "wt", encoding="utf-8") as f:
          #       f.write(header + "\n" + "\n".join(rows) + "\n")
          #   dict_header = "action;match;search;value;term;tag"
          #   dict_rows = ["d;f;3;NULL;Brand2;", "r;f;3;NULL;Apple;fruit", "a;f;3;NULL;Brand1;added_by_brand"]
          #   (ddir / "merged_dictionary.csv").write_text(dict_header + "\n" + "\n".join(dict_rows) + "\n", encoding="utf-8")
          #   print(pl.read_csv(raw / "sample.csv.gz").head())
          #   print(pl.read_csv(ddir / "merged_dictionary.csv"))
          # Cell 11 — markdown. "Run the pipeline".
          # Cell 12 — code:
          #   result: ReportResult = process_data(report_dir, settings_dir=settings_dir)
          #   print(result)
          # Cell 13 — markdown. "Inspect the result" — explain fields.
          # Cell 14 — code (consume outputs):
          #   print("rows_loaded:", result.rows_loaded)
          #   print("rows_exported:", result.rows_exported)
          #   print("timings:", result.timings)
          #   print("export_dir:", result.export_dir)
          #   print("null_data_files:", result.null_data_files)
          #   clean = next(result.export_dir.glob("*.csv.gz"))
          #   print("clean_data sample:"); display(pl.read_csv(clean).head())
          #   if result.null_data_files:
          #       nd = result.null_data_files[0]
          #       print("null_data sample:"); display(pl.read_csv(nd).head())
          # Cell 15 — markdown. Notes/troubleshooting (pinning D10/D11; tag not cut → use local dev install; settings live under a temp dir per run; no committed artifacts).

  - path: examples/README.md
    targets:
      - type: file
    semantic_anchors:
      - "New file"
    changes:
      - action: add_code
        description: >
          Short English doc (C1) for the example. Sections: Purpose;
          Prerequisites (Python >=3.12, %pip install git+...@v0.1.0 or uv local);
          How to run (open in Jupyter, run all); Cell map (install → import →
          initialize_settings → layout → synthetic sample → process_data →
          consume); Synthetic sample description (7 rows, HEADER, 3 rules,
          expected rows_loaded=7/rows_exported=3 + 2 null rows) and the
          index_column=ADID override rationale; Expected outputs (clean_data
          + null_data_*.csv.gz paths, ReportResult fields); Troubleshooting
          (tag v0.1.0 not cut yet → use dev venv / uv sync and skip install;
          SettingsNotInitializedError → run initialize_settings first).
        code_hint: |
          # Example notebook: mko_data_cleaner_pipeline_example.ipynb

          ... (exact sections per description above)

  - path: tests/test_notebook.py
    targets:
      - type: module
      - type: test_function
    semantic_anchors:
      - "New file"
    changes:
      - action: add_code
        description: >
          Structure-only tests (fast, always green; no kernel execution —
          env-friable execution is the T6/T7 gate). Reuses pythonpath=src so
          the package imports.
        code_hint: |
          """Tests for the example notebook (structure only)."""

          import nbformat
          from pathlib import Path

          NOTEBOOK = Path(__file__).resolve().parent.parent / "examples" / "notebooks" / "mko_data_cleaner_pipeline_example.ipynb"


          def _load():
              nb = nbformat.read(NOTEBOOK, as_version=4)
              nbformat.validate(nb)
              return nb


          def test_notebook_is_valid_nbformat():
              """Notebook is structurally valid (nbformat 4.5+)."""
              _load()


          def test_notebook_has_unique_nonempty_cell_ids():
              """Every cell has a non-empty, unique id (nbformat >= 4.5)."""
              nb = _load()
              ids = [c.get("id", "") for c in nb.cells]
              assert all(ids), "some cells have empty/missing ids"
              assert len(ids) == len(set(ids)), "duplicate cell ids"


          def test_notebook_install_cell_pins_v0_1_0():
              """The install cell pins the v0.1.0 tag (D1/D11)."""
              nb = _load()
              joined = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
              assert "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0" in joined


          def test_notebook_imports_public_api():
              """Import cell references the top-level public API (B4)."""
              nb = _load()
              joined = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
              assert "initialize_settings" in joined
              assert "process_data" in joined
              assert "ReportResult" in joined

tests_to_create:
  - path: tests/test_notebook.py
    rationale: >
      Structure-only validation commits a fast, always-green gate for the
      notebook artifact (nbformat validity, cell ids, install pin, public API
      reference). Kernel execution is env-dependent (polars/adbc/SQLite FTS5)
      and is gated to T6 CI + the T7 against-tag clean-kernel run, per the
      dependency ordering (B5 precedes B7).
    tests:
      - test_notebook_is_valid_nbformat
        description: "nbformat.validate passes on the committed notebook."
      - test_notebook_has_unique_nonempty_cell_ids
        description: "All cells have non-empty unique ids (nbformat 4.5+)."
      - test_notebook_install_cell_pins_v0_1_0
        description: "Install cell pins git+...@v0.1.0."
      - test_notebook_imports_public_api
        description: "Notebook imports initialize_settings/process_data/ReportResult from top level."

acceptance_criteria:
  - "examples/notebooks/mko_data_cleaner_pipeline_example.ipynb exists with nbformat 4.5+ and valid cell ids"
  - "Install cell pins %pip install git+...@v0.1.0"
  - "Notebook cells follow: install → import+version → initialize_settings → layout → synthetic sample → process_data → consume result"
  - "Synthetic sample uses README uppercase header (YEAR;MONTH;ADVERTISERS;BRANDS;ADID;MEDIA;COST_RUB;QUANTITY) with index_column overridden to ADID"
  - "Dictionary has DELETE (Brand2), REPLACE (Apple→fruit), ADD (Brand1) rules targeting search=3 (BRANDS column)"
  - "nbformat + nbclient + ipykernel present in [dependency-groups] dev"
  - "uv run pytest tests/test_notebook.py passes (structure tests)"
  - "Notebook executes top-to-bottom in dev venv with install cell skipped (manual verification; against-tag execution is T7)"
  - "examples/README.md exists, English-only, documents cell flow + expected outputs + troubleshooting"
  - "43 MB data/example sample is NOT referenced by the notebook"
  - "uv run ruff check src/ tests/ is clean"

verification_commands:
  - "uv run python -c \"import nbformat, nbclient, ipykernel; print('notebook tooling OK')\""
  - 'uv run python -c "import nbformat; nb=nbformat.read('examples/notebooks/mko_data_cleaner_pipeline_example.ipynb', as_version=4); nbformat.validate(nb); print('notebook valid', nb.nbformat_minor)"'
  - 'uv run python -c "import nbformat; from nbclient import NotebookClient; from nbclient.exceptions import CellExecutionError; nb=nbformat.read('examples/notebooks/mko_data_cleaner_pipeline_example.ipynb', as_version=4); nbformat.validate(nb); import tempfile, os; os.chdir(tempfile.mkdtemp()); [nb.cells.remove(c) or None for c in nb.cells if 'install' in (c.get(\"metadata\",{}).get(\"tags\",[]))]; client=NotebookClient(nb, timeout=300, kernel_name='python3'); client.execute(); print('notebook executes OK in dev venv (install cell skipped)')"'
  - "uv run pytest tests/test_notebook.py -v"
  - "uv run ruff check src/ tests/"
  - "git status --short examples/ tests/test_notebook.py pyproject.toml"

out_of_scope:
  - "Clean-kernel execution AGAINST the pinned v0.1.0 tag (requires the tag to be cut in T7/B7); B5 verifies structure + dev-venv execution with the install cell guarded/skipped"
  - "B6 CI workflow (push/PR notebook-execution job) and wheel-content/against-tag import smoke"
  - "B7 version bump to 0.1.0 / git tag v0.1.0 / publish"
  - "python -m build / check-wheel-contents (T6 tooling)"
  - "README.md pipeline-API section (Doc-specialist step, post-B7)"
  - "Adding committed output cells (C5 — repo stays clean; outputs generated at run time)"
  - "Reproducing the 43 MB data/example sample (A7/C5 — synthetic sample is in-cell only)"
  - "Switching the notebook to full jupyter server install (nbclient + ipykernel is sufficient for validation/execution)"

risks:
  - "R1: Install cell pins @v0.1.0 (D1/D11) but the tag does not exist at B5 time. The import-guard makes the install cell a no-op in the dev venv (package already installed), so dev-venv execution still validates all downstream cells. Against-tag execution is explicitly deferred to T7; if T7 is not done, the notebook cannot be run by end users from a clean kernel."
  - "R2: README uppercase input shape (ADID) vs shipped template config (index_column: AdId). Resolved in the notebook by an explicit settings_dir-local override index_column=ADID. If the Implementor instead drops the override and uses the template as-is, headers MUST be AdId;researchDate;... (the proven B4 test_process_data_end_to_end shape) or the index lookup silently fails. The override cell is mandatory for the uppercase sample."
  - "R3: Synthetic sample exercises DELETE/REPLACE/ADD. REPLACE writes the dictionary's extra-column value (tag) per _apply_replace (verified db_service.py L645-672); the extra column must exist in the dictionary (col 5 'tag') and be added to the data table via set_data_tbl_columns(extra_cols=...). The data-table column layout (YEAR=0,MONTH=1,ADVERTISERS=2,BRANDS=3,ADID=4,MEDIA=5,COST_RUB=6,QUANTITY=7,tag=8) must match the dictionary search index (search=3 → BRANDS). Any header reordering requires updating search indices."
  - "R4: env-friable end-to-end execution depends on polars write_database ADBC adapter, SQLite, LIKE ... COLLATE NOCASE. The committed test_notebook.py is structure-only to avoid flakiness (B4 R12 pattern); real execution is a T6 CI + T7 gate. If polars/adbc/SQLite behavior differs, restructure the dictionary (the B4 mock-based return contract test already covers the API shape independently)."
  - "R5: dict file extension. Config dict_file_settings.extension=gz, but the sample dictionary is written as plain merged_dictionary.csv and read directly by get_dictionary (which ignores the extension setting for the final merged file — verified B4 e2e). Do NOT gzip the dictionary file; doing so with a .csv name would let polars detect gzip anyway, but a .csv.gz path mismatch with config dict_file ('dict/merged_dictionary.csv') would break get_dictionary. Keep the file name exactly dict/merged_dictionary.csv."
  - "R6: export file naming. clean_data files are data_table_{timestamp}_{idx}.csv.gz (prefix = table name); null_data files are null_data_{timestamp}_{idx}.csv.gz (prefix='null_data'). The consume cell must glob *.csv.gz in export_dir and use result.null_data_files (not a re-glob) to be robust to timestamp variance."
  - "R7: notebook uses tempfile.mkdtemp() for both settings and report dirs so re-runs are idempotent and the repo stays clean (C5). The consume cell reads outputs from the returned ReportResult paths, so no committed artifacts are needed."
  - "R8: tooling bloat. Adding nbformat + nbclient + ipykernel to dev increases `uv sync` cost. These are justified by T5 (R8/CI) and reused by T6. ipykernel is required only at execution time; it is acceptable as a dev-only dependency."
  ```

---

## B6 — T6: Smoke tests and build/install verification CI

**Agents:** Planner → Implementor.
**Return artifacts:** `{context_r}` → `{task_description}` appended below.

---

### {context_r} — Research findings (B6/T6)

**Confidence: HIGH** — all claims verified statically against committed B1–B5 state (HEAD `384044a`) and dynamically via reproduction on this machine (Windows 11, Python 3.14.7, uv 0.9.26). B6 is sequenced strictly after B1–B5; all five predecessor phases are committed and green (54 tests pass, `ruff check src/ tests/` clean).

#### Verified facts

| Fact | Value | How verified |
|---|---|---|
| Dev group (HEAD `384044a`) | `black, isort, pytest, pytest-mock, ruff, mypy, twine, ipykernel, nbclient, nbformat` | `git show HEAD:pyproject.toml` |
| `build` in dev? | **No** — not installed | `uv run python -c "import build"` → `ModuleNotFoundError` |
| `check-wheel-contents` in dev? | **No** — not in lockfile | verified absent from dev list |
| check-wheel-contents version | **0.6.3** | `uv tool run check-wheel-contents --version` |
| `twine check --strict` | **Exists**; `--strict    Fail on warnings` | `uv run twine check --help` |
| `.github/` dir | **Absent** | `Test-Path .github` → False |
| `scripts/` dir | **Absent** | `Test-Path scripts` → False |
| Entry point | `mko-data-cleaner = "mko_data_cleaner.app:app"` | `pyproject.toml` line 46 |
| `__all__` (top-level) | `['ReportResult', 'SettingsNotInitializedError', '__version__', 'initialize_settings', 'process_data']` | `uv run python -c "import mko_data_cleaner; print(mko_data_cleaner.__all__)"` |
| CLI commands | `init`, `run`, `list-settings` — all have `--settings-dir` | `git show HEAD:src/mko_data_cleaner/app.py` |
| Existing test modules | `test_app.py`, `test_metadata.py`, `test_notebook.py`, `test_csv_service.py`, `test_db_service.py`, `test_dict_service.py`, `conftest.py` | `uv run pytest --co -q` → 54 tests |
| `importlib.resources.files("mko_data_cleaner")` | Resolves to `src/mko_data_cleaner/`; `settings/app_config.yaml` and `settings/log_config.yaml` are_file() → True | verified on this machine |
| `MANIFEST.in` | Already corrected (B2); `recursive-include src/mko_data_cleaner/settings *.yaml`; no stale `mko_get_mediascope_data` | committed content |
| pyproject license | `license = "BSD-3-Clause"`, `license-files = ["LICENSE.txt"]`, `license-files` no stale classifiers | committed content |
| PyYAML for YAML validation | 6.0.3 (transitive dep of `nbformat`) | `uv run python -c "import yaml; print(yaml.__version__)"` |

#### Key design decisions

1. **CI runner: `ubuntu-latest` only; Python matrix 3.12/3.13/3.14 on `test`; single 3.14 on `build`/`install-smoke`/`notebook`.**
   - The spec says CI is "meant to be light" (F12 context). The project declares `platforms = ["Linux", "Windows"]` in setuptools, but a Linux-only CI matrix is the standard lightweight approach. Windows runners can be added later if platform-specific failures surface.
   - The `test` job uses a full 3-version matrix (3.12/3.13/3.14, matching `requires-python = ">=3.12"` and classifiers) to catch version-specific regressions. The `build`/`install-smoke`/`notebook` jobs use a single Python (3.14) — build/install is version-agnostic, and notebook execution needs a single kernel.

2. **Build mechanism: `uv run python -m build`**
   - Uses the `build` package (added to dev group in change #1) as the PEP 517 frontend. PEP 517 build isolation (via `[build-system] requires = ["setuptools>=77.0", "wheel>=0.37.1"]`) produces canonical sdist+wheel. `uv build` is equivalent but does not exercise the `build` package — using `python -m build` justifies its presence in the dev group and matches spec D10 ("python -m build").

3. **Smoke script: `scripts/smoke_install.py` (new file)**
   - Chosen over an inline CI heredoc because: (a) B7's against-tag verification gate needs the same script; (b) the Implementor must validate it locally (the task requires "smoke script executes green against a locally built wheel"); (c) 80+ lines of Python is cleaner as a file than as a CI heredoc. The `scripts/` directory is a standard convention with no existing collision.
   - Platform-agnostic: uses `settings_dir` parameter for isolation (no `XDG_CONFIG_HOME`/`APPDATA` env patching), `shutil.which("mko-data-cleaner")` for CLI discovery, `tempfile.mkdtemp()` for temp dirs. Works identically on Linux (CI) and Windows (local dev).

4. **Notebook handling: full execution via `nbclient.NotebookClient`, no cell removal.**
   - The notebook's install cell (cell 2, B5 committed) already uses an import guard: `try: import mko_data_cleaner / except ImportError: %pip install "git+...@v0.1.0"`. When `uv sync` installs the package from source before nbclient execution, the import succeeds and the `%pip install` line is never reached. The `get_ipython()` call is never invoked. No cell-tag removal needed — avoids modifying the committed notebook (out of scope).

5. **`check-wheel-contents`: CLI args, no config file.**
   - Uses `--package mko_data_cleaner --src-dir src/mko_data_cleaner dist/*.whl`. The `WC` check flags any wheel file not in the source tree (catches `data/` leakage, stale names). A separate explicit Python assertion checks YAML template presence (the `WC` check is unidirectional: wheel → src, so it does not flag missing files). No `check-wheel-contents.toml` config file is created — keeps scope minimal per "4. Nothing else."

6. **`uv sync` (without `--frozen`) on all jobs.**
   - The Implementor adds `build` and `check-wheel-contents` to pyproject.toml. If CI used `--frozen`, the lockfile would need regeneration first. `uv sync` resolves and updates the lockfile automatically — works in both local and CI contexts even if the lockfile is slightly stale. The Implementor should still run `uv lock` to commit the updated lockfile.

#### Risks

- **R1 (fresh-venv creation on CI):** `uv venv /tmp/smoke-venv --python 3.14` + `uv pip install --python <venv-python> dist/*.whl` — the `--python` flag must accept a venv Python path. Verified locally: this is the documented uv pip install syntax. If the flag differs across uv versions, the Implementor should test locally.
- **R2 (uv version pinning):** CI pins `uv-version: "0.9.26"`. If CI runners already have a newer uv via `setup-uv`, this is fine. If `setup-uv@v5` does not support pinning to 0.9.26, the flag should be adjusted to `@v5` without a version pin (or `uv-version: "latest"`).
- **R3 (notebook install-cell guard):** The import guard relies on `import mko_data_cleaner` succeeding in the `notebook` job. If `uv sync` fails, the `%pip install git+...@v0.1.0` line executes and fails (tag doesn't exist until B7). Mitigation: the `notebook` job runs `uv sync` before execution; if sync fails, the job fails before reaching the notebook.
- **R4 (smoke script platform):** The script is platform-agnostic (uses `settings_dir` param, `shutil.which`, `tempfile`). No platform-specific assumptions. Works on Linux (CI) and Windows (local dev). The `mko-data-cleaner` entry point resolves correctly via `shutil.which` on both platforms.
- **R5 (importlib.resources in source checkout):** `importlib.resources.files("mko_data_cleaner")` resolves to `src/mko_data_cleaner/` with `pythonpath=["src"]` (verified). Uses `Traversable.is_file()` / `.read_text()` — available on Python 3.12+ (project minimum). On 3.9-, the API differs, but the project requires >=3.12.
- **R6 (version constraints for new dev deps):** `build>=1.0.0` and `check-wheel-contents>=0.6.3` are safe minimums. The lockfile (via `uv lock`) will pin exact versions. If a future `build` version changes CLI behavior, `python -m build` syntax is stable (PEP 517).
- **R7 (artifact passing):** `actions/upload-artifact@v4` uploads `dist/`; `actions/download-artifact@v4` downloads to `dist/` in the `install-smoke` job. The artifact name (`dist`) and paths must match exactly. The `needs: build` dependency ensures correct ordering.
- **R8 (twine check --strict):** Verified: `--strict    Fail on warnings`. This makes `twine check` fail on license metadata deprecation warnings (D10). If future twine versions relax this behavior, the `test_pyproject_no_stale_license_classifier` test provides a backup guard.
- **R9 (smoke script E2E sample):** The end-to-end test reuses the exact synthetic sample from `tests/test_app.py::test_process_data_end_to_end` (header `AdId;researchDate;YEAR;MONTH;BRAND`, 3 rows, DELETE+ADD rules, search=4 → BRAND column). The template `app_config.yaml` has `index_column=AdId` and `date_column=researchDate`, which match the sample headers. Expected: `rows_loaded=3`, `rows_exported=2`.

---

### {task_description} — Implementation Task (B6/T6)

```yaml
id: b6_t6_ci_smoke_tests

title: "B6/T6 — Smoke tests and build/install verification CI"

priority: medium-high

depends_on:
  - b1_t1_remove_import_time_init
  - b2_t2_ship_yaml_templates
  - b3_t3_metadata_hygiene
  - b4_t4_public_api_contract
  - b5_t5_example_notebook

# ── Plan/Audit report source ──
# source_reference: .ai\plans\01-pip-installable-jupyter-pipeline-step.md
# source_section: B6 — T6: Smoke tests and build/install verification CI

description: >
  Add CI verification that would have caught defects F1/F2/F3/F8 (import crash,
  missing settings, missing YAML templates, broken end-to-end), lock in the
  packaging fixes from B2/B3, and make every push/PR a canonical build+install
  smoke-test gate. Three concrete deliverables: (1) two new dev dependencies
  (`build`, `check-wheel-contents`) in pyproject.toml, (2) a GitHub Actions
  workflow (`.github/workflows/ci.yml`) with four jobs — test (matrix 3.12–3.14),
  build (sdist+wheel via `python -m build`, `twine check --strict`,
  `check-wheel-contents`, YAML-in-wheel assertion), install-smoke (fresh venv,
  pip install wheel, run `scripts/smoke_install.py`), notebook (execute the
  example notebook via nbclient), and (3) a `tests/test_packaging.py` module
  with static + importlib.resources regression guards for MANIFEST.in,
  pyproject.toml, and package-data accessibility. A reusable smoke script at
  `scripts/smoke_install.py` validates import-without-crash,
  SettingsNotInitializedError, initialize_settings, CLI commands, end-to-end
  process_data, and __version__ from installed metadata.

goals:
  - "Add `build>=1.0.0` and `check-wheel-contents>=0.6.3` to [dependency-groups] dev in pyproject.toml"
  - "Create .github/workflows/ci.yml with test, build, install-smoke, and notebook jobs on push + pull_request"
  - "Create scripts/smoke_install.py — install-time smoke test for a fresh-venv pip-installed wheel"
  - "Create tests/test_packaging.py — importlib.resources + static guards for F3/F5/F10 regression"
  - "CI must fail on pre-fix code: twine check --strict, check-wheel-contents, YAML-in-wheel assertion, import+SettingsNotInitializedError smoke"

files:
  - path: pyproject.toml
    targets:
      - type: table
        name: dependency-groups.dev
    semantic_anchors:
      - "The dev dependency list, specifically the entry `\"nbformat>=5.10.0\"` followed by the closing `]`"
    changes:
      - action: modify
        description: >
          Append two entries to the end of the `dev` list, after
          `"nbformat>=5.10.0"` and before the closing `]`:
          `build>=1.0.0` (PEP 517 build frontend — `python -m build`, used
          canonically in the CI build job) and `check-wheel-contents>=0.6.3`
          (wheel-content validator, used in the CI build job). Version
          constraints are safe minimums; `uv lock` resolves exact versions.
        code_hint: |
          "nbformat>=5.10.0",
          "build>=1.0.0",
          "check-wheel-contents>=0.6.3",
        ]

  - path: .github/workflows/ci.yml
    targets:
      - type: file
    semantic_anchors:
      - "New file — repository has no .github/ directory (verified absent)"
    changes:
      - action: add_code
        description: >
          Create the CI workflow with four jobs (test, build, install-smoke,
          notebook). The test job runs a Python 3.12/3.13/3.14 matrix with
          `uv sync`, `uv run ruff check src/ tests/`, and `uv run pytest`.
          The build job runs `uv sync`, `uv run python -m build`,
          `uv run twine check --strict dist/*`,
          `uv run check-wheel-contents --package mko_data_cleaner --src-dir
          src/mko_data_cleaner dist/*.whl`, an inline Python assertion for
          YAML template presence + no data/ leakage, and uploads `dist/`
          as a workflow artifact. The install-smoke job depends on build,
          downloads the dist artifact, creates a fresh venv via
          `uv venv`, installs the wheel with `uv pip install`, and runs
          `scripts/smoke_install.py`. The notebook job runs `uv sync`
          and executes the example notebook via `nbclient.NotebookClient`.
        code_hint: |
          name: CI

          on:
            push:
              branches: [main]
            pull_request:

          env:
            UV_VERSION: "0.9.26"

          jobs:
            test:
              runs-on: ubuntu-latest
              strategy:
                fail-fast: false
                matrix:
                  python-version: ["3.12", "3.13", "3.14"]
              steps:
                - uses: actions/checkout@v4
                - uses: astral-sh/setup-uv@v5
                  with:
                    uv-version: "0.9.26"
                    python-version: ${{ matrix.python-version }}
                    enable-cache: true
                - run: uv sync
                - run: uv run ruff check src/ tests/
                - run: uv run pytest

            build:
              runs-on: ubuntu-latest
              steps:
                - uses: actions/checkout@v4
                - uses: astral-sh/setup-uv@v5
                  with:
                    uv-version: "0.9.26"
                    python-version: "3.14"
                    enable-cache: true
                - run: uv sync
                - run: uv run python -m build
                - run: uv run twine check --strict dist/*
                - run: uv run check-wheel-contents --package mko_data_cleaner --src-dir src/mko_data_cleaner dist/*.whl
                - name: Assert wheel contents (YAML templates + no leakage)
                  run: |
                    uv run python - <<'PYEOF'
            import zipfile, glob
            wheel = glob.glob('dist/*.whl')[0]
            names = zipfile.ZipFile(wheel).namelist()
            assert 'mko_data_cleaner/settings/app_config.yaml' in names, 'app_config.yaml MISSING from wheel'
            assert 'mko_data_cleaner/settings/log_config.yaml' in names, 'log_config.yaml MISSING from wheel'
            leaked = [n for n in names if '/data/' in n or n.startswith('data/')]
            assert not leaked, f'data/ leaked into wheel: {leaked}'
            stale = [n for n in names if 'mko_get_mediascope_data' in n]
            assert not stale, f'stale name leaked into wheel: {stale}'
            print(f'Wheel OK: {len(names)} entries, YAML templates present, no data/ leakage')
            PYEOF
                - uses: actions/upload-artifact@v4
                  with:
                    name: dist
                    path: dist/

            install-smoke:
              runs-on: ubuntu-latest
              needs: build
              steps:
                - uses: actions/checkout@v4
                - uses: astral-sh/setup-uv@v5
                  with:
                    uv-version: "0.9.26"
                    python-version: "3.14"
                - name: Download dist artifact
                  uses: actions/download-artifact@v4
                  with:
                    name: dist
                    path: dist/
                - name: Fresh venv install + smoke test
                  run: |
                    uv venv /tmp/smoke-venv --python 3.14
                    uv pip install --python /tmp/smoke-venv/bin/python dist/*.whl
                    /tmp/smoke-venv/bin/python scripts/smoke_install.py

            notebook:
              runs-on: ubuntu-latest
              steps:
                - uses: actions/checkout@v4
                - uses: astral-sh/setup-uv@v5
                  with:
                    uv-version: "0.9.26"
                    python-version: "3.14"
                    enable-cache: true
                - run: uv sync
                - name: Execute example notebook
                  run: |
                    uv run python - <<'PYEOF'
            import nbformat
            from nbclient import NotebookClient

            nb = nbformat.read('examples/notebooks/mko_data_cleaner_pipeline_example.ipynb', as_version=4)
            nbformat.validate(nb)
            client = NotebookClient(nb, timeout=300, kernel_name='python3')
            client.execute()
            print('Notebook executed OK')
            PYEOF

  - path: scripts/smoke_install.py
    targets:
      - type: file
    semantic_anchors:
      - "New file — scripts/ directory does not exist yet"
    changes:
      - action: add_code
        description: >
          Create a standalone smoke-test script that runs in a fresh venv
          where the wheel was pip-installed. Validates: (i) import + __version__
          from installed metadata; (ii) process_data without settings raises
          SettingsNotInitializedError (F1/F2); (iii) initialize_settings()
          creates YAML templates (F3); (iv) CLI --help + init + list-settings;
          (v) end-to-end process_data on the same synthetic sample as
          tests/test_app.py::test_process_data_end_to_end (F8); (vi) exit 0
          on all pass. Uses settings_dir parameter for platform-agnostic
          isolation — no env-var patching needed. The script reuses the
          AdId;researchDate;YEAR;MONTH;BRAND header family matching the
          shipped app_config.yaml template (index_column=AdId, date_column=
          researchDate) — no config override cell needed (unlike the notebook).
        code_hint: |
          #!/usr/bin/env python
          """Install-time smoke test (B6/T6) — run in a fresh venv with the wheel pip-installed.

          Validates that the pip-installed package:
            (i)   imports without crashing (F1/F2 regression gate)
            (ii)  process_data without settings raises SettingsNotInitializedError
            (iii) initialize_settings() ships YAML templates (F3 regression gate)
            (iv)  CLI commands work (--help, init, list-settings)
            (v)   end-to-end process_data on a tiny synthetic report (F8)
            (vi)  __version__ resolves from installed package metadata
          """

          import gzip
          import importlib.metadata
          import shutil
          import subprocess
          import sys
          import tempfile
          from pathlib import Path

          # ---------------------------------------------------------------------------
          # (i) Import + version from installed metadata
          # ---------------------------------------------------------------------------
          import mko_data_cleaner
          from mko_data_cleaner import (
              SettingsNotInitializedError,
              initialize_settings,
              process_data,
          )

          _version = importlib.metadata.version("mko_data_cleaner")
          assert mko_data_cleaner.__version__ == _version
          print(f"[PASS] (i) import OK, __version__ = {_version}")

          # ---------------------------------------------------------------------------
          # (ii) process_data WITHOUT settings → SettingsNotInitializedError (F1/F2)
          # ---------------------------------------------------------------------------
          _empty_settings = Path(tempfile.mkdtemp())
          _report_dir = Path(tempfile.mkdtemp()) / "report"
          _report_dir.mkdir()
          try:
              process_data(_report_dir, settings_dir=_empty_settings)
          except SettingsNotInitializedError as exc:
              _msg = str(exc)
              assert "initialize_settings()" in _msg
              assert "app_config.yaml" in _msg
              print("[PASS] (ii) SettingsNotInitializedError raised for missing settings")
          else:
              sys.exit("FAIL: process_data did not raise SettingsNotInitializedError")

          # ---------------------------------------------------------------------------
          # (iii) initialize_settings creates templates (F3)
          # ---------------------------------------------------------------------------
          _tmp_config = Path(tempfile.mkdtemp())
          _settings_path = initialize_settings(settings_dir=_tmp_config)
          assert (_settings_path / "app_config.yaml").exists()
          assert (_settings_path / "log_config.yaml").exists()
          print(f"[PASS] (iii) settings initialized at {_settings_path}")

          # ---------------------------------------------------------------------------
          # (iv) CLI smoke
          # ---------------------------------------------------------------------------
          _cli = shutil.which("mko-data-cleaner")
          assert _cli is not None, "mko-data-cleaner CLI not found in venv"
          subprocess.run([_cli, "--help"], check=True, capture_output=True)
          subprocess.run(
              [_cli, "init", "--settings-dir", str(_tmp_config), "--force"],
              check=True, capture_output=True,
          )
          subprocess.run(
              [_cli, "list-settings", "--settings-dir", str(_tmp_config)],
              check=True, capture_output=True,
          )
          print("[PASS] (iv) CLI smoke (--help, init, list-settings)")

          # ---------------------------------------------------------------------------
          # (v) End-to-end on synthetic sample (reuses test_app.py::test_process_data_end_to_end)
          # ---------------------------------------------------------------------------
          _e2e = Path(tempfile.mkdtemp())
          _raw = _e2e / "raw_data"
          _raw.mkdir(parents=True)
          _ddir = _e2e / "dict"
          _ddir.mkdir(parents=True)

          # Header matches app_config.yaml template: index_column=AdId, date_column=researchDate
          _csv_lines = [
              "AdId;researchDate;YEAR;MONTH;BRAND",
              "id1;2024-01-01;2024;01;Brand1",
              "id2;2024-01-15;2024;01;Brand2",
              "id3;2024-02-01;2024;02;Brand1",
          ]
          with gzip.open(_raw / "sample.csv.gz", "wt", encoding="utf-8") as f:
              f.write("\n".join(_csv_lines) + "\n")

          # Dictionary: DELETE Brand2 (row deleted), ADD tag for Brand1
          # search=4 → BRAND column (0-based: AdId=0, researchDate=1, YEAR=2,
          # MONTH=3, BRAND=4, tag=5-after-merge)
          _dict_lines = [
              "action;match;search;value;term;tag",
              "d;f;4;NULL;Brand2;",
              "a;f;4;NULL;Brand1;added_by_brand",
          ]
          (_ddir / "merged_dictionary.csv").write_text(
              "\n".join(_dict_lines) + "\n", encoding="utf-8"
          )

          _result = process_data(_e2e, settings_dir=_tmp_config)
          assert _result.rows_loaded == 3, f"expected 3 rows loaded, got {_result.rows_loaded}"
          assert _result.rows_exported == 2, f"expected 2 rows exported, got {_result.rows_exported}"
          assert _result.export_dir == _e2e / "clean_data"
          assert _result.import_dir == _e2e / "raw_data"
          assert _result.report_dir == _e2e
          assert _result.export_dir.exists()
          assert len(list(_result.export_dir.glob("*.gz"))) > 0
          assert isinstance(_result.null_data_files, list)
          assert _result.duration > 0.0
          assert set(_result.timings.keys()) == {"import", "mapping", "export"}
          print(f"[PASS] (v) end-to-end: loaded={_result.rows_loaded}, exported={_result.rows_exported}")

          # ---------------------------------------------------------------------------
          # (vi) __version__ matches installed metadata (already asserted in (i))
          # ---------------------------------------------------------------------------
          print(f"[PASS] (vi) __version__ resolved from installed metadata: {_version}")

          print("\n✅ All smoke tests passed")

  - path: tests/test_packaging.py
    targets:
      - type: module
      - type: test_function
    semantic_anchors:
      - "New file — tests/ directory exists; no prior test_packaging.py"
    changes:
      - action: add_code
        description: >
          Create a test module with 7 test functions. Tests run from a source
          checkout (pythonpath=["src"]) — no wheel build required. They guard:
          F3 (YAML templates accessible via importlib.resources), F5 (no
          deprecated license classifiers in pyproject.toml), F10 (no stale
          mko_get_mediascope_data in MANIFEST.in or pyproject.toml), and
          B3/D4 (SPDX license + package-data declaration). The
          importlib.resources test fails on pre-fix code (B2 not applied)
          because the templates would not be package-accessible.
        code_hint: |
          """Packaging tests (B6/T6): package-data regression gate (F3),
          MANIFEST.in hygiene (F10), and metadata guards (F5, D4).

          These tests run from a source checkout (pythonpath=["src"]) — no
          wheel build required. They complement the CI build job's wheel-
          content assertion by testing the source-level invariants that
          must hold before a wheel is ever built.
          """

          from pathlib import Path

          ROOT = Path(__file__).resolve().parent.parent
          PYPROJECT = ROOT / "pyproject.toml"
          MANIFEST = ROOT / "MANIFEST.in"


          def test_template_yamls_accessible_as_package_data():
              """Both YAML settings templates are accessible via importlib.resources.

              At pre-fix HEAD (B2 not applied), the wheel had 18 entries and
              zero settings/*.yaml. This test fails on such code because
              importlib.resources.files() cannot locate non-package data
              without [tool.setuptools.package-data].
              """
              import importlib.resources as resources

              pkg = resources.files("mko_data_cleaner")
              assert (pkg / "settings" / "app_config.yaml").is_file(), \
                  "app_config.yaml not accessible as package data"
              assert (pkg / "settings" / "log_config.yaml").is_file(), \
                  "log_config.yaml not accessible as package data"


          def test_template_yamls_parse():
              """Shipped YAML templates parse with expected top-level keys."""
              import importlib.resources as resources
              import yaml

              pkg = resources.files("mko_data_cleaner")
              app_cfg = yaml.safe_load(
                  (pkg / "settings" / "app_config.yaml").read_text(encoding="utf-8")
              )
              log_cfg = yaml.safe_load(
                  (pkg / "settings" / "log_config.yaml").read_text(encoding="utf-8")
              )
              assert "data_paths" in app_cfg
              assert "handlers" in log_cfg


          def test_manifest_in_no_stale_project_name():
              """MANIFEST.in has no stale mko_get_mediascope_data references (F10)."""
              assert "mko_get_mediascope_data" not in MANIFEST.read_text()


          def test_manifest_in_includes_yaml_templates():
              """MANIFEST.in includes the YAML templates in the sdist."""
              content = MANIFEST.read_text()
              assert "recursive-include" in content
              assert "*.yaml" in content
              assert "settings" in content


          def test_pyproject_no_stale_license_classifier():
              """pyproject.toml has no deprecated License classifiers (F5)."""
              assert "License ::" not in PYPROJECT.read_text()


          def test_pyproject_no_stale_project_name():
              """pyproject.toml has no stale mko_get_mediascope_data references (F10)."""
              assert "mko_get_mediascope_data" not in PYPROJECT.read_text()


          def test_pyproject_uses_spdx_license():
              """pyproject.toml uses PEP 639 SPDX license expression (B3/D4)."""
              content = PYPROJECT.read_text()
              assert 'license = "BSD-3-Clause"' in content
              assert 'license-files = ["LICENSE.txt"]' in content


          def test_pyproject_declares_package_data():
              """pyproject.toml declares [tool.setuptools.package-data] for YAMLs."""
              content = PYPROJECT.read_text()
              assert "settings/*.yaml" in content

acceptance_criteria:
  - "pyproject.toml [dependency-groups] dev includes `build>=1.0.0` and `check-wheel-contents>=0.6.3`"
  - "`uv lock` succeeds after adding the new dev dependencies"
  - "`.github/workflows/ci.yml` exists and parses as valid YAML (verifiable with PyYAML)"
  - "CI test job runs on Python 3.12/3.13/3.14 matrix with `uv sync`, `ruff check`, `pytest`"
  - "CI build job runs `python -m build`, `twine check --strict dist/*`, `check-wheel-contents`"
  - "CI install-smoke job downloads wheel artifact, creates fresh venv, `pip install` wheel, runs `scripts/smoke_install.py`"
  - "CI notebook job executes the example notebook via nbclient"
  - "Smoke script passes against a locally built wheel: import (F1), SettingsNotInitializedError (F2), initialize_settings (F3), CLI (iv), end-to-end (v), __version__ (vi)"
  - "twine check --strict fails on license metadata deprecation warnings"
  - "check-wheel-contents flags stale-name or data/ leakage in the wheel"
  - "New tests in tests/test_packaging.py pass from source checkout: importlib.resources YAML access, MANIFEST.in clean, pyproject no stale license classifier"
  - "`uv run pytest` green (54 existing + 7 new packaging tests = 61 total)"
  - "`uv run ruff check src/ tests/` clean"
  - "CI workflow on push/PR would fail on pre-fix code (F1/F2/F3/F8) when pointed at it"

tests_to_create:
  - path: tests/test_packaging.py
    rationale: >
      Static + importlib.resources regression guards for F3 (missing YAML
      templates), F5 (deprecated license classifier), and F10 (stale project
      name). These run from source checkout (pythonpath=["src"]) — no wheel
      build needed. The importlib.resources test is the primary F3 regression
      gate: it fails if [tool.setuptools.package-data] is missing (pre-fix B2).
      Complements the CI build job's wheel-content assertion by testing
      source-level invariants before a wheel is built.
    tests:
      - test_template_yamls_accessible_as_package_data
        description: >
          Both app_config.yaml and log_config.yaml are accessible via
          importlib.resources.files("mko_data_cleaner"). Fails on pre-fix code
          where package-data is not declared (B2 not applied).
      - test_template_yamls_parse
        description: >
          Templates parse as valid YAML with expected top-level keys
          (data_paths, handlers).
      - test_manifest_in_no_stale_project_name
        description: >
          MANIFEST.in contains no mko_get_mediascope_data (F10).
      - test_manifest_in_includes_yaml_templates
        description: >
          MANIFEST.in has recursive-include for settings/*.yaml (sdist).
      - test_pyproject_no_stale_license_classifier
        description: >
          pyproject.toml has no "License ::" classifier string (F5).
      - test_pyproject_no_stale_project_name
        description: >
          pyproject.toml has no mko_get_mediascope_data (F10).
      - test_pyproject_uses_spdx_license
        description: >
          pyproject.toml has license = "BSD-3-Clause" and license-files =
          ["LICENSE.txt"] (B3/D4, PEP 639).
      - test_pyproject_declares_package_data
        description: >
          pyproject.toml has settings/*.yaml in package-data declaration
          (B2).

verification_commands:
  - "uv run ruff check src/ tests/"
  - "uv run pytest"
  - "uv lock && uv run python -m build"
  - "uv venv /tmp/smoke-b6 --python 3.14"
  - "uv pip install --python /tmp/smoke-b6/bin/python dist/*.whl"
  - "/tmp/smoke-b6/bin/python scripts/smoke_install.py"
  - "uv run python -c \"import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('CI YAML valid')\""
  - "uv run twine check --strict dist/*"
  - "uv run check-wheel-contents --package mko_data_cleaner --src-dir src/mko_data_cleaner dist/*.whl"

out_of_scope:
  - "README.md changes (Doc-specialist step)"
  - "B7 version bump to 0.1.0 / git tag v0.1.0 / publish (Trusted Publishing)"
  - "B5 example notebook changes (already committed)"
  - "Windows CI runner (ubuntu-only per design decision R1)"
  - "check-wheel-contents.toml config file (CLI args used in CI)"
  - "Modifications to app.py, core/*, __init__.py (B1-B4 already committed)"
  - "MANIFEST.in changes (B2 already committed — verified correct)"

risks:
  - "R1 (fresh-venv creation on CI): `uv venv` + `uv pip install --python <venv-python>` must resolve the wheel artifact path. Mitigated by downloading artifact to `dist/` before install."
  - "R2 (uv version pinning): CI pins uv-version 0.9.26. If setup-uv@v5 does not support this exact version, remove the pin or use 'latest'."
  - "R3 (notebook install-cell guard): relies on `import mko_data_cleaner` succeeding after `uv sync`. If sync fails, the %pip install @v0.1.0 line executes and fails — but the job fails before reaching the notebook."
  - "R4 (smoke script platform): uses settings_dir param for isolation (no env-var patching). Verified cross-platform by using shutil.which + tempfile.mkdtemp."
  - "R5 (importlib.resources in source checkout): `files('mko_data_cleaner')` resolves to src/mko_data_cleaner/ with pythonpath=['src']. Requires Python >=3.12 (project minimum — safe)."
  - "R6 (check-wheel-contents 0.6.3 CLI): --package and --src-dir flags are stable in 0.6.x. Version 0.6.3 used locally for verification."
  - "R7 (artifact passing): upload-artifact@v4 / download-artifact@v4 with name 'dist' and path 'dist/'. Must match exactly in build and install-smoke jobs."
  - "R8 (smoke script E2E sample): reuses tests/test_app.py::test_process_data_end_to_end sample shape. Depends on polars write_database ADBC adapter + SQLite. If it fails in CI, the mock-based test in test_app.py still covers the API contract."
```

---

## B7 — T7: First release and pinning

**Agents:** Planner → Implementor.

**Preamble — decisions made by the Planner (not to be re-decided by the Implementor):**

- **Commit message:** `chore(release): bump version to 0.1.0 and fix CI trigger branch` — follows the
  repo's established conventional-commit convention (`chore(scope): message`, seen as `chore(deps):`,
  `chore(meta):`). Scope `release` captures both the version bump and the CI-fix in one atomic commit
  per the task requirement. The single commit includes ALL FOUR files (pyproject.toml, __init__.py,
  ci.yml, uv.lock).

- **CI trigger fix:** `branches: [main]` → `branches: [main, master]`. The repo's default branch is
  `master` (verified via `git branch --show-current` on the dev machine and remote inspection:
  `origin` remote, branches `master / refactoring / clean_distinct`), so `[main]` alone never fires on
  default-branch pushes — the workflow is dead on its intended target. Adding `master` is a minimal
  one-element change that restores coverage. `main` is retained to future-proof against a potential
  branch rename; `pull_request:` (no branch filter) is unchanged.

- **Tag/branch approach:** Annotated tag `v0.1.0` is created on the bump commit, which lives on the
  current branch `feat-pynote-support` (HEAD `b390d04`), not the default `master`. This is
  **acceptable and required**: Git tags are branch-independent objects; `pip install
  "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"` resolves by tag name, not by branch.
  The plan explicitly lists "merging branches" as out of scope, so no merge to `master` is needed or
  requested. Tag message: `Release v0.1.0`.

- **Working-tree exclusion:** The working tree contains in-progress modifications to `README.md`
  (Doc-specialist), `.kilo/**` (audit agents — staged M, deleted D), and untracked `.ai/**`,
  `docs/**`. The B7 commit stages **ONLY**: `pyproject.toml`, `src/mko_data_cleaner/__init__.py`,
  `.github/workflows/ci.yml`, `uv.lock`. The Implementor must use `git add <specific files>` — never
  `git add .` or `git add -A`. `git status --short` after staging must show only those 4 files.

- **Risks summary:** (R1) tag push auth — if `git push origin v0.1.0` fails due to credentials, STOP
  and report; do NOT force-push, create new remotes, or store credentials; skip the GitHub install
  verification. (R2) network — fresh-venv `pip install git+...@v0.1.0` requires GitHub access + PEP 517
  source build. (R3) build time — source build from tag into an isolated env takes 2–5 min. (R4) lock
  refresh scope — `uv lock` diff must be limited to the root package version only.

After B7: Documentation (Doc-specialist: README per R10 + notebook docs), then Final Validation
(Validator).

---

### {context_r} — Research findings (B7/T7)

**Confidence:** HIGH — all claims verified statically against committed B1–B6 state at HEAD `b390d04`
and dynamically via `uv`/`python` on this machine (Windows 11, Python 3.14.7, uv 0.9.26).

#### 0. Verified current state at HEAD `b390d04`

- **Branch:** `feat-pynote-support` (current); remote `origin
  https://github.com/manicko/mko_data_cleaner.git`; default branch = `master`; no tags exist.
- **pyproject.toml `[project].version`:** `"0.0.1"` (line 7 of `[project]` table).
- **`src/mko_data_cleaner/__init__.py` fallback:** `__version__ = "0.0.1"` inside the
  `except PackageNotFoundError` branch (B3-set, with a `NOTE:` comment tracking pyproject).
- **`uv.lock` root package:** `[[package]] name = "mko-data-cleaner"` / `version = "0.0.1"` /
  `source = { editable = "." }` (line 741–743). No other package entry references "0.0.1".
- **`.github/workflows/ci.yml`:** `on: push: branches: [main]` (line 5) — defect: never fires on
  the default `master` branch.
- **Notebook install cell** (B5, `examples/notebooks/
  mko_data_cleaner_pipeline_example.ipynb`, line 36): already pins
  `'install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"'`.
- **`scripts/smoke_install.py`** (B6): already asserts `mko_data_cleaner.__version__ ==
  importlib.metadata.version("mko_data_cleaner")` (lines 52–55), so a built wheel with version 0.1.0
  is self-verifying.
- **`tests/test_metadata.py`** (B3): `test_version_is_non_empty_string` (no hardcoded value) and
  `test_version_matches_installed_metadata_when_available` (compares to
  `importlib.metadata.version` — skip in source mode). No test in `tests/` hardcodes "0.0.1"
  (grep-verified: zero matches for `0.0.1` in `tests/`).

#### 1. Commit convention (from `git log --oneline` at HEAD)

```
b390d04 fix(deps): cap polars below 2.0 for reader API compatibility
1562fc6 ci: add build/install smoke gates and packaging regression tests
384044a fix(app): guard stdout reconfigure for kernels without reconfigure support
762d681 chore(deps): lock notebook tooling dev dependencies
0fe72ef docs(examples): add runnable pipeline-step notebook with synthetic sample
5c4bfdd feat(api): return ReportResult from process_data and thread settings_dir
37140aa chore(meta): correct license metadata, declare rich, expose __version__, remove stale references
948a2c7 fix(build): ship settings YAML templates in wheel and sdist
```

Convention: `type(scope): message` — types used: `fix`, `chore`, `ci`, `docs`, `feat`, `style`.
Recommendation `chore(release): ...` is consistent. Authors are human (`Matveichev Nikolay`) and AI
(`kilo-agent`); the commit will be authored by the Implementor's git identity.

#### 2. `uv lock` refresh behavior

Running `uv lock` after bumping `version` in `pyproject.toml` updates the root `[[package]]` entry's
`version` field from `"0.0.1"` to `"0.1.0"` in `uv.lock`. Since no dependencies were added, removed,
or version-capped in B7, the lock diff should be a single-line change (the root package version
only). The Implementor must verify with `git diff uv.lock` that no dependency hashes or versions
changed — if they did, the lockfile was stale before B7 and the delta must be investigated.

#### 3. Build verification

`uv run python -m build` (already in dev group per B6) produces `dist/mko_data_cleaner-0.1.0-py3-none-any.whl`
and `dist/mko_data_cleaner-0.1.0.tar.gz`. The wheel `METADATA` file must contain
`Requires-Dist: polars<2.0.0,>=1.38.1` (from `pyproject.toml` line 21). `twine check --strict dist/*`
must pass with exit 0 and no warnings (verifies SPDX license + license-files are correct, carried over
from B3).

#### 4. Fresh-venv GitHub install (T7 acceptance / D1)

```
python -m venv /tmp/smoke-b7
/tmp/smoke-b7/bin/pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"
/tmp/smoke-b7/bin/python scripts/smoke_install.py
```

This requires the tag `v0.1.0` to be pushed first. `pip` fetches the tag, resolves build-system
requires (`setuptools>=77.0`), builds the wheel from source in an isolated env, and installs it.
On Windows, use `<venv>\Scripts\pip` / `<venv>\Scripts\python` instead of `/tmp/smoke-b7/bin/`.
If the tag push failed (R1), this step is skipped and the blocker is reported.

#### 5. Risks specific to B7

- **R1 (tag push auth):** pushing to `origin` (public GitHub) requires configured credentials. If
  `git push origin v0.1.0` fails, STOP — do not force-push, do not create new remotes, do not store
  credentials. Report the blocker. The GitHub install verification is skipped.
- **R2 (lock diff scope):** if `uv lock` produces changes beyond the root package version, investigate
  — the lockfile may have been stale at HEAD and the delta may include dependency updates that
  should have been part of a separate `chore(deps)` commit.
- **R3 (tag on feature branch):** tagging on `feat-pynote-support` is correct per analysis §4 above.
  If the team later expects tags only on `master`, that is a process decision outside this plan's
  scope (the plan does not request a merge).

---

### {task_description} — Implementation Task (B7/T7)

```yaml
id: b7_t7_first_release

title: "B7/T7 — First release: version 0.1.0, CI trigger fix, git tag, GitHub install verification"

priority: low-medium

depends_on:
  - b1_t1_remove_import_time_init
  - b2_t2_ship_yaml_templates
  - b3_t3_metadata_hygiene
  - b4_t4_public_api_contract
  - b5_t5_example_notebook
  - b6_t6_ci_smoke_tests

# ── Plan/Audit report source ──
# source_reference: .ai\plans\01-pip-installable-jupyter-pipeline-step.md
# source_section: B7 — T7: First release and pinning
# spec_reference: .ai\problems\01_pip_installable_jupyter_pipeline_step_spec.md
# spec_task: T7 (First release and pinning)
# spec_decisions: D1 (GitHub tag install only), D11 (version 0.1.0, tag v0.1.0), D6 (no compat shim / pre-1.0 SemVer)
# spec_requirements: R7 (demonstrated workflow), R6 (pinning by tag)

description: >
  Create the first reproducible-release anchor for the example notebook (R6/D1/D11):
  bump the project version from 0.0.1 to 0.1.0 (pre-1.0 SemVer per D6, where MINOR is
  the breaking-change axis), refresh uv.lock so the root package version matches, fix
  the CI push-trigger branch list (defect: default branch is `master` but the workflow
  only lists `main`), commit all four files as one atomic release commit, create an
  annotated tag `v0.1.0`, push it to origin, and verify from a fresh venv that
  `pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"` succeeds
  and that `scripts/smoke_install.py` passes against that install. The notebook install
  cell already pins `@v0.1.0` (B5); the smoke script already asserts version parity
  between `__version__` and `importlib.metadata.version("mko_data_cleaner")`.
  `tests/test_metadata.py` asserts `__version__` is a non-empty string and matches
  installed metadata. No code logic changes — B1–B6 are complete and green. PyPI
  publication is explicitly out of scope (D1: GitHub-tag install is the distribution
  path for v0.1.0).

goals:
  - "pyproject.toml [project].version = \"0.1.0\""
  - "src/mko_data_cleaner/__init__.py fallback __version__ constant = \"0.1.0\" (keeps NOTE tracking comment)"
  - "uv.lock root package (mko-data-cleaner) version refreshed to 0.1.0; diff limited to root package version only"
  - ".github/workflows/ci.yml push.branches covers the default branch (master) — changed from [main] to [main, master]"
  - "One atomic commit: chore(release): bump version to 0.1.0 and fix CI trigger branch"
  - "Annotated git tag v0.1.0 pushed to origin"
  - "uv run pytest all green (test_metadata.py asserts version behavior)"
  - "uv run ruff check src/ tests/ scripts/smoke_install.py clean"
  - "uv run python -m build produces mko_data_cleaner-0.1.0-py3-none-any.whl and .tar.gz"
  - "Built wheel METADATA Contains Requires-Dist: polars<2.0.0,>=1.38.1"
  - "uv run twine check --strict dist/* passes (exit 0, no warnings)"
  - "Fresh venv pip install git+...@v0.1.0 + scripts/smoke_install.py passes"

files:
  - path: pyproject.toml
    targets:
      - type: field
        name: project.version
    semantic_anchors:
      - "The version key in the [project] table (currently version = \"0.0.1\")"
      - "No other pyproject.toml field is modified in this block"
    changes:
      - action: modify
        description: >
          Bump the project version from 0.0.1 to 0.1.0 (pre-1.0 SemVer per D6;
          the notebook install cell and tag target 0.1.0 per D11). This is the
          single source of truth for the package version.
        code_hint: |
          version = "0.1.0"

  - path: src/mko_data_cleaner/__init__.py
    targets:
      - type: module
    semantic_anchors:
      - "The except PackageNotFoundError branch that sets __version__ fallback"
      - "The NOTE comment above the fallback constant that documents pyproject.toml tracking"
    changes:
      - action: modify
        description: >
          Bump the fallback constant from \"0.0.1\" to \"0.1.0\" so it tracks the
          pyproject.toml version. The NOTE comment above it documents this
          coupling and must remain accurate — no other change to the file.
        code_hint: |
          # NOTE: must track the top-level ``version`` in pyproject.toml.
          __version__ = "0.1.0"

  - path: .github/workflows/ci.yml
    targets:
      - type: workflow_trigger
        name: push.branches
    semantic_anchors:
      - "The branches list under on: push: (currently branches: [main])"
      - "No other workflow content (jobs, steps, runners) is modified"
    changes:
      - action: modify
        description: >
          Change the push trigger branch list from [main] to [main, master].
          The repo's default branch is master (verified via git branch / remote
          inspection), so [main] alone never fires on default-branch pushes.
          Adding master is a minimal one-element change that restores coverage.
          main is retained for future-proofing. pull_request (no branch filter)
          is unchanged.
        code_hint: |
          on:
            push:
              branches: [main, master]
            pull_request:

  - path: uv.lock
    targets:
      - type: lockfile_entry
        name: mko-data-cleaner
    semantic_anchors:
      - "The root [[package]] entry for mko-data-cleaner (name = \"mko-data-cleaner\", source = { editable = \".\" })"
      - "No dependency package entries are modified"
    changes:
      - action: modify
        description: >
          Refresh uv.lock via `uv lock` so the root package version changes from
          0.0.1 to 0.1.0. Verify with `git diff uv.lock` that the diff is limited
          to the version field of the root package entry (no dependency versions
          or hashes should change). If unexpected changes appear, investigate
          before committing — the lockfile may have been stale.
        code_hint: |
          # In uv.lock, the root package entry changes from:
          [[package]]
          name = "mko-data-cleaner"
          version = "0.0.1"       # ← before
          source = { editable = "." }
          # to:
          [[package]]
          name = "mko-data-cleaner"
          version = "0.1.0"       # ← after
          source = { editable = "." }

acceptance_criteria:
  - "pyproject.toml [project].version == \"0.1.0\""
  - "src/mko_data_cleaner/__init__.py fallback __version__ == \"0.1.0\" (NOTE comment preserved)"
  - "uv.lock root [[package]] name = \"mko-data-cleaner\" has version = \"0.1.0\""
  - "git diff uv.lock shows ONLY the root package version change (0.0.1 → 0.1.0), no dependency changes"
  - ".github/workflows/ci.yml on.push.branches includes master"
  - "uv run pytest passes (all tests green; test_metadata.py asserts __version__ non-empty + matches installed metadata)"
  - "uv run ruff check src/ tests/ scripts/smoke_install.py is clean"
  - "uv run python -c \"import mko_data_cleaner; print(mko_data_cleaner.__version__)\" prints 0.1.0 (source-mode fallback)"
  - "uv run python -m build produces dist/mko_data_cleaner-0.1.0-py3-none-any.whl and dist/mko_data_cleaner-0.1.0.tar.gz"
  - "Wheel METADATA contains Requires-Dist: polars<2.0.0,>=1.38.1"
  - "uv run twine check --strict dist/* passes (exit 0, no warnings)"
  - "dist/, build/, and *.egg-info removed after build verification"
  - "git status --short shows ONLY pyproject.toml, src/mko_data_cleaner/__init__.py, .github/workflows/ci.yml, uv.lock as staged (no README.md, .kilo/**, docs/**)"
  - "One atomic commit with message chore(release): bump version to 0.1.0 and fix CI trigger branch"
  - "Annotated tag v0.1.0 created on the bump commit (git cat-file -t v0.1.0 == tag)"
  - "git push origin v0.1.0 succeeds (exit 0)"
  - "Fresh venv: pip install git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0 → scripts/smoke_install.py exits 0 with all [PASS] checks"
  - "Fresh venv: importlib.metadata.version(\"mko_data_cleaner\") == \"0.1.0\""

verification_commands:
  # Phase 1 — source-mode version + lint + tests
  - "uv run python -c \"import mko_data_cleaner; print(mko_data_cleaner.__version__)\""
  - "uv run ruff check src/ tests/ scripts/smoke_install.py"
  - "uv run pytest"
  # Phase 2 — build + metadata verification
  - "uv run python -m build"
  - "uv run twine check --strict dist/*"
  - "uv run python -c \"import zipfile, glob; z=zipfile.ZipFile(glob.glob('dist/*.whl')[0]); names=z.namelist(); assert 'mko_data_cleaner/settings/app_config.yaml' in names, 'app_config.yaml MISSING'; assert 'mko_data_cleaner/settings/log_config.yaml' in names, 'log_config.yaml MISSING'; meta=[n for n in names if n.endswith('METADATA')][0]; content=z.read(meta).decode(); assert 'polars<2.0.0,>=1.38.1' in content, 'polars constraint missing'; print('Wheel + metadata OK:', len(names), 'entries')\""
  - "rm -rf dist/ build/ *.egg-info src/mko_data_cleaner.egg-info"
  # Phase 3 — lock refresh + diff verification
  - "uv lock"
  - "git diff uv.lock"
  # Phase 4 — staging (MUST exclude other agents' files)
  - "git add pyproject.toml src/mko_data_cleaner/__init__.py .github/workflows/ci.yml uv.lock"
  - "git status --short"
  # Phase 5 — commit + tag + push
  - "git commit -m \"chore(release): bump version to 0.1.0 and fix CI trigger branch\""
  - "git tag -a v0.1.0 -m \"Release v0.1.0\""
  - "git push origin v0.1.0"
  # Phase 6 — GitHub install verification (fresh venv, after tag push succeeds)
  - "python -m venv /tmp/smoke-b7"
  - "/tmp/smoke-b7/bin/pip install \"git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0\""
  - "/tmp/smoke-b7/bin/python -c \"import importlib.metadata; print(importlib.metadata.version('mko_data_cleaner'))\""
  - "/tmp/smoke-b7/bin/python scripts/smoke_install.py"

out_of_scope:
  - "README.md changes (Doc-specialist step runs next; README is actively being edited by another agent in the working tree — do NOT stage or modify)"
  - "Publishing to PyPI (D1: GitHub-tag install is the distribution path for v0.1.0; PyPI Trusted Publishing is a later phase)"
  - "Merging feat-pynote-support into master (not requested by the plan)"
  - "Modifying pyproject.toml beyond the version field (no dependency, classifier, or license changes)"
  - "Modifying __init__.py beyond the fallback constant (no __all__, no re-exports — already done in B3/B4)"
  - "Creating new tests (test_metadata.py already covers version assertions; test_packaging.py already covers build invariants; smoke_install.py covers install-time version parity)"
  - "Changing CI workflow jobs, runners, steps, or the pull_request trigger (only the push.branches list is corrected)"
  - "Modifying uv.lock beyond the root package version (run uv lock; verify diff limited to version field)"
  - "Touching docs/** or .kilo/** files (actively being modified by other agents)"
  - "Modifying the notebook (install cell already pins @v0.1.0 from B5)"
  - "Trusted Publishing / PyPI automation (later phase)"

risks:
  - "R1 (push auth): git push origin v0.1.0 requires GitHub credentials. If the push fails (HTTP 403, authentication error, etc.), the Implementor MUST STOP and report the blocker. Do NOT retry with --force, do NOT create alternative remotes, do NOT store credentials. The fresh-venv GitHub install verification (Phase 6) is skipped if the tag push failed."
  - "R2 (network): pip install git+...@v0.1.0 requires network access to GitHub. The install builds the wheel from source in an isolated PEP 517 build environment (setuptools>=77.0, wheel). If the network is unavailable in the dev environment, the already-committed CI workflow (B6) serves as the canonical install-smoke gate."
  - "R3 (fresh-venv build time): building from a git tag via pip requires fetching the tag, resolving build-system requires, building the wheel from source, and installing all dependencies (polars, adbc-driver-sqlite, pandas, pyarrow, etc.). This can take 2-5 minutes. No progress bar in a subprocess call."
  - "R4 (lock refresh scope): `uv lock` may refresh the lock revision or dependency hashes if the lockfile was stale relative to the registry at HEAD. The Implementor MUST verify with `git diff uv.lock` that the only change is the root package version (0.0.1 → 0.1.0). If unexpected dependency changes appear, investigate before committing — they may indicate a stale lockfile requiring a separate chore(deps) commit."
  - "R5 (working-tree exclusion): the working tree contains in-progress modifications to README.md (Doc-specialist), staged .kilo/ audit files, and untracked .ai/ and docs/ files. The commit must stage ONLY the 4 release files. Use `git add <specific files>` — never `git add .` or `git add -A`."
  - "R6 (tag on non-default branch): the bump commit lives on feat-pynote-support, not the default master. Git tags are branch-independent — pip resolves @v0.1.0 by tag object. This is acceptable: the plan explicitly lists 'merging branches' as out of scope. If the team later expects tags only on master, that is a process decision outside this plan's scope."
  - "R7 (version fallback sync): if a future release bumps pyproject.toml but forgets the __init__.py fallback, test_version_is_non_empty_string still passes (non-empty string check) but the source-mode fallback value would be stale. The NOTE comment in __init__.py documents this coupling; B3 R3 already flagged this as a known manual-sync requirement."
```
