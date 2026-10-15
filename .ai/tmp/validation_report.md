# Validation Report — `feat-pynote-support` (12 commits, HEAD `340fb1c`)

**Project:** `mko_data_cleaner` — standalone uv project, Windows 11 + Python 3.14.7, uv 0.9.26.
**Scope:** Validate the 12 implementation commits (`6723c86..HEAD`) against spec `.ai/problems/01_pip_installable_jupyter_pipeline_step_spec.md` (defects F1–F12, tasks T1–T7, decisions D1–D11).
**Baseline:** `6723c86` · remote `https://github.com/manicko/mko_data_cleaner.git` · 0 tags at baseline.
**Method:** Inspect implementation at HEAD + execute all gates locally. No code was changed.

> **Note on working-tree state:** `README.md` and several `.kilo/commands/audit/phases/*` files show UNCOMMITTED modifications/deletions in the working tree produced by *other* agents after the commits were made. These are **not** part of the 12 validated commits. All source/test/config verification below was performed against the **committed (HEAD) tree** via `git show HEAD:...` and `git grep HEAD`. Uncommitted non-implementation files (`.ai/problems`, `.ai/plans`, `.ai/report`, `.ai/reports`, `docs/`, `.kilo/commands/docs/`, `.kilo/commands/audit/phases/02-audit-config.md`) are inputs or sibling-agent work, not deliverables of this workflow. **Note on commit count:** the task header says "11 commits" but lists 12; `git rev-list 6723c86..HEAD` confirms **12 commits**. A framing miscount, not a code defect.

---

## 1. Plan completeness — defects F1–F12

| ID | Spec defect | Verdict | Evidence (at HEAD `340fb1c`) |
|----|-------------|---------|------------------------------|
| F1 | `import mko_data_cleaner.app` crashes in fresh env (`LoggingSettings(**None)`) before `initialize_settings()` | **RESOLVED** | `app.py:12` imports `get_app_service` (function), not the singleton. `core/app_service.py` has **no module-level singleton** and **no module-level `dictConfig`**. `dictConfig` now runs only inside `get_app_service()` *after* the settings gate passes. `yaml_to_dict` returns `{}` in both `except` branches. Fresh-env import smoke: `"import OK, __version__ = 0.1.0, settings dir exists: False"`. `test_import_app_does_not_create_settings_dir` PASSED; `test_import_survives_stdout_without_reconfigure` PASSED. |
| F2 | `init` CLI unreachable (circular w/ F1) | **RESOLVED** | `app.py` no longer triggers the singleton at import; `init` command calls `init_project()` directly. `test_init_cli_creates_settings_in_tmp_dir` & `test_init_cli_with_settings_dir` PASSED. |
| F3 | YAML templates missing from wheel/sdist; stale MANIFEST rules | **RESOLVED** | `pyproject.toml` has `[tool.setuptools.package-data] mko_data_cleaner = ["settings/*.yaml"]`. `MANIFEST.in` corrected (`recursive-include src/mko_data_cleaner/settings *.yaml`; stale `requirements.txt`/`mko_get_mediascope_data` rules removed). Build output: wheel **20 entries including both YAMLs**; sdist **40 entries including both YAMLs**; no `"no files found matching requirements.txt"`/`mko_get_mediascope_data"` warnings. `test_template_yamls_accessible_as_package_data`, `test_template_yamls_parse`, `test_manifest_in_includes_yaml_templates`, `test_manifest_in_no_stale_project_name` PASSED. |
| F4 | `rich` imported but undeclared | **RESOLVED** | `pyproject.toml` `[project].dependencies` includes `rich>=13.0.0`. Built METADATA `Requires-Dist: rich>=13.0.0`. |
| F5 | License classifier MIT vs BSD-3-Clause; deprecated classifier style | **RESOLVED** | `pyproject.toml`: `license = "BSD-3-Clause"`, `license-files = ["LICENSE.txt"]`, **no** License classifiers; `build-system.requires = ["setuptools>=77.0", "wheel>=0.37.1"]`. Built METADATA: `License-Expression: BSD-3-Clause`, `License-File: LICENSE.txt`. `twine check --strict` PASSED with no deprecation warnings. `LICENSE.txt` L1 = `Copyright (c) mko`; clause 3 = `Neither the name of mko nor...`. `test_pyproject_no_stale_license_classifier`, `test_pyproject_uses_spdx_license` PASSED. |
| F6 | Empty `__init__.py`; no `__version__` | **RESOLVED** | `src/mko_data_cleaner/__init__.py` defines `__version__` via `importlib.metadata.version("mko_data_cleaner")` with `PackageNotFoundError` fallback `"0.1.0"` (matches `pyproject.toml` version `0.1.0`). `test_version_is_non_empty_string`, `test_version_matches_installed_metadata_when_available`, `test_top_level_exports` PASSED. |
| F7 | `process_data` docstring falsely "async"; returns `None` | **RESOLVED** | Docstring reads `"Synchronous entry point for non-CLI contexts (Jupyter / Airflow)."`. Signature `-> ReportResult`, body `return get_app_service(settings_dir).run_report(path)`. |
| F8 | `list_settings` references non-existent `app_paths.reports` | **RESOLVED** | `list_settings(settings_dir)` now lists `build_app_paths(settings_dir).user_settings_dir` via `list_files_in_directory`; gracefully prints a diagnostic when the dir is missing (no raise, no `.reports` access). `test_list_settings_lists_when_dir_exists`, `test_list_settings_does_not_raise_when_dir_missing` PASSED. |
| F9 | No app-layer tests | **RESOLVED** | New test modules: `tests/test_app.py` (23 tests), `tests/test_metadata.py` (2), `tests/test_packaging.py` (8), `tests/test_notebook.py` (4). |
| F10 | Stale `mko_get_mediascope_data` refs; stale `telebot`/`tests` loggers; missing `requirements.txt` include | **RESOLVED** | `git grep HEAD -- src/ tests/ MANIFEST.in .gitignore README.md LICENSE.txt pyproject.toml .kilo/` finds **0** stale refs in implementation files (the only `mko_get_mediascope_data` hits are inside `tests/test_packaging.py` *assertions* verifying the name's *absence*). `git grep HEAD -- .kilo/` = 0. `log_config.yaml` loggers object contains only `__main__` (no `tests`/`telebot`). `.gitignore` stale lines removed. `MANIFEST.in` has no `requirements.txt`/`mko_get_mediascope_data` rules. |
| F11 | No git tags / no notebooks; `build` absent from dev deps | **RESOLVED** | Tag `v0.1.0` exists locally and on remote (`refs/tags/v0.1.0^{}` → `27ae816`). Notebook `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb` exists, is valid nbformat 4.5+, and executes successfully. `pyproject.toml` `[dependency-groups] dev` now includes `build>=1.0.0`, `twine>=5.0.0`, `check-wheel-contents>=0.6.3`, `ipykernel`, `nbclient>=0.10.0`, `nbformat>=5.10.0`. |
| F12 | 43 MB sample not in dist; README shape differs from sample | **RESOLVED (by design)** | Wheel/sdist contain **no `data/`** content (assertion: `leaked` empty). Notebook generates a 7-row synthetic sample in-cell; the 43 MB file is never referenced by the notebook. |

## 2. Plan completeness — tasks T1–T7

| Task | Verdict | Evidence |
|------|---------|----------|
| T1 — Remove import-time init (factory + `SettingsNotInitializedError` + runtime logging) | **DONE** | `get_app_service(settings_dir=None)` factory; `_require_settings` gate raises `SettingsNotInitializedError(FileNotFoundError)` listing missing files + `initialize_settings()` remediation; `dictConfig` moved into factory post-construction; `__main__` block uses `get_app_service().run_report(...)`. `_import_data` still calls `prepare_log_paths()` but only after the gate. |
| T2 — Ship YAML templates in wheel/sdist | **DONE** | See F3. |
| T3 — Metadata/dependency/hygiene | **DONE** | See F4/F5/F6/F8/F10. |
| T4 — Public API contract | **DONE** | Top-level `__init__.py` re-exports `initialize_settings, process_data, ReportResult, SettingsNotInitializedError, __version__` with `__all__` (exactly 5 names). `ReportResult` Pydantic model (10 typed fields) in `core/models.py`. `process_data(...) -> ReportResult`. `settings_dir` threaded to `process_data`, `initialize_settings`, `init_project`, and CLI `init`/`run`/`list-settings`. |
| T5 — Example notebook | **DONE** | Notebook valid, pinned install `git+...@v0.1.0`, synthetic sample, executed successfully (see §4). |
| T6 — Smoke tests + CI | **DONE** | `test_packaging.py`, `test_notebook.py`, `scripts/smoke_install.py`; `.github/workflows/ci.yml` with `test`/`build`/`install-smoke`/`notebook` jobs. |
| T7 — First release | **DONE** | `version = "0.1.0"`; tag `v0.1.0` → `27ae816` (local + remote); notebook pins `@v0.1.0`. |

## 3. Cross-block integration

- **Import chain has no cycles:** `__init__` → `app` → `core.app_service`/`core.init_service`/`core.utils` → `core.{csv_service,db_service,dict_service,errors,models,paths}` → none back to top-level `__init__`. `core.init_service` imports only `core.paths` (platformdirs + pydantic). Verified by static inspection + successful import (no `ImportError`).
- **`init` → settings → `process_data` end-to-end:** `test_process_data_end_to_end` PASSED (3 rows loaded → 2 exported after DELETE rule); notebook cell 12 (`process_data`) executed with 7 rows loaded → 5 exported.
- **`settings_dir` directory semantics agree:** `settings_dir` is the user *base* dir; YAMLs resolve to `settings_dir / "settings"` everywhere. `test_initialize_settings_threads_settings_dir` → `user_dir/"settings"`; `test_init_project_with_settings_dir` → `user_dir/"settings"`; `test_init_cli_with_settings_dir` → `X/"settings"`; `test_process_data_end_to_end`/`process_data(..., settings_dir=populated_settings)` works. Consistent with the plan's consistency matrix.
- **Notebook and smoke script use the same public API:** both call top-level `initialize_settings(settings_dir=...)` / `process_data(report_dir, settings_dir=...)` / consume `ReportResult`.
- **`__init__.py` fallback tracks pyproject version:** both `__version__ = "0.1.0"` and `pyproject.toml` `version = "0.1.0"`; `importlib.metadata.version("mko_data_cleaner")` resolves to `0.1.0` in the smoke test.

## 4. Architectural consistency

- **Factory pattern in `core/app_service.py`:** `get_app_service()` / `build_app_paths()` / `_require_settings()` — no module-level singleton.
- **No module-level singleton:** grep across `src/` confirms the only module-level construction is `APP_PATHS = AppPaths(...)` in `core/paths.py` (path-string computation only — **side-effect-free**, no filesystem mutation). `AppService`, `CSVWorker`, `DBWorker`, `MappingDict` are constructed only inside factory/method bodies.
- **No import-time `dictConfig`:** the only `logging.config.dictConfig(...)` call is inside `get_app_service()` (line 337). No module-level call.
- **`sys.stdout.reconfigure` is guarded:** `app.py` wraps it in `if hasattr(sys.stdout, "reconfigure")` (line 23) — survives ipykernel `OutStream` without `reconfigure`. `console = Console()` remains module-level (non-crashing, pre-existing; noted as residual in the plan).
- **Error class placement:** `SettingsNotInitializedError(FileNotFoundError)` lives in `core/errors.py` alongside `ConfigError`/`DataValidationError`.
- **Pydantic v2 style:** `ReportResult(BaseModel)` in `core/models.py` follows the existing model conventions (type-hinted fields, English docstring).
- **Project rules respected:** new user-visible CLI messages are English (commit `1be3956`); no new abstractions beyond spec; `StrEnum`/`enum` constants untouched; no `print()` used in core logic (only pre-existing `print` statements in `app_service.run_report` progress which predates this work and is out of scope).

## 5. Regressions — executed validation results

| Gate | Command | Result |
|------|---------|--------|
| Fresh-env import | subprocess patching `platformdirs.user_config_dir` → `import mko_data_cleaner.app` + top-level API | **PASS** — import OK, `__version__ = 0.1.0`, settings dir **not** created |
| Test suite | `uv run pytest` | **62 passed**, 1 warning (pre-existing polars `read_csv_batched` deprecation, not introduced here) |
| Lint | `uv run ruff check src/ tests/` | **All checks passed!** |
| Build | `uv run python -m build` | **Built** `mko_data_cleaner-0.1.0-py3-none-any.whl` + `.tar.gz` (setuptools 84.0.0) |
| Metadata | `uv run twine check --strict dist/*` | **PASSED** (wheel + sdist; no warnings) |
| Wheel contents | `uv run check-wheel-contents --package src/mko_data_cleaner dist/*.whl` | **OK** |
| YAML-in-wheel + no leakage | unpack wheel → assert both YAMLs present, no `data/` | **PASS** — 20 entries, both YAMLs present, no `data/` leakage |
| YAML-in-sdist | unpack sdist → assert both YAMLs present | **PASS** — 40 entries, both YAMLs present |
| METADATA | inspect built wheel METADATA | `Name: mko_data_cleaner` / `Version: 0.1.0` / `License-Expression: BSD-3-Clause` / `License-File: LICENSE.txt` / **no License classifiers** / `Requires-Dist: rich>=13.0.0` / `Requires-Dist: polars<2.0.0,>=1.38.1` / 13 direct deps |
| Notebook execution | `nbclient` executes notebook top-to-bottom (clean kernel) | **PASS** — `Notebook executed OK`; cell 14 output: `rows_loaded: 7`, `rows_exported: 5`, `timings: {'import':…, 'mapping':…, 'export':…}`, `export_dir: .../clean_data`, `null_data_files: [...]`; clean-data sample shape (5, 9) incl. `tag`; null-data sample shape (2, 2) with `Orange;null`, `Pear;null` |
| Build artifacts | cleanup | `dist/`, `build/`, `*.egg-info` **removed**; implementation files git-tree clean |

**Note on `check-wheel-contents`:** despite the setuptools warning `Package 'mko_data_cleaner.settings' is absent from the packages configuration` (because `settings/` has no `__init__.py`, by design per plan B2 R3), `check-wheel-contents` reports **OK** and both YAMLs are present in the wheel. The warning is noise from setuptools' package-discovery, not a correctness defect.

## 6. Tests & quality gates (depth, not trivia)

Tests assert real behavior, not tautologies:
- **Settings-gate errors** (`test_app.py`): `test_process_data_without_settings_raises_settings_error` (raises `SettingsNotInitializedError`); `test_process_data_error_is_file_not_found` (isinstance `FileNotFoundError`, NOT `ValidationError`); `test_process_data_error_names_initialize_settings` (message contains `initialize_settings()` + `app_config.yaml`).
- **e2e pipeline** (`test_app.py::test_process_data_end_to_end`): asserts `rows_loaded==3`, `rows_exported==2`, export dir contents, timings keys, duration>0.
- **ReportResult contract** (`test_app.py`): `test_report_result_model_construction_and_serialization` (all 10 fields, types, `model_dump()` serialization); `test_process_data_returns_report_result` (mocked path returns `ReportResult` with exact field values).
- **`settings_dir` threading** (`test_app.py`): 4 tests covering `initialize_settings`, `init_project`, CLI `init`, and `None`-path `APP_PATHS` monkeypatch.
- **Packaging signals** (`test_packaging.py`): 8 tests asserting YAML-as-package-data, YAML parseability, MANIFEST staleness/license/SPDX/package-data.
- **Notebook structure** (`test_notebook.py`): 4 tests — nbformat validity, unique cell ids, `@v0.1.0` install pin, public-API import references.
- **csv_service return contract** (`test_csv_service.py`): 2 tests asserting `export_sql_to_csv` returns `int` (3 rows; 0 for empty table).
- **Metadata** (`test_metadata.py`): `__version__` non-empty string + matches `importlib.metadata` when installed.

No test is weakened: every assertion checks a concrete, spec-relevant property (error type, message content, count equality, path equality, exit code, field type/values, `__all__` exactness).

## 7. Documentation

- **Committed `README.md` (HEAD `340fb1c`):** documents the top-level API (`from mko_data_cleaner import initialize_settings, process_data`); `ReportResult` with a full field table; `settings_dir` parameter on `initialize_settings()`/`process_data()`; `SettingsNotInitializedError` (subclass of `FileNotFoundError`) with remediation; the example notebook link; the pinned install `%pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"`; CLI `--settings-dir`/`list-settings` flags. `git grep HEAD -- README.md` finds **0** occurrences of `mko_get_mediascope_data` / `core.app_service.app_service`.
- **`examples/README.md` (committed):** accurate cell-flow table, synthetic sample layout, and verified-outputs section that **matches** the executed notebook (`rows_loaded=7`, `rows_exported=5`, timings keys, clean-data shape (5,9), null-data shape (2,2)). Accurate.
- **Uncommitted changes (out of scope):** `README.md` has +45/-15 uncommitted edits by another agent; several `.kilo/commands/audit/phases/*` files are uncommitted M/D. **These are not part of the 12 validated commits and are not treated as regressions.** The committed deliverables already satisfy all documentation requirements.

## 8. Unrelated changes — per-commit file scope (HEAD `6723c86..HEAD`)

| Commit | Files touched | Block scope |
|--------|---------------|-------------|
| `76b1fc8` fix(app) | app.py, core/app_service.py, core/errors.py, core/utils.py, tests/test_app.py | B1/T1 |
| `1be3956` style(app) | app.py | B1/T3 follow-up (English CLI msgs) |
| `948a2c7` fix(build) | MANIFEST.in, pyproject.toml | B2/T2 |
| `37140aa` chore(meta) | .gitignore, .kilo phases 01+02, LICENSE.txt, pyproject.toml, __init__.py, app.py, settings/log_config.yaml, tests/test_metadata.py | B3/T3 (the two `.kilo` edits are in-scope F10 stale-name fixes) |
| `5c4bfdd` feat(api) | __init__.py, app.py, core/app_service.py, core/csv_service.py, core/init_service.py, core/models.py, tests/test_app.py, tests/test_csv_service.py | B4/T4 |
| `0fe72ef` docs(examples) | examples/README.md, notebook, pyproject.toml, tests/test_notebook.py | B5/T5 |
| `762d681` chore(deps) | uv.lock | B5 (notebook tooling lock only) |
| `384044a` fix(app) | notebook, app.py, tests/test_app.py | B5/T5 follow-up (stdout reconfigure guard) |
| `1562fc6` ci | .github/workflows/ci.yml, pyproject.toml, scripts/smoke_install.py, tests/test_packaging.py, uv.lock | B6/T6 |
| `b390d04` fix(deps) | pyproject.toml, uv.lock | B6 follow-up (polars cap) |
| `27ae816` chore(release) | .github/workflows/ci.yml, pyproject.toml, src/mko_data_cleaner/__init__.py, uv.lock | B7/T7 |
| `340fb1c` docs(readme) | README.md | Documentation step (R10) |

**No unrelated files committed:** `docs/` is entirely untracked (sibling-agent input, not this workflow); `.ai/*` is untracked input/spec, never committed; `uv.lock` changes are dependency-only (versions/locks); `README.md` appears only in the doc commit (`340fb1c`).

- **Tag:** `v0.1.0` (annotated) → commit `27ae816` (`chore(release): bump version to 0.1.0`). Confirmed locally (`git rev-parse "v0.1.0^{commit}" == 27ae816`) and remotely (`refs/tags/v0.1.0^{}` → `27ae816`).

## 9. Residual issues / warnings

1. **CI cannot be executed in this environment.** The `.github/workflows/ci.yml` jobs (test/build/install-smoke/notebook) are verified by inspection and by running their equivalent commands locally (build, twine check, check-wheel-contents, fresh-venv smoke equivalent, notebook execution). CI itself is run by GitHub Actions — untested by definition in this sandbox. *(Low risk.)*
2. **Windows cp1251 / Unicode caveat (out of spec).** `core/utils.py::progress_bar` and `core/app_service.py` emit Unicode block glyphs (`█`, `░`) and an em-dash (`—`). On a legacy Windows code-page console these raise `UnicodeEncodeError` unless stdout is UTF-8. Mitigated for the shipped smoke script (`scripts/smoke_install.py` reconfigures stdout/stderr to `utf-8`) and for kernel imports (`app.py` guards `sys.stdout.reconfigure` with `hasattr`). The cleaning/CLI-encoding path is explicitly out of scope (spec §11 — no ETL rewrite). Documented as a known residual, not a defect of this work. *(Low risk, out of scope.)*
3. **Polars capped `<2.0.0`:** intentional (commit `b390d04` follow-up) for reader-API compatibility, not a defect. Confirmed in `pyproject.toml` and built METADATA (`Requires-Dist: polars<2.0.0,>=1.38.1`). *(N/A — intended.)*
4. **Branch is `feat-pynote-support`, not `master`:** CI `push` trigger is `[main, master]`; direct pushes to the feature branch won't fire `push` CI, but `pull_request` triggers cover them. *(Low risk.)*
5. **`examples/README.md` troubleshooting inaccuracy (advisory, not a regression):** the troubleshooting bullet claims "Cell 4 installs a no-op `reconfigure` shim", but the actual notebook cell 4 only imports (no shim) and `app.py`'s `hasattr(sys.stdout, "reconfigure")` guard already handles ipykernel. The shim is no longer needed/installed. Pure documentation drift in a committed file — minor, does not affect acceptance. *(Advisory.)*

## 10. Warnings (architectural / rollout / dependency)

- **Setuptools package-discovery warning:** `mko_data_cleaner.settings` is flagged as "absent from `packages` configuration" because `settings/` intentionally has no `__init__.py` (per plan B2 R3). The data still ships via `[tool.setuptools.package-data]` (`check-wheel-contents` OK, both YAMLs present), but the warning recurs on every `uv build`. A later refactor could add `license-files`/`exclude_package_data` or convert `settings/` to a real subpackage — but that is explicitly out of scope (C1/C3: smallest change, no new abstractions). *(Maintainability noise, not a correctness risk.)*
- **Heavy import for `__version__`:** top-level `import mko_data_cleaner` pulls in `app` → `core.app_service` → polars + adbc (per B4 import-cycle analysis, accepted). No import-time side effects remain, but the import is not lightweight. A future PEP 562 `__getattr__` could lazy-load (documented in plan as deferred). *(Evolvability note.)*
- **Dependency hygiene residue (out of scope):** `pydantic-settings>=2.12.0` is declared but never imported in `src/`. Left untouched per spec scope (C2 minimal change). *(Advisory.)*

## 11. Required fixes

None required for acceptance. The implementation is complete and green.

The only recommended follow-up (advisory, not mandatory):
- **(A)** Correct the `examples/README.md` troubleshooting bullet that references a non-existent `reconfigure` shim (cell 4 no longer installs one; `app.py`'s `hasattr` guard handles ipykernel). One-line doc fix.

## 12. Advisory recommendations (non-blocking)

- **(B)** Silence the recurring `mko_data_cleaner.settings` package-discovery warning by either (a) documenting the intentional non-package `settings/` under `[tool.setuptools]` or (b) a later refactor to treat `settings/` as package data via `exclude_package_data` — if a zero-warning build becomes a requirement.
- **(B)** Remove the unused `pydantic-settings` runtime dependency (not imported anywhere) in a future dependency-hygiene pass.
- **(B)** Consider moving `build_app_paths` to `core/paths.py` so `init_service` and `app_service` share one definition without pulling polars/adbc into `init_service`'s import graph (flagged as R10 in the plan's risk notes).

---

## Final verdict: ✅ **ACCEPT**

All 12 defects (F1–F12) are **resolved** at HEAD `340fb1c`; all 7 tasks (T1–T7) are **implemented**; all PO decisions (D1–D11) are honored. Every validation gate executed locally passes:
- fresh-env `import mko_data_cleaner.app` succeeds with no settings-dir side effects (`__version__ = 0.1.0`);
- `uv run pytest` → **62 passed**;
- `uv run ruff check src/ tests/` → **All checks passed**;
- `uv run python -m build` → wheel + sdist built; both `uv run twine check --strict` and `uv run check-wheel-contents` pass; wheel (20 entries) and sdist (40 entries) both contain `settings/app_config.yaml` + `settings/log_config.yaml` and leak no `data/`;
- METADATA is correct (SPDX `BSD-3-Clause`, `License-File: LICENSE.txt`, no license classifiers, `rich>=13.0.0` and `polars<2.0.0` declared, v0.1.0);
- the example notebook executes top-to-bottom under `nbclient` with outputs matching `examples/README.md` (`rows_loaded=7`, `rows_exported=5`, timings `{import, mapping, export}`, `export_dir=clean_data/`, `null_data_files` populated);
- git tag `v0.1.0` → commit `27ae816` (local + remote).

Each of the 12 commits touches only its block's files; no unrelated trees were committed by this workflow. The only uncommitted working-tree differences (`README.md`, several `.kilo/commands/audit/phases/*` files) are produced by other agents after the commits and are explicitly out of validation scope — they are **not** treated as regressions. No required fixes block acceptance.
