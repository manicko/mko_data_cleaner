# 01 — pip-installable Jupyter pipeline step (`mko_data_cleaner`) — Analytical Specification

- **Status:** Product Owner decisions **confirmed 2026-10-14** (Q1–Q11 answered; recommended options accepted for all, with two deviations: `list_settings` is repointed instead of removed, and the result object carries paths + clean row counts, not full statistics). Ready for implementation planning.
- **Date:** 2026-10-14
- **Source request:** `.ai/plans/Problem_01.md` (business request + third-party auditor recommendations)
- **Evidence base:** repository inspection, two Researcher investigations (packaging/first-run verification with empirical builds; public API / notebook / CI best practices). Summary in §6, full evidence in Appendix A.

---

## 1. Problem statement

The owner wants `mko_data_cleaner` — a tool that cleans Mediascope (TV / TV_REG) CSV exports (CSV → Polars → SQLite → rules → export) — usable as **one step inside a data pipeline built on Jupyter Notebook**, installed via `pip` (`%pip install`), and demonstrated by a ready-to-use `.ipynb` example.

A third-party auditor proposed a plan ("do not rewrite the project; fix first-run import and packaging; stabilize the public API; add smoke tests; tag the first release"). The owner asked to verify the auditor's claims very carefully and to adopt only what fits modern practice.

Verification result: **all four auditor claims are TRUE, confirmed with empirical builds and a fresh-venv import reproduction.** The project currently **cannot be installed and used from a clean environment at all**: importing `mko_data_cleaner.app` crashes before `initialize_settings()` can ever be called (circular dependency), and the wheel does not contain the YAML templates that `init` needs to copy. Additional defects were found that the auditor missed (broken `list_settings` command, missing `__version__`, empty `__init__.py`, stale project-name references, out-of-date license classifier style).

The work to be specified: make the package reliably installable, define a stable pipeline-facing public API, provide a working example notebook, and add reproducible build/install verification — without rewriting the tool's internal processing pipeline.

---

## 2. Scope

**In scope**
- Fixing the first-run import crash (import must never require user configuration).
- Fixing wheel/sdist contents so YAML templates ship inside the package.
- Declaring the `rich` direct dependency; correcting license metadata to SPDX form.
- Defining and implementing the public library API consumed from a notebook (`initialize_settings`, `process_data`) with an explicit, self-remediating error when settings are missing.
- A ready, valid `.ipynb` example with a tiny self-contained synthetic sample dataset.
- Smoke tests + CI verification of build/install (wheel content, fresh-venv import, metadata checks).
- First release tagged (`v0.1.0`) so the notebook can pin a reproducible version.

**Out of scope (see §10)**

---

## 3. Confirmed requirements (facts, not proposals)

| # | Requirement | Evidence |
|---|---|---|
| F1 | `import mko_data_cleaner.app` / `from mko_data_cleaner.app import process_data` **crashes in a fresh environment** with `TypeError: LoggingSettings() argument after ** must be a mapping, not NoneType` — before `initialize_settings()` can run. Root cause chain: `app.py:12` → module-level singleton `app_service.py:248` → `prepare_log_paths()` `:23` → `log_config` `:26-28` → `yaml_to_dict` returns `None` on missing file (`core/utils.py:144-147`, missing `return`) → `LoggingSettings(**None)`. Even a `{}` return would fail: `formatters/handlers/loggers/root` are required fields (`core/models.py:50-57`). | Reproduced in a fresh venv with the built wheel (both current and packaging-fixed builds). |
| F2 | The `init` CLI command that would fix F1 is itself unreachable, because registering it requires importing the crashing module. | `app.py:12,26-34`. |
| F3 | Built wheel (`mko_data_cleaner-0.0.1-py3-none-any.whl`) contains **19 entries and no `settings/*.yaml`**; sdist likewise contains none. Causes: no `[tool.setuptools.package-data]` in `pyproject.toml`, and `MANIFEST.in:4` references the stale old package name `mko_get_mediascope_data` (build emits "no files found matching" warnings). `init` copies templates from `APP_PATHS.app_settings_dir` (`core/init_service.py:12`), so after install `init` would copy nothing. | `uv build` output + wheel/sdist listings. Fix verified: adding `[tool.setuptools.package-data] "mko_data_cleaner" = ["settings/*.yaml"]` + corrected MANIFEST.in puts both YAMLs in wheel **and** sdist. |
| F4 | `rich` is imported in `app.py:10` but not declared in `[project].dependencies`; it arrives only transitively via `typer>=0.24.0` (confirmed in `uv.lock` / `uv tree`). Implicit dependency — a future typer change would break imports. | `uv tree`, `uv.lock`, wheel METADATA `Requires-Dist`. |
| F5 | License mismatch: `LICENSE.txt` is **BSD-3-Clause** text; `pyproject.toml:35` declares `License :: OSI Approved :: MIT License`. setuptools 82+ also deprecates license classifiers in favor of SPDX `License-Expression` (PEP 639) — build emits `SetuptoolsDeprecationWarning`. | LICENSE.txt content; build output. |
| F6 | `src/mko_data_cleaner/__init__.py` is **empty** → `from mko_data_cleaner import process_data` fails; there is no `__version__`; version exists only in `pyproject.toml:7` (`0.0.1`). | Repository inspection. |
| F7 | `process_data` docstring falsely claims it is "async"; it is synchronous and returns `None`, writing results to `<report>/clean_data/` and `null_data_*.csv.gz`. | `app.py:73-80`; `core/app_service.py:218-238`. |
| F8 | `list_settings` CLI command calls `app_service.app_paths.reports`, which does not exist on `AppPaths` → guaranteed `AttributeError` when invoked (import succeeds; call fails). | `app.py:61`; `core/paths.py:32-54`. |
| F9 | No test covers the app layer (`mko_data_cleaner.app`, `init`, `process_data`) — only dict/db/csv services. This is why F1 went undetected. | `tests/conftest.py`, `tests/test_*.py`. |
| F10 | Stale references to the abandoned project name `mko_get_mediascope_data` remain in `MANIFEST.in:4,7`, `app.py:2`, `.gitignore:224-225`, `.kilo/commands/*`, and the plan document. Stale loggers `tests`/`telebot` remain in `settings/log_config.yaml:51-60`. `MANIFEST.in:3` includes a non-existent `requirements.txt`. | Search results. |
| F11 | There are **no git tags and no notebooks** in the repository yet; the public GitHub remote is `manicko/mko_data_cleaner`. `python -m build` is not installed and `build` is absent from `[dependency-groups] dev` (only `twine`). | `git tag` (empty), `git remote`, `pyproject.toml:166-175`. |
| F12 | Sample data: `data/example/raw_data/example_data.csv` (43.7 MB, Cyrillic headers, README-canonical shape NOT matching) is tracked in git but excluded from distributions; `data/raw_data/example_data.csv` is a 31-byte placeholder. | File sizes; content reads. |

**Requirements derived from the business goal (to be built):**

- **R1 — Clean-environment importability.** Importing the package (top-level and via `app`) must never require, read, or create user configuration. Side-effect-free import.
- **R2 — Self-contained distributions.** Every built wheel and sdist contains `settings/app_config.yaml` and `settings/log_config.yaml`; a source checkout is not needed after install.
- **R3 — Explicit first-run failure.** Calling `process_data` (or CLI `run`) without initialized settings raises a dedicated, catchable error that names the missing setting and the exact remediation (`initialize_settings()`). Silent writes to the user config dir are not acceptable behavior.
- **R4 — Stable pipeline-facing API.** The notebook-facing surface is exactly two functions — `initialize_settings(...)` and `process_data(...)` — importable from the top-level package, with stable signatures, documented return types, and `__all__`. Internal `core` modules are not part of the public contract.
- **R5 — Pipeline-consumable result.** `process_data` returns a structured result object instead of `None`: result paths (report, import/export dirs, db, `null_data` files), start/finish timestamps and duration, and clean-data row counts (rows loaded into the database and rows exported to `clean_data/`). Per-rule statistics and non-mapped row counts are **not** part of the object (the pipeline can derive them from the listed `null_data` files if needed).
- **R6 — Reproducible installation.** Releases are pinned by git tag (later PyPI version); the example notebook installs via `%pip install "git+...@v0.1.0"`-style pinning, never a moving branch.
- **R7 — Demonstrated workflow.** A valid, executable `.ipynb` example in the repository shows: install → initialize settings → expected input layout → run → consume results. The example must run from a tiny self-contained synthetic dataset (no 43 MB file), mirroring the README's documented input shape.
- **R8 — Verified packaging.** CI on every push/PR builds sdist+wheel, validates metadata (`twine check`), checks wheel contents, asserts the YAML templates are inside the wheel, installs into a fresh venv and imports the package, and runs the test suite. Publishing on tags is automated later via Trusted Publishing (Product Owner decision D1).
- **R9 — Correct metadata.** License metadata matches `LICENSE.txt` (SPDX form); every direct import is a declared dependency; no stale project-name references remain in active files.
- **R10 — Documentation parity.** README documents the pipeline (notebook) usage, where `initialize_settings()` writes settings per OS, the expected report directory layout (`raw_data/`, `dict/merged_dictionary.csv`), and the outputs (`clean_data/`, `null_data_*.csv.gz`). README stops recommending `from mko_data_cleaner.core.app_service import app_service`.

---

## 4. Conceptual development tasks

### T1 — Remove import-time initialization (critical path, blocks everything)
- **Purpose:** break the circular dependency that makes a fresh install unusable: import must not construct `AppService`, must not read the user config dir, and must not configure logging as an import side effect.
- **Expected outcome:** `import mko_data_cleaner.app` succeeds in a fresh venv with no settings; `init` becomes reachable; service construction and logging setup happen only when a command/function actually runs, using a factory that validates settings presence.
- **Dependencies:** none.
- **Acceptance:** fresh-venv import smoke passes; existing services (`dict/db/csv`) tests still pass; CLI `init` works without prior settings.

### T2 — Ship package templates in wheel and sdist
- **Purpose:** `initialize_settings()` needs the packaged YAML templates to exist after `pip install` (F3).
- **Expected outcome:** `pyproject.toml` declares package data for `settings/*.yaml`; `MANIFEST.in` corrected (drop stale `mko_get_mediascope_data` rules and non-existent `requirements.txt`); verified wheel contains both YAMLs and the sdist is complete enough to rebuild the wheel.
- **Dependencies:** independent of T1, but both are required before any usable release (T2 alone does not fix F1 — proven empirically).
- **Acceptance:** build → unpack wheel → assert `mko_data_cleaner/settings/{app_config.yaml,log_config.yaml}` present; no "no files found matching" build warnings.

### T3 — Metadata, dependency and hygiene fixes
- **Purpose:** make the project publishable and its imports trustworthy (F4, F5, F6 partial, F8, F10).
- **Expected outcome:** SPDX license expression matching `LICENSE.txt` (confirmed D4); `rich` declared; runtime `__version__` available; `list_settings` repointed to list the user settings directory (confirmed D5, deviation from the remove recommendation); stale `mko_get_mediascope_data` references and stale `telebot`/`tests` loggers removed from active files; `process_data` docstring corrected (it is synchronous).
- **Dependencies:** none (license decision D4 needed before publishing).
- **Acceptance:** `twine check` passes with no license deprecation warnings; `import mko_data_cleaner; mko_data_cleaner.__version__` works; no grep hits for the old project name in active files.

### T4 — Public API contract for pipeline use
- **Purpose:** give the notebook/pipeline one stable, documented surface (R4, R5, R3).
- **Expected outcome:** top-level `__init__.py` re-exports `initialize_settings`, `process_data`, `__version__` with `__all__`; `process_data` returns a structured result object containing result paths (report dir, import dir, export dir, db path, `null_data` file list), started/finished timestamps, duration, and clean-data row counts (rows loaded and rows exported); missing settings raise the new dedicated error (`SettingsNotInitializedError`, subclassing `FileNotFoundError`) with remediation text; optional `settings_dir` parameter for pipeline-local configuration (confirmed D3); README rewritten to the notebook workflow and stopped recommending `core.app_service`.
- **Dependencies:** T1 (factory/lazy service construction must exist first).
- **Acceptance:** the public API contract in §6/Researcher-2 is fulfilled, with the result-object scoped to paths + timings + clean row counts per confirmed D8; a test asserts the uninitialized-settings error type and message; README's pipeline example matches the implemented signatures.

### T5 — Example notebook with synthetic sample data
- **Purpose:** prove and demonstrate the "one pipeline step" usage (R7).
- **Expected outcome:** `examples/notebooks/<name>.ipynb` (nbformat 4.5+, valid cell ids, validated with `nbformat`), cell flow: install (pinned tag) → import + version → `initialize_settings` → expected input layout → create tiny synthetic report (raw CSV + dictionary CSV) → `process_data` → consume result (read `clean_data/`, inspect `null_data_*`); synthetic sample mirrors the README canonical UPPERCASE header shape with ADD/REPLACE/DELETE dictionary rows. The 43 MB file is not referenced by the notebook.
- **Dependencies:** T1–T4 (the API contract must be in place for cells 3 and 7 to be real).
- **Acceptance:** notebook executes top-to-bottom in a clean kernel against the pinned tag; output cells show non-empty results.

### T6 — Smoke tests and build/install verification CI
- **Purpose:** lock in the fixes and make packaging verifiable without a developer machine (R8, F9).
- **Expected outcome:** tests covering (a) import without settings, (b) `initialize_settings()` on a temp config dir, (c) uninitialized-`process_data` raises the dedicated error, (d) end-to-end mini run on the synthetic sample, (e) wheel-content assertion; a GitHub Actions workflow on push/PR running the suite plus `python -m build` (add `build` to dev group), `twine check`, `check-wheel-contents`, and a fresh-venv install+import smoke.
- **Dependencies:** T1–T4 (tests target the new contract). CI can land with a subset first.
- **Acceptance:** the CI run on a PR would have caught F1 and F3 (i.e., it fails on the pre-fix code when pointed at it).

### T7 — First release and pinning
- **Purpose:** create the reproducible install target for the example notebook (R6).
- **Expected outcome:** version set to `0.1.0` in `pyproject.toml`; git tag `v0.1.0`; notebook pins the tag; publish target per decision D1 (GitHub install only / TestPyPI / PyPI). Trusted Publishing automation (later) per §6.
- **Dependencies:** T1–T6 complete and green.
- **Acceptance:** from a clean machine: `%pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"` → notebook cells run; `importlib.metadata.version("mko_data_cleaner") == "0.1.0"`.

---

## 5. Product Owner decisions (confirmed 2026-10-14)

All decisions were presented with options and recommendations; the Product Owner accepted every recommendation except D5 and D8, which are marked **DEVIATION** below.

- **D1 — Publication target (Q1): GitHub tag install only for the first release.** The example notebook installs via `%pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"`. PyPI publication is a later phase; TestPyPI may be used as a dry run before it.
- **D2 — Uninitialized settings behavior (Q2): dedicated error.** `process_data`/CLI `run` raise `SettingsNotInitializedError` (subclass of `FileNotFoundError`) naming the missing file and the exact remediation (`initialize_settings()`). No silent or auto writes to the user config directory. An `auto_init` parameter may be added later as explicit opt-in only.
- **D3 — Configuration location (Q5): platformdirs default + optional `settings_dir` parameter** on both public functions. Portable on all platforms (Windows has no env-var equivalent for the config dir).
- **D4 — License (Q3): BSD-3-Clause, with the copyright line corrected to the project author.** `LICENSE.txt` keeps the BSD-3-Clause terms; the third-party template attribution is removed. SPDX `license = "BSD-3-Clause"` replaces the deprecated MIT classifier; `license-files = ["LICENSE.txt"]`.
- **D5 — `list_settings` (Q6): DEVIATION — repoint, do not remove.** The command lists files of the **user settings directory** instead of the non-existent `reports` path (fixes F8).
- **D6 — Compatibility policy (Q7): no shim.** The internal `core.app_service` import path disappears; README documents the migration to the top-level API. Justified by pre-1.0 SemVer.
- **D7 — `process_data` input (Q8): directory only.** The report directory containing `raw_data/` + `dict/merged_dictionary.csv`; the misleading "file not found" wording is corrected.
- **D8 — Result object scope (Q9): DEVIATION — paths + clean row counts, not full statistics.** The result object contains: result paths (report dir, import dir, export dir, db path, `null_data_*.csv.gz` file list), started/finished timestamps and duration, and clean-data row counts (rows loaded into the database; rows exported to `clean_data/`). Per-rule statistics and non-mapped row counts are **out** of the object.
- **D9 — Example notebook and sample data (Q10): `examples/notebooks/` with a tiny synthetic sample** (~8 rows) mirroring the README's documented UPPERCASE input shape, plus a small dictionary exercising DELETE/REPLACE/ADD. The 43 MB tracked sample is not referenced by the notebook.
- **D10 — CI (Q11): verification workflow included in this work.** Push/PR workflow: run tests → `python -m build` → `twine check` → `check-wheel-contents` → unpack wheel and assert both YAML templates present → fresh-venv install + import smoke.
- **D11 — First release version (Q4): 0.1.0**, tagged `v0.1.0`. 0.x SemVer convention applies (MINOR is the breaking axis until 1.0).

---

## 6. Research summary

**Auditor claim verification (Researcher 1 — empirical, builds in scratch).** All four claims TRUE:
1. First-run import crash — TRUE, reproduced in a fresh venv with both the original and the packaging-fixed wheel (identical `TypeError`), including the insight that the packaging fix alone does not cure it (it reads the *user* config dir) and that `init` is unreachable (circular).
2. YAML missing from wheel — TRUE (19-entry wheel, zero YAMLs; "no files found matching" warnings). Fix verified: `[tool.setuptools.package-data]` + corrected MANIFEST.in → both YAMLs in wheel (21 entries) and sdist.
3. `rich` undeclared — TRUE; arrives transitively via typer (uv.lock/uv tree evidence); declared direct dependency required.
4. License mismatch — TRUE (BSD-3-Clause file vs MIT classifier; classifiers deprecated in setuptools 82+ → SPDX `License-Expression` per PEP 639).
Extra findings beyond the auditor: broken `list_settings` (F8), empty `__init__.py`/no `__version__` (F6), lying async docstring (F7), no app-layer tests (F9), stale `mko_get_mediascope_data` references and `telebot`/`tests` loggers (F10), missing `requirements.txt` reference, `python -m build` absent from dev deps (F11).

**Best practices (Researcher 2 — cited sources).** Key adoptions:
- Public surface: re-export from top-level `__init__.py` with `__all__`; keep `core` internal; `process_data` returns a structured Pydantic result (recommended over returning `None` or a bare `Path`); failures explicit (dedicated `SettingsNotInitializedError`) rather than auto-init surprises.
- Versioning: single-source version in `pyproject.toml` + `importlib.metadata.version` with a guard test; SemVer 0.x convention (MINOR is the breaking axis until 1.0); pipelines pin tags/full commit SHAs, never branches.
- Notebooks: `%pip install` (installs into the kernel interpreter — IPython docs/source); tiny self-contained synthetic sample instead of the 43 MB file; nbformat 4.5+ validity incl. cell ids, validated via `nbformat`.
- CI: `python -m build` → `twine check` → `check-wheel-contents` → unpack-and-assert-YAML → fresh-venv install+import on push/PR; Trusted Publishing (`pypa/gh-action-pypi-publish`, `id-token: write`) on tags only, TestPyPI first.
Sources include packaging.python.org (packaging projects, distributing with setuptools, single-source version, GitHub Actions publishing), build.pypa.io, PEP 440/610/639/20, semver.org, docs.python.org (importlib.metadata), ipython.readthedocs.io, platformdirs docs, nbformat docs/schema, pypi.org/project/check-wheel-contents.

---

## 7. Assumptions

- A1. The internal processing pipeline (CSV → Polars → SQLite → rules → export) is functionally adequate; this work changes packaging, API surface, and packaging-time behavior — not cleaning logic. (Auditor-aligned.)
- A2. "Installable via pip" initially means installable from the public GitHub repo at a pinned tag; PyPI is a later phase (D1).
- A3. The owner is the sole maintainer; a solo-maintainer CI setup is acceptable; no marketplace/enterprise distribution requirements exist today.
- A4. Windows 11 + `uv` is the primary development environment; targets remain cross-platform (`platforms = ["Linux", "Windows"]` already declared; requires-python `>=3.12`).
- A5. The README's UPPERCASE Mediascope CSV header shape is the canonical input format for examples (the tracked 43 MB sample uses Cyrillic headers and is not the documented shape).
- A6. Users of the pipeline are comfortable running `initialize_settings()` once per environment and reviewing the generated YAML (as the README already instructs).
- A7. `data/` sample files are not distributed with the package (current state, F12) and stay that way; the notebook sample lives under `examples/notebooks/`.

## 8. Constraints

- C1. Do not rewrite the project or change the build backend (setuptools + src-layout + `pyproject.toml` stay).
- C2. No new heavy abstractions; prefer the smallest change that removes the first-run/publishing blockers (project rule: avoid overengineering; single responsibility).
- C3. All user-visible additions follow the project's i18n/English-comments/logging/Pydantic-v2 conventions where applicable inside the package.
- C4. Public API must stay minimal: exactly two documented functions (plus `__version__`); CLI remains for terminal use only.
- C5. The example notebook must not embed or reference the 43 MB sample; repository size matters.
- C6. Any behavior change must be covered by tests runnable in the project's existing pytest setup (`testpaths=["tests"]`, `pythonpath=["src"]`).

## 9. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| R1. `LoggingSettings` requires `formatters/handlers/loggers/root`, so merely fixing `yaml_to_dict` is insufficient — partial fixes leave an obscure `ValidationError`. | Medium | T1 validation must guard "settings missing" explicitly and raise `SettingsNotInitializedError`, not leak pydantic errors. |
| R2. Removing the `app_service` singleton changes behavior existing user code depends on (README's `core.app_service` example). | Low–Medium | Pre-1.0 SemVer policy (D6); README migration note; pinning by tag. |
| R3. License wording is a legal/business choice (D4), not purely technical. | Low | Owner decision before first publication; keep it out of the critical path until T7. |
| R4. Row counts in the result object touch the processing code (`_import_data` return; one count at export time). Per D8 the scope is only loaded rows + clean exported rows — no per-rule or non-mapped statistics. | Low | Rows loaded are already accumulated in `_import_data`; clean rows are one count on the data table at export time. |
| R5. Synthetic sample may not exercise every dictionary feature (FTS etc.). | Low | Keep the 43 MB sample untouched for manual runs; notebook demonstrates the core DELETE/REPLACE/ADD path. |
| R6. `python -m build` unavailable locally; environment drift between dev and CI. | Low | Add `build` to dev group; CI canonicalizes the build. |
| R7. First PyPI publish is irreversible-ish (name squatting/renames). | Medium | D1 sequencing: tag + GitHub install first; TestPyPI before prod. |

## 10. Open questions

**None remaining.** All business questions (Q1–Q11) were answered by the Product Owner on 2026-10-14 and are recorded as confirmed decisions in §5. Two interpretations are fixed here to remove residual ambiguity:

- "Clean row counts" (D8) = rows loaded into the database (input) and rows exported to `clean_data/` (output). Counts of unmatched/`null_data` rows are not returned by the API; the pipeline can derive them from the result's `null_data` file list if needed.
- D5's "user settings directory" = the directory `initialize_settings()` writes to (platformdirs user config dir by default, or `settings_dir` when provided).

## 11. Out of scope

- Rewriting or optimizing the cleaning/ETL logic, rule engine, or SQLite usage.
- Adapters for orchestrators (Airflow/Prefect/etc.), Docker images, or a plugin system.
- Switching build backend (Poetry/Hatch), monorepo restructuring, or per-notebook packaging.
- Publishing automation details for later releases beyond "Trusted Publishing on tags" as a recommendation.
- GUI/web surfaces; publishing binaries; multi-language docs (README stays Russian/English as it is today — no i18n task is created for docs).
- The 43 MB sample dataset's future (kept as-is for manual use).

## 12. Definition of ready (for implementation planning)

All definition-of-ready conditions are **met**:
1. Product Owner confirmed D1, D2, D4, D11 (2026-10-14) plus all remaining decisions D3, D5–D10.
2. T1 and T2 are treated as one critical-path pair; both must ship before any release candidate exists.
3. The API contract from §6/Researcher-2 is accepted as the target shape for T4, with the result object scoped per confirmed D8 (paths + timings + clean row counts).
4. The example notebook's install pin (`@v0.1.0`) is accepted, acknowledging the notebook's run cell depends on T4/T7 completion.
5. T6's CI scope (D10) is confirmed: packaging verification ships in this work.

---

## Appendix A — Verified defect inventory (evidence table)

| ID | Defect | Location | Verdict |
|---|---|---|---|
| F1 | Import-time crash in fresh env (`TypeError`, `LoggingSettings(**None)`) | `app.py:12`; `core/app_service.py:248-250,23,26-28`; `core/utils.py:144-147`; `core/models.py:50-57` | TRUE (reproduced) |
| F2 | `init` unreachable (circular dependency with F1) | `app.py:26-34` | TRUE |
| F3 | YAML templates missing from wheel/sdist; stale MANIFEST rule; no package-data | `pyproject.toml`; `MANIFEST.in:3-4` | TRUE (build evidence) |
| F4 | `rich` imported but undeclared | `app.py:10`; `pyproject.toml:15-29` | TRUE |
| F5 | License classifier MIT vs BSD-3-Clause LICENSE.txt; deprecated classifier style | `pyproject.toml:35`; `LICENSE.txt` | TRUE |
| F6 | Empty `__init__.py`; no `__version__` | `src/mko_data_cleaner/__init__.py`; `pyproject.toml:7` | TRUE |
| F7 | `process_data` docstring claims async; returns `None` | `app.py:73-80` | TRUE |
| F8 | `list_settings` references non-existent `app_paths.reports` | `app.py:61`; `core/paths.py` | TRUE |
| F9 | No app-layer/end-to-end tests | `tests/` | TRUE |
| F10 | Stale `mko_get_mediascope_data` refs; stale `telebot`/`tests` loggers; missing `requirements.txt` include | `MANIFEST.in:3-4,7`; `app.py:2`; `.gitignore:224-225`; `settings/log_config.yaml:51-60`; `.kilo/commands/*` | TRUE |
| F11 | No git tags/notebooks; `build` not in dev deps | repo state; `pyproject.toml:166-175` | TRUE |
| F12 | 43 MB tracked sample not distributed; README shape differs from sample shape | `data/example/raw_data/example_data.csv` | TRUE |
