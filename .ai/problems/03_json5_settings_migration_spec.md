# 03 — Migrate settings from YAML/PyYAML to JSON5 — Analytical Specification

- **Status:** ✅ Research complete (Researcher Task #1). ✅ Product Owner decisions confirmed (hard break, no backward compatibility; PyYAML fully removed; `yaml_to_dict` → `config_to_dict`). Ready for implementation.
- **Date:** 2026-10-17
- **Source request:** user request — "switch settings from yaml and pyaml to json5 research what should be done for this to work and which file have to be converted"
- **Evidence base:** repository inspection of `.ai/problems/` (01, 02) for spec format; full codebase scan for all YAML/PyYAML references (code, tests, docs, CI, packaging); Researcher investigation of the `json5` PyPI package API and feature coverage.

---

## 1. Problem statement

`mko_data_cleaner` v0.2.0 is a Python package (Python ≥3.12, setuptools backend, src-layout) that ships two YAML configuration templates — `app_config.yaml` (application settings validated via Pydantic v2 models) and `log_config.yaml` (logging dictConfig) — loaded at runtime through `core/utils.py:yaml_to_dict()` which uses `yaml.safe_load` from the `PyYAML` dependency.

The product owner wants to **switch the settings format from YAML (and the PyYAML dependency) to JSON5**, reducing the dependency footprint and adopting a format that is a strict superset of JSON while retaining human-friendly features (comments, trailing commas, unquoted keys).

The migration is **mechanically straightforward** because both templates already use YAML flow-style syntax (`{ ... }`), which maps 1:1 onto JSON5 object syntax. The only required textual transformations are: comment delimiters (`#` → `//`), Python booleans (`True`/`False` → `true`/`false`), and nulls (`None` → `null`). However, the change ripples across **21 files** spanning code, tests, packaging, CI, documentation, and examples — each must be identified and converted or updated.

This specification identifies every affected file and maps the required code changes.

---

## 2. Scope

**In scope**

- Replacing the `PyYAML` runtime dependency with `json5` (and the `json5` PyPI package).
- Converting the two template files from `.yaml` to `.json5` with the mechanical text transformations.
- Replacing `yaml.safe_load` / `yaml.YAMLError` with `json5.load` / `ValueError` in the config-loading code path.
- Updating all path resolution, settings-gate, CLI listing, packaging manifests, and tests to use `.json5` filenames.
- Updating documentation, CI wheel-content assertions, and the example notebook.

**Out of scope (see §11)**

- Changing the structure or keys of the settings templates (the Pydantic models and `DataSettings` / `LoggingSettings` contracts stay unchanged — only the file format changes).
- Switching to `json` (stdlib) — JSON5 is explicitly required because it supports comments and trailing commas that users need.
- Touching non-settings YAML usage (the project's own `.kilo/` command/agent config, which uses YAML for internal agent orchestration, is unrelated to the runtime settings format).

---

## 3. Confirmed requirements (facts)

| # | Requirement | Evidence |
|---|---|---|
| F1 | Two runtime YAML settings templates exist: `src/mko_data_cleaner/settings/app_config.yaml` and `log_config.yaml`. | Repository listing + file contents read. |
| F2 | Templates are consumed via `core/utils.py:yaml_to_dict()` which calls `yaml.safe_load()` and catches `yaml.YAMLError`. | `core/utils.py:7,131-149`. |
| F3 | `AppService.log_config` and `AppService.app_config` (cached properties) call `utils.yaml_to_dict()` with paths from `AppPaths`. | `core/app_service.py:44,48`. |
| F4 | `AppPaths.app_config` and `AppPaths.log_config` (in `core/paths.py`) hardcode filenames `"app_config.yaml"` and `"log_config.yaml"`. | `core/paths.py:46-51`. |
| F5 | `_require_settings()` in `core/app_service.py` checks for the existence of `app_config.yaml` and `log_config.yaml` and raises `SettingsNotInitializedError` if missing. | `core/app_service.py:309-324`. |
| F6 | `list_settings` CLI command uses `list_files_in_directory` with extensions `("yaml", "yml")`. | `app.py:115-117`. |
| F7 | `list_files_in_directory` in `core/utils.py` defaults to `extensions=("yaml", "json")`. | `core/utils.py:104-106`. |
| F8 | `pyproject.toml` declares `"PyYAML>=6.0.1"` as a runtime dependency (line 27) and `"settings/*.yaml"` in `[tool.setuptools.package-data]` (line 60). | `pyproject.toml:27,60`. |
| F9 | `MANIFEST.in` line 3 includes `recursive-include src/mko_data_cleaner/settings *.yaml`. | `MANIFEST.in:3`. |
| F10 | CI workflow `.github/workflows/ci.yml:48` asserts `app_config.yaml` and `log_config.yaml` are present in the built wheel. | `ci.yml:48`. |
| F11 | Tests directly import `yaml` in `tests/conftest.py:6`, `tests/test_packaging.py:48`, and reference `yaml_to_dict` in `tests/test_app.py:20`. | `tests/conftest.py:6,24`, `test_packaging.py:48`, `test_app.py:20`. |
| F12 | `scripts/smoke_install.py:71-72,83-84` asserts `app_config.yaml` and `log_config.yaml` exist after `initialize_settings()`. | `smoke_install.py:71-72,83-84`. |
| F13 | `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb` imports `yaml` and uses `yaml.safe_load`/`yaml.safe_dump` to edit `app_config.yaml`. | Notebook cells at lines 113, 115, 125. |
| F14 | Both templates are already written in YAML flow-style (`{ ... }`), meaning structural syntax overlaps 1:1 with JSON5. Only comment syntax, booleans, and nulls need conversion. | Template file contents read and analyzed. |
| F15 | The `json5` PyPI package (v0.17.3, pure-Python, zero runtime deps, Apache-2.0) provides `load(fp)`, `loads(s)`, `dump(obj, fp)`, `dumps(obj)` mirroring stdlib `json` API, and accepts file objects. It raises plain `ValueError` (not `yaml.YAMLError`) on malformed input. | Researcher Task #1. |

---

## 4. Conceptual development tasks

### T1 — Convert template files and update the config loader
- **Purpose:** Rename the two settings templates from `.yaml` to `.json5`, apply mechanical conversions (`#` → `//`, `True`/`False` → `true`/`false`, `None` → `null`), and replace `yaml.safe_load` with `json5.load` in `core/utils.py`.
- **Expected outcome:** `yaml_to_dict()` becomes `config_to_dict()` (or stays as a renamed alias); the loader uses `json5.load(fp)` and catches `ValueError` instead of `yaml.YAMLError`; both templates parse cleanly under JSON5. `list_files_in_directory` default extensions updated to `("json5", "csv")` (was `("yaml", "json")`).
- **Dependencies:** none — hard break to JSON5 only (no backward compatibility, per PO decision).
- **Acceptance:** Both `.json5` files parse with `json5.load`; `config_to_dict()` returns the same dict structure as the old `yaml_to_dict` did; tests for missing-file and parse-error cases pass with `ValueError`.

### T2 — Update path resolution and settings gate
- **Purpose:** Change `AppPaths.app_config` and `AppPaths.log_config` in `core/paths.py` to resolve `.json5` filenames; update `_require_settings()` in `core/app_service.py` to check for `.json5` files; update error messages.
- **Expected outcome:** Settings paths point to `app_config.json5` / `log_config.json5`; `_require_settings` checks those files; `SettingsNotInitializedError` message names `.json5` files.
- **Dependencies:** T1.
- **Acceptance:** `get_app_service()` raises `SettingsNotInitializedError` naming `app_config.json5` and `log_config.json5` when they are absent; `AppPaths.app_config` / `AppPaths.log_config` return `.json5` paths.

### T3 — Update CLI `list_settings` and `list_files_in_directory`
- **Purpose:** Change the `list_settings` CLI command to enumerate `.json5` files instead of `.yaml`/`.yml`; ensure `list_files_in_directory` default extensions reflect the new format.
- **Expected outcome:** `mko-data-cleaner list-settings` lists `.json5` files in the user settings directory.
- **Dependencies:** T2.
- **Acceptance:** `test_list_settings_lists_when_dir_exists` passes with `.json5` filenames in output.

### T4 — Update packaging manifests and dependencies
- **Purpose:** Replace `"PyYAML>=6.0.1"` with `"json5>=0.17.3"` in `pyproject.toml` `[project].dependencies`; update `[tool.setuptools.package-data]` from `"settings/*.yaml"` to `"settings/*.json5"`; update `MANIFEST.in` from `*.yaml` to `*.json5`.
- **Expected outcome:** Built wheel and sdist contain `app_config.json5` and `log_config.json5` instead of `.yaml` files; `PyYAML` removed from runtime deps.
- **Dependencies:** T1 (file conversion done first).
- **Acceptance:** `uv build` → unpack wheel → assert `mko_data_cleaner/settings/{app_config.json5,log_config.json5}` present; no `settings/*.yaml` in wheel.

### T5 — Update tests
- **Purpose:** Update all test files that reference `.yaml` settings files or import `yaml` directly.
- **Expected outcome:** `tests/test_app.py`, `tests/test_packaging.py`, `tests/conftest.py` use `.json5` filenames and `json5` instead of `yaml`.
- **Dependencies:** T1–T4.
- **Acceptance:** Full test suite passes (`uv run pytest`).

### T6 — Update CI and smoke-test assertions
- **Purpose:** Update `.github/workflows/ci.yml` wheel-content assertion and `scripts/smoke_install.py` to check for `.json5` files.
- **Expected outcome:** CI asserts `app_config.json5` and `log_config.json5` in the wheel; smoke test checks `.json5` existence.
- **Dependencies:** T4.
- **Acceptance:** CI workflow file contains `.json5` assertions; smoke test passes against a built wheel.

### T7 — Update documentation and examples
- **Purpose:** Update `README.md`, `docs/00-overview/*.md`, and `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb` to reference `.json5` instead of `.yaml`; update the notebook to use `json5.load`/`json5.dumps` instead of `yaml.safe_load`/`yaml.safe_dump`.
- **Expected outcome:** All user-facing docs reference JSON5 settings; notebook example runs end-to-end with `.json5`.
- **Dependencies:** T5.
- **Acceptance:** No `.yaml` references in user-facing docs; notebook cell that edits config uses `json5`.

### T8 — Remove PyYAML from all dependencies

- **Purpose:** Remove `PyYAML` entirely from both `[project].dependencies` and `[dependency-groups] dev` in `pyproject.toml`, since no backward-compatibility fallback is needed and tests no longer import `yaml`.
- **Expected outcome:** `uv lock` produces no `PyYAML` entry; `import yaml` is no longer used in any source or test file.
- **Dependencies:** T5 (tests updated to not use `yaml`).
- **Acceptance:** `uv tree` shows no `PyYAML` in the production dependency graph; no `import yaml` in `src/` or `tests/`.

---

## 5. Product Owner decisions (confirmed 2026-10-17)

The Product Owner confirmed that **no backward compatibility is required**. This resolves all migration-strategy questions:

- **D1 — Backward compatibility: hard break.** The project switches exclusively to `.json5` files. There is no dual-format fallback, no auto-conversion, and no migration utility command. The project is pre-1.0 (v0.2.0), so breaking existing user `.yaml` configs is acceptable.
- **D2 — PyYAML fully removed.** `PyYAML` is removed from both `[project].dependencies` and `[dependency-groups] dev` in `pyproject.toml`. No code path or test retains `import yaml`.
- **D3 — `yaml_to_dict` renamed to `config_to_dict`.** The loader function is renamed since it no longer loads YAML. No backwards-compatible alias is kept.
- **D4 — No migration assistance.** Users who have previously run `init` must re-run `mko-data-cleaner init --force` to generate `.json5` files. Documentation will state this clearly.

---

## 6. Research summary

Conducted by Researcher Task #1 (completed 2026-10-17). Full report available in the task result; key findings:

- **`json5` package (v0.17.3):** Pure-Python, zero runtime dependencies, Apache-2.0, Python 3.8+. Provides `load(fp)`, `loads(s)`, `dump(obj, fp)`, `dumps(obj)` — a near-drop-in replacement for `json`/`yaml`. Accepts file objects (not just paths). Raises plain `ValueError` (not `json.JSONDecodeError`, not `json5.JSON5DecodeError`) on malformed input. This is the **critical non-drop-in aspect**: the `except yaml.YAMLError` clause in `yaml_to_dict()` must become `except ValueError`.

- **Feature coverage:** JSON5 supports line comments (`//`), block comments (`/* */`), trailing commas (objects + arrays), unquoted identifier keys, single/double-quoted strings, `Infinity`, `NaN`, hex numbers. JSON5 does **not** support Python-style booleans (`True`/`False`) or `None` — these **must** be converted to `true`/`false`/`null`. JSON5 does **not** support backtick template literals (ECMAScript 5.1 base) — multi-line strings use `\` line continuation. For these config files, this is not a limitation (no multi-line strings).

- **Migration difficulty:** **Trivial** — both templates are already in YAML flow-style (`{ ... }`), so structural syntax maps 1:1. Only mechanical changes needed: `#` → `//` (comments), `True`/`False` → `true`/`false` (booleans). No `None`/null values exist in the current templates, but user-extended configs may have them.

- **`json5.dumps` for readability:** Supports `indent=2`, `trailing_commas=True`, `quote_keys=False` — produces clean, human-readable output. However, `json5.dumps` does **not** preserve comments, so conversion must be manual/text-based, not a programmatic round-trip.

- **Package naming alert:** Two PyPI packages provide `import json5` — `json5` (pure-Python, v0.17.3) and `pyjson5` (Cython-based, v2.0.1). They conflict. The project must declare `json5` explicitly in `pyproject.toml`.

- **No stdlib alternative:** Python's stdlib `json` does not support comments, trailing commas, or unquoted keys. A third-party library is required.

---

## 7. Assumptions

- A1. The Pydantic models (`DataSettings`, `LoggingSettings`, etc. in `core/models.py`) remain unchanged — only the file format and loader mechanism change. The parsed dict structure is identical.
- A2. The project is pre-1.0 (v0.2.0), so a hard format break (no backward compatibility) is acceptable. Existing user `.yaml` configs will require re-initialization via `init --force` (D1, D4).
- A3. Users who have not initialized settings will get `.json5` templates going forward (via `init` / `initialize_settings`).
- A4. The `json5` library will remain unmaintained-free (zero deps, pure Python, small surface) for the foreseeable lifetime of this transition.
- A5. Documentation updates are part of the Definition of Done for this change (i18n/English-only project rule applies to all docstrings and error messages).
- A6. The `.kilo/` directory's own YAML files (agent configs, command definitions) are internal tooling and **not** part of this migration — they are not runtime settings.

---

## 8. Constraints

- C1. Do not change the structure or keys of `app_config` or `log_config` — only the file format changes. The Pydantic model contracts (`DataSettings`, `LoggingSettings`) must still validate the parsed data.
- C2. The `json5` dependency must be explicit in `pyproject.toml` `[project].dependencies` — no transitive resolution.
- C3. All user-visible strings (error messages, docstrings) must follow the project's English-only rule.
- C4. Package templates must ship inside the wheel/sdist (same requirement as spec F3/R2 in `.ai/problems/01_…`).
- C5. Changes must be covered by the existing pytest setup (`testpaths=["tests"]`, `pythonpath=["src"]`).
- C6. The example notebook must execute end-to-end after the change.
- C7. `PyYAML` must be fully removed from all dependency groups (D2) — no dev-only retention.

---

## 9. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| R1. `json5` raises `ValueError` (not `yaml.YAMLError`) — the `except` clause in `yaml_to_dict()` must be updated. | High — parse errors would crash instead of returning `{}` | T1: update exception class; add test for invalid JSON5 returning `{}`. |
| R2. `json5` and `pyjson5` PyPI package name conflict — wrong package could be installed. | Medium — subtle import errors | C2 + explicit `json5>=0.17.3` in `pyproject.toml`; add test asserting `json5.__name__` imports correctly. |
| R3. `json5.dumps()` does not preserve comments — a programmatic round-trip would strip user annotations. | Low — conversion is manual text replacement, not a programmatic round-trip | Templates are converted by hand; T8 ensures `PyYAML` is fully removed so no code accidentally uses `dumps`. |
| R4. Tests that directly `import yaml` (conftest.py, test_packaging.py) need updating AND `PyYAML` must be removed from dev deps. | Medium — test breakage if not caught | T5 + T8: replace all `import yaml` with `import json5`; remove PyYAML from dev deps; `uv lock` and `uv tree` must show no PyYAML. |
| R5. CI wheel-content assertion hardcodes `.yaml` filenames — must be updated or CI fails on first build. | Medium — CI red on build job | T6: update CI assertion to check for `.json5`. |
| R6. The notebook example uses `yaml.safe_load`/`yaml.safe_dump` to edit config — the cell must be rewritten for JSON5 and the notebook must re-install `json5`. | Low — notebook won't run with old code | T7: rewrite notebook cell using `json5`; notebook install cell must include `json5` (now a declared dependency, so automatic). |
| R7. Users with existing `.yaml` configs get `SettingsNotInitializedError` on upgrade (hard break, no migration). | High — users blocked until they re-run `init --force` | D1 + T7: README and docs must clearly state the breaking change and the `init --force` remediation. |

---

## 10. Precise file inventory

### Files to convert (rename + content transform)

| Current file | New file | Conversion required |
|---|---|---|
| `src/mko_data_cleaner/settings/app_config.yaml` | `src/mko_data_cleaner/settings/app_config.json5` | `#` → `//`; `True`/`False` → `true`/`false`; flow-style braces need no change |
| `src/mko_data_cleaner/settings/log_config.yaml` | `src/mko_data_cleaner/settings/log_config.json5` | `#` → `//`; `False` → `false`; flow-style braces need no change |

### Code files to modify

| File | Lines | Change |
|---|---|---|
| `src/mko_data_cleaner/core/utils.py` | 7 | `import yaml` → `import json5` |
| `src/mko_data_cleaner/core/utils.py` | 104–106 | `list_files_in_directory` default `extensions=("yaml", "json")` → `("json5", "csv")` |
| `src/mko_data_cleaner/core/utils.py` | 131–149 | `yaml_to_dict()` → `config_to_dict()` (rename + `yaml.safe_load` → `json5.load`; `except yaml.YAMLError` → `except ValueError`) |
| `src/mko_data_cleaner/core/paths.py` | 46–51 | `AppPaths.app_config` / `log_config` return `.json5` filenames |
| `src/mko_data_cleaner/core/app_service.py` | 44, 48 | `utils.yaml_to_dict(...)` → `utils.config_to_dict(...)` |
| `src/mko_data_cleaner/core/app_service.py` | 316–319 | `_require_settings` checks for `.json5` filenames |
| `src/mko_data_cleaner/core/errors.py` | 21 | `SettingsNotInitializedError` docstring: `app_config.yaml`/`log_config.yaml` → `.json5` |
| `src/mko_data_cleaner/app.py` | 116 | `list_settings` extensions: `("yaml", "yml")` → `("json5",)` |
| `src/mko_data_cleaner/app.py` | 134–135 | Docstring: `app_config.yaml`/`log_config.yaml` → `.json5` |

### Packaging files to modify

| File | Lines | Change |
|---|---|---|
| `pyproject.toml` | 27 | `"PyYAML>=6.0.1"` → `"json5>=0.17.3"` (runtime, full replacement per D2) |
| `pyproject.toml` | 60 | `"settings/*.yaml"` → `"settings/*.json5"` (package-data) |
| `MANIFEST.in` | 3 | `*.yaml` → `*.json5` |

### Test files to modify

| File | Lines | Change |
|---|---|---|
| `tests/test_app.py` | 20 | `import yaml_to_dict` → `config_to_dict` |
| `tests/test_app.py` | 36, 212–213, 242–251, 323–324, 332–337, 348–349, 365–366, 378 | All `app_config.yaml`/`log_config.yaml` references → `.json5` |
| `tests/test_app.py` | 242–251 | `test_yaml_to_dict_*` → `test_config_to_dict_*`; exception expectations updated |
| `tests/test_packaging.py` | 25–58 | `test_template_yamls_*` → `test_template_json5_*`; `yaml.safe_load` → `json5.load`; `.yaml` → `.json5` |
| `tests/test_packaging.py` | 66–71 | `test_manifest_in_includes_yaml_templates` → check `*.json5` |
| `tests/test_packaging.py` | 94–99 | `test_pyproject_declares_package_data` → check `settings/*.json5` |
| `tests/conftest.py` | 6, 14, 23–24 | `import yaml` → `import json5`; `app_config.yaml` → `.json5`; `yaml.safe_load(f)` → `json5.load(f)` |

### CI and scripts to modify

| File | Lines | Change |
|---|---|---|
| `.github/workflows/ci.yml` | 48 | Wheel-content assertion: `app_config.yaml`/`log_config.yaml` → `.json5` |
| `scripts/smoke_install.py` | 71–72, 83–84, 128 | `app_config.yaml`/`log_config.yaml` references → `.json5` |

### Example notebook to modify

| File | Lines | Change |
|---|---|---|
| `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb` | 113, 115, 125, 86–87 | `import yaml` → `import json5`; `yaml.safe_load` → `json5.load`; `yaml.safe_dump` → `json5.dumps`; `app_config.yaml` → `.json5` |

### Documentation files to modify

| File | References |
|---|---|
| `README.md` | Lines 50–51, 56, 69–70, 119, 161, 194 |
| `docs/00-overview/architecture.md` | Lines 33, 56, 92, 94, 183, 186–188 |
| `docs/00-overview/current-behavior.md` | Lines 33, 39, 46–47, 56–57, 66–67, 286–287, 298, 314 |
| `docs/00-overview/data-flow.md` | Line 31 |
| `docs/00-overview/product-overview.md` | Lines 80, 85 |
| `docs/00-overview/open-questions.md` | Lines 57, 66, 73, 110 (and D-8 explained §3) |
| `docs/00-overview/open-questions.md` | Lines 110–114 (stale loggers in log_config.yaml) |
| `examples/README.md` | Lines 41, 126 |

---

## 11. Out of scope

- Changing the semantic structure of `app_config` or `log_config` (keys, nesting, Pydantic model contracts).
- Switching to stdlib `json` (does not support comments/trailing commas — JSON5 is required).
- Migrating internal `.kilo/` YAML files (agent configs, command definitions) — these are not runtime settings.
- Converting `.ai/plans/` and `.ai/problems/` historical documents (they are archived analysis artifacts).
- Adding a general-purpose config file format library selection — `json5` is the decided format.
- Providing backward compatibility or migration utilities (D1: hard break, no shim).

---

## 12. Definition of ready (for implementation planning)

**All conditions are met:**

1. **PO decisions locked:** D1 (hard break, no backward compat), D2 (PyYAML fully removed), D3 (`yaml_to_dict` → `config_to_dict` rename, no alias), D4 (users re-init via `init --force`).
2. **Research:** ✅ Complete (Researcher Task #1 — `json5` package verified, API and feature map confirmed, migration difficulty assessed).
3. **File inventory:** ✅ Complete (§10 lists every affected file with exact line references).
4. **Acceptance criteria:** ✅ Defined per-task in §4.
5. **Test strategy:** Tests already exist for the settings gate, init, and config loading — they need updating, not creation (T5).
6. **`PyYAML` removal verified:** No `import yaml` remains in any `src/` or `tests/` file after T5+T8 complete.
