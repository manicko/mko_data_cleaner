# Execution Plan 05 — Migrate Settings from YAML/PyYAML to JSON5

- **Status:** Planning
- **Created:** 2026-10-17 by Tech Lead (multi-agent workflow)
- **Source problem note:** `.ai/problems/03_json5_settings_migration_spec.md` (research-complete, PO decisions D1–D4 confirmed)
- **Code context (Auditor):** `.ai/tmp/code_context.md` — `{code_context}` from Auditor agent (full report, 2026-10-17)
- **Baseline HEAD:** `8e68923` (`chore(docs) clean up`)
- **Test gate:** `uv run pytest` → 85 passed in 6.82s (baseline verified green; standalone `uv` project, no Docker/DB)

---

## TL;DR

Migrate the runtime settings format from YAML (PyYAML) to JSON5 across ~25 files. The spec
(`03_json5_settings_migration_spec.md`) is **research-complete** and **accurate** (auditor verified F1–F15 and R1–R7 all match the code), **but its §10 file inventory is incomplete** — it misses 5 references that would break tests/docs if not migrated. This Execution plan corrects those gaps and decomposes implementation into 3 validated commits + final validation.

PO decisions are locked (D1 hard break, D2 PyYAML removed, D3 `yaml_to_dict`→`config_to_dict`, D4 users re-init). No new research uncertainty.

---

## 1. Auditor verdict (Step 0)

**Accuracy: HIGH** — all 15 facts (F1–F15) and 7 risks (R1–R7) match the code exactly; spec §10 line numbers are accurate.

**Completeness: MODERATE** — §10 misses 5 references. These are folded into the task records below:

| # | File | Line(s) | Content | Risk if unfixed |
|---|---|---|---|---|
| M1 | `tests/test_app.py` | 160 | `assert "app_config.yaml" in message` | Test FAILS (error msg gains `.json5`) |
| M2 | `tests/test_app.py` | 186-187 | `assert "app_config.yaml"/"log_config.yaml" in result.output` | Test FAILS (`list-settings` lists `.json5`) |
| M3 | `docs/00-overview/current-behavior.md` | 130 | `app_config.yaml` in prose | Stale doc |
| M4 | `docs/00-overview/current-behavior.md` | 270 | `log_config.yaml` in prose | Stale doc |

**Structural corrections incorporated:**
- **D2 corrected:** PyYAML is ONLY in `[project].dependencies` (pyproject.toml:27), NOT in `[dependency-groups] dev`. T8 removes it from runtime deps only.
- **T8 acceptance corrected:** acceptance is "no PyYAML as *direct* dependency; no `import yaml` in `src/` or `tests/`" — NOT "uv.lock has no PyYAML" (PyYAML remains as a transitive dev dep of `bandit` + `nbformat`).
- **T1 default extensions:** use `("json5", "json")` (1:1 migration of `("yaml", "json")`), NOT the spec's proposed `("json5", "csv")` — the `"csv"` addition is unjustified and no caller uses the default. Also fix the stale docstring at `utils.py:114` (`('csv', 'txt')` → `('json5', 'json')`) and the inaccurate `| None` return type at `utils.py:131`.
- **T7 acceptance scoped:** "no settings-related `.yaml` references in user-facing docs" — do NOT touch ```` ```yaml ```` code fences or YAML frontmatter in `doc-maintenance-rules.md` (those describe documentation format, not settings).

**Additional minor references folded into tasks** (auditor §11): `app_service.py:310` docstring, `test_app.py:23`/`:240` comments, `test_packaging.py:95`/`:98`, `ci.yml:48` "YAML templates present", `smoke_install.py:7`/`:128`, `current-behavior.md:287` (`yaml_to_dict` prose), `product-overview.md:90/127`, `examples/README.md:129`, notebook lines 121/189/275 ("YAML null").

**Confirmed unrelated:** `polars>=2.0.0,<3.0.0` (pyproject.toml:22) is the already-applied polars-v2 migration (spec 02) — unrelated to this task. `json5` is NOT installed anywhere; `init_service.py` uses `rglob("*")` so it needs no code change (templates auto-copied as `.json5`).

---

## 2. Global constraints

- **C1.** Standalone `uv` project — all commands use `uv run`/`uv`. No Docker/DB for tests.
- **C2.** Each commit must keep `uv run pytest` green and `uv run ruff check` clean.
- **C3.** Commit only the files of the current block, conventional-commit style. No `git reset`/`checkout`, no history rewrite.
- **C4.** Do NOT modify `.ai/audit/` files; do NOT modify existing plan/problem files.
- **C5.** Hard break — no backward-compat shim, no `yaml_to_dict` alias (D1, D3).
- **C6.** English-only strings (docstrings, errors, comments).

---

## 3. Block decomposition (dependency order)

| Block | Summary | Depends on | Risk | Agents |
|---|---|---|---|---|
| **B1** | Core migration: templates→`.json5`, loader `config_to_dict`+ValueError, paths, settings-gate, CLI, packaging, tests, PyYAML removal, example notebook | — | High | Implementor |
| **B2** | CI wheel-assertion + smoke-test gate → `.json5` | B1 (templates/packaging committed) | Medium | Implementor |
| **B3** | Markdown documentation → JSON5 (README, `docs/00-overview/*`, `examples/README.md`) | B1 | Low | Doc-specialist |
| **VAL** | Final validation of completed implementation | B1, B2, B3 | High | Validator |

### Skipped per-block agents (justification)
- **Auditor (per-block):** full read-only audit already completed at Step 0 (this file, §1). No per-block code investigation required.
- **Researcher (per-block):** `json5` package API/feature-coverage already researched in spec §6 (load/loads/dump/dumps, raises `ValueError`). No new technical alternatives to evaluate.
- **Planner (Step 1):** the spec is already decomposed into T1–T8 with file inventories, exact line refs, acceptance criteria, and confirmed PO decisions. This plan re-groups T1–T8 into reviewable, CI-green commits and corrects the gaps the Auditor found. No separate Planner run needed.

---

## 4. Owner decisions (Step 2 — resolved)

All business decisions are already locked in the spec (§5 D1–D4). No user questions are needed (the migration is mechanical; no backward-compat or format-choice uncertainty remains). This plan adopts D1–D4 verbatim.

| Decision | Value | Source |
|---|---|---|
| D1 Backward compatibility | Hard break, no shim | Spec §5 |
| D2 PyYAML removal | From runtime deps only (not in dev deps) | Spec §5 + Auditor §3.1 correction |
| D3 Function rename | `yaml_to_dict` → `config_to_dict` (no alias) | Spec §5 |
| D4 User remediation | Re-init via `init --force`; documented | Spec §5 |

---

## 5. Per-block task records

> Format follows `.ai/tasks/templates/task_template.yaml` — semantic code units only (no line numbers). Line refs are from the Auditor report (`.ai/tmp/code_context.md`) for traceability only.

---

### B1 — Core migration: templates + loader + paths + gate + CLI + packaging + tests + PyYAML removal + notebook
*(Spec T1, T2, T3, T4, T5, T8 and the example notebook from T7-code. Notebook bundled here because CI executes it and it must stay consistent with the code change.)*

**Agents:** Implementor → (self-validation: pytest + ruff + mypy + build + wheel + nbclient)

#### {task_description}

```yaml
id: task_05_b1_core_migration_json5

title: "B1 — Migrate settings from YAML/PyYAML to JSON5 (core + tests + notebook)"

priority: high

depends_on: []

source_reference: .ai\problems\03_json5_settings_migration_spec.md

source_section: "T1–T5, T7(notebook), T8"

description: >
  Hard-break migration of runtime settings from YAML (PyYAML) to JSON5.
  Rename shipped templates app_config.yaml / log_config.yaml to .json5 with
  mechanical text conversions (# → //, True/False → true/false, no None in
  templates). Replace the yaml.safe_load loader with json5.load and rename
  yaml_to_dict → config_to_dict (catch ValueError instead of yaml.YAMLError).
  Update AppPaths, _require_settings settings-gate, list_settings CLI, packaging
  (pyproject.toml PyYAML→json5 + package-data; MANIFEST.in), all tests, uv.lock,
  and the example notebook. Remove PyYAML from [project].dependencies. NO alias
  kept. Notebook updated to json5.load/json5.dumps; validated by nbclient.

goals:
  - templates renamed app_config.json5 / log_config.json5 (yaml versions deleted)
  - core/utils.py: import json5; config_to_dict uses json5.load + except ValueError
  - list_files_in_directory default → ("json5", "json"); stale docstring fixed
  - core/paths.py: AppPaths.app_config / log_config → .json5
  - core/app_service.py: config_to_dict(...) calls; _require_settings checks .json5;
    docstring updated
  - core/errors.py: SettingsNotInitializedError docstring → .json5
  - app.py: list_settings extensions ("json5",); docstrings → .json5
  - pyproject.toml: PyYAML>=6.0.1 → json5>=0.17.3 (runtime); package-data settings/*.json5
  - MANIFEST.in: *.yaml → *.json5
  - uv.lock: re-locked (json5 added, PyYAML no longer direct dep)
  - tests: conftest.py / test_app.py / test_packaging.py → json5, config_to_dict,
    all .yaml→.json5 (including test_app.py:160,186-187 missed by spec)
  - notebook: import json5; json5.load/json5.dumps; app_config.yaml→.json5; "YAML null"→"JSON5"
  - no `import yaml` anywhere in src/ or tests/
  - uv run pytest green; ruff clean; mypy clean; wheel contains .json5 only;
    notebook executes via nbclient

files:
  - path: src/mko_data_cleaner/settings/app_config.yaml
    action: rename-to
    new: src/mko_data_cleaner/settings/app_config.json5
    changes:
      - action: convert
        description: >
          # → // comments; True/False → true/false; flow-style braces unchanged
  - path: src/mko_data_cleaner/settings/log_config.yaml
    action: rename-to
    new: src/mko_data_cleaner/settings/log_config.json5
    changes:
      - action: convert
        description: >
          # → // comments; False → false; flow-style braces unchanged
  - path: src/mko_data_cleaner/core/utils.py
    targets:
      - type: function
        name: yaml_to_dict
    changes:
      - action: rename
        description: yaml_to_dict → config_to_dict (no alias)
      - action: modify
        description: import yaml → import json5
      - action: modify
        description: yaml.safe_load(cfg) → json5.load(cfg)
      - action: modify
        description: except yaml.YAMLError → except ValueError
      - action: modify
        description: "fix return type dict[str, Any] | None → dict[str, Any]"
      - action: modify
        description: "list_files_in_directory default ('yaml','json') → ('json5','json'); fix stale docstring ('csv','txt')"
  - path: src/mko_data_cleaner/core/paths.py
    targets:
      - type: property
        name: AppPaths.app_config
      - type: property
        name: AppPaths.log_config
    changes:
      - action: modify
        description: "app_config.yaml → app_config.json5"
      - action: modify
        description: "log_config.yaml → log_config.json5"
  - path: src/mko_data_cleaner/core/app_service.py
    targets:
      - type: property
        name: AppService.log_config
      - type: property
        name: AppService.app_config
      - type: function
        name: _require_settings
    changes:
      - action: modify
        description: "utils.yaml_to_dict(...) → utils.config_to_dict(...) (lines 44, 48)"
      - action: modify
        description: "_require_settings checks 'app_config.json5' / 'log_config.json5'; message names .json5"
      - action: modify
        description: "docstring (app_service.py:310) → .json5"
  - path: src/mko_data_cleaner/core/errors.py
    targets:
      - type: class
        name: SettingsNotInitializedError
    changes:
      - action: modify
        description: "docstring app_config.yaml/log_config.yaml → .json5"
  - path: src/mko_data_cleaner/app.py
    targets:
      - type: function
        name: list_settings
    changes:
      - action: modify
        description: "list_files_in_directory extensions ('yaml','yml') → ('json5',)"
      - action: modify
        description: "docstring (app.py:134-135) → .json5"
  - path: pyproject.toml
    targets:
      - type: dependency
        name: PyYAML
      - type: package-data
        name: settings
    changes:
      - action: modify
        description: '"PyYAML>=6.0.1" → "json5>=0.17.3"'
      - action: modify
        description: '"settings/*.yaml" → "settings/*.json5"'
  - path: MANIFEST.in
    changes:
      - action: modify
        description: "recursive-include src/mko_data_cleaner/settings *.yaml → *.json5"
  - path: uv.lock
    changes:
      - action: modify
        description: "uv lock — json5 added, PyYAML no direct dep"
  - path: tests/conftest.py
    changes:
      - action: modify
        description: "import yaml → import json5; app_config.yaml → app_config.json5; yaml.safe_load(f) → json5.load(f); docstring"
  - path: tests/test_app.py
    changes:
      - action: modify
        description: "import yaml_to_dict → config_to_dict; rename test_yaml_to_dict_* → test_config_to_dict_*; all .yaml→.json5 refs incl. lines 160,186-187,212-213; comments 23,240"
  - path: tests/test_packaging.py
    changes:
      - action: modify
        description: "import yaml → import json5; yaml.safe_load → json5.load; .yaml→.json5; test_template_yamls_* → test_template_json5_*; *.yaml→*.json5; MANIFEST/pyproject assertions; rename var yaml_patterns → json5_patterns"
  - path: examples/notebooks/mko_data_cleaner_pipeline_example.ipynb
    changes:
      - action: modify
        description: "import yaml → import json5; yaml.safe_load → json5.load; yaml.safe_dump → json5.dumps; app_config.yaml → app_config.json5; 'YAML null' → 'JSON5' (lines 121,189,275)"

changes:
  - action: add_code
    description: Convert templates to JSON5 flow-style, update loader, paths, gate, CLI, packaging, tests, remove PyYAML, update notebook.

acceptance_criteria:
  - app_config.json5 + log_config.json5 ship in package data (rglob copies them via init)
  - config_to_dict(file) returns same dict structure as old yaml_to_dict; invalid JSON5 returns {}; missing file returns {}
  - AppPaths.app_config / log_config resolve .json5 paths
  - _require_settings raises SettingsNotInitializedError naming .json5 files
  - list-settings lists .json5 files
  - pytest: 85 tests pass (0 failures)
  - ruff check src/ tests/ scripts/: clean
  - mypy src/mko_data_cleaner: no new errors (ValueError catch accepted)
  - uv build succeeds; wheel contains mko_data_cleaner/settings/{app_config.json5,log_config.json5}; no .yaml in wheel
  - nbclient executes notebook without error
  - no `import yaml` in src/ or tests/ ; no `yaml_to_dict` anywhere
  - uv.lock: PyYAML not a direct dependency of mko_data_cleaner

validation_steps:
  - uv run pytest -q
  - uv run ruff check src/ tests/ scripts/
  - uv run mypy src/mko_data_cleaner
  - uv run python -m build
  - uv run python -c "import zipfile,wsgiref; ..."  # wheel content: .json5 present, no .yaml
  - uv run python -c "import nbformat; from nbclient import NotebookClient; ..."  # notebook executes
  - uv run python -c "import json5; print(json5.__name__)"
commit_message: "refactor(core): migrate settings from YAML/PyYAML to JSON5"
```

---

### B2 — CI wheel-assertion + smoke-test gate → JSON5
*(Spec T6. CI `build` job and `install-smoke` job must assert `.json5` templates; smoke_install.py must check `.json5`.)*

**Agents:** Implementor → (self-validation: build + fresh-venv smoke)

#### {task_description}

```yaml
id: task_05_b2_ci_smoke_json5

title: "B2 — Update CI wheel assertion and smoke test for JSON5"

priority: high

depends_on:
  - task_05_b1_core_migration_json5

source_reference: .ai\problems\03_json5_settings_migration_spec.md

source_section: "T6"

description: >
  Update the CI build-job wheel-content assertion (ci.yml:48) to check for
  app_config.json5 / log_config.json5 instead of .yaml, and rename the print
  string "YAML templates present" → "JSON5 templates present". Update
  scripts/smoke_install.py: lines 71-72, 83-84, and docstring line 7 /
  comment line 128 to reference .json5 instead of .yaml.

goals:
  - ci.yml:48 asserts app_config.json5 + log_config.json5 in wheel; print string "JSON5 templates present"
  - smoke_install.py: asserts "app_config.json5" in error message; checks .json5 files exist after init; docstring/comment updated

files:
  - path: .github/workflows/ci.yml
    targets:
      - type: step
        name: "Assert wheel contents"
    changes:
      - action: modify
        description: ".yaml assertions → .json5; 'YAML templates present' → 'JSON5 templates present'"
  - path: scripts/smoke_install.py
    changes:
      - action: modify
        description: "lines 71-72: 'app_config.yaml' → 'app_config.json5' in error-message assertion"
      - action: modify
        description: "lines 83-84: app_config.yaml / log_config.yaml → .json5 in existence checks"
      - action: modify
        description: "line 7 docstring + line 128 comment: YAML → JSON5"

acceptance_criteria:
  - ci.yml build step asserts app_config.json5 + log_config.json5 present in wheel, no .yaml
  - smoke_install.py references .json5 everywhere (no .yaml)
  - fresh-venv smoke run: all 6 [PASS] (build wheel → fresh venv pip install → python scripts/smoke_install.py)

validation_steps:
  - uv run python -m build
  - uv run python -c "import zipfile; ..."  # wheel: .json5 present, no .yaml
  - # Primary: true fresh-venv gate (CI-equivalent):
  - uv venv /tmp/mko-smoke-venv --python 3.14 ; uv pip install --python /tmp/mko-smoke-venv/bin/python dist/*.whl ; /tmp/mko-smoke-venv/bin/python scripts/smoke_install.py
  - # Fallback (if uv pip install is permission-gated): run smoke against the dev venv:
  - uv run python scripts/smoke_install.py

commit_message: "ci: update wheel assertion and smoke test for JSON5"
```

---

### B3 — Markdown documentation → JSON5
*(Spec T7-docs. Pure markdown docs only — notebook is in B1.)*

**Agents:** Doc-specialist → (self-validation: grep for stale references)

#### {task_description}

```yaml
id: task_05_b3_docs_json5

title: "B3 — Update documentation and examples to reference JSON5 settings"

priority: medium

depends_on:
  - task_05_b1_core_migration_json5

source_reference: .ai\problems\03_json5_settings_migration_spec.md

source_section: "T7 (docs only)"

description: >
  Update user-facing markdown documentation to reference .json5 instead of .yaml.
  Per docs/00-overview/doc-maintenance-rules.md, this is an API/format change
  (settings format) — an update trigger. Scope EXCLUDES ```yaml code fences and
  YAML frontmatter in doc-maintenance-rules.md itself (those describe doc format,
  not settings).

goals:
  - README.md: .yaml→.json5 (lines 50-51,56,69-70,119,161,194); any "YAML" prose → "JSON5"
  - architecture.md: .yaml→.json5 (33,56,92,94,183,186-188); "YAML" prose → "JSON5"
  - current-behavior.md: .yaml→.json5 (incl. missed lines 130,270); yaml_to_dict→config_to_dict (287); "YAML" prose
  - data-flow.md: .yaml→.json5 (31)
  - product-overview.md: .yaml→.json5 (80,85); "YAML templates" prose (90,127) → "JSON5"
  - open-questions.md: .yaml→.json5 (57,66,73,110); D-8 stale-loggers note
  - examples/README.md: .yaml→.json5 (41,126); "YAML null" (129) → "JSON5 null"
  - Doc-maintenance-rules.md: do NOT touch ```yaml fences / "YAML frontmatter" prose (format docs, not settings)

files:
  - path: README.md
  - path: docs/00-overview/architecture.md
  - path: docs/00-overview/current-behavior.md
  - path: docs/00-overview/data-flow.md
  - path: docs/00-overview/product-overview.md
  - path: docs/00-overview/open-questions.md
  - path: examples/README.md

acceptance_criteria:
  - no settings-related `.yaml` filename references remain in these docs
  - `yaml_to_dict` renamed references → `config_to_dict`
  - uppercase "YAML" prose referring to settings format → "JSON5" (keep "YAML" only where it describes documentation frontmatter format)
  - doc-maintenance-rules.md frontmatter/code-fence "YAML" untouched (out of scope)
  - frontmatter valid on all modified docs (id/domain/tags/related present)

validation_steps:
  - grep -rn "app_config.yaml\|log_config.yaml\|yaml_to_dict\|settings/*.yaml" README.md docs/ examples/
  - grep -rn "YAML" README.md docs/00-overview/*.md examples/README.md  # review each; keep only doc-format usages

commit_message: "docs: update settings documentation to reference JSON5"
```

---

### VAL — Final validation
*(Workflow Step 5)*

**Agents:** Validator

```yaml
id: task_05_val_final

title: "VAL — Final validation of JSON5 settings migration"

priority: high

depends_on:
  - task_05_b1_core_migration_json5
  - task_05_b2_ci_smoke_json5
  - task_05_b3_docs_json5

description: >
  Independent validation that the migration is complete and consistent.

acceptance_criteria:
  - Plan completeness: all spec T1-T8 addressed; auditor's 5 missed references fixed
  - Cross-block integration: code, tests, packaging, CI, smoke, docs, notebook all consistent with .json5
  - Architectural consistency: config_to_dict is the single loader; no yaml in src/tests; json5 is direct dep
  - Regression: 85 tests pass; fresh-venv smoke 6/6 PASS; notebook executes
  - Tests and quality gates: pytest green; ruff clean
  - Documentation: settings docs reference .json5; doc-maintenance-rules followed
  - Unrelated changes: none
```

---

## 6. Implementation sequence

1. **B1 (Implementor)** → commit `refactor(core): migrate settings from YAML/PyYAML to JSON5`
2. **B2 (Implementor)** → commit `ci: update wheel assertion and smoke test for JSON5`
3. **B3 (Doc-specialist)** → commit `docs: update settings documentation to reference JSON5`
4. **VAL (Validator)** — independent review; if issues, one Implementor fix-commit.

**Concurrency:** max 2 subagents; only 1 Implementor at a time; no background launches. B3 (Doc-specialist) may run after B2 completes (docs depend only on B1 code being committed, but for safety/consistency docs are updated last).

---

## 7. Risk register (from spec §9 + auditor)

| Risk | Level | Mitigation | Owner |
|---|---|---|---|
| R1 `json5` raises `ValueError` not `yaml.YAMLError` | High | B1: `except (FileNotFoundError, ValueError)`; test for invalid input returns `{}` | Implementor |
| R2 `json5`/`pyjson5` package-name conflict | Medium | C5: explicit `json5>=0.17.3` direct dep | Implementor |
| R4 Tests import `yaml`; PyYAML removal | Medium | B1 T5+T8: replace all `import yaml`→`import json5`; remove PyYAML from runtime deps; `uv lock` | Implementor |
| R5 CI wheel assertion hardcodes `.yaml` | Medium | B2: ci.yml asserts `.json5`; smoke checks `.json5` | Implementor |
| R6 Notebook uses `yaml.safe_load`/`safe_dump` | Low | B1: notebook → `json5`; nbclient execution validation | Implementor |
| R7 Users with `.yaml` configs blocked | High | B3 docs state hard break + `init --force` remediation | Doc-specialist |
| M1-4 (auditor missed refs) | High | Folded into B1/B3 task records | Implementor / Doc-specialist |
| Spec T8 "uv lock no PyYAML" impossible | Medium | Acceptance corrected: no PyYAML as *direct* dep; transitive via bandit/nbformat (dev) acceptable | Implementor |

---

## 8. Definition of done (DoD)

- [ ] B1: `uv run pytest` → 85 passed; `ruff check` clean; `mypy` clean; wheel has `.json5` only; notebook executes via nbclient; no `import yaml` in `src/`/`tests/`; `config_to_dict` is the single loader.
- [ ] B2: fresh-venv `smoke_install.py` → all 6 `[PASS]`.
- [ ] B3: settings docs reference `.json5`; doc-maintenance-rules.md frontmatter untouched.
- [ ] VAL: Validator sign-off (plan completeness, integration, no regressions, no unrelated changes).

(End of file)
