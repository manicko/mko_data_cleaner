---
phase: "02"
phase_name: "config"
id_prefix: "CFG"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/02-audit-config.md#severity-taxonomy"
code_ref: "c0fd0e1"
validated_at: "2026-10-19"
validated_by: "validator"
---

# Validation Report — Configuration Management (Phase 02)

## Summary

7 findings from `.ai/audit/02-config/findings.md` validated against commit
`c0fd0e1` (HEAD). All 7 **confirmed** — no re-grades, no re-types, no
rejections. Highest severity **HIGH** (CFG-001). CRITICAL band remains empty
(the settings gate does raise). All runtime evidence reproduced; all static
claims re-derived via `grep`/`git ls-files`/`python -m`/`pytest`.

## Dispositions

| ID | Severity | Anchor | Verdict | Type |
|---|---|---|---|---|
| CFG-001 | HIGH | `core/utils.py::config_to_dict` | confirmed | SPEC-DEVIATION |
| CFG-002 | MEDIUM | `pyproject.toml::[project].dependencies` | confirmed | BEST-PRACTICE |
| CFG-003 | MEDIUM | `core/utils.py::merge_dicts` | confirmed | BEST-PRACTICE |
| CFG-004 | LOW | `app.py::__main__` | confirmed | BEST-PRACTICE |
| CFG-005 | LOW | `core/init_service.py::_copy_templates` | confirmed | BEST-PRACTICE |
| CFG-006 | LOW | `core/app_service.py::__main__` | confirmed | BEST-PRACTICE |
| CFG-007 | LOW | `docs/00-overview/current-behavior.md` | confirmed | DOC-UPDATE |

**Tally:** confirmed 7 · re-graded 0 · re-typed 0 · merged 0 · not substantiated 0 · unsettled 0.

---

### CFG-001 — Confirmed [HIGH] / SPEC-DEVIATION

**Re-derivation (ran):** Executed `uv run python .ai/tmp/repro_consolidated.py`.
Output reproduced verbatim:

```text
--- malformed app_config.json5 + valid log_config.json5 ---
CONSTRUCTED OK (gate passed; app_config is lazily validated): AppService
--- both settings files missing ---
SettingsNotInitializedError (gate works): Missing file(s): app_config.json5, log_config.json5.
--- process_data with malformed app_config.json5 ---
USER-FACING ERROR TYPE: pydantic_core._pydantic_core.ValidationError
is ConfigError? False
first line: 6 validation errors for DataSettings
```

**Why this band (rubric check):** Phase 02 HIGH = "Malformed-but-present config
passes the gate and surfaces as an opaque downstream error." CFG-001 matches
exactly; it correctly declines MEDIUM (not mere unconsumed config). The CRITICAL
band is empty because the gate does raise on *absence*
(`_require_settings`, app_service.py:316-319 — verified in git).

**Anchor resolution (gate 2):** `utils.py::config_to_dict` exists (utils.py:131);
disambiguator `return {}` present at utils.py:146. `app_service.py::AppService.app_config`
exists (app_service.py:46-52); disambiguator `DataValidationError` present at
app_service.py:51 and defined at errors.py:12.

**Side that moves:** Code (load+validate chain + gate) plus a doc update at
`current-behavior.md:286` (which falsely claims a `ConfigError` wrap). Code is
load-bearing.

**Substantiation checklist:**
- `config_to_dict` returns `{}` on `ValueError` (utils.py:144-146) — masks
  parse errors as a blank dict. Confirmed by `test_config_to_dict_returns_empty_dict_on_invalid_json5` (test_app.py:250) and `..._on_missing_file` (test_app.py:245), both PASSED.
- `_require_settings` checks only `.exists()` (app_service.py:316-319) — gate is
  existence-only, not validation.
- `app_config` is a `@cached_property` (app_service.py:46), accessed only inside
  `run_report` via `import_path` (app_service.py:68) etc. — never at construction.
- `except DataValidationError as e` (app_service.py:51) is dead: `grep -n "raise DataValidationError" src tests` → 0 matches; `issubclass(DataValidationError, ValidationError)` → False (verified at runtime).
- `log_config` *is* eagerly evaluated via `prepare_log_paths()` in `__init__`
  (app_service.py:39 → line 44), so a malformed `log_config.json5` surfaces at
  construction — but a malformed `app_config.json5` does not.

**Remediation blockers:** The two tests at test_app.py:245 and :250 assert the
masking behaviour (return `{}` on parse error / missing file). Per the template
rule, the finding is retained; these tests must be rewritten alongside the fix.

### CFG-002 — Confirmed [MEDIUM] / BEST-PRACTICE

**Re-derivation (static):** `grep -rn "pydantic_settings\|urllib3\|dateutil" src tests`
→ 0 matches. Confirmed at runtime: `import mko_data_cleaner.app` leaves neither
`pydantic_settings` nor `urllib3` in `sys.modules`. All three remain declared in
`pyproject.toml` (lines 25-26, 32). Severity matches phase 02 MEDIUM =
"declared dependencies never imported."

**Anchor resolution:** `pyproject.toml::[project].dependencies` exists;
disambiguator tokens `pydantic-settings`, `python-dateutil`, `urllib3` present
in the `[project.dependencies]` block (verified at read).

**Side that moves:** `pyproject.toml` (remove the 3 unused entries).

**Carry-out safety:** Removing them must keep `uv run pytest` (93 passed) and
`uv run mko-data-cleaner --help` green — no source imports them.

### CFG-003 — Confirmed [MEDIUM] / BEST-PRACTICE

**Re-derivation (static):** `grep -n "merge_dicts" src tests` → only
`utils.py:149` (definition) and `utils.py:167` (recursive self-call). Zero
external call sites. The phase 02 specification corpus (block 3: "no overlay
merge") explicitly requires configs be loaded directly without overlay merging,
so `merge_dicts` is not a missing integration — it is dead code from a prior
design.

**Side that moves:** `utils.py` (delete `merge_dicts`).

### CFG-004 — Confirmed [LOW] / BEST-PRACTICE

**Re-derivation (ran):** `git ls-files src/mko_data_cleaner/__main__.py` → empty.
`Test-Path src/mko_data_cleaner/__main__.py` → False. `uv run python -m mko_data_cleaner`
→ "No module named `mko_data_cleaner.__main__`; 'mko_data_cleaner' is a package
and cannot be directly executed." The submodule form `python -m mko_data_cleaner.app`
works. Severity matches phase 02 LOW = "missing `__main__.py` so the bare package
entry fails."

**Side that moves:** Code — add `src/mko_data_cleaner/__main__.py` containing
`app()` (mirroring app.py:153-154).

### CFG-005 — Confirmed [LOW] / BEST-PRACTICE

**Re-derivation (static):** `_copy_templates` (init_service.py:26-27) calls
`target_path.parent.mkdir(...)` then `shutil.copy2(item, target_path)` directly
— no temp-file + `os.replace` pattern. On `--force` this is non-atomic: a
crash mid-copy leaves a truncated file at the target. Severity matches phase 02
LOW = "non-atomic template copy."

**Side that moves:** `init_service.py::_copy_templates` (atomicize via
`tempfile` + `os.replace`).

### CFG-006 — Confirmed [LOW] / BEST-PRACTICE

**Re-derivation (static):** `__main__` guards found at:
- `app_service.py:343-344` → `get_app_service().run_report(r"data\mvideo")`
- `init_service.py:56-57` → `init_project(force=True)`
- `app.py:153-154` → `app()` (the legitimate Typer entry; not a dev-only block)

`grep -n "data\\\\mvideo" src` → app_service.py:344 only. The first two are
stale dev scaffolding hard-coded into production modules. Severity matches phase 02
LOW = "Stale dev-only execution blocks in production modules."

### CFG-007 — Confirmed [LOW] / DOC-UPDATE

**Re-derivation (static + tested):** Code is correct; docs are stale on three
config-relevant claims.

| Doc claim (stale) | Verified reality | Evidence |
|---|---|---|
| `current-behavior.md:315-317` — log_config.json5 "still contains unused tests and telebot logger entries" | Only `__main__` logger; no `tests`/`telebot` | `grep "tests\|telebot" settings/log_config.json5` → 0 matches |
| `current-behavior.md:35` / `open-questions.md D-6` — "initialize_settings ignores settings_dir" | `initialize_settings(settings_dir=tmp)` writes to `tmp/settings/` | app.py:148-150 forwards it; test_app.py:322 PASSED |
| `current-behavior.md:272` — "process_data currently returns None" | Returns `ReportResult` | app.py:145 returns `run_report`; test_app.py:389 PASSED |
| `current-behavior.md:286` — "Pydantic validation error wrapped in ConfigError" | Raw `ValidationError`, not `ConfigError` | CFG-001 repro: `is ConfigError? False` |
| `open-questions.md D-8.6` — "tests/telebot loggers at lines 51-60" | Absent; loggers section ends at line 51 | `grep` → 0 matches |

**Side that moves:** Docs only (`current-behavior.md`, `open-questions.md`).
Code is load-bearing and correct.

**Out of phase scope (correctly deferred by the input):** Packaging-metadata
staleness in D-8 (`rich` "not declared" — it IS declared at pyproject.toml:28;
"No `__version__`" — it IS `0.2.0`; "empty `__init__.py`" — it re-exports) and
the `basedpyright` 39 type-narrowing diagnostics. These belong to the packaging
(12) and typing phases; verified they exist at c0fd0e1 but not filed here.

---

## Cross-Finding Analysis

### Cross-phase: Phase 01 ↔ Phase 02

| Phase-01 finding | Phase-02 finding | Relationship |
|---|---|---|
| ENT-001 (HIGH) `AppService.app_config` dead `except DataValidationError` | CFG-001 (HIGH) | **Cross-reference.** Same sub-concern (dead handler at app_service.py:49-52); both conclude the ConfigError wrap is dead. Distinct scope: ENT-001 root-cause is "wrong exception type in the except," fix = catch `(DataValidationError, ValidationError)`. CFG-001 root-cause adds three more (existence-only gate, `config_to_dict` masking, lazy `app_config`), fix = 3-part (raise not mask / eager-validate / wrap `ValidationError`). Same conclusion → **not a conflict**. Not merged — different phase ownership and fix surface. |
| ENT-004 (LOW) doc-drift (entry/API surface) | CFG-007 (LOW) doc-drift (config-relevant) | **Cross-reference.** Both flag the `tests`/`telebot` logger drift, `settings_dir` forwarding, and `process_data` return as stale docs. ENT-004 is the broad entry-surface sweep (includes `__init__.py`, `__version__`, `rich`, project-name, test-count). CFG-007 is the config-scoped subset + the `ConfigError` wrap claim; correctly defers packaging items to phase 12. Same conclusion → **not a conflict**. |

No conflicting evidence. No phase-02 finding duplicates another phase-02 finding
(CFG-IDs are disjoint in anchor/symbol).

### Merge candidates

None within phase 02. CFG-001 subsumes part of ENT-001 but not vice-versa; the
shared dead-handler sub-concern is a cross-reference to keep both live.

### Dependency & ordering

Independent targets: `utils.py::config_to_dict` (CFG-001), `pyproject.toml` (CFG-002),
`utils.py::merge_dicts` (CFG-003), `__main__.py` new file (CFG-004),
`init_service.py::_copy_templates` (CFG-005), `app_service.py`/`init_service.py`
`__main__` blocks (CFG-006), docs (CFG-007). CFG-001 is the only finding with
remediation-blocker tests (test_app.py:245, :250).

---

## Rollout Safety

| Finding | What the fix breaks | Test that must stay green |
|---|---|---|
| CFG-001 | Changes `config_to_dict` return semantics; changes exception type on malformed config | test_app.py:245, :250 must be rewritten with the fix; all 93 tests stay green |
| CFG-002 | Removes 3 deps from `pyproject.toml` | `uv run pytest` (93), `uv run mko-data-cleaner --help` |
| CFG-004 | Adds package entry point | `python -m mko_data_cleaner --help`, no import-time side effects (test_app.py:81) |
| CFG-005 | Swaps `shutil.copy2` for temp+`os.replace` | `test_init_*` (test_app.py:203, 322, 343, 355) — `--force` overwrite must still work |
| CFG-006 | Deletes `__main__` blocks in `app_service.py`/`init_service.py` | `uv run pytest`, `uv run mko-data-cleaner --help` (app.py:153 `__main__` is the legit entry) |

All fix targets resolve in `git ls-files` at c0fd0e1.

---

## VAL- Findings

None discovered. Baseline gates confirmed independently:
- `uv run ruff check src/ tests/` → All checks passed!
- `uv run pytest` → 93 passed
- `uv run basedpyright src/` → 39 errors, 0 warnings (matching the input's claim;
  these are type-narrowing diagnostics in processing/SQLite engine modules — not
  config-scope, correctly excluded).

---

## Appendices

### Appendix A — Reproduction output (CFG-001)

```text
--- malformed app_config.json5 + valid log_config.json5 ---
CONSTRUCTED OK (gate passed; app_config is lazily validated): AppService
--- both settings files missing ---
SettingsNotInitializedError (gate works): Missing file(s): app_config.json5, log_config.json5.
--- process_data with malformed app_config.json5 ---

----------  Обработка стартовала: 2026-10-19 15:15:27 ----------

19-10-26 03:15:27 PM - ERROR - <string>:1 Unexpected "k" at column 1 - mko_data_cleaner.core.utils
USER-FACING ERROR TYPE: pydantic_core._pydantic_core.ValidationError
is ConfigError? False
first line: 6 validation errors for DataSettings
```

*(supports: "gate passes; app_config is lazily validated; raw ValidationError, not ConfigError")*

### Appendix B — Static evidence (CFG-002 / CFG-003)

```text
grep -rn "pydantic_settings\|urllib3\|dateutil" src tests
(no matches — 0 hits)

grep -n "merge_dicts" src tests
src/mko_data_cleaner/core/utils.py:149: def merge_dicts(...)
src/mko_data_cleaner/core/utils.py:167:     dict1[k] = merge_dicts(dict1[k], dict2[k])
```

*(supports: unused deps have zero imports; merge_dicts has zero external call sites)*

### Appendix C — Runtime evidence (CFG-004)

```text
$ git ls-files src/mko_data_cleaner/__main__.py
(empty)

$ uv run python -m mko_data_cleaner
No module named 'mko_data_cleaner.__main__'; 'mko_data_cleaner' is a package
and cannot be directly executed

$ uv run python -m mko_data_cleaner.app --help
Usage: python -m mko_data_cleaner.app [OPTIONS] COMMAND [ARGS]...
  | init  ...
  | run   ...
```

*(supports: bare package entry fails; submodule form works)*

### Appendix D — Cross-phase overlap evidence

```text
grep -n "raise DataValidationError" src tests
(no matches — 0 hits across all code and tests)

python -c "from mko_data_cleaner.core.errors import DataValidationError; \
from pydantic import ValidationError; print(issubclass(DataValidationError, ValidationError))"
False
```

*(supports: ENT-001 and CFG-001 both conclude the `except DataValidationError`
handler is dead — same verdict, not a conflict)*
