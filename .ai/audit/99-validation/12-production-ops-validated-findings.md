---
phase: "12"
phase_name: "production-ops"
id_prefix: "OPS"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/12-audit-production-ops.md#severity-taxonomy"
code_ref: "c0fd0e1"
validated_at: "2026-10-19"
validated_by: "validator"
---

# Audit Findings Validation — Production Operations

## Summary

Six findings from `.ai/audit/12-production-ops/findings.md` were validated against
commit `c0fd0e1` (HEAD). All six are **confirmed** — every anchor resolves, every repro
reproduces, and every root-cause claim holds against the executing path. Highest severity
is **HIGH** (OPS-003, missing `__main__.py`). Three illustrative items (OPS-001, OPS-004,
OPS-008) were already resolved in the tree and correctly not filed. Three validation-level
defects were discovered **in the audit itself**: the findings' Fix Order declares "Merge
candidates: none share a root cause" yet three same-root-cause cross-phase overlaps exist but
were not declared; the findings front-matter deviates from the report template; and one
stale locator (`pyproject.toml:27` for `rich`, actual: `:28`) survives inside a finding.

## Disposition Tally

| Disposition | Count | Items |
|---|---|---|
| confirmed | 6 | OPS-002, OPS-003, OPS-005, OPS-006, OPS-007, OPS-009 |
| re-typed / re-graded / merged / not substantiated / unsettled | 0 | — |

## Per-Finding Validation

### OPS-002 — [MEDIUM] scope cites non-existent phase files 03 and 09
**Dispositions:** confirmed · **type:** DOC-UPDATE · **verified:** ran

**Block 1:** Phase file exists at `.kilo/commands/audit/phases/12-audit-production-ops.md`;
the text "Deferred by this phase" resolves at line 30. `Get-ChildItem .kilo/commands/audit/phases/`
yields 01,02,04,05,10,11,12,13,16,17,99 — no 03, no 09. The claim holds.

**Block 2:** Support is `ls` of the phases directory — no green control relied upon.

**Block 3 — grade:** Phase MEDIUM = "documentation/coverage drift." A scope that defers to
deleted phase files is coverage drift. **Grade holds.**

**Block 4 — moves:** Documentation (update the scope block in the phase file or restore
redirects for 03/09). Code is unaffected.

**Block 5:** The recommendation (remove references to 03/09 or restore/redirect) names a
real target (the phase file scope block). Carriable.

**Cross-phase (Block 6):** OPS-002 is owned by phase 12 itself (it is a self-referential
scope defect). No sibling claims the same concern.

### OPS-003 — [HIGH] package not runnable as `python -m mko_data_cleaner`
**Dispositions:** confirmed · **type:** SPEC-DEVIATION · **verified:** ran

**Block 1:** `git ls-files 'src/mko_data_cleaner/__main__.py'` → empty.
`uv run python -m mko_data_cleaner --help` → "No module named mko_data_cleaner.__main__;
'mko_data_cleaner' is a package and cannot be directly executed."
`uv run python -m mko_data_cleaner.app --help` → Typer usage (works). All three repro steps
reproduce verbatim.

**Block 2:** Support is `git ls-files` + runtime execution. No green control is the sole basis.

**Block 3 — grade:** Phase HIGH = "a documented claim about the system that is false,
misdirecting a downstream auditor into a non-existent execution path." The Python packaging
convention (PEP 366) implies `python -m <package>` works for an installed package; the
package violates this with no `__main__.py`. **Grade holds.** (The phase file's own Block 4
also flags this at line 94.)

**Block 4 — moves:** Code — add `src/mko_data_cleaner/__main__.py` calling `app()`
(mirroring `app.py:153`).

**Block 5:** Target is stable; the fix is a new 2-line file; no test pins the absence of
`__main__.py`.

**Cross-phase (Block 6):** **Overlaps CFG-004** (phase 02, [LOW], same root cause:
missing `__main__.py`). Phase 02 validated report confirms CFG-004 as confirmed [LOW].
The two are same-root-cause duplicates; phase 12 asserts HIGH (convention violation),
phase 02 asserts LOW (missing entry point). No opposite conclusions — not a conflict,
but the findings' Fix Order failed to declare this overlap.

### OPS-005 — [MEDIUM] run_report bare print() bypasses dictConfig and crashes on legacy consoles
**Dispositions:** confirmed · **type:** SPEC-DEVIATION (rules 1 + 11: English only, no
print) · **verified:** ran + static

**Block 1:** `rg -n 'print\('` in run-path files:
```text
core/app_service.py:113  (start banner, Cyrillic)
core/app_service.py:259  (synchronizing notice)
core/app_service.py:274  (end banner, Cyrillic)
core/utils.py:34         (progress_bar, box-drawing chars)
core/utils.py:41         (progress_bar newline)
core/db_service.py:347   (get_table_sample — OFF run path, debug helper)
core/db_service.py:348   (get_table_sample — OFF run path, debug helper)
```
`rg -n "progress_bar"` call sites: `app_service.py:219`, `csv_service.py:195` — both on
the run path. `get_table_sample` has zero call sites in `run_report`; it is off-path.
Stale locators in the phase file correctly identified by the findings:
`db_service.py:308-309` (actually SQL in `tbl_exists`), `app_service.py:257,272`
(actual: 259, 274), `log_config.yaml` (actual: `log_config.json5`).

**Block 2:** `test_run_cli_success` (test_app.py) asserts presence of the rich `console.print`
summary, not the bare `print()` banners. No green control pins the defect. Support is grep
+ repro output + the `smoke_install.py` workaround (which force-reconfigures stdout to
UTF-8 at lines 38-40, admitting the crash exists).

**Block 3 — grade:** Phase MEDIUM = "operability degraded with a workaround." The crash on
legacy Windows code pages is a real degradation; the smoke test workaround is external to
the product. **Grade holds.**

**Block 4 — moves:** CODE (route `print()` → `logger`; normalize stdout to UTF-8 at CLI
entry). The smoke-test workaround at `scripts/smoke_install.py:38-40` is the product
admitting it needs this fix.

**Block 5:** Targets are stable: `run_report` (app_service.py:110), `progress_bar`
(utils.py:18), `app.py` CLI entry. No test pins the bare `print()` output.

**Cross-phase (Block 6):** **Overlaps ENT-003** (phase 01, [MEDIUM],
"`print()` + Russian text in core orchestrator; progress via `\r`"). Same root cause:
`print()` in `run_report` + `progress_bar` bypassing dictConfig. Phase 01 focuses on the
architectural rule violation (English-only, no print in core); phase 12 adds the Windows
encoding crash. The findings' Fix Order failed to declare this overlap.

### OPS-006 — [MEDIUM] malformed log_config.json5 surfaces as an opaque ValidationError
**Dispositions:** confirmed · **type:** SPEC-DEVIATION · **verified:** ran + static

**Block 1:** `config_to_dict` (utils.py:131-146): catches `(FileNotFoundError, ValueError)`
and returns `{}` (line 146). `LoggingSettings` (models.py:52-58) has required fields
`formatters`, `handlers`, `loggers`, `root` — all required (no defaults). With a malformed
`log_config.json5`, `config_to_dict` returns `{}` → `LoggingSettings(**{})` raises
`pydantic_core.ValidationError` for 4 missing fields. `dictConfig` is applied at
`app_service.py:339` (inside `get_app_service()`, after construction) — too late to route
the failure. Verified: `LoggingSettings(**{})` raises ValidationError with "4 validation
errors for LoggingSettings."

**Block 2:** `test_config_to_dict_returns_empty_dict_on_invalid_json5` (test_app.py:250)
asserts the `{}` masking behavior — it pins the defect as current behavior, not as a
green control that refutes the finding. Support is repro + code reading.

**Block 3 — grade:** Phase MEDIUM = "operability degraded; the safe path is 'don't
misconfigure the shipped template'." A single typo in a user-edited shipped file produces
an opaque error. **Grade holds.**

**Block 4 — moves:** CODE (`utils.py::config_to_dict` — re-raise/wrap parse error with
file path; or make `LoggingSettings` tolerate empty and log a fallback before dictConfig`).

**Block 5:** Recommendation names real targets (`config_to_dict`, `LoggingSettings`).
However, the narrower fix is insufficient in isolation for the same reason as CFG-001:
`config_to_dict` masking affects both `app_config.json5` and `log_config.json5`.

**Cross-phase (Block 6):** **Overlaps CFG-001** (phase 02, [HIGH],
"Malformed config passes the gate and surfaces as an opaque ValidationError"). Both assert
`config_to_dict` returns `{}` on parse error. CFG-001 is the broader treatment
(also covers lazy `app_config` validation and the dead `ConfigError` wrap); OPS-006 is the
specific `log_config.json5` → `LoggingSettings` → `dictConfig`-too-late path. The findings'
Fix Order failed to declare this overlap.

### OPS-009 — [MEDIUM] typecheck never gates a release
**Dispositions:** confirmed · **type:** SPEC-DEVIATION · **verified:** ran

**Block 1:** `ci.yml:27-29`:
```text
- name: Type check (non-blocking)
  continue-on-error: true
  run: uv run mypy src/mko_data_cleaner
```
`git grep -n "basedpyright" .github/workflows/ci.yml` → no match (0 results).
`uv run mypy src/mko_data_cleaner` → 45 errors. `uv run basedpyright src/` → 39 errors.

**Block 2:** The "green control" (CI passing) is not the sole support — the finding is
substantiated by the literal `continue-on-error: true` and the absence of a basedpyright
step. The 93 tests pass and ruff is green, but neither gates types.

**Block 3 — grade:** Phase MEDIUM = "a control configured but not enforced on the shipping
path." mypy is configured (`pyproject.toml:138-150`) but non-blocking; basedpyright
(configured per `pyproject.toml:152-154`, dev dep at `:185`) is absent from CI. **Grade
holds.** (The phase file's text "mypy is absent" is stale — mypy is present but non-blocking;
the findings correctly corrected this.)

**Block 4 — moves:** CI config (`.github/workflows/ci.yml`) + `pyproject.toml` — remove
`continue-on-error`, add a basedpyright step, or consolidate to one checker.

**Block 5:** Recommendation names stable targets (ci.yml, pyproject.toml). The fix is
process-level: a CI step that fails on type errors.

**Cross-phase (Block 6):** The findings' Fix Order notes OPS-009 overlaps phase 10
(code-quality) at the type-checking-control boundary. Phase 12 owns "CI gates that decide
whether a change ships" (its stated scope); phase 10 owns type-checking discipline. This
overlap IS declared in the findings' Fix Order. Correct.

### OPS-007 — [LOW] shipped docs describe the pre-rework state
**Dispositions:** confirmed · **type:** DOC-UPDATE · **verified:** ran + tested

**Block 1:** All seven doc-drift claims verified against code at c0fd0e1:

| Doc claim (stale) | Verified reality | Evidence |
|---|---|---|
| open-questions.md D-8 #3: "No `__version__`" | `__version__ = "0.2.0"`; pyproject version 0.2.0 | `__init__.py:11-15`; `pyproject.toml:7`; `test_top_level_exports` PASSED |
| open-questions.md D-8 #4: "`__init__.py` is empty" | Re-exports `process_data`, `initialize_settings`, etc. | `__init__.py:5-8,17-24`; `test_top_level_exports` PASSED |
| open-questions.md D-8 #5: stale `mko_get_mediascope_data` in `app.py:2` / `.gitignore:224-225` | 0 matches in src/ and .gitignore | `git grep -n "mko_get_mediascope_data" -- src/ .gitignore` → no matches |
| open-questions.md D-8 #6: stale `tests`/`telebot` loggers at `log_config.json5:51-60` | Only `__main__` logger | `log_config.json5:44-51`; grep `tests\|telebot` → 0 matches |
| product-overview.md status: "Not done: top-level re-exports, `rich` declared, `__version__`" | All done | `__init__.py:17-24`; `pyproject.toml:28`; `__init__.py:11-15` |
| architecture.md:165-167: "`__init__.py` is currently empty" | Populated; re-exports + `__version__` | `__init__.py` ✓ |
| architecture.md:178-179: "`rich` imported but not declared" | Declared `rich>=13.0.0` at `pyproject.toml:28` | `pyproject.toml:28`; `app.py:10` ✓ |
| current-behavior.md:35: "`settings_dir` not forwarded to `init_project`" | Forwarded | `app.py:150` → `init_project(force=force, settings_dir=settings_dir)` ✓ |

**Note on locator:** OPS-007(c) cites `pyproject.toml:27` for the `rich` declaration;
the actual line is `pyproject.toml:28` (`"rich>=13.0.0"`). See VAL-003.

**Block 2:** Support is `git grep` + code reading + test evidence (`test_top_level_exports`,
`test_initialize_settings_threads_settings_dir`, `test_import_app_does_not_create_settings_dir`
all PASSED). No green control is the sole basis.

**Block 3 — grade:** Phase LOW = "documentation / hygiene drift, no runtime consequence
today." All claims are doc-only drift. **Grade holds.**

**Block 4 — moves:** Documentation only (`open-questions.md`, `product-overview.md`,
`architecture.md`, `current-behavior.md`). Code is correct and load-bearing.

**Block 5:** All doc targets exist and are stable. Fix is text-only.

**Cross-phase (Block 6):** The findings' Fix Order correctly notes OPS-007's unused-dep
residue (`pydantic-settings`, `python-dateutil`, `urllib3`) is owned by phase 02 (CFG-002,
validated) — do not duplicate. This overlap IS declared.

---

## Cross-Finding Analysis

### Cross-phase overlaps omitted from the findings' Fix Order

| Phase-12 finding | Sibling finding | Relationship | Phase owns |
|---|---|---|---|
| OPS-003 [HIGH] missing `__main__.py` | CFG-004 [LOW] same | **Same root cause** — missing package `__main__.py` | Phase 02 (config/entry) filed first at [LOW]; phase 12 re-asserted at [HIGH] (convention violation) |
| OPS-005 [MEDIUM] print() in run path | ENT-003 [MEDIUM] print() + Russian in core | **Same root cause** — `print()` in `run_report` + `progress_bar` bypassing dictConfig; OPS-005 adds the Windows encoding crash | Phase 01 (entry architecture) owns the rule violation; phase 12 owns the operability angle |
| OPS-006 [MEDIUM] log_config masking | CFG-001 [HIGH] app_config masking | **Same root cause** — `config_to_dict` returns `{}` on parse error; CFG-001 is the broader treatment | Phase 02 (config) owns the comprehensive fix at [HIGH]; phase 12 is a specific LOG_CONFIG instance |

**Verdict:** No opposite conclusions (no conflicts). Three same-root-cause overlaps were
not declared in the findings' Fix Order, and the section states "Merge candidates: none share
a root cause" — which is false. See VAL-001.

### Merge candidates

OPS-003 and CFG-004 share one root cause (missing `__main__.py`). The fixes are identical
(add `src/mko_data_cleaner/__main__.py`). The severity differs because phase 12 grades
it as a convention violation (HIGH) while phase 02 grades it as a missing entry point (LOW).
This should be a cross-reference (or merge), not two independent findings.

---

## Rollout Safety

| Finding | What the fix touches | Test that must stay green | Risk |
|---|---|---|---|
| OPS-002 | Phase file scope text (docs only) | n/a | None — docs only |
| OPS-003 | New file `src/mko_data_cleaner/__main__.py` | `test_import_app_does_not_create_settings_dir` (test_app.py:81), all 93 tests | Low — additive; must not create settings dir at import |
| OPS-005 | `run_report` print→logger; `progress_bar`; stdout normalization | `test_run_cli_success`, `test_process_data_end_to_end` (test_app.py) | Medium — touches the run path; verify no output assertions break |
| OPS-006 | `config_to_dict` (utils.py); `LoggingSettings` (models.py) | `test_config_to_dict_returns_empty_dict_on_invalid_json5` (test_app.py:250) — must be rewritten with the fix | Medium — changes error semantics; remediation-blocker test exists |
| OPS-007 | Docs only | n/a | None — docs only |
| OPS-009 | CI workflow + pyproject.toml | `uv run ruff check` (green), `uv run pytest` (93 passed) | Low — CI config only; may initially fail until type errors are addressed |

All fix targets resolve in `git ls-files` at `c0fd0e1`.

---

## Val-Level Findings (Audit Defects)

| ID | Severity | Subject | Verdict |
|---|---|---|---|
| VAL-001 | MEDIUM | Findings' Fix Order declares "Merge candidates: none share a root cause" but three same-root-cause cross-phase overlaps exist and were not declared: OPS-003↔CFG-004, OPS-005↔ENT-003, OPS-006↔CFG-001 | Recorded as cross-phase analysis above; the findings' Fix Order must be corrected |
| VAL-002 | LOW | Findings front-matter deviates from the report template: `phase` is `12-audit-production-ops` (template expects `"12"`); `phase_name` omitted; `severity_taxonomy` embedded as YAML object instead of a string pointer; Findings index table (ID/Severity/Anchor/Title) omitted after `## Findings` | Template compliance drift; does not affect finding validity |
| VAL-003 | LOW | OPS-007(c) cites `pyproject.toml:27` for the `rich` declaration, but rich is at `pyproject.toml:28` (confirmed: `git grep -n "rich" pyproject.toml` → line 28). The phase file (12-audit-production-ops.md:167) has the same off-by-one | Stale locator in the findings file; the substance (rich IS declared) is correct |

---

## Appendices

### Appendix A — Anchor resolution gate

```text
$ git ls-files 'src/mko_data_cleaner/__main__.py'
(empty — file absent, confirms OPS-003)

$ git grep -n "log_config.yaml" -- src/ docs/ .kilo/
(no matches — the actual file is log_config.json5; phase file locators are stale)

$ git grep -n "yaml_to_dict" -- src/
(no matches — function is config_to_dict; phase file locator is stale)

$ git grep -n "basedpyright" .github/workflows/ci.yml
(no matches — basedpyright absent from CI, confirms OPS-009)

$ git grep -n "mko_get_mediascope_data" -- src/ .gitignore
(no matches — stale name removed from shipped files, confirms OPS-007(c))

$ git grep -n "rich" -- pyproject.toml
28:    "rich>=13.0.0",
(confirms rich at line 28, not 27 — see VAL-003)

$ uv run python -m mko_data_cleaner --help
No module named mko_data_cleaner.__main__; 'mko_data_cleaner' is a package
and cannot be directly executed
(confirms OPS-003)

$ uv run python -m mko_data_cleaner.app --help
Usage: python -m mko_data_cleaner.app [OPTIONS] COMMAND [ARGS]...
  | init  ...
  | run   ...
(confirms the submodule form works)
```

### Appendix B — Type-check controls (supports OPS-009)

```text
$ uv run ruff check src/ tests/
All checks passed!

$ uv run pytest -q
93 passed

$ uv run mypy src/mko_data_cleaner
Found 45 errors in 6 files (checked 13 source files)

$ uv run basedpyright src/
39 errors, 0 warnings, 0 notes
```

### Appendix C — Runtime verification (supports OPS-006)

```text
$ uv run python -c "from mko_data_cleaner.core.utils import config_to_dict; \
from mko_data_cleaner.core.models import LoggingSettings; \
try: LoggingSettings(**config_to_dict('nonexistent.json5')) \
except Exception as e: print(type(e).__name__, '-', e)"
ValidationError - 4 validation errors for LoggingSettings
  formatters
    Field required [type=missing, input_value={}, input_type=dict]
  handlers
    Field required [type=missing, input_value={}, input_type=dict]
  loggers
    Field required [type=missing, input_value={}, input_type=dict]
  root
    Field required [type=missing, input_value={}, input_type=dict]
```
*(supports: config_to_dict swallows FileNotFoundError → {}; LoggingSettings(**{})
raises ValidationError naming missing fields, not the missing file)*

### Appendix D — Sibling reports compared

| Report | Prefix | Status | Findings validated |
|---|---|---|---|
| 01-entry-architecture | ENT | validated | ENT-003 covers print() in run_report (overlaps OPS-005) |
| 02-config | CFG | validated | CFG-001 covers config_to_dict masking (overlaps OPS-006); CFG-004 covers missing __main__.py (overlaps OPS-003) |
| 04-data-pipeline | DAT | raw | No overlap with OPS findings |
| 05-config-templates | CFG | raw | No overlap with OPS findings |
| 10-code-quality | QLT | raw | Referenced by OPS-009 cross-phase note (type-check control) |
| 11-test-coverage | TST | raw | No overlap with OPS findings |
| 12-production-ops | OPS | raw (this input) | 6 findings, all confirmed |
| 13-performance | PERF | raw | No overlap with OPS findings |

No sibling report reaches an opposite conclusion on any OPS finding. The three
undeclared overlaps (VAL-001) are same-root-cause cross-references, not conflicts.
