---
phase: "01"
phase_name: "entry-architecture"
id_prefix: "ENT"
mode: "problems-only"
report_status: "validated"
executor: validator
severity_taxonomy: ".kilo/commands/audit/phases/01-audit-entry-architecture.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings Validation — Entry Points & Process Architecture

## Summary

All five filed findings reproduce against `c0fd0e1`. Four are **confirmed** unchanged
(ENT-002 stdout reconfigure, ENT-003 `print()`/Russian in core, ENT-004 doc drift,
ENT-005 `__all__` leak); ENT-001 is **merged** into the sibling phase-02 finding
`CFG-001` because both assert the same malformed-config root cause and `CFG-001` is
the phase that owns the comprehensive config-validation fix. Two validation-level
defects were found **in the audit itself** (cross-phase duplicate filings) and one
grading-nuance note. No filed finding is "not substantiated."

## Disposition Tally

| Disposition | Count | Items |
|---|---|---|
| confirmed | 4 | ENT-002, ENT-003, ENT-004, ENT-005 |
| merged | 1 | ENT-001 → CFG-001 (same root cause; phase 02 owns the comprehensive fix) |
| re-typed / re-graded / not-substantiated / unsettled | 0 | — |

## Per-Finding Validation

### ENT-001 — [HIGH] Malformed config raises ValidationError, not ConfigError
**Dispositions:** merged → `CFG-001` · **type:** SPEC-DEVIATION · **verified:** ran

**Block 1 — re-derived from executing path:** `AppService.app_config` (app_service.py:46–52)
wraps `DataSettings(**config_data)` in `except DataValidationError`. `DataValidationError`
(errors.py:12, `class ... (Exception)`) is a plain `Exception`, **never raised** by pydantic.
Pydantic raises `pydantic_core._pydantic_core.ValidationError` — a different class, so the
`except` is dead. Replicated locally:

```text
[A-bad-enum] pydantic_core._pydantic_core.ValidationError: 1 validation error for DataSettings
  data_file_settings.extension  Input should be 'csv'/'csv'/'gz'  [type=enum, input_value='xml']
[B-syntax-broken] pydantic_core._pydantic_core.ValidationError: 6 validation errors for DataSettings
  data_paths Field required ... (config_to_dict swallowed the parse error -> {})
```
`grep -rn "raise DataValidationError" src tests` → **0 matches** (only the dead `except`).
`basedpyright src/` → `app.py:24 ... reconfigure` + 38 core = **39 errors**. The dead `except`
is structurally dead code (Block 2's "green control" check: no test pins `ValidationError`
on this path; the only `ValidationError` assertion, `test_report_result_rejects_negative_row_counts`,
targets `ReportResult`, a different path — unblocked).

**Block 3 — grade vs phase-01 rubric:** HIGH = "a startup contract that fails to hold."
The malformed-config contract (fail as `ConfigError`, per current-behavior.md:286) fails.
CRITICAL is correctly *not* used (the gate raises, not silent). **Grade holds.**

**Block 4 — side that moves:** CODE (current-behavior.md:286 documents the *intended*
ConfigError wrap; code violates it). The finding's asserted cause is factually correct.

**Block 5 — can the recommendation be carried out?** The lone recommendation (catch
`ValidationError` at app_service.py:51) names a real, stable target. **BUT it is
insufficient in isolation**: for a syntax-broken JSON5 file, `config_to_dict` (utils.py:144–146)
swallows the parse error → `{}` → a misleading "6 missing fields" `ValidationError` (repro
Case B above). Phased-01's bar ("fail later as ConfigError") is met by the narrow fix, but
Phase-02's actionable bar (name the file, fail fast at construction) is not. This is why
merging into `CFG-001` (Targets: `utils.py::config_to_dict`, `app_service.py::app_config /
log_config / _require_settings`) is the correct disposition — `CFG-001` already records the
masking tests `test_config_to_dict_returns_empty_dict_on_invalid_json5` / `..._missing_file`
as remediation blockers the narrower recommendation missed.

**Block 6/7 — cross-phase / namespace:** `ENT-` (phase 01) vs `CFG-` (phase 02) are
distinct declared prefixes. Both handbooks claim the settings-gate/malformed-config
concept; `CFG-001` is the owning, comprehensive treatment. No opposite conclusions (no
conflict), only same-root-cause duplication → **merge**.

### ENT-002 — [MEDIUM] Import-time stdout reconfiguration mutates host stdout
**Dispositions:** confirmed · **type:** SPEC-DEVIATION (violates rule 11 + doc-maintenance
§Output Guidelines) · **verified:** ran

**Block 1:** app.py:23–24 (`if hasattr(sys.stdout, "reconfigure"): sys.stdout.reconfigure(line_buffering=True)`)
is module-scope; `__init__.py:5` imports `app`, so `import mko_data_cleaner` runs it.
Repro:

```text
HAS_RECONFIGURE=True
BEFORE=False
AFTER=True
```

**Block 2:** existing `test_import_survives_stdout_without_reconfigure` (test_app.py:104) only
verifies the *no-reconfigure* kernel survives — it does **not** assert the absence of the
mutation on a normal host. Support is the repro + code reading, not a green control.

**Block 3 — grade:** Phase-01 MEDIUM = "any unsuppressed import-time side effect." Exact fit.
**Block 4 — moves:** CODE (delete app.py:23–24; rely on `rich.Console` at app.py:26).
**Block 5:** target stable; no shipped test pins the mutation; deletion keeps
`test_import_survives_stdout_without_reconfigure` green (import no longer touches stdout).
**Block 7:** `reconfigure` is `io.TextIOWrapper`-only → `basedpyright` error
`app.py:24:16 Cannot access attribute "reconfigure" for class "TextIO" (reportAttributeAccessIssue)`.

### ENT-003 — [MEDIUM] `print()` + Russian text in core orchestrator; progress via `\r`
**Dispositions:** confirmed · **type:** SPEC-DEVIATION (rule 1 English-only; rule 11 no
`print`); also a doc-corpus *accuracy* note · **verified:** ran + static

**Block 1:** `rg -n 'print\('`:
```text
core/app_service.py:113, 259, 274
core/utils.py:34, 41
```
Russian confirmed: app_service.py:114 `Обработка стартовала…`, :275–276
`Обработка завершена… / Общее время` (utils.progress_bar uses `print("\r…")`).

**Block 2:** `test_run_cli_success` asserts *presence* of `"Report complete"` / `"3 loaded → 2
clean"` (rich `console.print`), never the `run_report` `print()` output — so it is not a
green control pinning the defect. Support = grep + doc-rules.
doc-maintenance-rules.md §Output Guidelines corroborates (`use logger in core/`; `never use
print() with \r`; progress bars belong to the CLI layer). architecture.md:235–240 *describes*
this exact behavior as current + flags D-5 — no doc/code conflict, the defect is live and
acknowledged-but-unfixed.

**Block 3 — grade:** Phase-01 MEDIUM band text is narrowly "settings_dir forwarding … any
unsuppressed import-time side effect"; `print()` in `run_report` is *call-time*, not
import-time. The auditor extended the band via "operational/maintainability degradation
(bypasses dictConfig; library/Jupyter callers get uncapturable stdout)". Blast radius =
every `process_data(...)` caller, and the rule violations (1, 11) are real. **Defensible but
borderline** (see VAL-003). Not re-graded.
**Block 4 — moves:** CODE (`print`→`logger.info` in `run_report`; `progress_bar` CR-print →
`rich.progress.track` or move to `app.py`; translate Russian). Architecture.md:239 already
documents this as the targeted D-5 gap.
**Block 5:** stable targets; `test_run_cli_success` unaffected (CLI uses rich console, not the
stripped prints).

### ENT-004 — [LOW] Overview docs describe a stale entry/API surface
**Dispositions:** confirmed · **type:** DOC-UPDATE · **verified:** ran + tested

**Block 1 — re-derived:** every doc claim in Appendix B was checked against `c0fd0e1` code/tests.

| Doc claim (stale) | Verified reality at c0fd0e1 | Source |
|---|---|---|
| arch:165 "`__init__.py` empty; import process_data fails" | `__init__.py` re-exports `process_data`/`initialize_settings`; `__version__="0.2.0"`; `import mko_data_cleaner` works | __init__.py:5–7,15; runtime |
| arch:179 / D-8 #1 "rich imported but not declared" | declared `rich>=13.0.0` | pyproject.toml:28 |
| D-8 #3 "No __version__"; version 0.0.1 | `__version__="0.2.0"`; pyproject version 0.2.0 | __init__.py:15; pyproject.toml:7 |
| current-behavior:35 / D-6 "initialize_settings ignores settings_dir" | forwards to `init_project(...)`→`AppPaths`; writes `tmp/settings/app_config.json5` | app.py:148-150; init_service.py:44-50; test_app.py:322 |
| current-behavior:272 / data-flow.md:165 / D-6 "process_data returns None" | returns `ReportResult` | app.py:145; test_app.py:389,395 |
| D-8 #5 "stale mko_get_mediascope_data in app.py:2/.gitignore:224-225" | `grep mko_get_mediascope_data src/ .gitignore` → **0 hits** (app.py:2 = `mko_data_cleaner …`) | repo grep |
| D-8 #6 "log_config.json5 has tests/telebot loggers" | shipped template has only `__main__` | log_config.json5:45 |
| "35 tests" (arch:24 / current-behavior:24) | `pytest` → **93 passed** (9.60s) | runtime |

**Block 2:** support is code+tests+runtime (test_top_level_exports, test_process_data_end_to_end,
test_initialize_settings_threads_settings_dir all pass; ruff `All checks passed!`). No green
control is the *sole* basis — each claim is re-derived from the executing path.
**Block 3 — grade:** LOW = "documentation/comment drift about surfaces or config." Correct.
**Block 4 — moves:** DOCS (code is correct; `__init__.py` re-exports, `ReportResult`,
`settings_dir` forwarding, `rich`/`__version__`, 93 tests all verified in code). **Block 5:**
all doc targets exist and are stable; the fix is text-only.

**Cross-phase (Block 6):** `CFG-007` (phase 02) flags overlapping doc-drift on the *same* doc
targets — `process_data returns None` and `initialize_settings ignores settings_dir` (current-behavior.md:35/:272, open-questions D-6). Per the phase handbooks, Phase 01 owns
"public-API surface hygiene" + `settings_dir` forwarding (entry); Phase 02 owns
config-relevant doc divergence (loggers, the ConfigError-wrap doc) and **defers packaging
metadata (rich, `__version__`)** to phase 12. Verdict: the two shared sub-claims are a
*same-root-cause cross-reference* (doc corpus lagged commits `5c4bfdd`/`37140aa`); ENT-004
keeps the entry-surface scope, CFG-007 keeps the config-logger scope, and the pure
packaging-metadata hygiene is phase 12. No conflict (both agree the docs lag).

### ENT-005 — [MEDIUM→LOW] `app.py` `__all__` over-exposes internal `get_app_service`
**Dispositions:** confirmed · **type:** BEST-PRACTICE · **verified:** static

**Block 1:** app.py:157–164 `__all__` lists `get_app_service` (line 164), imported from internal
`core/app_service.py` (app.py:12). architecture.md:160: "Not part of the contract: everything
under `mko_data_cleaner.core.*`".
**Block 2:** `test_top_level_exports` (test_app.py:364) enforces the *package-root* `__init__.py`
`__all__` (exactly 6 names, no `get_app_service`) but does **not** assert `app.py`'s `__all__`
— so there is no green control pinning the leak; support is code + architecture.md.
**Block 3 — grade:** Phase-01 LOW = "documentation/comment drift about surfaces … stale
project-name references" — `app.py` `__all__` leakage is surface-hygiene drift, fitting LOW.
*Validator note:* ENT-005 sits between MEDIUM (Blast radius: star-imports coupling to an
internal factory) and LOW. The auditor chose LOW as the safer end; the finding's own impact is
described as "Mild." **Grade holds** (no re-grade needed; band-empty is allowed).
**Block 4 — moves:** CODE (remove `get_app_service` from `app.py.__all__`; stays importable
internally). **Block 5:** target stable; `get_app_service` stays importable; no test pins
`app.__all__`.

## Validation-Level Findings (audit defects)

| ID | Severity | Subject | Verdict |
|---|---|---|---|
| VAL-001 | MEDIUM | ENT-001 and CFG-001 assert the same malformed-config root cause (dead `except DataValidationError`), filed under two phases without a merge; phase handbooks overlap on the settings-gate concept with no ownership handoff | Adjudicated: merged ENT-001 → CFG-001 in the disposition above |
| VAL-002 | MEDIUM | ENT-004 and CFG-007 co-assert the same doc-drift sub-claims (`process_data returns None`, `settings_dir ignored`) on the same doc targets (current-behavior.md:35/:272, open-questions D-6) with no cross-reference | Adjudicated: same-root-cause cross-reference recorded under ENT-004; no opposite conclusions |
| VAL-003 | LOW | ENT-003 graded MEDIUM, but phase-01's MEDIUM band text ("settings_dir forwarding; any unsuppressed *import-time* side effect") does not enumerate *call-time* `print()` in core; the auditor extended the band via an "operational degradation" label | Advisory; finding remains confirmed. Recommend tightening phase-01 MEDIUM text or re-basing ENT-003 under a maintainability band |

## Rollout Safety

- **ENT-001:** subsumed by `CFG-001`. Do NOT patch only app_service.py:51 — it leaves
  `config_to_dict` masking and lazy validation in place (repro Case B). Apply `CFG-001`'s
  fuller fix so a syntax-broken file fails fast at construction with a file-naming error.
- **ENT-002:** deleting app.py:23–24 keeps `test_import_survives_stdout_without_reconfigure`
  and `test_import_app_does_not_create_settings_dir` green (93 passed baseline intact).
- **ENT-003:** `print`→`logger` and `progress_bar`→`rich.progress` do not affect
  `test_run_cli_success` (asserts rich console strings, not `run_report` prints).
- **ENT-004:** docs-only; no test impact (93 passed baseline intact).
- **ENT-005:** removing `get_app_service` from `app.py.__all__` changes only star-import
  surface; `get_app_service` remains importable; no test asserts `app.__all__`.

## Required Fixes (from the validated findings)

1. Apply `CFG-001` (covers ENT-001): make `config_to_dict` surface parse errors, eagerly
   validate both settings models in `get_app_service`, and wrap `pydantic.ValidationError`
   in a `ConfigError` that names the offending file. Rewrite the masking tests as part of it.
2. ENT-002: delete the module-scope `sys.stdout.reconfigure(...)` (app.py:23–24).
3. ENT-003: `print`→`logger.info` in `run_report`; replace `utils.progress_bar` CR-`print`
   with `rich.progress.track` (or move to `app.py`); translate the Russian banners.
4. ENT-004: refresh `docs/00-overview/*` (`architecture.md`, `current-behavior.md`,
   `open-questions.md` D-6/D-8, `product-overview.md` Status) to the implemented surface;
   the packaging-metadata subset (rich/`__version__` hygiene) stays phase 12's lane.
5. ENT-005: drop `get_app_service` from `app.py.__all__`.

## Advisory Recommendations

- Resolve VAL-001/VAL-002 at the family level: add a cross-phase index so the same root cause
  is not double-filed under `ENT-`/`CFG-`; or explicitly assign "malformed-config error
  contract" to phase 02 and "settings_dir forwarding + entry doc-drift" to phase 01 with a
  documented handoff.
- Address VAL-003 for grading discipline: either narrow phase-01's MEDIUM text or re-grade
  call-time core `print()` defects under a maintainability band, so ENTRY-003's band is
  mechanically justified.

