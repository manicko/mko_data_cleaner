---
phase: "01"
phase_name: "entry-architecture"
id_prefix: "ENT"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/01-audit-entry-architecture.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Entry Points & Process Architecture

## Summary

Five findings span the entry-layer error contract, an import-time stdout side
effect, console output leaked into the core orchestrator, and two
documentation-drift items. Highest severity is **HIGH**: a malformed settings
file fails with a raw `ValidationError` instead of the documented `ConfigError`,
breaking the self-remediating error contract pipelines depend on. The overview
docs describe an entry/API surface state the code and the 93-test suite had
already implemented past.

## Scope

**Examined:** `app.py`, `__init__.py`, `notebook.py`, `core/app_service.py`
(`get_app_service`, `_require_settings`, `build_app_paths`, `run_report`),
`core/init_service.py`, `core/paths.py`, `core/errors.py`, `core/utils.py`,
`core/models.py`. Entry surfaces (CLI, console script, `python -m
mko_data_cleaner.app`, library API); import-time behaviour; settings gate +
`settings_dir` forwarding; dependency direction; process topology (sync,
single-shot, DBWorker teardown).

**Tools:** `git`, `grep`, `ruff`, `basedpyright`, `pytest`, `python -c`.
**Not verified:** `data/example/` 43 MB sample, FTS5 path, gzip input (phase 04
processing-engine internals, out of scope); packaging metadata posture
(phase 12, out of scope).

| ID | Severity | Anchor | Title |
|---|---|---|---|
| ENT-001 | HIGH | `core/app_service.py::AppService.app_config` | Malformed config raises `ValidationError`, not `ConfigError` (dead `except`) |
| ENT-002 | MEDIUM | `app.py::"sys.stdout.reconfigure(line_buffering=True)"` | Import-time stdout reconfiguration mutates host stdout |
| ENT-003 | MEDIUM | `core/app_service.py::AppService.run_report` | `print()` + Russian text in core; progress via `\r` in `utils.py::progress_bar` |
| ENT-004 | LOW | `docs/00-overview/product-overview.md::"__init__.py is empty"` | Overview docs describe a stale entry/API surface the code+tests already implement |
| ENT-005 | LOW | `app.py::__all__` | `app.py` `__all__` over-exposes internal `get_app_service` |

#### ENT-001 — [HIGH] Malformed config raises ValidationError, not ConfigError
**Anchor:** `src/mko_data_cleaner/core/app_service.py::AppService.app_config` · **Disambiguator:** `DataValidationError` · **Checked:** ran
**Why this band:** Phase HIGH = "a startup contract that fails to hold"; Block 2
states a malformed-but-present config must "fail later as ConfigError." It does
not. CRITICAL is for *silent* admission+proceed (the gate raises, so not silent).
**Problem:** `AppService.app_config` (app_service.py:46-52) wraps the load in
`except DataValidationError as e: raise ConfigError(...)`. `DataValidationError`
(errors.py:12) is never raised — pydantic raises `pydantic.ValidationError` (a
different class) — so the `except` never matches and the raw `ValidationError`
propagates. The gate (`_require_settings`) checks existence only, so a present
but malformed `app_config.json5` passes it.
**Repro:** `initialize_settings(settings_dir=tmp)` → corrupt `app_config.json5`
(syntax-broken AND a valid-json5 bad enum) → `svc = get_app_service(tmp)`
(gate passes) → `svc.app_config` raises `pydantic.ValidationError`, **not**
`ConfigError`. `grep -rn "raise DataValidationError" src tests` → 0 hits.
**Impact:** Callers catching `ConfigError` (the documented contract,
current-behavior.md) miss this and propagate a long pydantic `ValidationError`
instead of the curated `ConfigError(f"Invalid config: {e}")` — degrades the
self-remediating error contract (D2/R3).
**Root cause:** Wrong exception type in the `except` clause.
**Fix:** In `AppService.app_config`, catch the real type:
`except (DataValidationError, ValidationError)` (import pydantic ValidationError;
drop DataValidationError if unused). Target: app_service.py:51.
**Verify:** new test needed `test_malformed_config_raises_config_error`. No
shipped test pins the current `ValidationError` on this path → unblocked.
**Related:** D-6, D-8, Block 2, Block 4.

#### ENT-002 — [MEDIUM] Import-time stdout reconfiguration mutates host stdout
**Anchor:** `src/mko_data_cleaner/app.py::"sys.stdout.reconfigure(line_buffering=True)"` · **Disambiguator:** `reconfigure` · **Checked:** ran
**Why this band:** Phase MEDIUM = "any unsuppressed import-time side effect." The
`hasattr` guard only suppresses kernel `OutStream`s lacking `reconfigure`; for
every normal host it runs at import.
**Problem:** app.py:23-24 calls `sys.stdout.reconfigure(line_buffering=True)` at
module scope. `__init__.py` imports `app`, so this runs on
`import mko_data_cleaner` and `python -m mko_data_cleaner.app` — a bare library
import mutates host stdout buffering with no opt-out (the sole import-time side
effect, as the phase acknowledges).
**Repro:** `sys.stdout.line_buffering` was `False` before import, `True` after
`import mko_data_cleaner.app`. The Jupyter case is guarded
(`test_import_survives_stdout_without_reconfigure`, test_app.py:104) but that
guard is the only suppression.
**Impact:** Dependent packages reconfigure host stdout as an import-time bomb
(test runners capturing stdout, piped/CI output, latency-sensitive streaming).
Also untyped: `basedpyright` errors app.py:24
"Cannot access attribute 'reconfigure' for class 'TextIO'" (`reportAttributeAccessIssue`)
— `reconfigure` is a CPython `io.TextIOWrapper` detail absent from the `TextIO` stub.
**Root cause:** A stdout-buffering decision lives in entry-module import time.
**Fix:** Delete app.py:23-24; rely on the already-constructed `rich.Console`
(app.py:26) or set buffering in the CLI entry `app()` only. Target: app.py:23-24.
**Verify:** new test `test_bare_import_does_not_reconfigure_stdout`; update
`test_import_survives_stdout_without_reconfigure` docstring.
**Related:** Block 1, Block 3.

#### ENT-003 — [MEDIUM] `print()` + Russian text in core orchestrator; progress via `\r`
**Anchor:** `src/mko_data_cleaner/core/app_service.py::AppService.run_report` ·
**Secondary:** `src/mko_data_cleaner/core/utils.py::progress_bar` · **Disambiguator:** `print(` · **Checked:** ran + static
**Why this band:** MEDIUM — operational/maintainability degradation; console
presentation lives in `core/` (internal) instead of the entry layer, bypassing
the `dictConfig` logging the factory sets up.
**Problem:** `run_report` uses `print()` three times (app_service.py:113
"Обработка стартована…", :259 English sync notice, :274 "Обработка завершена…");
`utils.progress_bar` (utils.py:34,41) uses `print("\r…")` with manual CR. This
violates rule 11 (no `print`), rule 1 (English only — D-5/OQ-10), and
`doc-maintenance-rules.md` §Output Guidelines ("use `logger` in `core/`"; "never
use `print()` with `\r`"; "progress bars belong to the CLI layer, not business
logic").
**Repro:** `rg -n 'print\('` → app_service.py:113,259,274; utils.py:34,41.
`test_run_cli_success` asserts only `"Report complete"`/`"3 loaded → 2 clean"`
(app.py rich console), not the `run_report` prints.
**Impact:** Library/Jupyter `process_data(...)` prints uncapturable,
unconfigurable stdout that bypasses `log_config.json5`; Russian banners breach
English-only.
**Root cause:** Presentation/progress concerns placed in the internal orchestrator.
**Fix:** Replace `print`→`logger.info` in `run_report`; replace
`utils.progress_bar` CR-print with `rich.progress.track` (or move to `app.py`);
translate Russian banners. Targets: app_service.py:113/259/274, utils.py:18-41.
**Verify:** new test `test_run_report_logs_not_prints`; `test_run_cli_success`
stays green.
**Related:** D-5, OQ-10, Block 5.

#### ENT-004 — [LOW] Overview docs describe a stale entry/API surface the code+tests already implement
**Anchor:** `docs/00-overview/product-overview.md::"__init__.py is empty"` · **Disambiguator:** `__init__.py is empty` (line 130) · **Checked:** ran + tested
**Why this band:** Phase LOW = documentation/comment drift about surfaces or config.
**Problem:** Docs assert entry/API states the code contradicts. (Full table → Appendix.)
In short: `architecture.md:165` " `__init__.py` is empty; `from mko_data_cleaner
import process_data` fails" → runtime `'process_data' in dir(mko_data_cleaner)`
=`True`; `open-questions D-8 #3` "No `__version__`" → `__version__`="0.2.0";
`D-8 #1/architecture:179` "rich imported but not declared" → `pyproject.toml:28`
declares `rich>=13.0.0`; `current-behavior.md:35 / D-6` "initialize_settings
ignores settings_dir" → `initialize_settings(settings_dir=tmp)` wrote
`tmp/settings/app_config.json5` (`test_initialize_settings_threads_settings_dir`);
`current-behavior.md:272 / data-flow.md:165` "process_data returns None" →
`test_process_data_end_to_end` asserts a `ReportResult`; `D-8 #5`
"stale mko_get_mediascope_data in app.py/.gitignore" → `grep` finds 0 hits in
`src/ .gitignore`; `D-8 #6` "stale tests/telebot loggers" → `log_config.json5`
has only `__main__`; "35 tests" → `pytest` = **93 passed**.
**Impact:** Maintainers/planners make wrong decisions (avoid the clean
top-level import; reach into `core.*`; trust `settings_dir` is ignored).
Breaches `doc-maintenance-rules.md` (docs must track reality) and the docs' own
`[code-verified]` citations.
**Root cause:** Overview docs predate the R4/D-8 entry-surface fixes and were
not refreshed.
**Fix:** [DOC-UPDATE] No code change (code is correct/better). Align
product-overview §Status, architecture §"Public Interfaces", open-questions D-8
#1/#3/#4/#5/#6 + D-6, current-behavior.md:35/:272 to verified code; 35→93 tests.
**Verify:** `uv run python -c "import mko_data_cleaner;print(mko_data_cleaner.__version__)"`→0.2.0;
`test_top_level_exports` (test_app.py:364), `test_process_data_end_to_end` (test_app.py:389).
**Related:** D-3, D-5, D-6, D-8, R4.

#### ENT-005 — [LOW] `app.py` `__all__` over-exposes internal `get_app_service`
**Anchor:** `src/mko_data_cleaner/app.py::__all__` · **Disambiguator:** `get_app_service` · **Checked:** static
**Why this band:** Phase LOW = surface drift. Package-root `__init__.py` `__all__`
is clean and enforced (`test_top_level_exports`, test_app.py:364 — exactly 6 names),
but the entry module's `__all__` leaks an internal factory.
**Problem:** app.py:157-163 `__all__` bundles the two-function contract with CLI
command symbols and `get_app_service` (which lives in internal `core/`, per
architecture.md "not part of the contract"), inviting `from
mko_data_cleaner.app import *` consumers to couple to it. (`app` stays for the
console script; Typer commands are needed for registration.)
**Repro:** `rg -n 'get_app_service'` app.py → 12 (import), 164 (`__all__`).
**Impact:** Mild: downstream star-imports can silently depend on an internal
factory, hardening its future removal.
**Root cause:** `__all__` mirrors "everything defined here" vs the two-function
contract.
**Fix:** Remove `get_app_service` from `app.py.__all__` (stays importable
internally; only `from app import *` changes). Target: app.py:157-163. Effort:
trivial.
**Verify:** new test `test_app_all_excludes_internal_factory`.
**Related:** D-8, Block 5.

## Fix Order
- **Dependencies:** none. ENT-001 (app_config) and ENT-003 (run_report) touch
  `app_service.py` disjointly; ENT-002 (reconfigure) and ENT-005 (`__all__`)
  touch `app.py` disjointly.
- **Merge candidates:** ENT-002 + ENT-003 — same root theme "stdout handled
  outside the entry layer"; fix ENT-002 first, then ENT-003 removes the remaining
  non-logging stdout writes.
- **Conflicting evidence:** none.
- **Cross-phase conflicts:** the 38 other `basedpyright` errors live in
  `core/{app_service,csv_service,db_service,dict_service,models,paths}.py` —
  processing/SQLite-engine type debt (phase 04), not entry architecture.
- **Rollout risk:** ENT-001 changes the malformed-config exception
  `ValidationError`→`ConfigError` (no test catches `ValidationError` on this path;
  `ReportResult` ValidationError test is a different path — green). ENT-002:
  `test_import_survives_stdout_without_reconfigure` still passes. ENT-003:
  `test_run_cli_success` checks app.py console output, not `run_report` prints —
  green. ENT-005: no test asserts `app.__all__`; `get_app_service` stays
  importable. ENT-004: docs only.

## Appendices

### Appendix A — Surface inventory (Block 1 evidence)
Tracked (`git ls-files`) entry surfaces at c0fd0e1: CLI `app.py` (`init`,
`run`, `list-settings`); console script `pyproject.toml [project.scripts]` =
`mko-data-cleaner → "mko_data_cleaner.app:app"`; module entry `python -m
mko_data_cleaner.app` (app.py:153 `if __name__ == "__main__": app()`); library
API `process_data`/`initialize_settings` re-exported from `__init__.py`. `python
-m mko_data_cleaner` fails — `No module named 'mko_data_cleaner.__main__'`
(glob `**/__main__.py` → none). Documented state, not a regression. Import-time
work of `import mko_data_cleaner`: only stdout reconfigure (app.py:23-24, the
sole side effect; ENT-002); `rich.Console()` (app.py:26) and `typer.Typer`
(app.py:27) do no I/O. No FS/network/`logging` at import
(`test_import_app_does_not_create_settings_dir`, test_app.py:81).

### Appendix B — ENT-004 doc-drift evidence table
| Doc claim (stale) | Verified reality | Source |
|---|---|---|
| `arch:165` "`__init__.py` empty; `from mko_data_cleaner import process_data` fails" | import works, `__version__="0.2.0"` | runtime + test_app.py:364,389 |
| D-8 #1/arch:179 "rich imported but not declared" | declared `rich>=13.0.0` | pyproject.toml:28 |
| D-8 #3 "No `__version__`" | `__version__="0.2.0"` | __init__.py:15, runtime |
| current-behavior:35 / D-6 "initialize_settings ignores settings_dir" | writes `tmp/settings/app_config.json5` | runtime + test_app.py:322 |
| current-behavior:272 "process_data returns None" | returns `ReportResult` | test_app.py:389 |
| D-8 #5 "stale name in app.py:2/.gitignore:224-225" | 0 hits in `src/ .gitignore` | `git grep mko_get_mediascope_data src/ .gitignore` |
| D-8 #6 "tests/telebot loggers in log_config.json5" | only `__main__` logger | log_config.json5:44-51 |
| "35 tests" (arch:24 / current-behavior:24) | `pytest → 93 passed` | runtime |

### Appendix C — Runtime evidence (captured)
```text
# bare import + re-exports + version
has_process_data=True  has_version=True  stdout_has_reconfigure=True
# stdout mutation on import
line_buffering before: False  after: True
# settings gate (missing)
GATE_MISSING: raised SettingsNotInitializedError OK
# settings_dir forwarding
INIT_FWD returned: .../tmp/settings  app_config in tmp: True
# malformed config (gate passes; late failure)
GATE_MALFORMED: gate passed OK / get_app_service constructed
GATE_MALFORMED: app_config raised ValidationError   # not ConfigError
# module entry
python -m mko_data_cleaner -> No module named 'mko_data_cleaner.__main__'
# baselines  ruff: All checks passed!   pytest: 93 passed (14.47s)
# basedpyright: app.py:24 reconfigure TextIO error + 38 core type errors = 39 total
rg -n 'print\(' core/app_service.py -> 113, 259, 274 ; core/utils.py -> 34, 41
rg -n 'raise DataValidationError' src tests -> (none)
```
