---
phase: 12-audit-production-ops
id_prefix: OPS
mode: problems-only
report_status: draft
code_ref: c0fd0e1
severity_taxonomy:
  CRITICAL: []
  HIGH: [OPS-003]
  MEDIUM: [OPS-002, OPS-005, OPS-006, OPS-009]
  LOW: [OPS-007]
report_template: .ai/audit/templates/audit-findings.md
phase_file: .kilo/commands/audit/phases/12-audit-production-ops.md
---

# Audit Findings — Production Operations

## Summary

Six production-operational problems were found across CI typing gates, packaging/entry-points, and observability. The highest-severity item is a packaging defect: the package ships no `__main__.py`, so the conventional `python -m mko_data_cleaner` entry point fails (only the `mko-data-cleaner` console script and `python -m mko_data_cleaner.app` work). A second operability risk is a real, observed crash: bare `print()` calls on the run path emit Unicode that aborts the tool on legacy Windows code pages — a crash the install smoke test already works around by force-encoding stdout to UTF-8, but the product itself does not. The illustrative findings OPS-001, OPS-004, and OPS-008 were checked against `c0fd0e1` and are no longer problems (see Appendix B).

## Scope

- **Examined:** `.github/workflows/ci.yml`; `pyproject.toml`; `src/mko_data_cleaner/{app.py, __init__.py, core/*, settings/*.json5}`; `tests/test_app.py`, `tests/conftest.py`; `scripts/smoke_install.py`; `docs/00-overview/*.md`; the phase file itself.
- **Tools:** grep (content search), `git ls-files`, `git grep`, `ruff check`, `pytest`, `python -m mko_data_cleaner`, and a `python -c` repro.
- **Green controls:** `ruff check src/ tests/ scripts/` → All checks passed; `pytest` → 93 passed.
- **Not verified:** actual PyPI publish / git-tag release (D11 not reached); end-to-end run on the 43 MB real sample (per `current-behavior.md` investigation limitations).

## Findings

### OPS-003 — [HIGH] package not runnable as `python -m mko_data_cleaner`

| Field | Value |
|---|---|
| **Anchor** | `src/mko_data_cleaner` (package root — no `__main__.py`) |
| **Disambiguator** | `No module named mko_data_cleaner.__main__` |
| **Checked** | ran |
| **Why this band** | Phase HIGH = "a false documented claim misdirecting into a non-existent execution path." Python packaging convention promises `python -m <package>` works for an installed pip package; this package is not runnable that way. (The phase's illustrative text also mentioned a stale tool name `mko-get-mediascope`; that claim is already resolved — shipped docs name `mko-data-cleaner` — but the missing `__main__.py` remains.) |
| **Problem** | The package exposes the `mko-data-cleaner` console script (`pyproject.toml [project.scripts]`) and a `__main__` guard inside `app.py:153`, but there is no package-level `src/mko_data_cleaner/__main__.py`. So `python -m mko_data_cleaner` fails; the only module form is `python -m mko_data_cleaner.app`. |
| **Repro** | 1. `uv run python -m mko_data_cleaner` → `No module named mko_data_cleaner.__main__; 'mko_data_cleaner' is a package and cannot be directly executed` 2. `uv run python -m mko_data_cleaner.app` → prints Typer usage (works). |
| **Impact** | Integrators/analysts following the Python convention are misdirected into a failing entry point; the message names an internal module rather than the supported console script. |
| **Root cause** | No `src/mko_data_cleaner/__main__.py` was ever added to bootstrap `python -m <package>`. |
| **Fix** | Add `src/mko_data_cleaner/__main__.py` that calls `app()` (mirroring `app.py:153`). |
| **Verify** | new test needed: `test_package_runnable_as_module` (assert `python -m mko_data_cleaner --help` exits 0). `scripts/smoke_install.py` does not cover this entry. |
| **Related** | OPS-007 |

**Evidence — `uv run python -m mko_data_cleaner`**:
```text
No module named mko_data_cleaner.__main__; 'mko_data_cleaner' is a package
and cannot be directly executed
```

**Evidence — `git ls-files 'src/mko_data_cleaner/__main__.py'`** *(empty — file absent)*:
```text
```

### OPS-002 — [MEDIUM] scope cites non-existent phase files 03 and 09

| Field | Value |
|---|---|
| **Anchor** | `.kilo/commands/audit/phases/12-audit-production-ops.md::"Deferred by this phase"` |
| **Disambiguator** | `(03) and (09)` |
| **Checked** | ran |
| **Why this band** | Phase MEDIUM (documentation/coverage drift). The scope statement defers to audit phase 03 (import-time side effects / bootstrap) and a phase 09 topic, asserting both are "deleted," but neither file exists, so that coverage is undefined. |
| **Problem** | The phase's scope block defers import-time/bootstrap behavior to phase 03 and another topic to phase 09, and states both phase files are deleted. Listing `.kilo/commands/audit/phases/` shows 03 and 09 are absent (present: 01, 02, 04, 05, 10, 11, 12, 13, 16, 17, 99). Anything those phases would have covered is not audited. |
| **Repro** | `Get-ChildItem .kilo/commands/audit/phases/` → 01,02,04,05,10,11,12,13,16,17,99 (no 03, no 09). |
| **Impact** | A reader trusting the scope believes phase 03 covered import-time/boot behavior; that coverage does not exist, so the gap is silent rather than owned. |
| **Root cause** | Phase files 03 and 09 were removed without updating the 12-scope references that defer to them. |
| **Fix** | Update the scope block to remove references to 03 and 09 (or restore/redirect them) and re-assign the deferred topics to an existing phase. |
| **Verify** | not-checked — documentation only. |
| **Related** | OPS-007 |

**Evidence — `Get-ChildItem .kilo/commands/audit/phases/`**:
```text
01-audit-entry-architecture.md
02-audit-config.md
04-audit-data-pipeline.md
05-audit-config-templates.md
10-audit-code-quality.md
11-audit-test-coverage.md
12-audit-production-ops.md
13-audit-performance.md
16-audit-rendered-surface.md
17-audit-arithmetic.md
99-audit-validate.md
```

### OPS-005 — [MEDIUM] run_report bare print() bypasses dictConfig and crashes on legacy consoles

| Field | Value |
|---|---|
| **Anchor** | `src/mko_data_cleaner/core/app_service.py::run_report` |
| **Disambiguator** | `print(` (run_report banners) |
| **Checked** | ran |
| **Why this band** | Phase MEDIUM (operability degraded; workaround exists but is outside the product). `print()` skips the dictConfig formatter/level/rotation; on non-UTF-8 Windows consoles the Unicode progress bar raises `UnicodeEncodeError` and aborts the run. |
| **Problem** | `run_report` emits user-facing stdout via bare `print()` at `app_service.py:113` (start banner), `:259` ("Synchronizing…"), `:274` (end banner), and via `utils.progress_bar` (`utils.py:34,41`, called from `app_service.py:219` and `csv_service.py:195`). None go through the `dictConfig`-configured pipeline, so output has no timestamp/level/name, no level filtering, no file rotation. `progress_bar` writes box-drawing chars (`█░—`) and `run_report` writes Cyrillic/emoji; on a default Russian Windows console (cp1251/cp866) the `print()` raises `UnicodeEncodeError`, aborting mid-run. The install smoke test (`scripts/smoke_install.py:34-40,92-95`) only passes because it force-reconfigures stdout to UTF-8 — a workaround the shipped CLI does not apply. (Phase locator `db_service.py:308-309` is stale; the only other `print()` is `db_service.py:347-348` in `get_table_sample`, a debug helper off the run path.) |
| **Repro** | 1. `uv run pytest tests/test_app.py::test_process_data_end_to_end -s` (real stdout = console code page on this host) → prints the un-formatted banner `Обработка стартовала: …` alongside formatted `… - INFO - … - mko_data_cleaner.core.csv_service` lines, then aborts in `utils.py:34` `print(` inside `progress_bar` with `UnicodeEncodeError: 'charmap' codec can't encode characters …`. 2. `uv run pytest` (no `-s`, stdout captured as UTF-8) → 93 passed; the crash only appears against a real non-UTF-8 stdout. |
| **Impact** | On the primary user's platform (Windows, default code page), a normal `mko-data-cleaner run` can abort at the first progress bar; because it is a bare `print()`, no configured log entry records the failure. |
| **Root cause** | User-facing/progress output written with `print()`/`console.print` instead of the logger, and stdout encoding is not normalized for legacy Windows code pages. |
| **Fix** | Route user-facing/progress output through `logger` (configured by dictConfig) and/or normalize `sys.stdout` to UTF-8 at startup (as `scripts/smoke_install.py` already does locally), so output is uniform and encoding-safe. |
| **Verify** | new test needed: `test_run_report_output_uses_logger` (no bare print on the run path) + `test_progress_bar_safe_on_cp1251` (no `UnicodeEncodeError` with a non-UTF-8 stdout). |
| **Related** | OPS-006, OPS-007 |

**Evidence — e2e run with `-s` (real stdout)**:
```text
----------  Обработка стартовала: 2026-10-19 19:11:31 ----------
19-10-26 07:11:31 PM - INFO - Reading of 1 files from folder ...\raw_data - mko_data_cleaner.core.csv_service
...
src\mko_data_cleaner\core\utils.py:34: in progress_bar
    print(
E       UnicodeEncodeError: 'charmap' codec can't encode characters in position 2-21:
character maps to <undefined>
```

**Evidence — `scripts/smoke_install.py:34-40`** (the product's own smoke test confesses and works around the crash outside the product):
```text
# Reconfigure stdout/stderr to UTF-8 so Unicode progress-bar output
# (block chars, arrows) does not crash on legacy Windows code pages
# (cp1251, cp1252).
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")
```

### OPS-006 — [MEDIUM] malformed log_config.json5 surfaces as an opaque ValidationError

| Field | Value |
|---|---|
| **Anchor** | `src/mko_data_cleaner/core/utils.py::config_to_dict` |
| **Disambiguator** | `return {}` |
| **Checked** | ran |
| **Why this band** | Phase MEDIUM (operability degraded; the safe path is "don't misconfigure the shipped template"). A typo in a user-edited, shipped file produces an opaque exception rather than a helpful, routed log entry. |
| **Problem** | `config_to_dict` (`utils.py:131-149`) swallows JSON5 parse errors and returns `{}`. `AppService.log_config` (`app_service.py:43-44`) then does `LoggingSettings(**{})`; `LoggingSettings` (`models.py:51-58`) requires `formatters/handlers/loggers/root`, so this raises a `pydantic…ValidationError`. Because `dictConfig` (`app_service.py:339`) runs only after the factory constructs the service, no logging pipeline is configured when this fails — the operator gets an opaque ValidationError and the `logger.error(err)` inside `config_to_dict` (`utils.py:145`) is emitted through the unconfigured root logger, not the configured pipeline. (Phase locator `yaml_to_dict` / `log_config.yaml` is stale: the loader is `config_to_dict` and the file is `log_config.json5`.) |
| **Repro** | 1. In a tmp user settings dir, copy shipped `app_config.json5` and replace `log_config.json5` with malformed JSON5 (`{ this is : not : valid ,,`). 2. `get_app_service(tmp_dir)` → `pydantic_core._pydantic_core.ValidationError`: "4 validation errors for LoggingSettings — formatters Field required, handlers Field required, loggers Field required, root Field required (input_value={})", with no indication `log_config.json5` was the malformed input. |
| **Impact** | A single typo in the user-edited `log_config.json5` (copied by `init`) makes the tool unusable with a parser-internal error instead of "log_config.json5 is invalid." |
| **Root cause** | `config_to_dict` returns `{}` on any `ValueError`, erasing the original parse error; the strict `LoggingSettings` model then turns the empty dict into a confusing ValidationError; `dictConfig` is applied too late to route the failure into the logging pipeline. |
| **Fix** | In `config_to_dict` (or the `log_config` property), re-raise/wrap the original parse error with the file path and a clear message; or make `LoggingSettings` tolerate a missing/empty config and log the specific failure through a fallback handler before `dictConfig`. |
| **Verify** | `tests/test_app.py::test_config_to_dict_returns_empty_dict_on_invalid_json5` asserts the `{}` behavior but no test asserts the user-facing error message — new test needed: `test_malformed_log_config_error_names_file`. |
| **Related** | OPS-005, OPS-007 |

**Evidence — repro output**:
```text
--- exception type ---
pydantic_core._pydantic_core.ValidationError
--- message ---
4 validation errors for LoggingSettings
formatters  Field required [type=missing, input_value={}, input_type=dict]
handlers    Field required [type=missing, ...]
loggers     Field required [...]
root        Field required [...]
```

### OPS-009 — [MEDIUM] typecheck never gates a release

| Field | Value |
|---|---|
| **Anchor** | `.github/workflows/ci.yml::"Type check (non-blocking)"` |
| **Disambiguator** | `continue-on-error: true` |
| **Checked** | ran |
| **Why this band** | Phase MEDIUM (a control configured but not enforced on the shipping path). The type checker does not fail CI, and the configured checker is not run in CI at all. |
| **Problem** | `[tool.mypy]` is configured (`pyproject.toml:138-150`) and `[tool.basedpyright]` is configured (`pyproject.toml:152-154`, dev dep `pyproject.toml:185`). In CI mypy runs only as a non-blocking step (`continue-on-error: true`, `ci.yml:27-29`; 45 errors locally); basedpyright — the checker the project actually typechecks against locally via pyproject — is never invoked in CI (39 errors locally, `git grep -n "basedpyright" .github/workflows/ci.yml` → no match). No type-check verdict can keep a change from shipping. (Phase claim that mypy is "absent from CI (ci.yml:26-27)" is stale — mypy is present but non-blocking.) |
| **Repro** | `uv run basedpyright src/` → 39 errors; `uv run mypy src/mko_data_cleaner` → 45 errors; `git grep -n "basedpyright" .github/workflows/ci.yml` → no match. |
| **Impact** | Type regressions (or the existing 39/45 errors) cannot block a release; the typing contract the project maintains locally is invisible to CI. |
| **Root cause** | The type-check job is wrapped in `continue-on-error: true` and basedpyright is omitted from the workflow entirely. |
| **Fix** | Remove `continue-on-error: true` from the mypy step (or drop mypy and gate on the single configured checker, basedpyright) so a non-zero type-check fails CI; run the chosen checker over the same scope CI uses. |
| **Verify** | `git grep -n "basedpyright" .github/` → 0 matches — new gate needed: a CI step that fails on type errors. |
| **Related** | OPS-002 |

**Evidence — `.github/workflows/ci.yml:26-29`**:
```text
      - run: uv run ruff check src/ tests/ scripts/
      - name: Type check (non-blocking)
        continue-on-error: true
        run: uv run mypy src/mko_data_cleaner
```

### OPS-007 — [LOW] shipped docs describe the pre-rework state

| Field | Value |
|---|---|
| **Anchor** | `docs/00-overview/open-questions.md::"No __version__"` |
| **Disambiguator** | `0.0.1` |
| **Checked** | ran |
| **Why this band** | Phase LOW (documentation/hygiene drift, no runtime consequence today; distorts priority). The docs' own gap register and status table still describe fixed problems as open. |
| **Problem** | Shipped docs assert the package is in the pre-rework state in several places that are now false: (a) `open-questions.md:95-114` gap register D-8: "No `__version__` (version only in pyproject, 0.0.1)" — but `__init__.py:11-15` exposes `__version__` (fallback `0.2.0`), `pyproject.toml:7` is `0.2.0`, and `test_top_level_exports` asserts it; (b) `open-questions.md:101` "top-level `__init__.py` is empty, so `from mko_data_cleaner import process_data` fails" — false: `__init__.py` re-exports the public API (`test_top_level_exports` asserts the 6-name `__all__`); (c) `open-questions.md:106-108` "stale `mko_get_mediascope_data` in `app.py:2` and `.gitignore:224-225`" — `git grep` finds the name only in this gap-register line; `app.py:2` and `.gitignore` are clean; (d) `open-questions.md:110-114` "stale `tests`/`telebot` loggers in `log_config.json5:51-60`" — `log_config.json5` `loggers` holds only `__main__:`, no `tests`/`telebot`; (e) `product-overview.md:127-133` "Status at the time of writing" lists as "Not done": top-level re-exports, `rich` declared, `__version__` — all now done; (f) `architecture.md:165-167` "`__init__.py` is currently empty" and `architecture.md:178-179` "`rich` imported but not declared" — false (`__init__.py` populated; `rich` declared at `pyproject.toml:27` and imported at `app.py:10`); (g) `current-behavior.md:35` "settings_dir … not forwarded to init_project" — `app.py:150` and `app_service` now forward it; `test_initialize_settings_threads_settings_dir` asserts it. |
| **Repro** | `git grep -n "mko_get_mediascope_data" -- src/ .gitignore` → 0 matches; `git grep -n "__version__" -- src/mko_data_cleaner/__init__.py` → `__version__ = "0.2.0"`; `rich` import + declaration present. |
| **Impact** | Responders/maintainers read a gap register and status table that understate what is already shipped, distorting priority. No runtime impact. |
| **Root cause** | Docs were not updated when the import-time fix, the `rich` declaration, `__version__`, top-level re-exports, `settings_dir` forwarding, and the name cleanup landed. |
| **Fix** | Mark D-8 #3-#6 resolved in `open-questions.md`; correct the `product-overview.md` "Status" table and the `architecture.md` lines 165-167 / 178-179 and `current-behavior.md:35`. |
| **Verify** | existing — `test_top_level_exports`, `test_initialize_settings_threads_settings_dir`, `test_import_app_does_not_create_settings_dir` cover the behavior. |
| **Related** | OPS-002, OPS-003 |

**Evidence — `git grep -n "mko_get_mediascope_data" -- src/ .gitignore`** *(empty — name absent from shipped files)*:
```text
```

**Evidence — `src/mko_data_cleaner/__init__.py:11-15`**:
```text
    __version__ = version("mko_data_cleaner")
except PackageNotFoundError:
    __version__ = "0.2.0"
```

## Fix Order

- **Dependencies:** OPS-006 before OPS-005 (logging config must surface a usable message before routing run_report output through it). OPS-003 and OPS-005 are independent of the docs fixes.
- **Merge candidates:** none share a root cause.
- **Conflicting evidence:** none.
- **Cross-phase conflicts:** OPS-007's unused-dep residue (`pydantic-settings`, `python-dateutil`, `urllib3` — confirmed not imported in `src/`) is owned by phase 02 (`CFG-002`, validated) — do not duplicate here. OPS-009 (type-check gating) overlaps phase 10 (`QLT` code-quality) at the type-checking-control boundary — coordinate, do not duplicate.
- **Rollout risk:** OPS-005 is the only fix touching the run path (print→logger / UTF-8 stdout); `tests/test_app.py::test_run_cli_success` and `test_process_data_end_to_end` must stay green. OPS-003 (add `__main__.py`) is additive and low-risk.

## Appendices

### Appendix A — Controls (Block 1): green except typing

```text
$ uv run ruff check src/ tests/ scripts/
All checks passed!
$ uv run pytest -q
93 passed
$ uv run basedpyright src/      # not in CI, 39 errors
<39 errors>
$ uv run mypy src/mko_data_cleaner   # CI step, continue-on-error, 45 errors
<45 errors>
$ git grep -n "basedpyright" .github/workflows/ci.yml
(no match)
```

Note: `.kilo/rules/commands.md` states the lint scope is `src/ tests/` ("scripts/ excluded"), but `pyproject.toml`'s ruff `exclude` and `.github/workflows/ci.yml:26` do not exclude `scripts/`, and CI lints `scripts/` (green). This declared-vs-actual scope drift is documented for completeness; it is green today, so it is noted rather than filed.

### Appendix B — Resolved illustrative findings (checked, not filed)

The phase's illustrative list also named OPS-001, OPS-004, and OPS-008. Evidence at `c0fd0e1` shows each is no longer a problem, so none is filed (a passing check is not a finding):

- **OPS-001** ("Purpose claims async + no database"): `process_data` docstring is synchronous (`app.py:132`); `grep async` finds no claim in shipped docs; SQLite is acknowledged (`architecture.md:136`, `current-behavior.md:147-150`). The async docstring was corrected in phase 01 (F7).
- **OPS-004** ("dictConfig at import time"): `dictConfig` runs only at `app_service.py:339`, inside `get_app_service()`, after the settings gate; `app.py` import performs no logging/filesystem side effects (`tests/test_app.py:81`).
- **OPS-008** ("OPS- prefix collision"): `grep "OPS-" src/ tests/ .github/ docs/` → 0 matches in shipped code; the collision concern is phantom.

### Appendix C — Release/rollback checks (Block 7)

- Version is hardcoded `0.2.0` (`pyproject.toml:7`; `__init__.py:15` fallback `0.2.0`); `product-overview.md:119` plans a pinned `v0.2.0` git-tag release. Acceptable for a small standalone tool; not filed.
- `uv sync` in CI runs without `--frozen`/`--locked` (`ci.yml:25,41,83`); `uv.lock` is committed. If the lockfile drifts from `pyproject.toml`, CI silently re-resolves instead of failing. Recommended CI hardening: `uv sync --frozen`.
- `init` is idempotent and non-destructive (`init_service.py:23-24` skip-when-present unless `force`; `mkdir parents=True, exist_ok=True`), so rollback via `init --force` is safe — no defect.
