---
phase: "12"
phase_name: "Production Operations, Security & Observability"
phase_file: ".kilo/commands/audit/phases/12-audit-production-ops.md"
source: ".ai/reports/phase-12-auditor-report.md"
author: researcher
verdict: REBASE
confidence: HIGH
code_ref: "b390d04"
phase_file_lines: 175
---

# Phase 12 — Researcher Recommendations

## 1. Executive Summary

The auditor verdict is **REBASE**. The Phase 12 spec file
(`12-audit-production-ops.md`, 175 lines) is framed around a **non-existent**
asynchronous, no-database architecture. The codebase is a **synchronous,
single-shot, local-only** CLI + library: `app.py → AppService.run_report →
CSVWorker → DBWorker / MappingDict`, with a disposable SQLite database per
invocation (created on entry, WAL-checkpointed + deleted on `DBWorker.__exit__`).

A repo-wide grep of `src/mko_data_cleaner/` for `asyncio | async def |
asyncio.run | async` returns **zero matches** — the only async reference is a
commented-out `# "--asyncio-mode=auto"` at `pyproject.toml:160`.
**No `__main__.py`** exists at the package root (only `app.py:153` provides
`python -m mko_data_cleaner.app`). The console script is `mko-data-cleaner`
(`pyproject.toml:46`), **not** the stale `mko-get-mediascope`. `dictConfig` was
moved from import-time to the `get_app_service()` factory (`app_service.py:337`)
by Phase 01 remediation (commit `76b1fc8`).

The 7 blocks are **structurally sound** and framework-agnostic. They need
**premise rewording**, not deletion: async → sync, "no database" → "disposable
SQLite", "dictConfig at import time" → "dictConfig in the factory". All 9
findings (OPS-001 through OPS-009) are adopted and assessed below, sorted by
severity (HIGH → MEDIUM → LOW).

**Evidence sources** (verified by direct file inspection at `b390d04`):
`app.py` (165 lines), `core/app_service.py` (342 lines), `core/utils.py` (173
lines), `core/init_service.py` (57 lines), `core/models.py` (195 lines),
`core/db_service.py` (788 lines), `core/paths.py` (54 lines),
`core/errors.py` (30 lines), `__init__.py` (22 lines); `pyproject.toml` (185
lines), `.github/workflows/ci.yml` (92 lines), `settings/app_config.yaml` (50
lines), `settings/log_config.yaml` (57 lines); `docs/00-overview/architecture.md`
(259 lines), `docs/00-overview/current-behavior.md` (330 lines);
`tests/test_app.py` (473 lines); prior reports `phase-11-auditor-report.md`,
`phase-13-researcher-recommendations.md`.

---

## 2. Proposed Final Block Set (1–7, rebased)

All blocks are retained. Each carries a **Premises** note (what to reword) and
an **Evidence locus** (what the auditor should inspect). The concept of each
block is framework-agnostic — only the framing drifts.

| # | Block (kept) | Premises to reword | Evidence locus |
|---|---|---|---|
| 1 | A control that runs, reports clean, and examines nothing | No async reframe needed; surface CI gaps: mypy configured in `pyproject.toml:140-150` but absent from `ci.yml:26-27` | `ci.yml:26-27`; `pyproject.toml:140-150` |
| 2 | CI/CD pipeline & gates | async → sync framing in the block prose | `ci.yml` (4 jobs: `test`, `build`, `install-smoke`, `notebook`) |
| 3 | What makes a check a gate vs. a report | No reframe needed — already sync | `ci.yml` `needs:` dependencies |
| 4 | Packaging & artifact identity | `mko-get-mediascope` → `mko-data-cleaner`; `python -m` → `python -m mko_data_cleaner.app` (no `__main__.py`) | `pyproject.toml:6,46`; glob `__main__.py` → not found |
| 5 | Observability: signals produced and consumed | "dictConfig at import time" → "dictConfig in `get_app_service()` factory after the settings gate" | `app_service.py:337`; `log_config.yaml:13` (unstructured format) |
| 6 | Runbooks & operational documentation | No async reframe needed; surface doc-vs-code drift | `architecture.md:165-168,178,161-164`; `current-behavior.md:35` |
| 7 | Release & rollback | No async reframe needed | `init_service.py:7-27`; `pyproject.toml:7` |

### Block 1 — A control that runs, reports clean, and examines nothing *(keep)*

**Premises:** No async/security reframe needed. The concept is already
sync-framed. The finding-worthy gap is **mypy configured but not enforced** —
a declared control (type safety) that goes unenforced on the path that decides
whether a change ships.

**Evidence locus:** `ci.yml:26-27` runs only `ruff check src/ tests/` and
`pytest`; **zero** `mypy` references in `ci.yml`. `[tool.mypy]` is fully
configured at `pyproject.toml:140-150` (`strict_optional`,
`disallow_any_generics`, `check_untyped_defs`, etc.). A repo-wide grep confirms
`rg -n 'mypy' .github/workflows/ci.yml` → no matches.

### Block 2 — CI/CD pipeline & gates *(keep, minor reword)*

**Premises:** Strip any async-polling or worker-pool language from the block
prose. The CI/CD pipeline is a standard GitHub Actions workflow with four
gating jobs — no event loop, no polling.

**Evidence locus:** `ci.yml` jobs: `test` (`ruff` + `pytest`), `build` (`python
-m build` + `twine check` + `check-wheel-contents` + wheel-content assertions),
`install-smoke` (fresh-venv install + smoke), `notebook` (nbclient execution).
`build` depends on `test` via `needs:`; `install-smoke` depends on `build`.

### Block 3 — What makes a check a gate vs. a report *(keep, no change)*

**Premises:** No reframe needed. Already framework-agnostic and sync-framed.

**Evidence locus:** `ci.yml` — identify which jobs are gating (`test`, `build`,
`install-smoke` via `needs:`) and which are informational (`notebook`).

### Block 4 — Packaging & artifact identity *(reword)*

**Premises:** Replace the stale console script name `mko-get-mediascope` →
`mko-data-cleaner`. Replace the generic `python -m` entry-point claim with the
specific `python -m mko_data_cleaner.app` (no package-level `__main__.py`
exists; `python -m mko_data_cleaner` fails).

**Evidence locus:** `pyproject.toml:6` (name = `mko_data_cleaner`),
`pyproject.toml:46` (`mko-data-cleaner = "mko_data_cleaner.app:app"`),
`src/mko_data_cleaner/__init__.py:1` (import name = `mko_data_cleaner`).
`glob src/mko_data_cleaner/__main__.py` → not found. Only `app.py:153`
(`if __name__ == "__main__": app()`) provides a module entry.

### Block 5 — Observability: signals produced and consumed *(reword)*

**Premises:** Replace "dictConfig at import time" with "dictConfig inside the
`get_app_service()` factory, after the settings gate passes." The synchronous
analog of observability is: structured logging via `dictConfig` + failure-state
`logger.error`/`logger.exception` calls. The finding is that `print()` calls in
`run_report` bypass the logging pipeline, and `yaml_to_dict` swallows errors
without raising.

**Evidence locus:** `app_service.py:337` (`dictConfig` inside factory);
`app_service.py:113-116, 257-261, 272-276` (three `print()` calls in
`run_report`); `db_service.py:308-309` (`get_table_sample` uses `print()`);
`utils.py:131-149` (`yaml_to_dict` catches errors, returns `{}`);
`app_service.py:43-44` (`log_config` cached property → `LoggingSettings(**yaml_to_dict(...))`);
`log_config.yaml:13` (unstructured `%(asctime)s - %(levelname)s - %(message)s - %(name)s` formatter).

### Block 6 — Runbooks & operational documentation *(keep)*

**Premises:** No async reframe needed. The block audits documentation that
describes a system other than the one shipped. Several claims in docs are stale.

**Evidence locus:** `architecture.md:165-167` (claims `__init__.py` is empty);
`architecture.md:178` (claims `rich` is "imported but not declared");
`architecture.md:161-164` (claims README shows stale import);
`current-behavior.md:35` (claims `initialize_settings` known gap: `settings_dir`
not forwarded); `__init__.py:16-22` (has full re-exports); `pyproject.toml:27`
(`rich` declared); `app.py:148-150` (forwards `settings_dir`);
`init_service.py:44-50` (honors `settings_dir`); `README.md:65`
(`from mko_data_cleaner.app import process_data`).

### Block 7 — Release & rollback *(keep)*

**Premises:** No async reframe needed. Idempotency and non-destructiveness of
`init_project` / `_copy_templates` are already correct.

**Evidence locus:** `init_service.py:7-27` (skips existing files unless
`force=True`; uses `exist_ok=True`); `pyproject.toml:7` (`version = "0.0.1"`
hardcoded, no build-time stamping); `ci.yml:25` (`uv sync` without
`--frozen`/`--locked`).

---

## 3. Confirmed Findings to Surface

Nine findings, sorted by severity (HIGH first, then MEDIUM, then LOW). The
`OPS-` prefix is retained — its collision warning is **phantom** (§4.1 /
OPS-008). All anchors verified against the working tree at `b390d04`.

| # | ID | Severity | Anchor (primary) | Title |
|---|---|---|---|---|
| 1 | OPS-001 | HIGH | `12-audit-production-ops.md:15-17` | Purpose falsely describes tool as "async" with "no database" |
| 2 | OPS-003 | HIGH | `12-audit-production-ops.md:86` | Block 4 references stale console script `mko-get-mediascope` and missing `__main__.py` |
| 3 | OPS-004 | HIGH | `12-audit-production-ops.md:98-99` | Block 5 falsely claims "dictConfig at import time" |
| 4 | OPS-002 | MEDIUM | `12-audit-production-ops.md:23,26` | Scope boundaries cite deleted phases (03) and (09) |
| 5 | OPS-005 | MEDIUM | `core/app_service.py:113,257,272` | `print()` calls in `run_report` bypass the dictConfig logging pipeline |
| 6 | OPS-006 | MEDIUM | `core/utils.py:131-149`; `core/app_service.py:43-44` | `yaml_to_dict` silently returns `{}` on parse error; log_config failure is unobserved |
| 7 | OPS-007 | LOW | `docs/00-overview/architecture.md:165-168,178,161-164`; `docs/00-overview/current-behavior.md:35` | Documentation drift: docs describe pre-rework state |
| 8 | OPS-008 | LOW | `12-audit-production-ops.md:169-171` | Phase 12 report-output `OPS-` prefix warning is phantom |
| 9 | OPS-009 | LOW | `.github/workflows/ci.yml:26-27`; `pyproject.toml:140-150` | CI does not enforce mypy despite configuring it |

### OPS-001 — [HIGH] Purpose falsely describes the tool as "async" with "no database"

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md::lines 15-17`
**Checked:** static (repo-wide grep + source read)
**Why this band:** The Purpose text describes a system that does not exist (async,
no database). An auditor operating from this Purpose will waste effort chasing
async bridges, event-loop teardown, and database-less deployment concerns that
have no referent in the codebase.
**Problem:** Lines 15–17 state the tool is "async" and has "no database." The
codebase is synchronous — grep for `asyncio|async def|asyncio.run` across
`src/mko_data_cleaner/` returns **0 matches** (verified). The only async
reference is a commented-out `# "--asyncio-mode=auto"` at `pyproject.toml:160`.
The codebase uses **SQLite** as a disposable per-invocation processing engine
(`app_service.py:146` opens `DBWorker`; `db_service.py`;
`architecture.md:136` and `current-behavior.md:78` confirm "The database is
created per run and deleted afterwards").
**Repro:** `rg -n 'async def|asyncio' src/mko_data_cleaner/` → no matches.
`rg -n 'sqlite|SQLite' src/mko_data_cleaner/core/db_service.py | head -5` → SQLite
referenced throughout.
**Impact:** Misdirects every downstream auditor; blocks accurate per-block Fit
assessment until corrected.
**Root cause:** The Phase 12 file was written against an older, pre-rework
architecture description that has not been updated since the Phase 01/02
synchronous rebase (commit `76b1fc8`).
**Fix:** Reword Purpose lines 15–17 to read: *"This is a synchronous, single-shot
CLI tool and Python library distributed via PyPA packaging — there are no
containers, no network, no reverse proxy, no health probes, and no backup
strategy; the only persistent runtime artifact is a disposable SQLite database
created and deleted per invocation."*
**Verify:** `rg -n 'asyncio' src/mko_data_cleaner/ app.py core/` → 0 matches
after fix; `rg -n 'sqlite|SQLite' src/mko_data_cleaner/core/db_service.py | head`
→ still matches.
**Related:** (Phase 01 rebase, commit `76b1fc8`)

### OPS-003 — [HIGH] Block 4 references stale console script `mko-get-mediascope` and missing `__main__.py`

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md::line 86`
**Checked:** static (pyproject read + glob)
**Why this band:** A packaging-identity block that names the wrong console script
undermines its own premise (verify artifact identity). An auditor checking "does
the published artifact coordinate match what the install path expects" starts
from a false name.
**Problem:** Line 86 says "the entry points (`mko-get-mediascope` console script,
`python -m`)." The actual console script is `mko-data-cleaner`
(`pyproject.toml:46`). The name `mko-get-mediascope` /
`mko_get_mediascope_data` is vestigial project-name residue. Additionally, there
is **no `__main__.py`** at the package root — `glob src/mko_data_cleaner/__main__.py`
returns nothing. `python -m mko_data_cleaner` fails; only
`python -m mko_data_cleaner.app` works via `app.py:153`.
**Repro:** `rg -n 'mko-get-mediascope|mko_get_mediascope' pyproject.toml src/` →
0 matches in shipped code. `Test-Path src/mko_data_cleaner/__main__.py` → False.
The only remaining hit for the stale name is `ci.yml:45` (a *negative* guard:
`assert not stale` for `mko_get_mediascope_data` in wheel contents) and
`open-questions.md:106-108` (doc drift still listing `app.py:2` as stale, though
`app.py:2` already reads `mko_data_cleaner`).
**Impact:** Misleads the auditor into checking for a non-existent entry point;
the `python -m` form is also misleading (no `__main__.py` exists).
**Root cause:** The Phase 12 file was not updated after the project rename
settlement (commit `37140aa`).
**Fix:** Line 86: replace `mko-get-mediascope` → `mko-data-cleaner`; add a note
that `python -m mko_data_cleaner.app` (not `python -m mko_data_cleaner`) is the
module entry point due to the missing `__main__.py`.
**Verify:** `rg -n 'mko-data-cleaner' pyproject.toml` → 1 match at line 46;
`Test-Path src/mko_data_cleaner/__main__.py` → False (documented as a known gap).

### OPS-004 — [HIGH] Block 5 falsely claims "dictConfig at import time"

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md::lines 98-99`
**Checked:** static (source read + grep)
**Why this band:** The observability block's central premise is wrong, so every
sub-question (eager file-handle creation, logging-config validation at load time)
is grounded in a non-existent execution path. An auditor will inspect import
time for a side effect that no longer exists.
**Problem:** Lines 98–99 state "The logging pipeline is configured via
`dictConfig` at import time." `dictConfig` now runs inside
`get_app_service()` (`app_service.py:337`), after the settings gate passes.
Importing `mko_data_cleaner.app` performs no logging configuration and no
filesystem side effects. This was the Phase 01 Block 1 remediation (commit
`76b1fc8`).
**Repro:** `rg -n 'dictConfig' src/mko_data_cleaner/` → 1 match at
`app_service.py:337` (inside `get_app_service`), 0 matches in `app.py`.
`test_app.py:81` (`test_import_app_does_not_create_settings_dir`) asserts importing
`mko_data_cleaner.app` creates no settings directory.
**Impact:** Misdirects the auditor to inspect module-level import for dictConfig;
the actual observability concern (unvalidated config, print() bypassing logging)
is never reached because the premise is false.
**Root cause:** The Phase 12 file was not updated after the Phase 01 rebase.
**Fix:** Lines 98–99: replace "at import time" → "inside the `get_app_service()`
factory, after the settings gate."
**Verify:** `rg -n 'dictConfig' src/mko_data_cleaner/app.py` → 0 matches.

### OPS-002 — [MEDIUM] Scope boundaries cite deleted phases (03) and (09)

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md::lines 22-28`
**Checked:** static (file-existence check)
**Why this band:** A scope boundary pointing to a deleted phase is an inaccurate
claim — it tells the auditor to defer a concern to a phase that no longer exists.
An operator cannot act on a deflection to a deleted phase.
**Problem:** Line 23 cites "(03)" for Semaphore/Queue/worker-pool concurrency;
line 26 cites "(09)" for the outbound API call contract. Both Phase 03 (async
concurrency) and Phase 09 (external API) were **deleted** —
`phase-03-auditor-report.md` and `phase-09-auditor-report.md` both recommend
DELETE with all blocks Fit:None.
**Repro:** `Test-Path .kilo/commands/audit/phases/03-audit-async-concurrency.md`
→ False. `Test-Path .kilo/commands/audit/phases/09-audit-external-api.md` →
False.
**Impact:** An auditor following these citations will look for a phase file that
does not exist, or will defer async-API concerns to a deleted phase.
**Root cause:** The Phase 12 file was not updated after Phases 03 and 09 were
deleted.
**Fix:** Remove "(03)" and "(09)" from the Scope Boundaries paragraph. Reword
"(04)" — currently "period resolution and data extraction" — to clarify it is
not scheduled-task period resolution but rather pipeline stage ordering (the tool
has no scheduler; Phase 04 owns data-pipeline stage ordering). Keep (01), (02),
(04) reworded, (05), (10), (11), (13).
**Verify:** `rg -n '03|09' .kilo/commands/audit/phases/12-audit-production-ops.md`
→ no remaining phase citations to deleted phases.

### OPS-005 — [MEDIUM] `print()` calls in `run_report` bypass the dictConfig logging pipeline

**Anchor:** `src/mko_data_cleaner/core/app_service.py::lines 113, 257, 272`
**Checked:** static (source read + grep)
**Why this band:** A failure state or lifecycle event that emits only `print()`
to stdout is invisible to the configured `rotating_file` handler
(`log_config.yaml:33-41`), so an operator relying on log output cannot tell
whether the tool is running, stalled, or failed. The logging pipeline is
configured but not consistently consumed.
**Problem:** Three `print()` calls in `run_report`:
- `app_service.py:113-116` — run-start banner (`Обработка стартовала`)
- `app_service.py:257-261` — "Synchronizing the index table…" message
- `app_service.py:272-276` — run-completion banner with timings (`Обработка завершена`)

None reach `logger.info`/`logger.debug`. The configured pipeline
(`log_config.yaml`) has both a `console` handler (StreamHandler → stdout) and a
`rotating_file` handler — but `print()` writes directly to `sys.stdout` without
going through `logging`, so it is **not** subject to the configured formatter,
level filtering, or file rotation. Additionally, `db_service.py:308-309`
(`get_table_sample`) uses `print()` instead of `logger` — a secondary instance.
**Repro:** `rg -n '^\s*print\(' src/mko_data_cleaner/core/app_service.py` →
matches at lines 113, 257, 272. `rg -n '^\s*print\('
src/mko_data_cleaner/core/db_service.py` → matches at 308, 309. None are wrapped
in `logger.*`.
**Impact:** Operators who tail `log.log` (the configured rotating file handler)
will **not** see run-start, run-end, or synchronization progress — only the
fine-grained `logger.info`/`logger.debug` entries from CSVWorker/DBWorker. The
high-level lifecycle signal is split across two I/O channels (stdout vs. logging).
**Root cause:** `print()` was used for CLI-friendly user output without
consolidating through the logging pipeline; the Phase 02 fix added CLI-level
`console.print()` (rich Console) in `app.py` but the internal `run_report`
method still uses bare `print()`.
**Fix:** Replace the three `print()` calls in `run_report` with `logger.info(...)`
so they flow through dictConfig. If CLI-friendly console output is still desired,
have the CLI layer (`app.py:run`) emit via `console` instead. The
`get_table_sample` `print()` calls (`db_service.py:308-309`) should similarly use
`logger.debug`.
**Verify:** `rg -n '^\s*print\(' src/mko_data_cleaner/core/` → 0 matches after
fix (excluding `progress_bar` in `utils.py:34` which is CLI-UX by design).
**Related:** OPS-006 — both are observability signal-loss paths.

### OPS-006 — [MEDIUM] `yaml_to_dict` silently returns `{}` on parse error; log_config failure is unobserved

**Anchor:** `src/mko_data_cleaner/core/utils.py::lines 131-149`; `core/app_service.py::lines 43-44`
**Checked:** static (source read + type inference)
**Why this band:** A malformed `log_config.yaml` does not produce a validated,
observable failure — it produces a pydantic `ValidationError` deep in the
`LoggingSettings` construction that is not caught, not logged through the
configured pipeline, and not surfaced as a meaningful error to the operator.
**Problem:** `yaml_to_dict` (`utils.py:131-149`) catches `yaml.YAMLError` and
`FileNotFoundError`, logs via `logger.error`, and returns `{}`. When called for
`log_config.yaml` (`app_service.py:43-44`: `LoggingSettings(**yaml_to_dict(self.app_paths.log_config))`),
a parse error → `{}` → `LoggingSettings(version=1, disable_existing_loggers=False)`
→ pydantic `ValidationError` (because `formatters`, `handlers`, `loggers`, `root`
are required — `models.py:54-57`). This `ValidationError` propagates uncaught
from `get_app_service()`, bypassing the configured logging pipeline entirely
(since logging isn't configured yet). The operator sees a traceback, not a
meaningful "your log_config.yaml is malformed" message.

The asymmetry is sharp: `app_config.yaml` has a `try/except DataValidationError →
ConfigError` wrapper at `app_service.py:49-52`; `log_config.yaml` has **none**.
**Repro:** Corrupt `log_config.yaml` with invalid YAML. `yaml_to_dict` returns
`{}`. `LoggingSettings(**{})` raises `pydantic.ValidationError` at
`app_service.py:44` (inside the `log_config` cached property). Since
`dictConfig` runs *after* the property is constructed (`app_service.py:337`),
the error is never routed through logging.
**Impact:** A corrupted `log_config.yaml` after `init` produces an opaque
pydantic `ValidationError` traceback rather than an actionable
"fix your log_config.yaml" message. Worse, since logging is not yet configured
at this point, the error is not logged through the intended pipeline.
**Root cause:** Asymmetric error handling: `app_config` has a
`DataValidationError` → `ConfigError` wrapper; `log_config` has none.
**Fix:** Apply the same `try/except ValidationError → ConfigError` pattern to
`log_config` construction in `app_service.py:43-44`, or validate the dictConfig
dict before passing to `dictConfig`.
**Verify:** New test needed: `test_log_config_malformed_yaml_raises_config_error`.
**Related:** OPS-005 — both are signal-loss paths in the observability surface.

### OPS-007 — [LOW] Documentation drift: architecture.md and current-behavior.md describe pre-rework state

**Anchor:** `docs/00-overview/architecture.md::lines 165-167, 178, 161-164`;
`docs/00-overview/current-behavior.md::line 35`
**Checked:** static (code-verified)
**Why this band:** Operational documentation that describes a system other than
the one shipped is a finding per Phase 12 Block 6 ("documentation describing a
system other than the one deployed is itself a finding"). An operator following
these docs will be misinformed.
**Problem:** Four stale claims:
1. `architecture.md:165-167`: "`__init__.py` is currently **empty**; top-level
   re-exports fail." → `__init__.py` now has full re-exports
   (`__init__.py:16-22`, `__all__` with 5 names). `test_top_level_exports`
   (`test_app.py:384-395`) confirms exactly 5 names are exported and
   `from mko_data_cleaner import process_data` works.
2. `architecture.md:178`: "`rich` — Console output (**imported but **not
   declared** — D-5)." → `rich>=13.0.0` **is** declared in
   `pyproject.toml:27`. D-5 is resolved.
3. `architecture.md:161-164`: "The README still shows the old
   `core.app_service.app_service` import, which no longer exists." → README has
   no such reference (grep README for `app_service` → 0 matches); README uses
   `from mko_data_cleaner.app import process_data` (`README.md:65`).
4. `current-behavior.md:35`: "`initialize_settings` known gap: `settings_dir`
   is accepted but not forwarded to `init_project`." → `app.py:148-150` forwards
   `settings_dir` to `init_project`; `init_service.py:44-50` honors it;
   `test_initialize_settings_threads_settings_dir`
   (`test_app.py:318-324`) confirms it writes to `settings_dir / "settings"`.
   The gap is **closed.**
**Repro:** `rg -n '__init__.py.*empty' docs/00-overview/architecture.md` → line
165. `rg -n 'mko_data_cleaner.app import process_data' README.md` → line 65.
`rg -n 'not declared' docs/00-overview/architecture.md` → line 178.
`rg -n 'settings_dir' src/mko_data_cleaner/app.py core/init_service.py` → matches
at all four forwarding sites.
**Impact:** An operator reading architecture.md believes `__init__.py` is empty
and that `rich` is un-declared, leading to incorrect import assumptions and
unnecessary dependency-audit work. A reader of `current-behavior.md` believes
`settings_dir` is not forwarded, contradicting the tested code path.
**Root cause:** Documentation was not updated alongside commits `37140aa`
(remove stale references, expose `__version__`), `5c4bfdd` (return
`ReportResult`, thread `settings_dir`), and `76b1fc8` (remove import-time init).
**Fix:** Update `architecture.md:165-168` to reflect current re-exports; update
`architecture.md:178` to remove the "not declared" annotation; update
`architecture.md:161-164` to note README is now correct; update
`current-behavior.md:35` to remove the "Known gap" note.
**Verify:** `grep -rn 'currently.*empty' docs/00-overview/architecture.md` → 0
matches after fix; `grep -rn 'not declared' docs/00-overview/architecture.md` →
0 matches.

### OPS-008 — [LOW] Phase 12 report-output `OPS-` prefix warning is phantom

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md::lines 169-171`
**Checked:** static (repo-wide grep)
**Why this band:** A phantom collision warning causes the auditor to either
invent a new namespace (fragmenting findings) or waste time searching for a
collision that does not exist. It is documentation drift in the phase file
itself.
**Problem:** Line 169 states "Finding-ID prefix: `OPS-` — already in use in
shipped source, tests, and workflows from a prior cycle." A repo-wide grep for
`OPS-` in `src/**/**/*.py`, `tests/*.py`, and `.github/workflows/*.yml` returns
**zero matches**. The prefix appears only in:
- `.ai/reports/phase-11-auditor-report.md:350` (mentioning phantom-prefix patterns
  alongside `QLT-` and `PERF-`)
- `.ai/reports/phase-13-researcher-recommendations.md:511` (same — phantom-prefix
  context)
- The Phase 12 file itself (line 169)
**Repro:** `rg -rn 'OPS-' src/ tests/ .github/` → no matches.
**Impact:** An auditor may mint a second prefix (e.g., `OPS2-`) to avoid a
non-existent collision, fragmenting the findings namespace.
**Root cause:** The Phase 12 file was written in an earlier cycle when phantom
prefixes were a known issue; the premise was never re-verified against the
current tree.
**Fix:** Line 169: remove the "already in use" warning; replace with "verify no
collision before minting (grep `OPS-` across `src/`, `tests/`, and `.github/`)."
**Verify:** `rg -rn 'OPS-' src/ tests/ .github/` → 0 matches.

### OPS-009 — [LOW] CI does not enforce mypy despite configuring it

**Anchor:** `.github/workflows/ci.yml::lines 26-27`; `pyproject.toml:140-150`
**Checked:** static (workflow read + grep)
**Why this band:** A type checker configured but never executed is a control that
"believes it is active" but is not on the path that decides whether a change
ships. This maps to Phase 12 Block 1's concern about declared scope vs. actual
scope.
**Problem:** `[tool.mypy]` is configured in `pyproject.toml:140-150` with
`strict_optional`, `disallow_any_generics`, `check_untyped_defs`, etc. The CI
workflow (`ci.yml:25-27`) runs `uv sync`, `uv run ruff check src/ tests/`, and
`uv run pytest` — mypy is never invoked. Type-checking is configured but not
enforced as a gate.
**Repro:** `rg -n 'mypy' .github/workflows/ci.yml` → 0 matches.
`rg -n 'mypy' pyproject.toml` → 1 match (line 140, `[tool.mypy]`).
**Impact:** Type violations can ship without detection. The control exists in
configuration but is absent from the enforcement path.
**Root cause:** CI was configured with minimal gates (ruff + pytest) and never
extended to match the configured toolbelt.
**Fix:** Add `uv run mypy src/` as a step in the `test` job of `ci.yml`,
between `ruff check` and `pytest`.
**Verify:** `rg -n 'mypy' .github/workflows/ci.yml` → ≥1 match after fix.

---

## 4. Report Output Settings

The Phase 12 §Report Output (current lines 163–175) is assessed against the
findings template (`.ai/audit/templates/audit-findings.md`) and repo convention.

| Item | Setting | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/12-production-ops/findings.md` | **Keep** | No prior findings directory exists at this path (`Test-Path .ai\audit\12-production-ops → False`). |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template; front matter defines `severity_taxonomy` and `code_ref`. |
| ID prefix | `OPS-` | **Retain, DISPROVE collision** | §4.1 below. Zero `OPS-NNN` finding identifiers found anywhere in the repo. |
| problems-only | `true` | **Keep** | All nine confirmed findings are code-verified defects or documentation drift — not passing checks. |
| per-pass lines | ≤100, incremental append | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` | **Keep** | Verbatim. |
| Shipped-test-asserts-current-behaviour | Phase 12 Block 1 | **Keep** | Applies as a guard: a finding is blocked from remediation if a shipped test asserts the current (defective) behaviour. |

### 4.1 The `OPS-` prefix collision warning — DISPROVEN

Phase 12 line 169-171 states:

> "Finding-ID prefix: `OPS-` — already in use in shipped source, tests, and
> workflows from a prior cycle; check for a collision before minting an ID and
> report it rather than creating a second namespace."

**This claim is disproven by the working tree (`b390d04`):**

- A repo-wide grep for `OPS-` across `src/`, `tests/`, `.github/`, and
  `.kilo/` finds **zero** `OPS-NNN` finding identifiers in any findings document,
  test file, shipped source, or workflow.
- The string `OPS-` appears only in narrative: the Phase 12 phase file's own
  warning text (lines 169-171), in this auditor report's §5 Report Output
  (table + §5.1), in `phase-11-auditor-report.md:350` (listing phantom-prefix
  patterns alongside `QLT-` and `PERF-`), and in
  `phase-13-researcher-recommendations.md:511` (same context). **None** are
  finding IDs.
- No findings directory `.ai/audit/12-production-ops/` exists (absent; only
  `templates/` and `problems/` exist under `.ai/audit/`).

**Recommendation:** Retain the `OPS-` prefix (it is the Phase 12 prefix per the
template convention and is unique among real findings — zero collisions found).
**Correct line 169** to state: "Finding-ID prefix: `OPS-` — verify no collision
with existing finding IDs before minting (grep `OPS-` across `src/`, `tests/`,
`.github/`)."

---

## 5. Severity Taxonomy (rebased)

The current taxonomy (lines 138–161) is sound in principle ("grade by effect
and blast radius, not by mechanism name; rate what is true now"). The
illustrative examples, however, carry async/security framing that does not
match the synchronous, local-only codebase. All examples are reframed below to
use CI/dictConfig/observability references drawn from the actual findings.

| Band | Current example (async/security-flavored) | Reworded (sync, ops-grounded) | Action |
|---|---|---|---|
| CRITICAL | *(not populated — no async equivalent)* | *(empty is valid — no finding meets this bar)* | KEEP empty |
| HIGH | "a failure state with no counter and no log" | "a documentation claim about the deployment model that is false, so a downstream auditor is misdirected" (OPS-001) | REPLACE example |
| HIGH | *(async: "gate loaded and green while examining nothing")* | "a packaging-identity claim that names the wrong artifact, so the auditor verifies against a non-existent entry point" (OPS-003) | REPLACE example |
| HIGH | *(async: "rollback that does not restore version/schema")* | *(remove — no async/rollback example exists in findings)* | REMOVE |
| MEDIUM | *(async: "control present and effective in part")* | "a control configured but not enforced on the shipping path" (OPS-009) | REPLACE example |
| MEDIUM | "degraded operability with a workaround" | "a lifecycle signal emitted via `print()` that bypasses the configured log pipeline" (OPS-005) | KEEP, reword to cite OPS-005 |
| LOW | "documentation and hygiene drift" | "a doc-vs-code drift in the operational documentation" (OPS-007) | KEEP, reword to cite OPS-007 |

**Rewritten severity taxonomy text (recommended):**

> Grade by **effect and blast radius**, not by mechanism name; rate what is true
> now, not the worst consequence if triggered. The bands are effect classes,
> not a closed list of mechanisms, so a defect whose mechanism is not named here
> still lands by the consequence it is producing.
>
> - **CRITICAL** — a control believed to be active that is not, on the path that
>   decides whether a change ships or a release publishes, so that nothing
>   distinguishes a vetted subject from an unvetted one for a reviewer or for
>   the system's own records. An empty band is a valid outcome — do not populate
>   it with a hypothetical, and do not promote an item for sounding alarming.
> - **HIGH** — a state the system is in **now** where the failure is neither
>   survivable nor observable **to the auditor**: a false deployment-model claim
>   that misdirects the entire audit (OPS-001); a packaging-identity claim that
>   names the wrong artifact so the auditor verifies the wrong entry point
>   (OPS-003); a logging-config claim that points the auditor at the wrong
>   execution path (OPS-004).
> - **MEDIUM** — degraded operability with a workaround, or a control configured
>   but not enforced on the shipping path: a control present and effective in
>   part (mypy configured but absent from CI — OPS-009); a lifecycle signal
>   emitted via `print()` that bypasses the configured log pipeline (OPS-005); a
>   config load path that silently falls back instead of failing observable
>   (OPS-006); scope-boundary citations to deleted phases that waste auditor
>   effort (OPS-002).
> - **LOW** — documentation and hygiene drift with no runtime consequence today:
>   a doc-vs-code drift in the operational documentation (OPS-007); a phantom
>   prefix-collision warning in the phase file (OPS-008).

---

## 6. Scope / Citation Cleanup

### 6.1 Phases cited in Phase 12 scope boundary (lines 22–28)

The current scope boundary reads:

> Deferred by this phase: import-time side effects and bootstrap **(01)**; settings values and credentials **(02)**; Semaphore/Queue/worker-pool concurrency **(03)**; period resolution and data extraction **(04)**; template validation **(05)**; the outbound API call contract **(09)**; code-quality discipline **(10)**; test-suite adequacy **(11)**; concurrency ceilings and retry cost **(13)**.

| Cited phase | Current citation text | Phase status | Action |
|---|---|---|---|
| (03) | "Semaphore/Queue/worker-pool concurrency" | **Deleted** — `phase-03-auditor-report.md` recommends DELETE; all 7 blocks Fit:None | **REMOVE** — async primitives never existed |
| (09) | "the outbound API call contract" | **Deleted** — `phase-09-auditor-report.md` recommends DELETE; no API/credentials/async exist | **REMOVE** — no outbound API exists |
| (04) | "period resolution and data extraction" | Re-based (sync) | **KEEP, reword** — Phase 12 does not own "period resolution" (no scheduled tasks exist). Reword to defer "data-pipeline stage ordering and extraction" (Phase 04 owns ordering; Phase 12 audits CI/observability of those stages). |
| (01), (02), (05), (10), (11), (13) | — | Exist | **KEEP** — no async framing |

> **Recommended reworked scope boundary (replaces lines 22–28):**
>
> > **Scope boundaries** — taken in: CI/CD gates, packaging, and observability.
> > Deferred by this phase: import-time side effects and bootstrap **(01)**;
> > settings values and credentials **(02)**; data-pipeline stage ordering and
> > extraction **(04)**; template validation **(05)**; code-quality discipline
> > **(10)**; test-suite adequacy **(11)**; concurrency ceilings and retry cost
> > **(13)**. There is no edge tier, no transport security boundary, no
> > health-probe contract, and no backup to this phase.
>
> *(01) remains valid — Phase 01 already audited and fixed import-time init
> (the `dictConfig`-at-import-time premise is now stale; see OPS-004). (13) is
> a forward reference to the in-rework Phase 13 — reword "retry cost" to
> "pipeline latency measurement" when that phase is rebased.*

### 6.2 Phases that CITE (12) — informational

| Phase | Line(s) | Citation text | Action needed when that phase is re-based |
|---|---|---|---|
| Phase 05 | — | (config value consistency) | None — no async framing |
| Phase 10 | — | (dead code sweep) | None — no async framing |
| Phase 11 | 350 | phantom-prefix discussion (lists `OPS-`) | None — already debunked |

---

## 7. Edit Delta — Line-by-Line Rewrite of the 175-line File

All references are to the **current** `.kilo/commands/audit/phases/12-audit-production-ops.md`
(175 lines, verified by Read).

| # | Lines | Action | Replacement (concise) | Rationale |
|---|---|---|---|---|
| 1 | 1–7 (front matter) | **Modify** — add fields | Keep `name`, `status`, `validated`, `executor`, `problems-only: true`. Add `code_ref: "b390d04"`, `phase_file_lines: 175`, `source: ".ai/reports/phase-12-auditor-report.md"`, `confidence: HIGH`. | Aligns with findings template front-matter (`audit-findings.md` lines 2-8). |
| 2 | 9 (H1) | **Keep** | `# Phase 12 — Production Operations, Security & Observability` | Title is not async-framed; no change. |
| 3 | 11–17 (Purpose) | **Modify** — reword premises | Replace "standalone **async** CLI tool" → "synchronous, single-shot CLI tool and Python library"; replace "no **database**" → "a disposable SQLite database created and deleted per invocation"; keep "no containers, no network, no reverse proxy, no health probes, no backup strategy." | OPS-001. async→sync and no-database→SQLite. |
| 4 | 18–19 (Purpose cont.) | **Modify** — correct call chain | Replace `app.py → AppService.run_report → CSVWorker → DBWorker / MappingDict` with the verified chain (same, but note no async). | Aligns Purpose with the actual synchronous call chain. |
| 5 | 22–28 (Scope Boundaries) | **Modify** — remove (03), (09); reword (04) | Per §6.1 above. Strip "(03)" and "(09)" citations; reword "(04)" from "period resolution and data extraction" to "data-pipeline stage ordering and extraction." | OPS-002. Deleted phases no longer exist. |
| 6 | 32–39 (Audit Blocks intro) | **Keep** | "Each block is independent..." methodology. | Generic; no async framing. |
| 7 | 41–55 (Block 1) | **Modify** — add mypy gap note | Append a note: "Gap: `mypy` is configured (`pyproject.toml:140-150`) but absent from `ci.yml:26-27` — see OPS-009." | Surfaces the declared-but-unenforced control as a finding-worthy gap. |
| 8 | 57–65 (Block 2) | **Keep** | CI/CD pipeline concept. | Already sync-framed. |
| 9 | 67–80 (Block 3) | **Keep** | Gate-vs-report concept. | Already sync-framed. |
| 10 | 82–93 (Block 4) | **Modify** — correct artifact identity | Line 86: replace `mko-get-mediascope` → `mko-data-cleaner`. Add note: "`python -m mko_data_cleaner.app` (not `python -m mko_data_cleaner`) is the module entry point — no package `__main__.py` exists (see OPS-003)." | OPS-003. Stale console script + missing `__main__.py`. |
| 11 | 95–107 (Block 5) | **Modify** — correct dictConfig premise | Line 98: replace "at import time" → "inside the `get_app_service()` factory, after the settings gate passes" (`app_service.py:337`). Remove the "eager file-handle creation" sub-question (no longer applies); replace with "does `dictConfig` validate config before applying?" | OPS-004. dictConfig premise is stale. |
| 12 | 109–120 (Block 6) | **Keep** | Runbooks & documentation concept. | Already sync-framed; OPS-007 surfaces the drift it finds. |
| 13 | 122–134 (Block 7) | **Keep** | Release & rollback concept. | Already sync-framed. |
| 14 | 138–161 (Severity Taxonomy) | **Modify** — replace examples | Per §5 above. Keep the principle + 4 bands + "empty band is valid." Replace async/security-flavored examples with CI/dictConfig/observability examples drawn from OPS-001 through OPS-009. Remove the HIGH example about "rollback that does not restore version/schema" (no rollback finding exists). | Aligns taxonomy with the synchronous codebase. |
| 15 | 163–175 (Report Output) | **Modify** — correct OPS- warning | Line 169: replace "already in use in shipped source, tests, and workflows from a prior cycle" with "Phase 12 finding-ID prefix per the findings template convention; no `OPS-NNN` collision found in the current tree (`b390d04`)." Keep all other items (path, template, problems-only, ≤100 lines, empty state). | OPS-008. Phantom collision warning. |

**Application order:** Edit 1 (front matter) is independent — do first. Then
Edits 3–5 (Purpose + Scope Boundaries, foundational framing) → Edits 7–13
(blocks 1–7, each self-contained) → Edit 14 (severity taxonomy) → Edit 15
(Report Output correction). Edits 7–13 may be applied in parallel (sections
are self-contained).

**Do not touch:** `problems-only: true`; the "≤100-line passes" rule; the
empty-state wording ("No problems found in this phase."); the front-matter `name`
and `status` fields.

---

## 8. Findings-to-Scope Mapping (for the findings-file author)

Findings are sorted by severity (HIGH first, then MEDIUM, then LOW) per the
findings template's "index order IS the work order" rule. `OPS-` prefix
retained (collision disproven, §4.1).

| Finding ID | Severity | Anchor (primary) | Phase-owned | Notes |
|---|---|---|---|---|
| OPS-001 | HIGH | `12-audit-production-ops.md:15-17` | Phase 12 Purpose | Unblocks all downstream Fit assessments. Trivial text fix. |
| OPS-003 | HIGH | `12-audit-production-ops.md:86` | Phase 12 Block 4 | Packaging identity correctness. Trivial text fix. |
| OPS-004 | HIGH | `12-audit-production-ops.md:98-99` | Phase 12 Block 5 | Unblocks OPS-005, OPS-006 re-assessment. Trivial text fix. |
| OPS-002 | MEDIUM | `12-audit-production-ops.md:23,26` | Phase 12 Scope | Citation accuracy. Trivial text fix. |
| OPS-005 | MEDIUM | `core/app_service.py:113,257,272` | Phase 12 Block 5 + Phase 06 | Observability signal integrity. Small code fix (print→logger). |
| OPS-006 | MEDIUM | `core/utils.py:131-149`; `core/app_service.py:43-44` | Phase 12 Block 5 | ConfigError symmetry for logging config. Medium code fix. |
| OPS-007 | LOW | `docs/00-overview/architecture.md:165-168,178,161-164`; `docs/current-behavior.md:35` | Phase 12 Block 6 | Operator-facing doc accuracy. Small doc fix. |
| OPS-008 | LOW | `12-audit-production-ops.md:169-171` | Phase 12 Report Output | Finding-namespace hygiene. Trivial text fix. |
| OPS-009 | LOW | `.github/workflows/ci.yml:26-27`; `pyproject.toml:140-150` | Phase 12 Block 1 | Gate enforcement completeness. Trivial CI fix. |

### Dependencies among findings

- **OPS-004** (dictConfig-at-import fix) must land before **OPS-005** and **OPS-006**
  can be accurately re-assessed — the auditor must first correct the false
  "import time" premise to inspect the right location.
- **OPS-001** (Purpose) and **OPS-003** (console script) are independent text fixes.
- **OPS-002** (scope boundaries) is independent.
- **OPS-007** (doc drift) and **OPS-008** (phantom OPS-) are independent.
- **OPS-009** (mypy in CI) is independent.

### Rollout safety

All Phase 12 findings are either (a) text/documentation fixes with zero runtime
impact, or (b) observability signal-integrity improvements that do not change
behavior — they route existing output through logging instead of stdout. No
finding changes a public API, a config schema, or the SQLite processing engine.
The `init_project` idempotency gate (Block 7) is already verified correct by
`test_app.py` tests (`test_init_project_*`, `test_initialize_settings_*`).

---

## 9. Evidence Grounding

Every recommendation in this document traces to a verified current-source fact
at `b390d04`:

- **Zero async:** `rg -n 'async def\|asyncio\|asyncio.run' src/mko_data_cleaner/` →
  0 matches; only `pyproject.toml:160` has a commented-out `# "--asyncio-mode=auto"`.
- **SQLite, not "no database":** `app_service.py:146` (`DBWorker` CM);
  `db_service.py` (788 lines, SQLite throughout); `architecture.md:136,149`;
  `current-behavior.md:78,194-196` ("deleted at the end of the run").
- **dictConfig in factory:** `app_service.py:337` (inside `get_app_service`);
  `rg -n 'dictConfig' src/mko_data_cleaner/app.py` → 0 matches.
- **Console script:** `pyproject.toml:46` (`mko-data-cleaner =
  "mko_data_cleaner.app:app"`); `rg -n 'mko-get-mediascope' pyproject.toml src/`
  → 0 matches in shipped code.
- **No `__main__.py`:** `glob src/mko_data_cleaner/__main__.py` → not found;
  only `app.py:153` (`if __name__ == "__main__": app()`) provides entry.
- **mypy not in CI:** `rg -n 'mypy' .github/workflows/ci.yml` → 0 matches;
  `[tool.mypy]` present at `pyproject.toml:140-150`.
- **OPS- phantom:** `rg -rn 'OPS-' src/ tests/ .github/` → 0 matches.
- **print() bypass:** `rg -n '^\s*print\(' src/mko_data_cleaner/core/` →
  matches at `app_service.py:113,257,272`, `utils.py:34,41`,
  `db_service.py:308,309`.
- **yaml_to_dict silent fallback:** `utils.py:131-149` (catches `YAMLError` +
  `FileNotFoundError`, returns `{}`); `app_service.py:43-44` uses it for
  `log_config`; `models.py:51-57` (`LoggingSettings` with required fields).
- **Doc drift:** `__init__.py:16-22` (full re-exports, `__all__` with 5 names);
  `pyproject.toml:27` (`rich` declared); `app.py:148-150` (forwards
  `settings_dir`); `init_service.py:44-50` (honors `settings_dir`);
  `README.md:65` (`from mko_data_cleaner.app import process_data`).
- **Idempotent init:** `init_service.py:23-24` (skips existing unless `force`);
  `init_service.py:13` (`exist_ok=True`); tested at `test_app.py:318-324`.

*End of recommendation. Evidence-grounded: every structural recommendation traces
to a verified current-source fact at `b390d04`. All file/line references
confirmed by direct inspection. No source files were modified by this report.*
