---
phase: "12"
phase_name: "Production Operations, Security & Observability"
phase_file: ".kilo/commands/audit/phases/12-audit-production-ops.md"
report_status: "draft"
scope: >-
  rework assessment of Phase 12 against a synchronous, single-shot,
  local-only CLI + library with no event loop, no network, no credentials,
  no containers, and a disposable per-invocation SQLite database.
code_ref: "b390d04"
recommendation: REBASE
---

# Audit Report — Phase 12 Production Operations (Rework Assessment)

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **synchronous, single-shot, local-only** CLI tool and Python
library that post-processes Mediascope TV advertising exports received as **files
on disk**. There is no outbound API, no credentials, no network, no asyncio, no
event loop, no containers, no reverse proxy, no health probes, and no backup
strategy anywhere in the architecture. The runtime is a straight synchronous call
chain:

```
app.py → AppService.run_report → CSVWorker → DBWorker / MappingDict
```

The only durable resource is a disposable SQLite database file created per
invocation and deleted on `DBWorker.__exit__` (WAL checkpoint + file deletion).
Packaging is setuptools/PyPA via `pyproject.toml`; the console script is
`mko-data-cleaner` (`pyproject.toml:46`), and `python -m mko_data_cleaner.app`
works via the `if __name__ == "__main__"` guard. There is **no `__main__.py`** at
the package root, so `python -m mko_data_cleaner` (without `.app`) fails.

CI/CD exists as a single GitHub Actions workflow
(`.github/workflows/ci.yml`, committed `1562fc6`) with four jobs — `test`
(ruff + pytest), `build` (`python -m build` + `twine check` +
`check-wheel-contents` + wheel-content assertions), `install-smoke` (fresh-venv
install + smoke test), and `notebook` (example-notebook execution). No mypy, no
secret scan, no dependency audit, no coverage gate is wired into CI.

Code-verified facts (`src/mko_data_cleaner/`, `tests/`, `pyproject.toml`,
`.github/workflows/ci.yml`):

- **No async.** Grep for `asyncio`, `async def`, `asyncio.run` across `src/`
  returns **0 matches**. The only `async`-related reference is a commented-out
  `#    "--asyncio-mode=auto"` in `pyproject.toml:160`.
- **SQLite, not "no database".** `app_service.py:146-151` opens a `DBWorker`
  context manager over a SQLite file (`db_service.py`); `architecture.md:136`
  and `data-flow.md:35` confirm "The database is created per run and deleted
  afterwards." The Phase 12 Purpose's "no database" claim is false.
- **dictConfig moved to factory.** `app_service.py:337` calls
  `logging.config.dictConfig(...)` inside `get_app_service()`, **not** at import
  time. `app.py` performs no logging configuration (Phase 01 Block 1 remediation,
  committed `76b1fc8`). The Phase 12 Purpose's "dictConfig at import time" claim
  is stale.
- **Console script identity.** `pyproject.toml:46`:
  `mko-data-cleaner = "mko_data_cleaner.app:app"`. The Phase 12 Block 4 claim of
  `mko-get-mediascope` is stale.
- **`python -m` entry point.** No `__main__.py` exists at
  `src/mko_data_cleaner/__main__.py`; only `app.py:153` (`if __name__ == "__main__":
  app()`) provides `python -m mko_data_cleaner.app`. The Phase 12 Block 4 claim
  of a generic `python -m` entry point is partially stale.
- **OPS- prefix is phantom.** Grep for `OPS-` across `src/`**/`**/*.py` and
  `tests/*.py` and `.github/workflows/*.yml` returns **zero matches**. The
  prefix appears only in prior audit reports discussing phantom prefixes
  (`phase-11-auditor-report.md:350`,
  `phase-13-researcher-recommendations.md:511`) and in the Phase 12 file itself
  (`12-audit-production-ops.md:169`). The Phase 12 report-output warning is
  itself phantom — the same class as the `QLT-`/`TST-`/`PERF-` warnings already
  debunked in prior phases.

---

## 2. Purpose Rework

**Current text (lines 13–19):**

> This is a standalone async CLI tool distributed via `uv` and PyPA packaging —
> there are no containers, no database, no reverse proxy, no health probes,
> and no backup strategy.

**Two false claims:**

| Claim | Reality | Evidence | Action |
|---|---|---|---|
| "standalone **async** CLI tool" | synchronous; no `asyncio` anywhere in `src/` | grep: 0 matches; `app.py` uses plain `typer.Typer` synchronous commands; `app_service.py` call chain is purely synchronous | remove "async" |
| "no **database**" | SQLite, disposable per-invocation | `app_service.py:146` (`DBWorker` CM); `db_service.py`; `architecture.md:136`; `data-flow.md:35` | replace "no database" → "a disposable SQLite database created and deleted per run" |

**Verdict:** Reword to
*"This is a synchronous, single-shot CLI tool and Python library distributed via
PyPA packaging — there are no containers, no network, no reverse proxy, no
health probes, and no backup strategy; the only persistent runtime artifact is a
disposable SQLite database created and deleted per invocation."*
**Effort: trivial.**

---

## 3. Scope Boundary Cleanup

**Current text (lines 22–28):**

> Deferred by this phase: …Semaphore/Queue/worker-pool concurrency **(03)**; …
> the outbound API call contract **(09)**; … concurrency ceilings and retry
> cost **(13)**.

**Stale references — two cite deleted phases:**

| Cited phase | Current disposition | Evidence | Action |
|---|---|---|---|
| (03) Semaphore/Queue/worker-pool concurrency | **DELETED** | `phase-03-auditor-report.md` — all 7 blocks Fit:None, async premise entirely fictional | remove (03) citation |
| (09) outbound API call contract | **DELETED** | `phase-09-auditor-report.md` — all 7 blocks Fit:None, no API/credentials/async exist | remove (09) citation |

Reference (13) — "concurrency ceilings and retry cost" — is async-framed and
owned by the Phase 13 rework; it is not deleted but is in-rework, so the citation
is valid as a forward reference.

**Verdict:** Strip the `(03)` and `(09)` deflections from the Scope Boundaries
paragraph. The remaining citations ((01), (02), (04), (05), (10), (11), (13)) are
to phases that exist (though several are themselves in rework).
**Effort: trivial.**

---

## 4. Per-Block Assessment (1–7)

Each block is assessed for **Fit** (does the concept apply to the real
architecture), **Outdated** (async/stale references that need fixing), and
**Replacement** (what the synchronous analog is, or what wording change is needed).

### Block 1 — A control that runs, reports clean, and examines nothing

**Fit: Full.** The concept — inventory every automated control, separate
existence / declared scope / actual scope, and judge whether declared and actual
scope differ — is framework-agnostic and directly applies.

**Outdated:** None specific to async. However, the audit reveals a **control gap**:
the CI workflow (`ci.yml:26-27`) runs `ruff check` and `pytest` but does **not**
run `mypy` (despite `[tool.mypy]` config in `pyproject.toml:140-150`) and does
**not** run any secret/credential scanner (no `bandit`, `pip-audit`, `trivy`, or
`detect-secrets` configured). This is a finding-worthy gap: a type checker is
configured but never executed, so a declared control (type safety) goes unenforced.

**Replacement:** No synchronous analog change needed — the block is already
synchronous. The finding is the missing mypy gate in CI.

### Block 2 — CI/CD pipeline & gates

**Fit: Full.** A CI/CD pipeline exists (`.github/workflows/ci.yml`, 4 jobs: `test`,
`build`, `install-smoke`, `notebook`), each gated on `push: [main]` and
`pull_request`. The gates are effective and cannot be bypassed: `build` depends on
`test` passing (implicitly via `needs: build` on `install-smoke`), and the
wheel-content assertions in `build` enforce that YAML templates are shipped and
no `mko_get_mediascope_data` name leaks into the wheel (`ci.yml:45`).

**Outdated:** None specific to async. The Phase 12 Purpose's "no database" and
"async" framing does not bleed into this block — the block text itself is clean.

**Replacement:** No re-basing needed for this block.

### Block 3 — What makes a check a gate vs. a report

**Fit: Full.** The concept — which checks can prevent a release, what triggers
automation, what dependency makes a verdict reach the artifact — applies directly
to the GitHub Actions workflow. All four jobs are gating: a failing `test` or
`build` job prevents the `install-smoke` job from running (via `needs: build`),
and a failing `install-smoke` prevents artifact publication (the `dist` artifact
is only uploaded on success).

**Outdated:** None.

**Replacement:** None.

### Block 4 — Packaging & artifact identity

**Fit: Full** for the *concept* (inventory external inputs, verify three
identifiers, check version immutability). **Partially outdated** in the *text*:

- **Stale console script name (line 86):** The phase file says
  `mko-get-mediascope` console script. The actual script is
  `mko-data-cleaner` (`pyproject.toml:46`). The stale name is a vestige of the
  prior project incarnation (`mko_get_mediascope_data`).
- **Stale `python -m` claim:** The phase file lists `python -m` as an entry-point
  form. There is **no `__main__.py`** at the package root
  (`glob src/mko_data_cleaner/__main__.py` → not found). `python -m
  mko_data_cleaner` fails; only `python -m mko_data_cleaner.app` works via
  `app.py:153`.

**Three identifiers (all consistent):**

| Identifier | Value | Source |
|---|---|---|
| pyproject name | `mko_data_cleaner` | `pyproject.toml:6` |
| import name | `mko_data_cleaner` | `src/mko_data_cleaner/__init__.py:1` |
| console script | `mko-data-cleaner` | `pyproject.toml:46` |

**External-input inventory:**

| External input | Pin verdict | Integrity verdict |
|---|---|---|
| `urllib3>=2.1.0` | Not pinned to exact version; not used in `src/` (grep: 0 imports) | Unused dependency — drift |
| `pydantic-settings>=2.12.0` | Not pinned; not used in `src/` (grep: 0 imports) | Unused dependency — drift |
| `pandas>=2.2.0` | Not pinned; only a doc comment in `db_service.py:512` | Unused dependency — drift |
| `rich>=13.0.0` | Not pinned; used (`app.py:10`) | Used; integrity OK |

**Version mutability:** Version is hardcoded `0.0.1` in `pyproject.toml:7` with
no git-tag derivation or build-time version stamping. A rollback to a prior
`uv.lock` does not pin an exact package version — it pins transitive dependency
resolution, not the project version itself.

**Replacement:** Replace `mko-get-mediascope` → `mko-data-cleaner`; add a note
that `python -m mko_data_cleaner.app` (not `python -m mko_data_cleaner`) is the
module entry point due to the missing `__main__.py`. **Effort: trivial.**

### Block 5 — Observability: signals produced and consumed

**Fit: Partial.** The concept — inventory emitted signals, check whether deployed
configuration consumes them, check failure-state observability — applies, but the
*specific premise* in the phase file is stale.

**Outdated:**

- **dictConfig at import time (line 99):** WRONG. `dictConfig` was moved to the
  `get_app_service()` factory (`app_service.py:337`) as part of Phase 01
  remediation (commit `76b1fc8`). Importing `mko_data_cleaner.app` performs no
  logging configuration and no filesystem side effects. The Phase 12 text still
  describes the pre-refactor state.
- **Failure-state counter/log check (line 100):** **Cannot be settled positively.**
  The project has no monitoring infrastructure (no metrics, no counters). In the
  observability surface that *does* exist:
  - `run_report` uses three `print()` calls (`app_service.py:113-116`,
    `app_service.py:257-261`, `app_service.py:272-276`) that bypass the logging
    pipeline entirely — they go to stdout, not through `dictConfig`-configured
    handlers, so they never reach the `rotating_file` handler in
    `log_config.yaml`.
  - `db_service.py:308-309` (`get_table_sample`) uses `print()` instead of
    `logger`.
  - `utils.py:34-41` (`progress_bar`) uses `print()` for progress display —
    acceptable for CLI UX but not structured observability.
  - `yaml_to_dict` (`utils.py:131-149`) returns `{}` silently on `YAMLError` or
    `FileNotFoundError` after logging an error — it logs but does not raise,
    so a malformed `log_config.yaml` produces `{}` → `LoggingSettings(**{})`
    → pydantic `ValidationError` at `app_service.py:43-44`, which propagates as
    an uncaught exception rather than a validated, observable failure.
  - The logging formatter is unstructured:
    `%(asctime)s - %(levelname)s - %(message)s - %(name)s`
    (`log_config.yaml:13`), not JSON or any structured format.
  - No failure counters exist; a quiet tool is indistinguishable from a working
    one at the metrics level (there is no metrics layer).

**Replacement:** Reword "dictConfig at import time" → "dictConfig inside the
`get_app_service()` factory, after the settings gate passes." The synchronous
analog of observability is: structured logging via `dictConfig` + failure-state
`logger.error`/`logger.exception` calls. The finding is that
`print()` calls in `run_report` and `get_table_sample` bypass the logging
pipeline, and `yaml_to_dict` swallows errors without raising.

### Block 6 — Runbooks & operational documentation

**Fit: Full.** The concept — verify every operational claim in install/run/incident
corpus against the executing path — applies directly. Several claims in the docs
are **stale**:

| Doc file:line | Claim | Reality | Evidence |
|---|---|---|---|
| `architecture.md:165-167` | `__init__.py` is "currently **empty**"; top-level re-exports "fail" | `__init__.py` now has full re-exports (`__all__`, `__version__`, `process_data`, `initialize_settings`) | `src/mko_data_cleaner/__init__.py:1-22` |
| `architecture.md:178` | `rich` "imported but **not declared** (D-5)" | `rich>=13.0.0` is declared in `pyproject.toml:27` | `pyproject.toml:27`; `app.py:10` |
| `architecture.md:161-164` | "README still shows the old `core.app_service.app_service` import" | README has no such reference; use `from mko_data_cleaner.app import process_data` | `README.md:65`; grep README for `app_service` → 0 matches |
| `current-behavior.md:35` | `initialize_settings` "does not forward `settings_dir`" (Known gap) | `app.py:148-150` forwards `settings_dir` to `init_project`; `init_service.py:30-53` honors it | `app.py:148-150`; `init_service.py:44-50` |
| `architecture.md:179` | `pydantic-settings` "unused residue" | Verified correct — not imported in `src/` | grep `pydantic_settings\|BaseSettings` → 0 matches |
| `architecture.md:183` | `pandas` "only referenced by unused DB constants" | Only a stray comment at `db_service.py:512`; not imported | grep `import pandas\|from pandas` → 0 matches |

**Outdated:** The Phase 12 Purpose's "async" and "no database" framing does not
directly contaminate Block 6, but the block is meant to audit documentation that
describes a system other than the one deployed — and the documentation has
already drifted on several points above.

**Replacement:** No syntactic re-basing needed. The block is already
synchronous; the findings are documentation-vs-code drift items.

### Block 7 — Release & rollback

**Fit: Full.** The concept — version pinning, lock-file fidelity, config-schema
compatibility, rollback identity, init idempotency — applies directly.

**Verified:**

- `init_project` / `_copy_templates` (`init_service.py:7-27`) is **idempotent
  and non-destructive**: existing files are skipped unless `force=True`
  (`init_service.py:23-24`); `destination.mkdir(parents=True, exist_ok=True)` uses
  `exist_ok` (`init_service.py:13`). ✓
- `pyproject.toml:7`: `version = "0.0.1"` — hardcoded, no build-time stamping.
- `uv.lock` exists (root-level lock file) but CI uses `uv sync` (`ci.yml:25,38,
  69,80`) without `--frozen` or `--locked`, so the lock file is advisory, not
  enforced in CI.

**Outdated:** None specific to async. The block text is clean.

**Replacement:** None needed for the block itself.

---

## 5. Report Output Assessment

| Item | Current (Phase 12 §Report Output) | Verdict |
|---|---|---|
| Findings path | `.ai/audit/12-production-ops/findings.md` | Valid — no prior findings directory exists at this path |
| Finding-ID prefix | `OPS-` (line 169: "already in use in shipped source, tests, and workflows") | **Phantom.** Grep for `OPS-` in `src/**/`**/*.py`, `tests/*.py`, and `.github/workflows/*.yml` returns **zero** matches. The prefix appears only in prior audit reports discussing phantom prefixes (`phase-11-auditor-report.md:350`, `phase-13-researcher-recommendations.md:511`) and in the Phase 12 file itself. No collision exists; minting `OPS-` IDs is safe. Remove the warning. |
| problems-only | `true` (front matter + line 173) | Keep |
| per-pass lines | ≤100, incremental append (line 172) | Keep |
| Empty state | `No problems found in this phase.` (line 174) | Keep |

**Recommendation on `OPS-` prefix:** Use `OPS-` without reservation. The
"already in use" warning is phantom — same class as the `QLT-`, `TST-`, `PERF-`
phantom-prefix warnings already debunked in Phases 10/11/13. Correct the phase
file line 169 to read: "Finding-ID prefix: `OPS-` — verify no collision with
existing finding IDs before minting (grep `OPS-` across `src/`, `tests/`, and
`.github/`)."

---

## 6. Findings Inventory

### OPS-001 — [HIGH] Phase 12 Purpose falsely describes the tool as "async" with "no database"

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md`::lines 13-17
**Checked:** static
**Why this band:** The Purpose text describes a system that does not exist (async,
no database). An auditor operating from this Purpose will waste effort chasing
async bridges, event-loop teardown, and database-less deployment concerns that
have no referent in the codebase.
**Problem:** Lines 15–17 state the tool is "async" and has "no database." The
codebase is synchronous (grep `asyncio|async def` → 0 matches in `src/`) and
uses SQLite as a disposable per-invocation processing engine
(`app_service.py:146`, `db_service.py`).
**Repro:** `rg -n 'async def|asyncio' src/mko_data_cleaner/` → no matches.
`rg -n 'sqlite|SQLite' src/mko_data_cleaner/core/db_service.py | head -5` →
`SQLite` referenced throughout.
**Impact:** Misdirects every downstream auditor; blocks accurate per-block Fit
assessment until corrected.
**Root cause:** The Phase 12 file was written against an older, pre-rework
architecture description that has not been updated since the Phase 01/02
synchronous rebase.
**Fix:** Reword Purpose lines 15–17 to "synchronous, single-shot CLI tool … a
disposable SQLite database created and deleted per invocation."
**Verify:** `rg -n 'asyncio' src/mko_data_cleaner/ app.py core/` → 0 matches after fix.
**Related:** (Phase 01 rebase, commit 76b1fc8; Phase 09 deletion — the same
async/dictConfig drift)

### OPS-002 — [MEDIUM] Scope boundaries cite deleted phases (03) and (09)

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md`::lines 22-28
**Checked:** static
**Why this band:** A scope boundary pointing to a deleted phase is an inaccurate
claim — it tells the auditor to defer a concern to a phase that no longer exists.
An operator cannot act on a deflection to a deleted phase.
**Problem:** Line 23 cites "(03)" for Semaphore/Queue/worker-pool concurrency;
line 26 cites "(09)" for the outbound API call contract. Both Phase 03
(async concurrency) and Phase 09 (external API) were **deleted** —
`phase-03-auditor-report.md` and `phase-09-auditor-report.md` both recommend
DELETE with all blocks Fit:None.
**Repro:** `Test-Path .kilo/commands/audit/phases/03-audit-async-concurrency.md`
→ False. `Test-Path .kilo/commands/audit/phases/09-audit-external-api.md` →
False.
**Impact:** An auditor following these citations will look for a phase file that
does not exist, or will defer async-API concerns to a deleted phase.
**Root cause:** The Phase 12 file was not updated after Phases 03 and 09 were
deleted.
**Fix:** Remove "(03)" and "(09)" from the Scope Boundaries paragraph; keep (01),
(02), (04), (05), (10), (11), (13).
**Verify:** `rg -n '03|09' .kilo/commands/audit/phases/12-audit-production-ops.md`
→ no remaining phase citations to deleted phases.

### OPS-003 — [HIGH] Block 4 references stale console script `mko-get-mediascope`

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md`::line 86
**Checked:** static
**Why this band:** A packaging-identity block that names the wrong console script
undermines its own premise (verify artifact identity). An auditor checking "does
the published artifact coordinate match what the install path expects" starts
from a false name.
**Problem:** Line 86 says "the entry points (`mko-get-mediascope` console script,
`python -m`)". The actual console script is `mko-data-cleaner`
(`pyproject.toml:46`). The name `mko-get-mediascope` /
`mko_get_mediascope_data` is vestigial project-name residue. Phase 09 auditor
report §1.3 and Phase 02 §2 F10 already recorded these as stale-name drift.
**Repro:** `rg -n 'mko-get-mediascope|mko_get_mediascope' pyproject.toml src/`
→ 0 matches in shipped code. The only remaining hit is the Phase 12 file itself
(line 86) and a stale CI assertion that checks for it as a *negative*
(`ci.yml:45`: `assert not stale` for `mko_get_mediascope_data` in wheel — this is
a guard, not a declaration).
**Impact:** Misleads the auditor into checking for a non-existent entry point;
the `python -m` form is also misleading (no `__main__.py` exists).
**Root cause:** The Phase 12 file was not updated after the project rename
settlement (commit `37140aa`, "chore(meta): correct license metadata, declare
rich, expose __version__, remove stale references").
**Fix:** Line 86: replace `mko-get-mediascope` → `mko-data-cleaner`; add note
that `python -m mko_data_cleaner.app` (not `python -m mko_data_cleaner`) is the
module entry due to the missing `__main__.py`.
**Verify:** `rg -n 'mko-data-cleaner' pyproject.toml` → 1 match at line 46.

### OPS-004 — [HIGH] Block 5 falsely claims "dictConfig at import time"

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md`::lines 98-99
**Checked:** static
**Why this band:** The observability block's central premise is wrong, so every
sub-question (eager file-handle creation, logging-config validation at load
time) is grounded in a non-existent execution path. An auditor will inspect import
time for a side effect that no longer exists.
**Problem:** Lines 98–99 state "The logging pipeline is configured via
`dictConfig` at import time." `dictConfig` now runs inside
`get_app_service()` (`app_service.py:337`), after the settings gate passes.
Importing `mko_data_cleaner.app` performs no logging configuration and no
filesystem side effects. This was the Phase 01 Block 1 remediation (commit
`76b1fc8`, "fix(app): remove import-time initialization and gate service creation
on settings").
**Repro:** `rg -n 'dictConfig' src/mko_data_cleaner/` → 1 match at
`app_service.py:337` (inside `get_app_service`), 0 matches in `app.py`.
`test_app.py:81` asserts importing `mko_data_cleaner.app` creates no settings
directory.
**Impact:** Misdirects the auditor to inspect module-level import for
dictConfig; the actual observability concern (unvalidated config, print()
bypassing logging) is never reached because the premise is false.
**Root cause:** The Phase 12 file was not updated after the Phase 01 rebase.
**Fix:** Lines 98–99: replace "at import time" → "inside the
`get_app_service()` factory, after the settings gate."
**Verify:** `rg -n 'dictConfig' src/mko_data_cleaner/app.py` → 0 matches.

### OPS-005 — [HIGH] `print()` calls in `run_report` bypass the dictConfig logging pipeline

**Anchor:** `src/mko_data_cleaner/core/app_service.py`::lines 113, 257, 272
**Checked:** static
**Why this band:** A failure state or lifecycle event that emits only `print()`
to stdout is invisible to the configured `rotating_file` handler
(`log_config.yaml:33-41`), so an operator relying on log output cannot tell
whether the tool is running, stalled, or failed. The logging pipeline is
configured but not consistently consumed.
**Problem:** Three `print()` calls in `run_report`:
- `app_service.py:113-116` — run-start banner
- `app_service.py:257-261` — "Synchronizing the index table…" message
- `app_service.py:272-276` — run-completion banner with timings

None of these reach `logger.info`/`logger.debug`. The configured pipeline
(`log_config.yaml`) has both a `console` handler (StreamHandler → stdout) and a
`rotating_file` handler — but `print()` writes directly to `sys.stdout` without
going through `logging`, so it is **not** subject to the configured formatter,
level filtering, or file rotation. If the console handler is disabled or
reconfigured, these messages vanish silently.
**Repro:** `rg -n '^\s*print\(' src/mko_data_cleaner/core/app_service.py` →
matches at lines 113, 257, 272. None are wrapped in `logger.*`.
**Impact:** Operators who tail `log.log` (the configured rotating file handler)
will **not** see run-start, run-end, or synchronization progress — only the
fine-grained `logger.info`/`logger.debug` entries from CSVWorker/DBWorker. The
high-level lifecycle signal is split across two I/O channels (stdout vs. logging).
**Root cause:** `print()` was used for CLI-friendly user output without
consolidating through the logging pipeline; the Phase 02 fix added CLI-level
`console.print()` (rich Console) in `app.py` but the internal `run_report`
method still uses bare `print()`.
**Fix:** Replace the three `print()` calls in `run_report` with
`logger.info(...)` so they flow through dictConfig. If CLI-friendly console
output is still desired, have the CLI layer (`app.py:run`) emit via `console`
instead. The `get_table_sample` `print()` calls (`db_service.py:308-309`) should
similarly use `logger.debug`.
**Verify:** `rg -n '^\s*print\(' src/mko_data_cleaner/core/` → 0 matches after fix.
**Related:** OPS-006 (yaml_to_dict silent fallback) — both are observability
signal-loss paths.

### OPS-006 — [MEDIUM] `yaml_to_dict` silently returns `{}` on parse error; log_config failure is unobserved

**Anchor:** `src/mko_data_cleaner/core/utils.py`::lines 131-149; `app_service.py:43-44`
**Checked:** static
**Why this band:** A malformed `log_config.yaml` does not produce a validated,
observable failure — it produces a pydantic `ValidationError` deep in the
`LoggingSettings` construction that is not caught, not logged through the
configured pipeline, and not surfaced as a meaningful error to the operator.
**Problem:** `yaml_to_dict` (`utils.py:131-149`) catches `yaml.YAMLError` and
`FileNotFoundError`, logs via `logger.error`, and returns `{}`. When called for
`log_config.yaml` (`app_service.py:43-44`:
`LoggingSettings(**utils.yaml_to_dict(self.app_paths.log_config))`), a parse
error → `{}` → `LoggingSettings(version=1, disable_existing_loggers=False)` →
pydantic `ValidationError` (because `formatters`, `handlers`, `loggers`, `root`
are required). This `ValidationError` propagates uncaught from
`get_app_service()`, bypassing the configured logging pipeline entirely (since
logging isn't configured yet). The operator sees a traceback, not a meaningful
"your log_config.yaml is malformed" message.
For `app_config.yaml` (`app_service.py:46-52`), `DataValidationError` is caught
and re-raised as `ConfigError` — but `log_config.yaml` has no equivalent guard.
**Repro:** If `log_config.yaml` contains invalid YAML, `yaml_to_dict` returns
`{}`; `LoggingSettings(**{})` raises `ValidationError` at `app_service.py:44`.
**Impact:** A corrupted `log_config.yaml` after `init` produces an opaque
pydantic traceback rather than a actionable "fix your log_config.yaml" message.
Worse, since logging is not yet configured at this point, the error is not
logged through the intended pipeline.
**Root cause:** Asymmetric error handling: `app_config` has a
`DataValidationError` → `ConfigError` wrapper; `log_config` has none.
**Fix:** Apply the same `try/except ValidationError → ConfigError` pattern to
`log_config` construction in `app_service.py:43-44`, or validate the
dictConfig dict before passing to `dictConfig`.
**Verify:** New test: `test_log_config_malformed_yaml_raises_config_error`.
**Related:** OPS-005 — both are signal-loss paths in the observability surface.

### OPS-007 — [MEDIUM] Documentation drift: architecture.md and current-behavior.md describe pre-rework state

**Anchor:** `docs/00-overview/architecture.md`::lines 165-167, 178, 161-164;
`docs/00-overview/current-behavior.md`::line 35
**Checked:** static (code-verified)
**Why this band:** Operational documentation that describes a system other than
the one shipped is a finding per Phase 12 Block 6 ("documentation describing a
system other than the one deployed is itself a finding"). An operator following
these docs will be misinformed.
**Problem:**
1. `architecture.md:165-167`: "`__init__.py` is currently **empty**; top-level
   re-exports fail." → `__init__.py` now has full re-exports
   (`src/mko_data_cleaner/__init__.py:16-22`). The `test_top_level_exports` test
   (`test_app.py:384`) confirms exactly 5 names are exported.
2. `architecture.md:178`: "`rich` — Console output (**imported but not declared**
   — D-5)." → `rich>=13.0.0` **is** declared (`pyproject.toml:27`). D-5 is
   resolved.
3. `architecture.md:161-164`: "The README still shows the old
   `core.app_service.app_service` import, which no longer exists." → README has
   no such reference (grep README for `app_service` → 0 matches); README uses
   `from mko_data_cleaner.app import process_data` (`README.md:65`).
4. `current-behavior.md:35`: "`initialize_settings` known gap: `settings_dir`
   is accepted but not forwarded to `init_project`." → `app.py:148-150` forwards
   `settings_dir`; `init_service.py:44-50` honors it. The gap is closed.
**Repro:** `grep -rn '__init__.py.*empty' docs/00-overview/architecture.md` →
line 165. `grep -rn 'mko_data_cleaner.app import process_data' README.md` →
line 65. `rg 'not declared' docs/00-overview/architecture.md` → line 178.
**Impact:** An operator reading architecture.md believes `__init__.py` is
empty and that `rich` is un-declared, leading to incorrect import assumptions
and unnecessary dependency-audit work.
**Root cause:** Documentation was not updated alongside commits `37140aa` (remove
stale references, expose `__version__`), `5c4bfdd` (return ReportResult,
thread settings_dir), and `76b1fc8` (remove import-time init).
**Fix:** Update `architecture.md:165-168` to reflect current re-exports; update
`architecture.md:178` to remove the "not declared" annotation; update
`architecture.md:161-164` to note README is now correct; update
`current-behavior.md:35` to remove the "Known gap" note.
**Verify:** `grep -rn 'currently.*empty' docs/00-overview/architecture.md` → 0
matches after fix; `grep -rn 'not declared' docs/00-overview/architecture.md` → 0
matches.

### OPS-008 — [LOW] Phase 12 report-output `OPS-` prefix warning is phantom

**Anchor:** `.kilo/commands/audit/phases/12-audit-production-ops.md`::lines 169-171
**Checked:** static (grep)
**Why this band:** A phantom collision warning causes the auditor to either
invent a new namespace (fragmenting findings) or waste time searching for a
collision that does not exist. It is documentation drift in the phase file itself.
**Problem:** Line 169 states "Finding-ID prefix: `OPS- — already in use in
shipped source, tests, and workflows from a prior cycle." Grep for `OPS-` in
`src/**/`**/*.py`, `tests/*.py`, and `.github/workflows/*.yml` returns **zero
matches**. The prefix appears only in:
- `.ai/reports/phase-11-auditor-report.md:350` (mentioning phantom-prefix
  patterns alongside `QLT-` and `PERF-`)
- `.ai/reports/phase-13-researcher-recommendations.md:511` (same — phantom-prefix
  context)
- The Phase 12 file itself (line 169)

**Repro:** `rg -n 'OPS-' src/mko_data_cleaner/**/*.py tests/*.py .github/workflows/*.yml`
→ no matches. `rg -n 'OPS-' .ai/reports/` → matches in prior reports
discussing phantom prefixes, not as actual finding IDs.
**Impact:** An auditor may mint a second prefix (e.g., `OPS2-`) to avoid a
non-existent collision, fragmenting the findings namespace.
**Root cause:** The Phase 12 file was written in an earlier cycle when phantom
prefixes were a known issue; the premise was never re-verified against the
current tree.
**Fix:** Line 169: remove the "already in use" warning; replace with "verify no
collision before minting (grep `OPS-` across `src/`, `tests/`, `.github/`)."
**Verify:** `rg -n 'OPS-' src/ tests/ .github/` → 0 matches.

### OPS-009 — [LOW] CI does not enforce mypy despite configuring it

**Anchor:** `.github/workflows/ci.yml`::lines 26-27; `pyproject.toml:140-150`
**Checked:** static
**Why this band:** A type checker configured but never executed is a control that
"believes it is active" but is not on the path that decides whether a change
ships. This maps to Phase 12 Block 1's concern about declared scope vs. actual
scope.
**Problem:** `[tool.mypy]` is configured in `pyproject.toml:140-150` with
`strict_optional`, `disallow_any_generics`, `check_untyped_defs`, etc. The CI
workflow (`ci.yml:25-27`) runs only `uv run ruff check src/ tests/` and `uv run
pytest` — mypy is never invoked. Type-checking is configured but not enforced
as a gate.
**Repro:** `rg -n 'mypy' .github/workflows/ci.yml` → 0 matches.
`rg -n 'mypy' pyproject.toml` → 1 match (line 140, `[tool.mypy]`).
**Impact:** Type violations can ship without detection. The control exists in
configuration but is absent from the enforcement path.
**Root cause:** CI was configured with minimal gates (ruff + pytest) and never
extended to match the configured toolbelt.
**Fix:** Add `uv run mypy src/` as a step in the `test` job of `ci.yml`.
**Verify:** `rg -n 'mypy' .github/workflows/ci.yml` → ≥1 match after fix.

---

## 7. Cross-Finding Analysis

**Root-cause theme: Phase 12 was written against a pre-rework architecture and
never updated.** Four findings (OPS-001, OPS-003, OPS-004, OPS-007) are
documented descriptions of the system that diverged from code after commits
`37140aa`, `5c4bfdd`, `76b1fc8`, and the mypy-not-in-CI gap
(`1562fc6`). The rework is **documentation + CI alignment**, not architectural —
the code has evolved; the audit phase file has not.

**Root-cause theme: observability signal fragmentation.** OPS-005 and OPS-006
share a root cause — the logging pipeline (`dictConfig` + `log_config.yaml`) is
configured correctly inside `get_app_service()` but is bypassed by `print()`
calls in `run_report` and by `yaml_to_dict`'s silent `{}` fallback for
log_config. These are the only two observability findings with actual runtime
impact (MEDIUM); the rest are documentation/CI drift.

**Dependencies among findings:**
- OPS-004 (dictConfig-at-import-time fix) must land before OPS-005 and OPS-006
  can be accurately re-assessed — the auditor must first correct the false
  "import time" premise to inspect the right location.
- OPS-001 (Purpose) and OPS-003 (console script) are independent text fixes.
- OPS-002 (scope boundaries) is independent.
- OPS-007 (doc drift) and OPS-008 (phantom OPS-) are independent.
- OPS-009 (mypy in CI) is independent.

## 8. Roadmap

| Priority | Finding | Effort | Rationale |
|---|---|---|---|
| Critical (doc) | OPS-001 — Purpose async/no-database | trivial | Unblocks all downstream Fit assessments |
| Critical (doc) | OPS-004 — dictConfig at import time | trivial | Unblocks OPS-005, OPS-006 re-assessment |
| High | OPS-003 — stale console script name | trivial | Packaging identity correctness |
| High | OPS-002 — deleted phase citations | trivial | Scope boundary accuracy |
| High | OPS-005 — print() bypasses logging | small | Observability signal integrity |
| Medium | OPS-006 — yaml_to_dict silent fallback for log_config | medium | ConfigError symmetry for logging config |
| Medium | OPS-007 — doc drift (architecture.md, current-behavior.md) | small | Operator-facing accuracy |
| Low | OPS-008 — phantom OPS- prefix warning | trivial | Finding-namespace hygiene |
| Low | OPS-009 — mypy not in CI | trivial | Gate enforcement completeness |

**Rollout safety:** All Phase 12 findings are either (a) text/documentation
fixes with zero runtime impact, or (b) observability signal-integrity improvements
that do not change behavior — they route existing output through logging instead
of stdout. No finding changes a public API, a config schema, or the SQLite
processing engine. The `init_project` idempotency gate (Block 7) is already
verified correct by `test_app.py` tests (`test_init_project_*`,
`test_initialize_settings_*`).

---

## 9. Verification Methodology

- **Tools used:** `rg` (ripgrep), PowerShell `Test-Path`, `grep`, Read.
- **Code files read in full:** `app.py` (165 lines), `core/app_service.py` (342
  lines), `core/init_service.py` (57 lines), `core/utils.py` (173 lines),
  `core/errors.py` (30 lines), `core/paths.py` (54 lines), `core/models.py`
  (195 lines), `__init__.py` (22 lines).
- **Config files read in full:** `pyproject.toml` (185 lines),
  `settings/app_config.yaml` (50 lines), `settings/log_config.yaml` (57 lines),
  `.github/workflows/ci.yml` (92 lines), `MANIFEST.in` (7 lines),
  `scripts/smoke_install.py` (167 lines), `tests/test_app.py` (473 lines),
  `tests/test_packaging.py` (87 lines), `tests/conftest.py` (107 lines).
- **Docs read:** `docs/00-overview/architecture.md` (259 lines, full),
  `docs/00-overview/current-behavior.md` (330 lines, relevant sections),
  `README.md` (201 lines, full).
- **Not verified:** Runtime execution in Docker — no Docker available in this
  environment. Not required for a static architecture-vs-documentation audit.
  Git history was verified via `git log --oneline -15`.
