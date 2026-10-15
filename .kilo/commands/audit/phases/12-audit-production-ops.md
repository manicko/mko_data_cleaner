---
name: 12-production-ops
status: draft
validated: no
executor: auditor
problems-only: true
code_ref: "27ae816"
source: ".ai/reports/phase-12-auditor-report.md"
confidence: HIGH
severity_taxonomy:
  CRITICAL: []
  HIGH: [OPS-001, OPS-003, OPS-004]
  MEDIUM: [OPS-002, OPS-005, OPS-006, OPS-009]
  LOW: [OPS-007, OPS-008]
---

# Phase 12 — Production Operations, Security & Observability

## Purpose

What the shipped package is made of and what decides its content; what
determines which checks gate a release; what an operator is told when something
is wrong; and what a failure costs in output. This is a synchronous, single-shot
CLI tool and Python library distributed via `uv` and PyPA packaging — there are
no containers, no network, no reverse proxy, no health probes, and no backup
strategy. The only durable runtime state is a disposable SQLite database
created and deleted per invocation: opened as a `DBWorker` context manager at
`app_service.py:146` and WAL-checkpointed + deleted on `__exit__`
(`db_service.py` `close`; `architecture.md:136`, `current-behavior.md:78,194-196`).
This file names what to examine and under which angle — the executing auditor
discovers the concrete artifacts.

**Scope boundaries** — taken in: CI/CD gates, packaging, and observability.
Deferred by this phase: import-time side effects and bootstrap (01); settings
values and credentials (02); data-pipeline stage ordering and extraction (04);
template validation (05); code-quality discipline (10); test-suite adequacy
(11); concurrency ceilings and pipeline latency (13). There is no edge tier, no
transport security boundary, no health-probe contract, and no backup to this
phase. (03) and (09) are omitted — both phases were deleted; (04) defers
pipeline-stage ordering, not scheduled "period resolution" (no scheduler
exists); (13) is a forward reference to the in-rework Phase 13.

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others,
and each carries its own evidence. Where a property cannot be settled in the
environment at hand, establish it from what is observable and state plainly what
could not be verified and why. A passing check is methodology, never a finding,
and any sound evidence is admissible: nothing here gates a finding on this
phase's own list.

### 1. A control that runs, reports clean, and examines nothing

Take every automated control that emits a pass/fail signal — the linter (`ruff`),
the type checker (`mypy`), and the test suite (`pytest`) — and separate three
questions per control: does it exist, what scope does it declare, what did it
actually examine. Establish what is configured to be enforced and which
suppressions are sanctioned before judging any result; never infer a missing tool,
and never treat a non-zero issue count as a defect by itself. A control whose
declared and actual scope differ is a finding however green it is.

Controls present in CI (`ci.yml:25-27`): `ruff` (lint) + `pytest` (test suite).
`mypy` is configured (`pyproject.toml:140-150`) but absent from CI (OPS-009).
No secret/credential scanner is wired in — this is a local-only, no-credential
project, so "a secret scan" is not an active control here.

Evidence: per control — existence, declared scope, the file / item / series
count it actually examined, and whether any shipped guard would notice a change
in that scope.

### 2. CI/CD pipeline & gates

Establish whether a CI/CD pipeline exists and which checks it gates behind a
release. For each gate — lint, typecheck, tests — check whether it is loaded,
green, and what its scope excludes. Check whether an artifact can be published
regardless of a failing gate, whether credentials reach a pre-gate step, and
what overlapping or re-entrant runs do to shared state. A gate whose
configuration sits in the repository but is never read from the directory it runs
in is a finding.

The pipeline is a single GitHub Actions workflow (`.github/workflows/ci.yml`,
4 jobs: `test` (`ruff` + `pytest`), `build` (`python -m build` + `twine check`
+ `check-wheel-contents` + wheel-content assertions), `install-smoke`
(`needs: build`), `notebook`). Gates cannot be bypassed: `install-smoke` blocks
publication on a failing `build`, which blocks on a failing `test`.

Evidence: per gate — loaded or not, green or not, scope; the path by which an
artifact or release proceeds with the check failing or not having run.

### 3. What makes a check a gate vs. a report

Establish which checks can prevent a release and what triggers the automation,
what dependency makes a verdict reach the artifact, and whether the artifact can
be published regardless of a verdict. Check whether the same checks can be run
locally by a contributor, and whether a local run reaches the same configuration
and scope as the CI run.

Evidence: per check — trigger events, the dependency that would make it
blocking, and the path by which an artifact proceeds with the check failing or
not having run; whether it is locally reproducible.

### 4. Packaging & artifact identity

Establish what decides the package content: the `pyproject.toml` build metadata
and the setuptools backend (`pyproject.toml:2-3`), pinned dependency versions,
and the entry points. The console script is `mko-data-cleaner`
(`pyproject.toml:46`, target `mko_data_cleaner.app:app`, import name
`mko_data_cleaner` at `__init__.py:1`); the module entry is
`python -m mko_data_cleaner.app` via the `if __name__ == "__main__"` guard
(`app.py:153`). There is **no package-level `__main__.py`**, so
`python -m mko_data_cleaner` fails (OPS-003). The three identifiers are
consistent: `pyproject.toml:6` name, `__init__.py:1` import name,
`pyproject.toml:46` console script.

Check whether the published artifact coordinators match what the install path
expects, whether a version is immutable after publish, and whether a rollback
selects an artifact identical to the forward path.

Evidence: the external-input inventory (build tools, dependencies) with a pin
verdict and integrity verdict each; the three identifiers compared; the
mutability of a revert target.

### 5. Observability: signals produced and consumed

Establish which signals the system emits and whether anything consumes them in
the deployed configuration. The logging pipeline is configured via `dictConfig`
**inside the `get_app_service()` factory, after the settings gate passes**
(`app_service.py:337`) — **not** at import time; importing
`mko_data_cleaner.app` performs no logging configuration and no filesystem side
effects (`app.py:27,148-150`; asserted by `tests/test_app.py:81`). Check whether
`dictConfig` validates its configuration before applying, whether a failure
state produces a log entry through the configured pipeline rather than only a
bare `print()` to stdout, and whether every log path reaches the same configured
output. There is no metrics/counters layer — a quiet tool is indistinguishable
from a working one; the only liveness signal is stdout.

Findings in this block: `print()` bypasses the handlers in `run_report`
(`app_service.py:113,257,272`) and `get_table_sample` (`db_service.py:308-309`)
(OPS-005); `yaml_to_dict` returns `{}` on parse error (`utils.py:131-149`) so a
malformed `log_config.yaml` produces an opaque pydantic `ValidationError` from
`LoggingSettings(**yaml_to_dict(...))` (`app_service.py:43-44`,
`models.py:51-57`) — OPS-006.

Evidence: the signal inventory with produced/consumed verdicts; one failure
state with a counter, a log entry, or neither; the logging config load path and
its validation.

### 6. Runbooks & operational documentation

Establish whether the procedures a responder is told to follow would run as
written: required state, required credentials, required files, assumed tool
versions. Check whether each operational claim in the install, run, and incident
corpus — a control that is live, a cadence, a value, a path — is still true of
the system as shipped. Treat a comment, docstring, or runbook line as a claim to
be tested against the executing path: documentation describing a system other
than the one deployed is itself a finding.

Findings here are documentation-vs-code drift (OPS-007): `architecture.md:165-167`
claims `__init__.py` is empty (it re-exports five names, `__init__.py:16-22`);
`architecture.md:178` claims `rich` is "not declared" (it is,
`pyproject.toml:27`); `current-behavior.md:35` still claims
`initialize_settings` does not forward `settings_dir`
(`app.py:148-150`, `init_service.py:44-50`).

Evidence: the operational-claim inventory with a true/false verdict each; one
procedure with the step that fails on the host and why.

### 7. Release & rollback

Establish what a release and a rollback require: version pinning in
`pyproject.toml`, the `uv` lock file, config schema compatibility. Check whether
a rollback restores the same package version and the same config schema the
forward path used, and whether a config-template change is backward-compatible
with an older code version. Check whether the `init` command's template copy is
idempotent and non-destructive to user edits.

`init_project` / `_copy_templates` (`init_service.py:7-27`) is idempotent and
non-destructive: existing files are skipped unless `force=True`
(`init_service.py:23-24`); `destination.mkdir(parents=True, exist_ok=True)`
(`init_service.py:13`). Version is hardcoded `0.1.0` (`pyproject.toml:7`) with
no build-time stamping; `uv sync` in CI runs without `--frozen`/`--locked`
(`ci.yml:25,38,69,80`), so the lock is advisory.

Evidence: the forward and revert paths compared; one config-template change with
its backward-compatibility verdict; the init command's behaviour on an existing
user directory.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. The bands are effect classes; the
findings surfaced by this phase are listed as illustrative entries under each
band (full records belong in the findings file). An empty band is a valid outcome
— do not populate it with a hypothesis.

- **CRITICAL** — a control believed to be active that is not, on the path that
  decides whether a change ships or a release publishes, so that nothing
  distinguishes a vetted subject from an unvetted one. *(Empty this cycle —
  no finding meets the CRITICAL bar.)*
- **HIGH** — a documented claim about the system that is false, so a downstream
  auditor is misdirected into a non-existent execution path:
  - **OPS-001** — Purpose describes the tool as "async" with "no database"
    (`12-audit-production-ops.md:15-17`); the tool is synchronous
    (`app.py:27`) and uses a disposable SQLite per run
    (`app_service.py:146`, `architecture.md:136`).
  - **OPS-003** — Block 4 names the wrong console script, `mko-get-mediascope`
    (`12-audit-production-ops.md:86`); the real script is `mko-data-cleaner`
    (`pyproject.toml:46`) and `python -m` is only valid as
    `python -m mko_data_cleaner.app` (`app.py:153`) — no `__main__.py` exists.
  - **OPS-004** — Block 5 claims "dictConfig at import time"
    (`12-audit-production-ops.md:99`); `dictConfig` actually runs inside the
    `get_app_service()` factory after the settings gate
    (`app_service.py:337`).
- **MEDIUM** — degraded operability with a workaround, or a control configured but
  not enforced on the shipping path:
  - **OPS-002** — scope boundaries cite deleted phases (03) and (09)
    (`12-audit-production-ops.md:23,26`); neither phase file exists.
  - **OPS-005** — `print()` calls in `run_report`
    (`app_service.py:113,257,272`, also `db_service.py:308-309`) bypass the
    dictConfig logging pipeline; stdout is not subject to the configured
    formatter, level, or file rotation (`log_config.yaml:13,33-41`).
  - **OPS-006** — `yaml_to_dict` returns `{}` on parse error without raising
    (`utils.py:131-149`); a malformed `log_config.yaml` surfaces only an opaque
    pydantic `ValidationError` from `LoggingSettings(**yaml_to_dict(...))`
    (`app_service.py:43-44`, `models.py:51-57`).
  - **OPS-009** — `mypy` is configured (`pyproject.toml:140-150`) but never
    invoked in CI (`ci.yml:26-27`); type safety is a declared control that
    never runs on the shipping path.
- **LOW** — documentation and hygiene drift with no runtime consequence today:
  - **OPS-007** — operator docs describe the pre-rework state:
    `architecture.md:165-167` claims `__init__.py` is empty (it re-exports
    five names, `__init__.py:16-22`); `architecture.md:178` claims `rich` is
    "not declared" (it is, `pyproject.toml:27`); `current-behavior.md:35`
    still claims `settings_dir` is not forwarded (`app.py:148-150`,
    `init_service.py:44-50`).
  - **OPS-008** — the `OPS-` prefix-collision warning is phantom;
    `rg -n 'OPS-' src/ tests/ .github/` returns 0 matches.

## Report Output

- Findings path: `.ai/audit/12-production-ops/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `OPS-` — this phase's prefix per the findings template
  convention; no `OPS-NNN` collision found in the current tree (`27ae816`:
  `rg -n 'OPS-' src/ tests/ .github/` → 0 matches)
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence
- Empty state, exactly: `No problems found in this phase.`
