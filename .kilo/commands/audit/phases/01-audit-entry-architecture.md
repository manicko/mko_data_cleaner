---
name: 01-entry-architecture
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 01 — Entry Points & Process Architecture

## Purpose

Audit the entry surfaces and startup topology of a local, synchronous, single-shot
CLI + library that post-processes Mediascope exports arriving as files on disk —
no network, no credentials, no async.

- Surfaces: Typer CLI (`init`, `run`, `list_settings`); console script
  `mko-data-cleaner`; module entry `python -m mko_data_cleaner.app` (note: no
  package `__main__.py`); library API `process_data` / `initialize_settings`.
- The service is built lazily by `get_app_service()`; a bare import performs no
  filesystem, network, or logging work (stdout reconfigure is the sole exception —
  see Block 3).

**Scope:** entry surfaces, import-time behaviour, configuration bootstrap,
dependency direction, public-API surface hygiene, `settings_dir` forwarding across
`init`/`run`.
**Out of scope:** SQLite processing-engine internals (04); test-suite entry points
(11); packaging/publishing posture (12).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Entry-Surface Inventory and Import-Time Behaviour

Enumerate every entry surface; per surface, what runs at import vs. is deferred:
- Typer CLI commands, the console script, the module entry, the library API.
- A bare import must not write files, open network, mutate logging, or touch stdout
  *(Block 3 is the exception)*.

Evidence: surface list split by import-time vs. call-time work; the import blast
radius.

### 2. Lazy Service Factory and Bootstrap

Inspect the lazy service factory, **not** an import-time singleton:
- Two tiers: import-time (stdout reconfigure only); call-time (gate → config load →
  `dictConfig` → log file).
- The gate checks existence only — a malformed-but-present config **passes** it,
  then fails later as `ConfigError`.

Evidence: the factory call graph; one induced missing config (must raise) and one
malformed config (where it fails).

### 3. stdout / sys Reconfiguration at Import Time

Establish whether any module-level statement reconfigures `sys.stdout` /
`sys.stderr`:
- One unconditional module-level reconfiguration runs on **every** import path,
  including library use.
- Confirm `core/*` does **not** touch stdout (bounds blast radius); assess
  host-stdout interference (Jupyter, test runner).

Evidence: the reconfiguration statement, proof it runs under a bare import, one
host scenario.

### 4. Configuration Bootstrap Gate

Examine the lazy, opt-in settings gate and `settings_dir` forwarding:
- Does the gate raise on missing files, and is it deferred to the factory call (not
  import)?
- Does `init`/the initializer honor a caller-supplied `settings_dir`, or write to a
  hardcoded dir? (D-6 divergence)
- Verify `init` materialises templates before the gate can read them.

Evidence: the gate's failure mode on a missing file; the init→gate ordering; the
`settings_dir` verdict.

### 5. Entry-Layer Discipline and Dependency Direction

Verify entry-layer discipline and dependency direction:
- Strictly entry → `core/*`; no back-import; the import graph is acyclic.
- The CLI must **not** embed domain rules/persistence that belong in `core/`.
- **API-surface gap (D-8):** is `__all__` minimal vs. the two-function contract,
  and does the package root re-export the API?

Evidence: import direction per layer; every back-import and cycle; `__all__`-vs-
contract gap.

### 6. Process Topology and Shutdown

Establish the **synchronous, single-shot** topology:
- One OS process per invocation; work is a straight sync chain: entry → orchestrator
  → workers.
- No event loop, no `asyncio.run`, no tasks, no cancellation surface.
- The only durable resource is the disposable SQLite DB (context manager:
  checkpoint WAL + delete on exit); no module-scope state.

Evidence: the synchronous call chain; the context-manager teardown; proof no state
persists across runs.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not mechanism name; rate what is true **now**.
An empty band is a valid outcome.

- **CRITICAL** — a gate that silently admits missing/malformed config and proceeds
  *(currently fail-fast → band stays empty: a regression guard, not a finding)*.
- **HIGH** — an entry surface that embeds domain rules/persistence; a startup
  contract that fails to hold.
- **MEDIUM** — `settings_dir` forwarding asymmetry between `init`/initializer and
  `run`/library API (D-6); any unsuppressed import-time side effect.
- **LOW** — documentation / comment drift about surfaces or config; stale
  project-name references in active files.

---

## Report Output

- Findings path: `.ai/audit/01-entry-architecture/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, distribution, cross-finding analysis, roadmap, rollout safety,
  appendices
- Finding-ID prefix: `ENT-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding needs
  runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
