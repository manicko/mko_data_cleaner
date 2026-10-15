---
name: 01-entry-architecture
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 01 — Entry Points & Process Architecture

## Purpose

A reusable handbook for auditing the **entry surfaces and startup topology** of a
standalone async CLI tool and Python library that fetches data from the Mediascope
API. Three entry surfaces exist: the Typer CLI (`init`, `run`, `list_reports`),
the library API (`run_report`, `run_report_async`, `initialize_settings`), and
the module entry (`python -m mko_data_cleaner`). There is no long-lived
server process — each invocation is one-shot — but the singleton `app_service`
instantiates at import time and performs configuration loading and logging
configuration as side effects.

This file states *what* to examine and *under which angle*. Concrete artifacts
— files, modules, entry scripts, identifiers — are discovered by the executing
auditor, never named here.

**Scope Boundaries** — owned here: entry surfaces, import-time behaviour,
configuration bootstrap, and dependency direction. Not owned here: async
Semaphore/Queue/worker-pool semantics (03); the blocking-work bridge mechanism
(09); config values and credential provenance (02); CI/CD and packaging posture
(12); fixed-value enum discipline (10); test-suite entry points (11).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Entry-Surface Inventory and Import-Time Behaviour

Enumerate every entry surface — the CLI entrypoint, the module entrypoint, the
console-script, and the library API — and for each establish what executes at
import versus what is deferred. Check whether importing a surface can write to
the filesystem, reconfigure stdout, connect to a network, or mutate global
logging state; whether configuration load and logging setup are ordered
deliberately on every surface; and whether each surface's configuration source
is explicit about which variant it selects.

Evidence: the entry-surface inventory with import-time versus deferred work per
surface; the filesystem, network, or stdout an import can reach; which gates
analyse which surfaces.

### 2. Singleton Bootstrap at Import Time

Establish what work the singleton service performs at import time: loading YAML
configuration, resolving paths, and configuring logging via `dictConfig`. Check
whether the singleton is created eagerly on every import path regardless of
whether the feature is used, whether a missing or malformed config file fails
fast or silently, and whether logging configuration has observable side effects
(file handle creation, log file writes) before the caller opts in.

Evidence: the bootstrap call graph; what side effects occur on a bare import;
one induced missing or malformed config and the result.

### 3. stdout / sys Reconfiguration at Import Time

Establish whether any import-time statement reconfigures `sys.stdout` or
`sys.stderr` — line buffering, encoding, or stream replacement. Check whether
the reconfiguration is unconditional, whether it runs on every import path
including library use, and whether it can interfere with a host process that
has its own stdout contract (e.g., Jupyter, Airflow, or a test runner).

Evidence: the reconfiguration statement; whether it runs under a bare import;
one host-process scenario it affects.

### 4. Configuration Bootstrap Gate

Establish how the bootstrap gate loads user settings from the config directory:
which files are read, how paths are resolved relative to the user or package
directory, and whether validation occurs before use. Check whether the gate can
be deferred (lazy load) or must succeed at import, whether a missing directory
or file fails with a actionable message, and whether the `init` command
correctly materialises templates before the bootstrap reads them.

Evidence: the file-resolution chain; one induced missing file and the message;
the init→bootstrap ordering dependency.

### 5. Entry-Layer Discipline and Dependency Direction

Establish the layering between the entry layer (CLI, public API), the core
orchestration layer, and the settings templates. Check whether any lower layer
imports back from the entry layer, whether the import graph stays acyclic, and
whether the public API surface is minimal and stable. Confirm that the CLI does
not embed domain rules that belong in core.

Evidence: the import-graph direction per layer; every back-import and cycle;
domain work found inside an entry module.

### 6. Process Topology and Shutdown

Establish that each invocation is single-shot and asynchronous: the CLI wraps
its work in a single `asyncio.run` call, the library API provides both sync and
async wrappers. Check whether the event loop is created once per call, whether
pending tasks are awaited or abandoned on cancellation, and whether the
singleton's state (semaphore, queue) is safely discarded between runs.

Evidence: the asyncio.run boundary; cancellation behaviour; one resource that
leaks across runs.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered.

- **CRITICAL** — an import-time side effect that corrupts a host process's
  stdout or logging without consent; a singleton that performs network or
  filesystem I/O on import and cannot be suppressed by a library caller; a
  bootstrap gate that silently admits a missing config and proceeds with hidden
  fallback values.
- **HIGH** — a documented entry point the lint or type gate never analyses; an
  entry module that embeds domain rules or persistence; a startup contract that
  does not hold (e.g., logging writes a file the caller did not request).
- **MEDIUM** — environment-tier divergence between the CLI and library entry
  surfaces that is not intentional; an import-time side effect that is
  unnecessary but not harmful.
- **LOW** — documentation or comment drift about which surfaces exist or which
  config they load, with no runtime consequence.

An empty band is a valid outcome — do not populate it with a hypothetical.

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
