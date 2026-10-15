---
phase: "01"
phase_name: "Entry Points & Process Architecture"
target_file: ".kilo/commands/audit/phases/01-audit-entry-architecture.md"
author: researcher
evidence_source: src/mko_data_cleaner (read + grep) + docs/00-overview/architecture.md + git log
---

# Phase 01 — Researcher Recommendations for Refining the Audit Handbook

## Context & Evidence Note (read before editing)

The auditor's report (`/.ai/report/phase-01-auditor-report.md`) is **largely accurate**, but the
codebase has **already been partially fixed**, which matters for how you read the evidence:

- `git log -- src/mko_data_cleaner/app.py` shows commit `76b1fc8` — *"remove import-time
  initialization and gate service creation on settings"* — already replaced the old
  import-time singleton with the lazy `get_app_service()` factory. The auditor's Block 2 rewrite
  and Block 6 replacement are therefore grounded in the **current** tree, not aspirational code.
- Commit `37140aa` — *"remove stale references"* — already corrected `app.py:2`.

A grep of `src/mko_data_cleaner` for `asyncio|async def|asyncio\.run|Semaphore|Queue` returns
**zero** matches; there is no `__main__.py` at the package root (only
`app.py`'s `if __name__ == "__main__"`). These facts underpin the rewrite.

---

## 1. Proposed Final Set of Blocks

One entry per section of the Phase 01 file. No blocks are added, removed, or merged — the
existing 6-block numbering is preserved (matching the sibling phases `02..17, 99`).

| # | Section | Action | Rationale |
|---|---------|--------|-----------|
| — | Purpose | **Modify** | Strip async/network fiction; correct surface names; drop singleton-at-import claim. |
| — | Scope Boundaries | **Modify** | Drop async/bridge/credential exclusions (none exist in this codebase). |
| 1 | Entry-Surface Inventory & Import-Time Behaviour | **Keep (re-ground)** | Concept is sound; re-anchor on real surfaces + module-entry quirk. |
| 2 | Singleton Bootstrap at Import Time | **Modify (rewrite)** | Rename → "Lazy Service Factory and Bootstrap"; document two-tier bootstrap. |
| 3 | stdout / sys Reconfiguration at Import Time | **Keep (as-is)** | Fully aligned; the one real import-time side effect. |
| 4 | Configuration Bootstrap Gate | **Keep (reframe)** | Reframe as lazy/opt-in; add the `settings_dir` asymmetry check (D-6). |
| 5 | Entry-Layer Discipline and Dependency Direction | **Keep (extend)** | Add the `__all__` over-export + missing re-export gaps (D-8). |
| 6 | Process Topology and Shutdown | **Modify (replace)** | Replace async/cancellation framing with synchronous single-shot topology. |
| 7 | Severity Taxonomy | **Keep (rewrite examples)** | Principle intact; every example must be re-based to real runtime. |
| 8 | Report Output | **Keep (no change)** | Formulation correct; only note path drift (see §4.6). |

> **No add / no remove / no merge.** Block 1↔3 and Block 2↔4 overlaps are mild and explicitly
> accepted by the auditor; they are resolved by depth (inventory vs. inspection vs. gate).

---

## 2. Suggested Formulations (checklist-style, independent, ≤6 lines each)

> **Convention note.** Consistent with the existing convention, the *Purpose* names the concrete
> surfaces; the *Audit Blocks* frame an angle and **never** prescribe files/modules/lines — the
> auditor discovers identifiers. Each formulation below is a self-contained checklist a second
> auditor could execute blind.

### Purpose + Scope Boundaries (rewrite together)

> Audit the entry surfaces and startup topology of a local, synchronous, single-shot CLI + library
> that post-processes Mediascope exports arriving as files on disk — no network, no credentials,
> no async.
> - Surfaces: Typer CLI (`init`, `run`, `list_settings`); console script `mko-data-cleaner`;
>   module entry `python -m mko_data_cleaner.app` (note: no package `__main__.py`); library API
>   `process_data` / `initialize_settings`.
> - The service is built lazily by `get_app_service()`; a bare import performs no filesystem,
>   network, or logging work (stdout reconfigure is the sole exception — see Block 3).
>
> **Scope:** entry surfaces, import-time behaviour, configuration bootstrap, dependency
> direction, public-API surface hygiene, `settings_dir` forwarding across `init`/`run`.
> **Out of scope:** SQLite processing-engine internals (04); test-suite entry points (11);
> packaging/publishing posture (12). *(Delete the async/Semaphore/Queue (03), blocking-work-bridge
> (09), and "credential provenance (02)" exclusions — none exist in this codebase.)*

### Block 1 — Entry-Surface Inventory and Import-Time Behaviour

> Enumerate every entry surface; per surface, what runs at import vs. is deferred:
> - Typer CLI commands, the console script, the module entry, the library API.
> - A bare import must not write files, open network, mutate logging, or touch stdout *(Block 3 is the exception)*.
> Evidence: surface list split by import-time vs. call-time work; the import blast radius.

### Block 2 — Lazy Service Factory and Bootstrap *(was "Singleton Bootstrap at Import Time")*

> Inspect the lazy service factory, **not** an import-time singleton:
> - Two tiers: import-time (stdout reconfigure only); call-time (gate → config load → `dictConfig` → log file).
> - The gate checks existence only — a malformed-but-present config **passes** it, then fails later as `ConfigError`.
> Evidence: factory call graph; one induced missing config (must raise) and one malformed config (where it fails).

### Block 3 — stdout / sys Reconfiguration at Import Time

> Establish whether any module-level statement reconfigures `sys.stdout` / `sys.stderr`:
> - One unconditional module-level reconfiguration runs on **every** import path, including library use.
> - Confirm `core/*` does **not** touch stdout (bounds blast radius); assess host-stdout interference (Jupyter, test runner).
> Evidence: the reconfiguration statement, proof it runs under a bare import, one host scenario.

### Block 4 — Configuration Bootstrap Gate

> Examine the lazy, opt-in settings gate and `settings_dir` forwarding:
> - Does the gate raise on missing files, and is it deferred to the factory call (not import)?
> - Does `init`/the initializer honor a caller-supplied `settings_dir`, or write to a hardcoded dir? (D-6 divergence)
> - Verify `init` materialises templates before the gate can read them.
> Evidence: the gate's failure mode on a missing file; the init→gate ordering; the `settings_dir` verdict.

### Block 5 — Entry-Layer Discipline and Dependency Direction

> Verify entry-layer discipline and dependency direction:
> - Strictly entry → `core/*`; no back-import; the import graph is acyclic.
> - The CLI must **not** embed domain rules/persistence that belong in `core/`.
> - **API-surface gap (D-8):** is `__all__` minimal vs. the two-function contract, and does the package root re-export the API?
> Evidence: import direction per layer; every back-import and cycle; `__all__`-vs-contract gap.

### Block 6 — Process Topology and Shutdown *(replacing the async/cancellation block)*

> Establish the **synchronous, single-shot** topology:
> - One OS process per invocation; work is a straight sync chain: entry → orchestrator → workers.
> - No event loop, no `asyncio.run`, no tasks, no cancellation surface.
> - The only durable resource is the disposable SQLite DB (context manager: checkpoint WAL + delete on exit); no module-scope state.
> Evidence: the synchronous call chain; the context-manager teardown; proof no state persists across runs.

### Severity Taxonomy (rewrite examples only; keep principle + labels + "empty band is valid")

> Grade by **effect and blast radius**, not mechanism name; rate what is true **now**. An empty band is a valid outcome.
> - **CRITICAL** — a gate that silently admits missing/malformed config and proceeds *(currently fail-fast → band stays empty: a regression guard, not a finding)*.
> - **HIGH** — an entry surface that embeds domain rules/persistence; a startup contract that fails to hold.
> - **MEDIUM** — `settings_dir` forwarding asymmetry between `init`/initializer and `run`/library API (D-6); any unsuppressed import-time side effect.
> - **LOW** — documentation / comment drift about surfaces or config; stale project-name references in active files.
> *(Delete all network / singleton / asyncio examples — none exist in this codebase.)*

### Report Output — keep verbatim

> Findings path `.ai/audit/01-entry-architecture/findings.md`, template
> `.ai/audit/templates/audit-findings.md`, prefix `ENT-`, incremental append ≤100 lines,
> `problems-only: true`. *(See §4.6 — the findings path may drift with the canonical layout.)*

---

## 3. Recommended Modern Practices / Analysis Angles

These add value at minimal length. Recommend folding the first two into the relevant blocks.

1. **Two-tier side-effect model (use in Block 2).** The single most useful discipline for auditing
   a sync entry surface: *classify every statement as import-time (module-level) vs. call-time
   (factory/constructor)*, then enumerate only the import-time ones. This is what makes
   "no logging at import" a crisp, provable property.

2. **Config-gate granularity lens (use in Block 4).** Distinguish an *existence-only* gate from
   a *content-validated* gate. The current gate is existence-only; the failure is deferred to
   Pydantic validation. This is a generic bug-class worth surfacing explicitly rather than
   burying in prose.

3. **Static import-graph verification (use in Block 5).** A repo-wide `compile` + grep check for
   back-imports and cycles is a sharper proof than eyeballing. Phrase the block as
   "prove direction; prove acyclicity; prove no domain logic in the entry layer."

4. **Resource-lifecycle trace (use in Block 6).** Trace the longest-lived resource — here the
   SQLite file — through construction → use → `__exit__` teardown, *including the failure path*
   (exception → cleanup still runs). This is more concrete than "no state leaks."

5. **Import-blast-radius litmus (use in Block 1).** The sanity check `python -c "import <surface>"`
   must be cheap and side-effect-free *except* where explicitly documented. It catches both the
   stdout reconfigure (Block 3) and accidental logging/FS writes at import.

6. **Evidence-before-mechanism severity (use in Taxonomy).** The existing principle already
   states this; reinforce it by having auditors *name the observable effect* before the band,
   so the CRITICAL band never reads like a mechanism catalog.

---

## 4. Remaining Quality or Brevity Concerns

Each carries a confidence assessment. None require source changes — they are handoff caveats for the doc-specialist.

### 4.1 One stale auditor citation: `app.py:2` stale project name (confidence HIGH)

The auditor's LOW severity example reads: *"the `app.py` module docstring's stale
`mko_get_mediascope_data` project name, `app.py:2`."* **This is no longer true in the current
tree.** Verified by a repo-wide grep for `mko_get_mediascope_data` in `src/mko_data_cleaner`:
**zero matches.** The current `app.py:2` reads `mko_data_cleaner — основной публичный интерфейс`.
Commit `37140aa` already removed the stale name here.

The stale token **does still persist** in `MANIFEST.in` (lines 4, 7), `.gitignore` (lines 224-225),
and `.kilo/commands/audit/phases/02-audit-config-secrets.md` (line 16). So the *underlying drift*
finding is real; the *citation* is mis-targeted.

**Handoff action:** do **not** treat `app.py:2` as an open finding. If a stale-name LOW item is
retained in the severity examples, scope it to the artifacts where it actually persists, not to
`app.py`.

### 4.2 A superseded scratch evidence file exists (`.ai/tmp/code_context.md`) (confidence HIGH)

`.ai/tmp/code_context.md` describes a **pre-fix** state that no longer exists: it claims `app.py`
imports a module-level `app_service` singleton and that `dictConfig` runs at import
(`code_context.md:17,119,219,255`). The **current** source (confirmed by direct read) already uses
the lazy `get_app_service()` factory. Its defect list (F1 import-crash, F2 unreachable CLI, F8
`.reports`, F9 no app-layer tests, etc.) describes a state already remediated by `76b1fc8`.

**Handoff action:** treat `.ai/tmp/code_context.md` as **superseded**. It is useful historical
context but will mislead if read as current. Point future auditors to the auditor's Phase 01 report
for the accurate current-state description. (Note: F9 "no app-layer tests" may still be TRUE at
the current HEAD independent of the singleton — verify separately, do not inherit it from the stale
scratch file.)

### 4.3 `docs/00-overview/architecture.md` says `__init__.py` is "empty" (confidence HIGH)

`architecture.md:165` states the package root `__init__.py` "is currently empty." The actual file
is **not** empty — it contains the `importlib.metadata`-backed `__version__` logic (10 lines, no
re-exports). The substantive point the auditor relies on (no re-exports, so the documented
top-level import fails) is **unchanged and correct**.

**Handoff action:** in Block 5, phrase it as *"version-only (`__version__`), no re-exports"* rather
than *"empty,"* to avoid a contradictory citation.

### 4.4 The "singleton state discarded between runs" concern is moot (confidence HIGH)

The original Block 6 worried that *"the singleton's state (semaphore, queue) is safely discarded
between runs."* No singleton, semaphore, or queue exists; there is no module-scope state. The
replacement topology block (§2, Block 6) already reflects this — just ensure the rewritten block
does not inherit the "discard state" question language.

### 4.5 Cross-phase consistency risk: Phase 03 (async concurrency) (confidence MEDIUM)

Phase 01's (old) scope excluded "async Semaphore/Queue/worker-pool semantics (03)." Since `asyncio`
has **zero** presence in the codebase, Phase 03's premise is likely fictional as well. This is out
of scope for the Phase 01 rewrite, but flag it to the coordinator: if Phase 03 is async-framed, it
will need the same deconstruction. Do not, however, let Phase 01 reference "Phase 03 covers async"
as a real deferred concern — state it as a question to resolve, not a given.

### 4.6 Findings-output path drift (confidence MEDIUM)

Phase 1 "Report Output" cites `.ai/audit/01-entry-architecture/findings.md`. The working layout
shows findings under `.ai/reports/audit/` (singular `.ai/report/` also exists alongside plural
`.ai/reports/`). Recommend the doc-specialist confirm the canonical findings path before
finalizing, since the Phase 01 file should not point auditors at a non-existent location.

### 4.7 Adjacent stale `__main__` dev guards (confidence HIGH, out of Phase 01 scope)

Both `core/app_service.py:306-307` (`if __name__ == "__main__": get_app_service().run_report(...)`)
and `core/init_service.py:45-46` carry `__main__` dev guards inside **internal** modules, each
hardcoding a data path. The auditor correctly files these as a LOW hygiene note under Block 5's
"entry module embeds domain execution" lens — keep that as a single line; do not expand it into a
block. This is a code smell, not an audit-handbook item.

### 4.8 Length discipline for the rewrites (confidence HIGH)

The task requires ≤6 lines per block formulation and "concise checklists." The original prose
paragraphs are method-neutral and longer. The doc-specialist should **not** expand the rewrites back
into multi-paragraph method narratives; treat the §2 formulations above as hard line-count budgets.
Block 6, in particular, tends to balloon — the §2 formulation is the cap to respect.

---

## 5. Precise Edit Delta for the Doc-Specialist

All references are to the **current** `.kilo/commands/audit/phases/01-audit-entry-architecture.md`.

| Lines | Replace with | Source § |
|------|--------------|----------|
| 11–30 (Purpose + Scope Boundaries) | §2 "Purpose + Scope Boundaries" formulation | §2 |
| 41–53 (Block 1) | §2 "Block 1" (keep; only re-ground surface names) | §2 |
| 55–65 (Block 2) | §2 "Block 2" (rename header; rewrite body) | §2 |
| 67–76 (Block 3) | §2 "Block 3" (keep; append the `core/*` narrowing note) | §2 |
| 78–88 (Block 4) | §2 "Block 4" (reframe as lazy/opt-in; add D-6 asymmetry check) | §2 |
| 90–99 (Block 5) | §2 "Block 5" (add D-8 API-surface gap) | §2 |
| 101–110 (Block 6) | §2 "Block 6" (full replacement: sync single-shot) | §2 |
| 114–133 (Severity Taxonomy examples) | §2 "Severity Taxonomy" (keep principle/labels; replace examples) | §2 |
| 137–147 (Report Output) | Keep verbatim, **verify** the findings path | §2 + §4.6 |

**Do not touch** the front matter `name`/`status`/`problems-only` unless the coordinator decides on
a front-matter `executor` change — that is a process call, not an architecture one.

---

*End of recommendation. This document is evidence-grounded: every structural recommendation is
traceable to a verified current-source fact (lazy factory, two-tier bootstrap, existence-only gate,
`db_service.py` `__exit__` cleanup, `__all__` over-export, `settings_dir` asymmetry, zero async
matches, no package `__main__.py`). The single correction to the auditor's own report is the stale
`app.py:2` citation (§4.1).*
