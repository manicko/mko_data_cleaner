---
phase: "01"
phase_name: "Entry Points & Process Architecture"
phase_file: ".kilo/commands/audit/phases/01-audit-entry-architecture.md"
report_status: "draft"
scope: "entry surfaces, import-time behaviour, configuration bootstrap, dependency direction"
---

# Audit Report — Phase 01 Entry Points & Process Architecture

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **local, synchronous, single-shot** CLI tool and Python
library that post-processes Mediascope TV advertising exports received as files
on disk. The current architecture (per `docs/00-overview/*`, code-verified)
directly contradicts several core assumptions baked into Phase 01:

- **No async, no event loop.** A repo-wide grep for `asyncio`, `async def`,
  `asyncio.run`, `Semaphore`, `Queue`, and worker-pool patterns across
  `src/mko_data_cleaner` returns **zero matches**. There is no concurrency layer
  at all ([source](../src/mko_data_cleaner/app.py),
  [app_service.py](../src/mko_data_cleaner/core/app_service.py),
  [utils.py](../src/mko_data_cleaner/core/utils.py)).
- **No singleton; lazy factory instead.** There is no module-level singleton.
  `import mko_data_cleaner.app` performs **no filesystem, network, or logging**
  side effects. The service is constructed lazily by
  `get_app_service()` ([app_service.py:290](../src/mko_data_cleaner/core/app_service.py#L290)),
  which the architecture calls "a factory function, not a singleton created at
  import time."
- **No external API / credentials.** The only external inputs are files on disk;
  Mediascope exports arrive manually. There are no credentials, no network, no
  authentication — nowhere in the code or dependency list.
- **Bootstrap ordering.** `get_app_service()` runs `_require_settings()` (checks
  `app_config.yaml` + `log_config.yaml` exist, else raises
  `SettingsNotInitializedError`, a `FileNotFoundError` subclass —
  [errors.py:20](../src/mko_data_cleaner/core/errors.py#L20)) **before**
  constructing `AppService` and applying `logging.config.dictConfig`
  ([app_service.py:299-303](../src/mko_data_cleaner/core/app_service.py#L299-L303)).
  Thus logging (and its file handle / `log.log` writes) happens only **after** the
  gate passes, i.e. on use, never at import.
- **One real import-time side effect.** `sys.stdout.reconfigure(line_buffering=True)`
  sits at module level in `app.py` ([app.py:16](../src/mko_data_cleaner/app.py#L16)).
  `core/*` modules do **not** reconfigure stdout ([verified by reading
  core/utils.py, core/paths.py, core/app_service.py]).
- **Real entry surfaces.**
  - Console script `mko-data-cleaner = "mko_data_cleaner.app:app"`
    ([pyproject.toml:46](../pyproject.toml#L46)); Typer commands are
    `init`, `run`, `list_settings` — **not** `list_reports`.
  - Module entry `python -m mko_data_cleaner.app` (the `if __name__ == "__main__":
    app()` block at [app.py:130](../src/mko_data_cleaner/app.py#L130)). **No**
    package `__main__.py` exists, so `python -m mko_data_cleaner` does **not**
    launch the CLI.
  - Library API: `process_data(report_path, settings_dir=None)` and
    `initialize_settings(force, settings_dir=None)` ([app.py:105,121](../src/mko_data_cleaner/app.py#L105)).
    `run_report` is an `AppService` method, not a public API function;
    `run_report_async` **does not exist**.
  - `__init__.py` exposes only `__version__` (via `importlib.metadata`) and
    **no** re-exports, so `from mko_data_cleaner import process_data` fails;
    users must import from `mko_data_cleaner.app` ([__init__.py](../src/mko_data_cleaner/__init__.py)).
- **Dependency direction.** Strictly `app.py → core/*`. A grep across
  `src/mko_data_cleaner/core` for imports of `mko_data_cleaner.app` finds **no
  code imports** — the only occurrence of the token `app` inside `core/` is the
  docstring text at [app_service.py:294](../src/mko_data_cleaner/core/app_service.py#L294).
  The import graph is acyclic.
- **Public-API gap.** The spec (R4) states the public API is exactly two
  functions with `core/*` internal, yet `app.py.__all__` also exports
  `init`, `run`, `list_settings`, `app`, and `get_app_service`, and the package
  root does not re-export the two contract functions ([app.py:134-142](../src/mko_data_cleaner/app.py#L134-L142)).

---

## 2. Per-Block Assessment (1–6)

### Block 1 — Entry-Surface Inventory and Import-Time Behaviour
- **Fit:** **High.** The concept (enumerate surfaces; distinguish import-time vs
  deferred work; check FS/network/stdout/logging impact of an import; confirm the
  config source) is exactly what a synchronous single-shot tool needs to verify.
  The evidence guidance is acceptable.
- **Outdated:** The block text itself is generic and survives, but it is framed
  by a Phase 01 *Purpose* that misnames the surfaces (`list_reports`,
  `run_report`, `run_report_async`). The block must be re-grounded on the real
  surfaces and must verify the module entry: `python -m mko_data_cleaner.app` is
  the working form; `python -m mko_data_cleaner` is **not** (no `__main__.py`).
- **Gap:** None within the block; it correctly points at the one real import-time
  side effect (stdout reconfigure — see Block 3).
- **Overlap:** Partial overlap with Block 2 (import-time behaviour) and Block 3
  (stdout). Mild; acceptable since Block 1 is the inventory and the others add
  depth.

### Block 2 — Singleton Bootstrap at Import Time
- **Fit:** **None (concept misapplied).** The block is predicated on a singleton
  created at import time that loads YAML and runs `dictConfig` eagerly. **That
  object does not exist.** There is no singleton and no import-time bootstrap:
  `import mko_data_cleaner.app` does neither FS IO nor logging setup.
- **Outdated:** Entire premise. Phrases to strike: "singleton," "instantiates at
  import time," "created eagerly on every import path," "logging configuration
  … before the caller opts in." Reality: `get_app_service()` is a lazy factory;
  YAML load, path resolution, and `dictConfig` run only on first call and only
  after the `_require_settings` gate. The "singleton's state must be discarded
  between runs" concern is moot (nothing persists across calls).
- **Gap:** The block fails to distinguish the **two** real tiers: (a) import-time
  (stdout reconfigure only, [app.py:16]) and (b) call-time bootstrap (settings
  gate → `dictConfig` → log-file creation, [app_service.py:299-303]). It also does
  not ask whether a *malformed-but-present* config fails fast at the gate — it
  does **not**: `_require_settings` checks only existence
  ([app_service.py:279-283](../src/mko_data_cleaner/core/app_service.py#L279-L283));
  malformed YAML passes the gate and fails later as `ConfigError` during
  `DataSettings` validation ([app_service.py:41-46](../src/mko_data_cleaner/core/app_service.py#L41-L46)).
- **Overlap:** Overlaps Block 1 (import-time behaviour) and Block 4 (the gate).
  Recommendation: **rename/rewrite as "Lazy Service Factory & Bootstrap."**

### Block 3 — stdout / sys Reconfiguration at Import Time
- **Fit:** **High (this block is fully aligned).** `app.py:16` is a genuine
  module-level `sys.stdout.reconfigure(line_buffering=True)` that runs on every
  `import mko_data_cleaner.app`, including library use, and can alter a host
  process's stdout contract (e.g. a Jupyter kernel or test runner capturing
  `stdout`). This is the one real, code-verified import-time side effect the
  phase must surface.
- **Outdated:** None. The block's question ("does any import-time statement
  reconfigure `sys.stdout`?") maps exactly to `app.py:16`.
- **Gap:** None.
- **Overlap:** None substantive (Block 1 only *counts* import-time effects;
  Block 3 *inspects* this one).
- **Note:** The architecture summary explicitly records this as the residual
  import-time side effect and confirms `core/*` does not touch stdout — so the
  block's scope is correctly bounded to `app.py`.

### Block 4 — Configuration Bootstrap Gate
- **Fit:** **High.** `_require_settings`, `build_app_paths`, `get_app_service`,
  `init_project`/`_copy_templates`, and `SettingsNotInitializedError` all exist
  and are the correct subjects.
- **Outdated:** Minor wording. "must succeed at import" is wrong — the gate is
  **lazy** (deferred to `get_app_service()`), not import-time. The block should
  be reframed so the gate is described as a call-time, opt-in check, not an
  import-time contract.
- **Gap (real):** The block does **not** ask whether `init`/`initialize_settings`
  honor the `settings_dir` override. They **do not**: `init_project()` and
  `_copy_templates()` always write to the hardcoded
  `APP_PATHS.user_settings_dir` ([init_service.py:30-42](../src/mko_data_cleaner/core/init_service.py#L30-L42)),
  while `run`/`process_data` read from `settings_dir` when provided
  ([app_service.py:258-269](../src/mko_data_cleaner/core/app_service.py#L258-L269)).
  Concretely, `initialize_settings(force=False, settings_dir=X)` accepts
  `settings_dir` but silently ignores it ([app.py:121-127](../src/mko_data_cleaner/app.py#L121-L127),
  with a `NOTE` comment admitting it). This is discrepancy **D-6** (owner-confirmed
  target work) and is a genuine entry-architecture problem: a user can `init` into
  a custom dir, then `run` pointing at that dir, and never hit `SettingsNotInitializedError`
  — but the reverse ordering (`run` then `init` with a custom dir) yields settings
  in the default location while `run` looks in the custom dir. Block 4 should
  explicitly check this `init`/`run` `settings_dir` asymmetry.
- **Overlap:** Overlaps Block 2 (bootstrap ordering). Acceptable after Block 2 is
  rewritten.

### Block 5 — Entry-Layer Discipline and Dependency Direction
- **Fit:** **High.** The layering, acyclicity, and "CLI must not embed domain
  rules" framing are correct and verified: no `core/*` module imports `app.py`,
  and call direction is strictly `app.py → core/*`
  ([architecture.md:78](../docs/00-overview/architecture.md#L78)).
- **Outdated:** None on direction/acyclicity.
- **Gap (medium):** The block asks whether "the public API surface is minimal and
  stable," but the current `__all__` in `app.py` over-exports internal/CLI
  symbols (`get_app_service`, `init`, `run`, `app`, `list_settings`) beyond the
  two-function contract the spec (R4) mandates, and the package root (`__init__.py`)
  does **not** re-export `process_data`/`initialize_settings`, so the documented
  `from mko_data_cleaner import process_data` fails. This is discrepancy **D-8 #4**
  and a real gap relative to Block 5's own "minimal and stable" criterion.
- **Overlap:** None.
- **Minor smell (not in block but adjacent):** a stale dev `__main__` block at the
  bottom of the **core** module `app_service.py:306-307`
  (`get_app_service().run_report(r"data\mvideo")`) hardcodes a data path and calls
  `run_report` (which needs settings). It is unreachable from normal entry but
  represents domain execution embedded in an internal orchestrator module —
  worth a LOW hygiene note.

### Block 6 — Process Topology and Shutdown
- **Fit:** **None.** This is the largest divergence. The block assumes an
  asynchronous, event-loop-based runtime that does not exist.
- **Outdated:** **Entirely.** Every sentence is wrong for the current
  architecture:
  - "single-shot and **asynchronous**" → synchronous.
  - "CLI wraps its work in a single `asyncio.run` call" → the CLI is plain Typer;
    no `asyncio.run` anywhere.
  - "the library API provides both sync and async wrappers" → only a sync
    `process_data`; no async wrapper exists.
  - "the event loop is created once per call" → there is no event loop.
  - "pending tasks are awaited or abandoned on cancellation" → no tasks, no
    cancellation surface.
  - "the singleton's state (semaphore, queue) is safely discarded between runs"
    → no singleton, no semaphore, no queue.
- **Gap:** The **actual** topology is uncovered by any surviving block:
  one OS process per CLI/library invocation; work is a straight synchronous call
  chain `app → AppService.run_report → CSVWorker/DBWorker/MappingDict`; the only
  long-lived resource is the disposable SQLite DB, owned by a `DBWorker` context
  manager that checkpoints the WAL and deletes the DB file on `__exit__`
  ([app_service.py:133](../src/mko_data_cleaner/core/app_service.py#L133),
  [db_service.py `__exit__`](../src/mko_data_cleaner/core/db_service.py)); no
  state persists between runs because nothing is cached at module scope.
- **Overlap:** Overlaps Block 2 (singleton state). Both reference a non-existent
  singleton. Recommendation: **replace Block 6 wholesale** with a synchronous
  single-shot topology block and **delete** the async/event-loop/cancellation
  framing, since asyncio has zero presence in the codebase (Phase 01 scope also
  defers Semaphore/Queue to phase 03, which is itself moot here).

---

## 3. Phase Purpose / Scope Assessment

The **Purpose** and **Scope Boundaries** sections describe a different project
than the one in `docs/00-overview/` and `src/`. Concrete corrections needed:

| Phase 01 text | Reality | Action |
|---|---|---|
| "standalone **async** CLI tool" | synchronous, no event loop, no asyncio ([grep: 0 matches](#1-high-level-architecture)) | remove "async" |
| "fetches data from the Mediascope API" | no network/credentials; exports arrive as files ([architecture.md:37-40](../docs/00-overview/architecture.md#L37-L40)) | remove Mediascope API fetch |
| `list_reports` | `list_settings` ([app.py:72](../src/mko_data_cleaner/app.py#L72)) | rename |
| `run_report`, `run_report_async`, `initialize_settings` | `process_data`, `initialize_settings` ([app.py:105,121](../src/mko_data_cleaner/app.py#L105)) | replace with real API names |
| "singleton `app_service` instantiates at import time … as side effects" | lazy factory `get_app_service()`; no import-time FS/network/logging ([app_service.py:290](../src/mko_data_cleaner/core/app_service.py#L290)) | reword to "lazy factory, opt-in bootstrap" |
| "module entry (`python -m mko_data_cleaner`)" | no `__main__.py`; working form is `python -m mko_data_cleaner.app` ([app.py:130](../src/mko_data_cleaner/app.py#L130)) | verify and correct |

**Scope boundaries** are also vestigial. They reference phases
"async Semaphore/Queue/worker-pool semantics (03)", "the blocking-work bridge
mechanism (09)", and "config values and credential provenance (02)." None of
these apply: there are **no async primitives, no blocking-work bridge, and no
credentials** in this codebase. The "credential provenance" exclusion is
particularly misleading because the project has no credentials at all. The scope
boundaries should be rewritten against the actual project and should (a) **drop
all async/concurrency references**, (b) drop the credential/provenance exclusion
as inapplicable, and (c) clarify that the **lazy bootstrap ordering** and the
**settings_dir forwarding asymmetry between init and run** are explicitly in
scope for this phase (currently implied, not stated).

The Purpose also does not mention the **import-time stdout reconfiguration**
([app.py:16](../src/mko_data_cleaner/app.py#L16)), which is the one real
import-time side effect — it should be called out as a core subject of Block 3.

---

## 4. Severity Taxonomy Assessment

The **principle** ("grade by effect and blast radius, not mechanism name; rate
what is true now, not the worst consequence if triggered") is sound and should be
**retained verbatim**. The **illustrative examples** are misaligned and must be
re-grounded:

- **CRITICAL example** mentions "a singleton that performs network or filesystem
  I/O on import" and "corrupts a host process's stdout or logging without
  consent." The singleton/network framing is inapplicable (no singleton, no
  network). The **stdout corruption** sub-example *is* relevant — `app.py:16`
  reconfigures `sys.stdout` on import — but it is better graded **MEDIUM** today
  (line-buffering is low-impact; it mutates the host stream without opt-in). The
  CRITICAL band should instead be anchored to: a bootstrap gate that **silently**
  proceeds on missing/malformed config — which currently does **not** happen
  (`_require_settings` raises). So the existing CRITICAL example overstates
  today's risk; the real CRITICAL would be *reverting* to silent fallback.
- **HIGH example** "an entry module that embeds domain rules or persistence" —
  **relevant** (app.py keeps all domain logic in core; the only bleed is the
  stale `app_service.py:306-307` dev block — a LOW).
- **MEDIUM example** mentions "environment-tier divergence between the CLI and
  library entry surfaces that is not intentional" — **relevant** and currently
  **alive**: `run`/`process_data` accept `settings_dir` but
  `initialize_settings`/`init` ignore it (D-6), an intentional-seeming
  **divergence that is not intentional**. This belongs in the MEDIUM band.
- **LOW example** "documentation or comment drift about which surfaces exist" —
  **relevant** (the Phase 01 Purpose itself, plus the `app.py` module docstring's
  stale `mko_get_mediascope_data` project name, [app.py:2](../src/mko_data_cleaner/app.py#L2),
  and the empty `__init__.py` failing the documented `from mko_data_cleaner import
  process_data` — D-8 #4/#5).
- The taxonomy's CRITICAL/HIGH/MEDIUM/LOW **labels** and the "empty band is a
  valid outcome" rule are fine and need no change; only the **mechanism-named
  examples** (network, singleton, asyncio) do.

**Net:** keep the banding principle and severity labels; rewrite every
example to reference the *actual* runtime — stdout reconfigure (import-time
mutation), fail-fast-vs-silent config (now fail-fast, so CRITICAL is a
"don't regress" guard), `settings_dir` asymmetry (MEDIUM), and docs/project-name
drift (LOW). Remove all async/singleton/network/credential examples.

---

## 5. List of Concrete Problems to Address

For the researcher who rewrites Phase 01. Each item maps to a specific edit in
`.kilo/commands/audit/phases/01-audit-entry-architecture.md`. **No source files
are modified by this report.**

1. **[PURPOSE] Strip the async/network fiction.** Remove "async" from the
   purpose; replace the Mediascope-API-fetch framing with "post-processes
   Mediascope exports received as files on disk; no network, no credentials."
   *Evidence: `docs/00-overview/architecture.md:37-40`; grep for `asyncio`/`async
   def` returns 0 matches in `src/`.*

2. **[PURPOSE] Correct the entry-surface names.** Replace `list_reports`→`list_settings`;
   replace the library API `run_report`/`run_report_async`/`initialize_settings`
   with `process_data`/`initialize_settings`; note the module entry is
   `python -m mko_data_cleaner.app`, not `python -m mko_data_cleaner` (no
   `__main__.py`).
   *Evidence: `app.py:72,105,121,130`; `pyproject.toml:46`; glob confirms no
   `__main__.py`.*

3. **[PURPOSE/Scope] Remove the singleton-at-import claim.** The service is a
   lazy factory; reword "singleton `app_state` instantiates at import time and
   performs configuration loading and logging configuration as side effects" to
   "the service is constructed lazily by `get_app_service()`; import has no
   filesystem/network/logging side effects; stdout reconfigure remains."
   *Evidence: `app.py:16`; `app_service.py:290-303`; `errors.py:20`.*

4. **[PURPOSE/Scope] Rewrite the Scope Boundaries.** Delete the
   async/Semaphore/Queue (03) and "blocking-work bridge" (09) exclusions and the
   "credential provenance" exclusion as inapplicable (no asyncio, no secrets).
   Add an explicit statement that the **lazy bootstrap ordering** and the
   **`settings_dir` forwarding asymmetry between `init`/`initialize_settings`
   and `run`/`process_data`** are in scope.

5. **[Block 2] Replace "Singleton Bootstrap at Import Time" with "Lazy Service
   Factory and Bootstrap."** Document: (a) what runs at import (stdout
   reconfigure only); (b) what runs at `get_app_service()` call (settings gate →
   config load → `dictConfig`); (c) that the gate raises on missing files; (d)
   that **malformed-but-present** YAML is **not** caught at the gate and fails
   later as `ConfigError`.
   *Evidence: `app.py:16`; `app_service.py:279-283` (existence-only gate);
   `app_service.py:41-46` (validation failure path);
   `app_service.py:299-303` (dictConfig after gate).*

6. **[Block 4] Add the `settings_dir` forwarding check.** The block must ask
   whether `init`/`initialize_settings` honor a caller-supplied `settings_dir`.
   They do **not** — `init_project`/`_copy_templates` always use the hardcoded
   `APP_PATHS.user_settings_dir`, while `run`/`process_data` honor the override.
   This is discrepancy D-6 and a real MEDIUM finding.
   *Evidence: `init_service.py:30-42`; `app.py:121-127` (NOTE comment);
   `app_service.py:258-269` (build_app_paths honors override).*

7. **[Block 5] Tighten the "minimal and stable public API" lens.** Flag that
   `app.py.__all__` over-exports `get_app_service`, `init`, `run`, `app`,
   `list_settings` beyond the two-function contract (R4), and that the package
   root `__init__.py` does not re-export `process_data`/`initialize_settings`,
   so the documented `from mko_data_cleaner import process_data` fails (D-8 #4).
   *Evidence: `app.py:134-142`; `__init__.py` (only `__version__`).*

8. **[Block 6] Replace with a synchronous single-shot topology block.** Remove
   all `asyncio.run` / event-loop / task-cancellation / semaphore / queue
   framing. Describe: one OS process per invocation; synchronous call chain
   `app → AppService.run_report → CSVWorker/DBWorker/MappingDict`; the only
   durable resource is the disposable SQLite DB owned by the `DBWorker` context
   manager (checkpoint WAL + delete DB on `__exit__`); no module-scope state, so
   nothing leaks across runs.
   *Evidence: `app.py:105-118` (sync `process_data`); `app.py:130-131` (Typer,
   no asyncio.run); `app_service.py:133` (`with DBWorker`); `db_service.py
   __exit__`; grep: 0 occurrences of `asyncio`.*

9. **[Block 3] Keep as-is.** It is the one block fully aligned with reality
   (`app.py:16` import-time stdout reconfigure). Optionally add that `core/*` does
   not reconfigure stdout (narrowing the blast radius to `app.py`).
   *Evidence: `app.py:16`; reading of core modules.*

10. **[Severity taxonomy] Re-base the examples.** Keep the principle and the
    CRITICAL/HIGH/MEDIUM/LOW labels and the "empty band is valid" rule. Rewrite
    examples to the real runtime: import-time stdout mutation (now MEDIUM, not
    CRITICAL); silent config fallback as the CRITICAL *regression guard* (the
    gate currently raises, so this band stays empty unless violated);
    `settings_dir` init/run asymmetry (MEDIUM); docs/project-name drift
    (`app.py:2` stale `mko_get_mediascope_data`) and the `__init__.py`
    re-export failure (LOW). Drop all network/singleton/asyncio examples.

11. **[Scope note] Verify/note the module entry.** Confirm whether
    `python -m mko_data_cleaner` is intended as a supported surface; with no
    `__main__.py` it currently only works as `python -m mko_data_cleaner.app`.
    Record this as evidence for whoever owns the entry-surface inventory.

12. **[Minor, adjacent] Flag the stale dev block in core.**
    `app_service.py:306-307` runs
    `get_app_service().run_report(r"data\mvideo")` under `if __name__ ==
    "__main__"` in an *internal orchestrator* module, hardcoding a data path.
    Out of scope for Phase 01 but surfaces as a LOW hygiene item under Block 5's
    "entry module embeds domain execution" lens.

### Dependency map for the rewrite
- Blocks most impacted by premise errors: **2** (singleton→factory) and **6**
  (async→sync). These two should be rewritten first; 4 is reframed around the
  D-6 gap; 1 is re-grounded on real surfaces; 3 and 5 are keep-with-evidence.
- The Purpose/Scope and Severity sections are **foundational fixes**: every
  downstream block inherits the async/singleton framing, so correcting them
  removes the largest class of errors at the source.
- No findings from this report depend on changes to `docs/00-overview/*` or to
  `src/` — the audit is a documentation-of-the-audit itself; the report only
  prescribes edits to the Phase 01 markdown.
