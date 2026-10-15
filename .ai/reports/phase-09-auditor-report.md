---
phase: "09"
phase_name: "External Integrations & API"
phase_file: ".kilo/commands/audit/phases/09-audit-external-api.md"
report_status: "draft"
scope: >-
  deletion analysis for the external-API audit phase against a
  synchronous, single-shot, local-only CLI + library with no event loop,
  no network, no credentials, and no external API integration.
recommendation: DELETE
---

# Audit Report — Phase 09 External Integrations & API

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **local, synchronous, single-shot** CLI tool and Python
library that post-processes Mediascope exports received as **files on disk**.
There is **no outbound API, no credentials, no network, no asyncio, no retry/backoff,
and no event-loop bridge** anywhere in the current architecture.

Baseline, code-verified against the working tree (`src/mko_data_cleaner/`,
`docs/00-overview/`, `pyproject.toml`):

- **Zero async primitives.** A repo-wide grep for `asyncio`, `async def`,
  `asyncio.run`, `Semaphore`, `Queue`, `requests`, `httpx`, `aiohttp`, `token`,
  `credential`, and `api_key` across `src/mko_data_cleaner` returns **1 match** —
  a documentation URL comment in `db_service.py:435`
  (`https://www.sqlite.org/fts5.html#external_content_tables`), not code.
  The grep for `*.py` files across the entire repo returns 2 matches, both in
  `.ai/tmp/verify_notebook.py` (a scratch Jupyter-verification helper, not
  production code). ([Phase 01 auditor report §1](../.ai/report/phase-01-auditor-report.md),
  [Phase 02 auditor report §1](../.ai/reports/phase-02-auditor-report.md)).
- **No external API / no credentials / no network.** `architecture.md:37-40`
  states "External systems: none." Exports arrive manually as files; nothing is
  fetched. `pyproject.toml` dependencies are `adbc-driver-sqlite`, `pandas`,
  `platformdirs`, `polars`, `pyarrow`, `pydantic`, `pydantic-settings`,
  `python-dateutil`, `PyYAML`, `rich`, `typer`, `Unidecode`, `urllib3` —
  **no `requests`/`httpx`/`aiohttp`/`aiohttp`/`mediascope-api-lib`.** No
  `.env*` files exist anywhere in the repo (`glob **/.env*` → none). Phase 02
  §1 confirms zero credentials and zero secrets.
- **No concurrency layer.** The runtime is a straight synchronous call chain
  `app → AppService.run_report → CSVWorker → DBWorker/MappingDict`, one OS
  process per invocation. The only durable resource is a disposable SQLite DB
  owned by a `DBWorker` context manager.
- **Stale project-name residue, not integration code.** References to
  `mko_get_mediascope_data` and `mko-get-mediascope` exist in `app.py:2`
  (stale docstring), `.gitignore:224-225`, `MANIFEST.in:4,7`, and
  `.kilo/commands/audit/phases/12-audit-production-ops.md:86` — all are
  vestigial strings from a prior project incarnation. They are **not** an API
  client, credential file, or network call. Phase 02 §2 Block 2 (F10) already
  recorded these as stale-name drift with no runtime surface.

Phase 09's own Purpose (lines 13–20) assumes "the Mediascope API is reached through
a synchronous client (`mediascope-api-lib`) wrapped in `asyncio.to_thread`,"
"retries bounded by a max-attempts limit and exponential backoff plus jitter,"
and "credentials enter via a JSON file." **None of this exists.** The only
consumer-facing I/O is local file reads/writes and a local SQLite database.

## 2. Per-Block Assessment (1–8)

Each block is assessed for **Fit** (does the concept apply to the real
architecture), **Outdated** (is the premise entirely invalid), and whether any
useful concept survives a re-basing to the synchronous model.

### Block 1 — The credential seam: where a secret enters a request
- **Fit: None.** No secrets, no credentials, no request construction, no
  on-the-wire form. The system has no outbound requests.
- **Outdated: Entire premise.** "Path, query, header, or body" credential entry;
  "absent, empty and malformed as three distinct states"; "wire form" — no
  credential exists at any layer. `pyproject.toml`, `app.py`, `app_service.py`,
  `csv_service.py`, `db_service.py` contain no secret/credential/token fields.
- **Concept survives? No.** There is no consumer of credentials and no
  credential-handling surface to re-base. Phase 02 owns settings/secret policy
  and has already confirmed zero credentials exist.

### Block 2 — Retry contract: what is replayed, what bounds the total
- **Fit: None.** No outbound calls, no retries, no backoff, no jitter, no
  retry budget.
- **Outdated: Entire premise.** "Per-call bound vs. whole-operation bound";
  "replay-eligible failure classes"; "lost response after the write" — there
  are no remote writes, no lost responses, no failure classes to classify for
  retries. The pipeline is a single local pass; failures raise or abort by
  design (Phase 04 owns error serialisation).
- **Concept survives? No.** Retry eligibility requires a retry mechanism.
  Phase 09 itself is the only phase that would own the retry/backoff contract;
  with no API to retry, there is nothing to re-base.

### Block 3 — Backoff cost and jitter
- **Fit: None.** No exponential backoff, no jitter, no retry sleep, no
  thundering-herd, no worst-case retry duration.
- **Outdated: Entire premise.** "Exponential backoff formula and its jitter
  range"; "worst-case total retry duration bounded by a caller-controllable
  input"; "retry sleep observable in logs or metrics" — no retries exist; no
  sleeps of any kind beyond the synchronous pipeline's natural I/O.
- **Concept survives? No.** Jitter and backoff cost are properties of a retry
  strategy that does not exist. The nearest analog (Polars chunked I/O memory
  pressure, SQLite WAL management) is owned by Phase 13, not this phase.

### Block 4 — The blocking-work bridge (`asyncio.to_thread`)
- **Fit: None.** No event loop, no `asyncio.to_thread`, no thread pool, no
  coroutine worker, no async/sync boundary.
- **Outdated: Entire premise.** "Synchronous call wrapped in
  `asyncio.to_thread`"; "connection released on the exception path";
  "worker cancelled while `to_thread` is in flight" — no event loop exists,
  no coroutines run, no cancellation surface. The runtime is one OS thread
  making synchronous calls to Polars (CSV) and ADBC (SQLite).
- **Concept survives? No — and redundant.** The underlying concern (clean
  resource release on exit/error) is already owned by Phase 01 Block 6
  ("Process Topology and Shutdown"), which records the `DBWorker` context
  manager (`checkpoint WAL + delete on __exit__`, `app_service.py:133`,
  `db_service.py __exit__`). Re-basing here would duplicate Phase 01.

### Block 5 — Degradation: what each dependency's failure takes down
- **Fit: None.** No external dependencies to degrade against. The only
  resources are local files and a local SQLite database.
- **Outdated: Entire premise.** "Mediascope API endpoints, the credentials
  file, the export folder" as a derived dependency set; "blast radius is one
  task or the whole run"; "raising guard turns targeted outage into total" —
  there are no remote endpoints, no credentials file, no API degradation. The
  pipeline runs as a single synchronous batch; a failure aborts the whole
  invocation by design, not by dependency-outage logic.
- **Concept survives? No.** Dependency blast-radius analysis requires
  dependencies that can fail independently. Local file/SQLite failures are
  owned by Phase 04 (data pipeline & crash safety), not this phase.

### Block 6 — A degraded result the consumer cannot tell from a real one
- **Fit: None.** No fallback values, no degraded substitution, no breaker/
  limiter, no result that could be confused with a real one.
- **Outdated: Entire premise.** "Fallback and default"; "substituted value
  stored where a real one was required"; "breaker or limiter open and nothing
  counts or logs it" — the pipeline performs no conditional substitution; there
  are no fallbacks, no circuit breakers, no limiters. `null_data_*` files are
  the product of a real, observable export path (`app_service.py:246-252`),
  not a silent fallback.
- **Concept survives? No.** Silent-substitution detection requires a
  substitution mechanism. The nearest analog (dead/unread config fields being
  silently defaulted) is owned by Phase 02 / Phase 05, not this phase.

### Block 7 — What the integration surface writes into logs
- **Fit: None.** No integration surface, no outbound client, no request
  values logged, no response/outcome logging for API calls.
- **Outdated: Entire premise.** "Outbound API client writes about values sent
  and outcomes received"; "sanitiser masks the credential or task key";
  "dependency failure state measurable via counter, log, or exception" — no
  API client exists to log from. The system's loggers (`app_service`,
  `csv_service`, `db_service`, `dict_service`, `utils`) log only local
  file/DB operations (rows, chunks, timings), not remote interactions. The
  logging surface is owned by Phase 12 (observability), not this phase.
- **Concept survives? No.** Integration logging requires an integration.
  Phase 12 already owns the observability surface for the real pipeline.

### Block 8 — Declared dependency vs. code
- **Fit: None** for its API/credential framing; **the *concept** is valid
  (doc-vs-code drift) but already owned by Phase 10 (code-quality convention &
  declared-contract enforcement) and Phase 02 (dead-code/dead-config sweep).
- **Outdated: Entire premise.** "Capability the repository claims to have";
  "credential or endpoint it says it fetches"; "runbook instruction" — the
  only "declared external capability" is the stale `mko_get_mediascope_data`
  name, which Phase 02 §2 Block 2 (F10) already recorded as vestigial string
  drift with zero runtime surface. No JSON credential file is declared in
  `pyproject.toml`, no `connections/` directory exists, no OAuth/token flow is
  claimed in any shipped config or runbook.
- **Concept survives? No — redundant.** The doc-vs-code-drift concern is real,
  but it maps to Phase 10's mandate ("asks of every claimed property whether
  anything checks it") and Phase 02's dead-config sweep. Keeping it under the
  Phase 09 umbrella would double-cover territory already assigned and, worse,
  would keep alive a file whose entire block set is API-framed and moot.

### Assessment summary
All eight blocks are **Fit: None**, **Outdated: entirely**. The underlying
engineering concerns (credential safety, retry safety, resource cleanup,
observability, doc-vs-code drift) are either **non-existent** in this codebase
(there are no credentials, no retries, no API, no async bridge to log) or
**already owned by other phases** that were re-based to the synchronous model
(Phase 01 for teardown, Phase 02 for dead-config, Phase 04 for error/
crash-safety, Phase 10 for declared-contract enforcement, Phase 12 for
observability). Phase 09 has no distinct synchronous subject to re-base onto —
unlike Phase 01 Block 2 (Singleton → Lazy Service Factory) or Phase 05 (re-theming
template conformance to real Pydantic models), Phase 09 has **no synchronous
analog** to any of its blocks.

## 3. Cross-Phase Citation Inventory

### Phases that 09 CITES (scope boundaries, lines 22–28)
09 names the following as out-of-scope ownership, all async/API-framed:

| Cited phase | Property 09 defers to it |
|---|---|
| 01 | entry/bootstrap and import-time behaviour |
| 02 | settings values and secrets |
| 03 | Semaphore/Queue/worker-pool semantics and the cancellation mechanism |
| 04 | period resolution, task generation, and error serialisation |
| 05 | template validation |
| 10 | enum and boundary-model discipline |
| 11 | test adequacy |
| 12 | CI/CD and observability |
| 13 | concurrency ceilings and retry cost |

All 09→N citations are to async/API/retry concepts that **do not exist** in the
current codebase. (Several of these (01, 02, 05) have already been re-based to
the synchronous model; 03, 04, 10, 11 are in various states of re-basing; 12,
13 are async-framed deletions/reworks in progress.)

### Phases that CITE 09 (who references Phase 09)

| Phase | Line(s) | Current text | Disposition |
|---|---|---|---|
| 10 | 17 | "the event-loop bridge mechanism (09)" | **remove** (09 deleted) |
| 12 | 25 | "outbound API call contract (09)" | **remove** (09 deleted) |
| 13 | 25–27 | "the retry and backoff contract (09)" / "the blocking-work bridge (09)" | **remove** (09 deleted) |
| 15 | 27 | "outbound API mechanics, retry contract, and credential-on-the-wire (09)" | **remove** (09 deleted) |

All four citing phases (10, 12, 13, 15) are themselves on the rework/deletion
list — they carry the same async/Mediascope-API fiction and require their own
re-basing (mirroring how Phase 01/02 were re-based to "files on disk, no
network"). The citation removals from each are **mechanical** (strip the `(09)`
reference and its surrounding async/API phrase); the broader async re-basing of
12/13/15 is follow-on work, not part of this deletion.

**Orchestration files:** `audit-multi-phase.md` enumerates phases by listing
the `phases/` directory, so deleting `09-audit-external-api.md` removes it
from the selectable set automatically — no hard-coded sequence to edit.
`audit-orphaned.md`, `audit-phase-gaps.md`, `audit-refine.md`, and
`audit-phase-refine.md` contain no hard-coded Phase 09 sequence reference
(grep for `09` over `.kilo/commands/audit/` returns matches only in this phase
file and in Phase 12 line 109's "HTTP 404", which is unrelated HTTP-status
notation, not a phase citation).

## 4. Severity Taxonomy Assessment

Phase 09's taxonomy (lines 142–166) opens with the **sound principle** ("grade
by effect and blast radius, not by mechanism name; rate what is true now, not
the worst consequence if triggered; an empty band is a valid outcome"). That
principle should be retained project-wide.

However, **every illustrative example is API/credential/retry-framed and
therefore moot** for this codebase:

- **CRITICAL:** "a credential reachable without any gate"; "a credential made
  usable by a path the system does not control"; "a retryable write that is not
  idempotent, so a lost response triggers a duplicate side effect at the far
  side." → No credentials exist; no remote writes occur; no responses can be
  lost.
- **HIGH:** "an operation reports success while the far side never received
  it"; "a substituted result stored where a real one was required and nothing
  can tell them apart"; "a per-process bound that is unbounded once a single
  invocation multiplies its own retries"; "one dependency's failure taking down
  a whole report rather than a single task." → No far side; no substitution;
  no retries; the pipeline is a single local pass whose failure aborts the
  whole invocation by design (owned by Phase 04 / Phase 01, not here).
- **MEDIUM:** "a replay bounded per attempt but not across the operation";
  "a fallback with a log but no counter"; "a guard applied on one path and
  absent from a sibling reaching the same dependency"; "a retry-exact backoff
  without jitter." → No replay, no fallback, no guard, no backoff.
- **LOW:** "a declared dependency the code does not implement"; "a comment or
  docstring naming a predicate the executing path does not evaluate"; "a log
  line reporting an attempt as a confirmation"; "a boundary accepting a
  rejected field." → No declared external dependency; the only "declared
  capability" is the stale `mko_get_mediascope_data` name (Phase 02 §2 F10,
  already filed as LOW drift).

**Decision: the taxonomy does not survive.** Because the entire phase is
deleted, there is no surviving taxonomy to re-base. The generic concerns the
examples tried to name (credential safety, observable failure, clean teardown)
are either absent from this codebase or already covered by Phase 01's
synchronous taxonomy (clean teardown, CRITICAL = fail-fast gate) and Phase 05's
config-accuracy taxonomy (doc-vs-code drift). Re-basing this taxonomy in
isolation would add a phase whose severity bands could **never** be populated —
every band is empty by design, and the underlying conditions (credentials,
remote writes, retries) do not exist to populate them.

## 5. Report Output Assessment

| Item | Current (Phase 09 §Report Output) | Verdict |
|---|---|---|
| Findings path | `.ai/audit/09-external-api/findings.md` | **Void.** Directory confirmed absent — no findings were ever produced (`Test-Path` → `False`). With no API surface to audit, no findings path is needed. |
| ID prefix | `EXT-` | **Void.** Unique in the repo (no existing `EXT-` collision), but unused — no findings will ever be minted. |
| problems-only | `true` (front matter + §176) | **Moot.** No problems exist; the only valid output would be the empty-state string, but the phase is deleted so even that output path goes with it. |
| per-pass lines | ≤100, incremental append (§175) | **Moot.** No findings to append. |
| Empty state | `No problems found in this phase.` (§178) | **Moot.** Phase deletion removes the phase entirely; the empty-state string is not needed. |
| Shipped-test-asserts-current-behaviour | §183: "a shipped test asserting current behaviour is a remediation blocker" | **Moot.** No API tests exist; `pyproject.toml` has no async test markers (the `#    "--asyncio-mode=auto"` line is commented out). |

**Decision: the report output does not survive.** There are no findings, no
output directory, and no ID prefix to carry forward. Deleting the phase file
removes all five output specifications with it.

## 6. Candidate Findings

**None.** No API exists, no credentials exist, no async primitives exist, no
retry/backoff exists, no network exists. Every block assesses to Fit: None
with an entirely outdated premise. There are no candidate findings to emit
under the `EXT-` prefix, and the empty state `No problems found in this phase.`
is itself rendered moot by the deletion recommendation.

## 7. Conclusion — Should Phase 09 Be Deleted Entirely?

**Yes — delete Phase 09 entirely. No re-basing.**

Rationale:

1. **No external API exists.** The current architecture is synchronous and
   single-shot with no event loop, no network, and no credentials. A repo-wide
   grep for `asyncio`, `async def`, `asyncio.run`, `Semaphore`, `Queue`,
   `requests`, `httpx`, `aiohttp`, `token`, `credential`, and `api_key` across
   `src/` returns **zero code matches** (the single `https://` hit in
   `db_service.py:435` is a documentation URL comment). `pyproject.toml`
   declares no HTTP/async dependency. No `.env*` files exist. Phase 09's
   central subject — the Mediascope API client (`mediascope-api-lib`),
   `asyncio.to_thread` bridge, JSON credentials, exponential backoff with
   jitter, credential-on-the-wire, dependency degradation, degraded-result
   indistinguishability, and integration logging — is **entirely fictional**
   for this codebase.

2. **All eight blocks are Fit: None.** Every block's premise (credential seam,
   retry contract, backoff/jitter, blocking-work bridge, blast radius,
   fallback indistinguishability, integration logging, declared-dependency-vs-
   code) has zero analog. The only genuinely portable concept (Block 8's
   doc-vs-code drift) is already owned by Phase 10 (declared-contract
   enforcement) and Phase 02 (dead-config sweep). Re-basing would produce a
   phase with no distinct responsibility — worse, it would keep alive a file
   whose entire vocabulary (API, credentials, retries, async bridge) has no
   referent in the code.

3. **No surviving concept is unique to this phase.** Unlike Phase 01 Block 2
   (transformed "Singleton → Lazy Service Factory") or Phase 05 (re-themed
   template-conformance blocks to real Pydantic models), Phase 09 has **no
   synchronous analog** to any of its eight blocks. The synchronous analog of
   "clean resource release on error" is already Phase 01 Block 6; "local file
   crash-safety" is Phase 04; "observability surface" is Phase 12; "doc-vs-code
   drift" is Phase 10. There is no gap left to fill.

4. **The downstream phases are async-framed themselves.** Phases 10, 12, 13
   (and 15) carry the same async/Mediascope-API fiction and need the same
   re-basing treatment Phase 01/02 received. Deleting Phase 09 removes one
   citation target; the four `(09)` citation references in Phases 10, 12, 13,
   15 must be dropped mechanically (§3 table), with the broader async re-basing
   of 12/13/15 tracked as separate, sequenced follow-on work.

5. **No findings, no output, no precedent to preserve.** Phase 09 produced
   **zero findings** (`.ai/audit/09-external-api/` does not exist). Its
   front matter is `status: draft`, `validated: no`. There is no remediation
   blocker to honour. The Phase 03 auditor report (which applied this exact
   deletion logic to the async-concurrency phase) and the Phase 02 auditor
   report §2 Block 2 (which already deleted the parallel "credential seam"
   from Phase 02) both establish the precedent: when an audit phase's entire
   premise is async/API/credential-framed and the codebase is synchronous-
   local-only, the phase is deleted, not re-based.

**Recommended action:** Delete
`.kilo/commands/audit/phases/09-audit-external-api.md` and remove the `(09)`
out-of-scope citations in Phases 10 (line 17), 12 (line 25), 13 (lines 25–27),
and 15 (line 27) as a follow-on mechanical edit. The findings output path
`.ai/audit/09-external-api/findings.md` and Finding-ID prefix `EXT-` are
correspondingly void.

**Effort to delete: trivial** (one file removal + ≤3 citation line-edits in
Phases 10/12/15 whose async re-basing is already tracked). Phase 13's two
`(09)` references are removed as part of its own async re-basing (Phase 13
Block 6 owns retry/backoff cost).
**Priority: recommended** — not mandatory, since Phase 09 has `status: draft`
and produced no findings, but safe to remove now and prevents auditors from
chasing a phantom Mediascope-API model that does not exist.

---

## Appendix A — Evidence pedigree

- **Architecture source files read:** `src/mko_data_cleaner/app.py` (full, 165
  lines), `src/mko_data_cleaner/core/app_service.py` (full, 342 lines),
  `src/mko_data_cleaner/core/csv_service.py` (full, 375 lines),
  `src/mko_data_cleaner/core/db_service.py` (first 50 lines + grep hits).
- **Dependency declaration read:** `pyproject.toml` (full, 183 lines) — no
  `requests`/`httpx`/`aiohttp`/`mediascope-api-lib`; `urllib3` listed but
  unused in `src/` (grep for `urllib` in `*.py` → only
  `.ai/tmp/verify_notebook.py`).
- **Primitive grep:** `src/mko_data_cleaner/` for
  `asyncio|async def|asyncio.run|Semaphore|Queue|requests|httpx|aiohttp|
  token|credential|api_key` → **1 match** (`db_service.py:435`, URL comment).
  Repo-wide `*.py` grep for `asyncio|async def|aiohttp|httpx|requests\.|
  http.client|urllib` → **2 matches**, both in `.ai/tmp/` (scratch, not
  production).
- **Environment files:** `glob **/.env*` → **0 files**.
- **Findings directory:** `Test-Path .ai/audit/09-external-api` → **False**
  (never created; no findings ever produced).
- **Cross-phase citations:** grep over `.kilo/commands/audit/phases/` for
  `09|external-api|mediascope|asyncio|credential|retry` — 4 citing phases
  confirmed (10, 12, 13, 15); 9 phases cited by 09 (01–05, 10–13).
- **Precedent:** Phase 03 auditor report
  (`.ai/reports/phase-03-auditor-report.md`) — same deletion logic applied to
  the async-concurrency phase; Phase 02 auditor report §2 Block 2 — already
  deleted the parallel "credential seam" block from Phase 02.
- **Docker runtime:** Not available in this environment (`docker` command not
  found on PATH). Not required — this audit is a static architecture-vs-doc
  comparison; the codebase has no runtime to start (no server, no API endpoint,
  no containerised service to probe). The grep evidence above is conclusive.
