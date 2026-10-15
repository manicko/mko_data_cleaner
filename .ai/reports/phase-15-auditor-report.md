---
phase: "15"
phase_name: "Authorization & Credential Lifecycle"
phase_file: ".kilo/commands/audit/phases/15-audit-authorization.md"
report_status: "draft"
scope: >-
  deletion analysis for the authorization audit phase against a
  synchronous, single-shot, local-only CLI + library with no event loop,
  no network, no credentials, no external API, and no auth-failure surface.
recommendation: DELETE
---

# Audit Report — Phase 15 Authorization & Credential Lifecycle

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **local, synchronous, single-shot** CLI tool and Python
library that post-processes Mediascope exports received as **files on disk**
(`.csv`, `.csv.gz`). There is **no outbound API, no credentials, no network, no
asyncio, no retry/backoff, and no event-loop bridge** anywhere in the current
architecture.

Code-verified baseline against the working tree (`src/mko_data_cleaner/`,
`docs/00-overview/`, `pyproject.toml`, `core/errors.py`):

- **Zero auth primitives.** A repo-wide grep for `asyncio|async def|asyncio.run|
  Semaphore|Queue|requests|httpx|aiohttp|token|credential|api_key` across
  `src/mko_data_cleaner/` returns **zero code matches**. The only hit for
  `credential` or `token` in the entire `src/` tree is `app.py:1` — a stale
  reference to the old package name `mko_get_mediascope_data` in a docstring
  (vestigial string, not an API/credential symbol). (`core/errors.py` defines
  exactly five exception classes: `NoReportFoundError`, `WrongDataSettings`,
  `DataValidationError`, `ConfigError`, `SettingsNotInitializedError` — no
  `AuthorizationError`.)
- **No external API, no credentials, no network.** Architecture (`docs/00-
  overview/architecture.md:37-40`) states "External systems: none." Exports
  arrive manually as files; nothing is fetched. `pyproject.toml` dependencies
  are `adbc-driver-sqlite`, `pandas`, `platformdirs`, `polars`, `pyarrow`,
  `pydantic`, `pydantic-settings`, `python-dateutil`, `PyYAML`, `rich`,
  `typer`, `Unidecode`, `urllib3` — **no `requests`/`httpx`/`aiohttp`/
  `mediascope-api-lib`**. `urllib3` is declared but unused in `src/` (grep for
  `urllib` in `*.py` → zero matches in production). No `.env*` files exist
  (`glob **/.env*` → none). No `connections/` directory exists
  (`Test-Path src\mko_data_cleaner\connections` → `False`).
- **No concurrency layer.** The runtime is a straight synchronous call chain
  `app → AppService.run_report → CSVWorker → DBWorker/MappingDict`, one OS
  process per invocation. The only durable resource is a disposable SQLite DB
  owned by a `DBWorker` context manager (`app_service.py:133`).

Phase 15's own Purpose (lines 13–20) assumes "a standalone async CLI tool that
fetches data from the Mediascope API," "the outbound API call is a synchronous
client wrapped in `asyncio.to_thread`," "retries bounded by a max-attempts
limit and exponential backoff plus jitter," "an `AuthorizationError` is handled
as a distinct, non-retriable 401 path," "a `refresh_token_func` hook is declared
as a planned extension," and "credentials enter via a JSON file in `connections/`."
**None of this exists.** The only consumer-facing I/O is local file reads/writes
and a local SQLite database.

## 2. Per-Block Assessment (1–4)

Each block is assessed for **Fit** (does the concept apply to the real
architecture), **Outdated** (is the premise entirely invalid), and why no
concept survives.

### Block 1 — Auth-failure classification and retry eligibility
- **Fit: None.** No 401 responses, no `AuthorizationError`, no 403, no retry
  mechanism, no backoff budget, no failure-class counter or log. The only
  failures are local file/SQLite errors handled by Phase 04's error
  serialisation layer (`errors.py` exceptions).
- **Outdated: Entire premise.** "401 distinguished from transient/permanent
  failures"; "401 or `AuthorizationError` excluded from retry"; "403
  classified differently from 401"; "refreshable 401 retried with a refreshed
  token" — there are no HTTP responses of any status, no auth error class,
  no refresh mechanism, and no retry budget to exhaust.
- **Concept survives? No.** Retry eligibility requires a retry mechanism that
  does not exist (Phase 09 Block 2, now deleted, covered the same ground).

### Block 2 — Token and credential lifecycle: acquisition, refresh, rotation
- **Fit: None.** No credential acquisition at session start, no refresh
  mechanism, no token expiry/rotation/revocation handling, no
  `refresh_token_func` hook.
- **Outdated: Entire premise.** "Credential acquired at session start"; "refresh
  mechanism exists and when triggered"; "expiry, rotation, revocation handled";
  "declared-but-unimplemented hook as a distinct state" — no credential enters
  the system; no JSON file in `connections/`; no token to refresh.
- **Concept survives? No.** The credential lifecycle has zero referent. Phase 02
  owns credential sourcing/protection-at-rest and has confirmed zero credentials
  exist.

### Block 3 — Credential leakage in failure paths
- **Fit: None.** No credentials, no token values, no 401 handler, no debug
  output of auth context, no exception strings carrying secrets.
- **Outdated: Entire premise.** "Authorization context — credential, token
  value, request arguments, headers — reaches a log line, exception string,
  serialised error file, or referrer" — no credential material exists at any
  layer. The loggers (`app_service`, `csv_service`, `db_service`, `dict_service`,
  `utils`) log only local file/DB operations (rows, chunks, timings), not remote
  interactions.
- **Concept survives? No.** Leakage detection requires material to leak. Phase 12
  owns the observability surface for the real pipeline.

### Block 4 — Scope and authority validation
- **Fit: None.** No credential scope, no authority bound to operations (task
  build, send, poll, result extraction), no under-scoped credential to detect.
- **Outdated: Entire premise.** "Credential's scope or authority validated
  against operations"; "under-scoped credential detected and reported distinctly
  from an outright auth failure"; "scope mismatch returns partial data without
  warning" — there are no API operations to scope against; the pipeline reads
  local files and writes to local SQLite.
- **Concept survives? No.** Scope validation requires an authority to validate.
  The only "scope" in this codebase is the Polars/CSV processing pipeline, owned
  by Phase 04.

### Assessment summary
All four blocks are **Fit: None**, **Outdated: entirely**. The underlying
concerns (credential safety, auth-failure classification, scope validation,
credential-token lifecycle) are either **non-existent** in this codebase (no
credentials, no 401, no tokens, no refresh, no retry budget) or **already
owned by other phases** (Phase 02 for credential sourcing; Phase 04 for error
serialisation; Phase 12 for observability). Phase 15 has **no distinct
synchronous subject** to re-base onto — unlike Phase 01 Block 2 (transformed
"Singleton → Lazy Service Factory") or Phase 05 (re-themed template conformance
to real Pydantic models), Phase 15 has no synchronous analog to any of its
blocks.

## 3. Cross-Phase Citation Inventory

### Phases that 15 CITES (scope boundaries, lines 22–28)
15 names the following as out-of-scope ownership, all auth/API-framed:

| Cited phase | Property 15 defers to it |
|---|---|
| 02 | credential sourcing, validation, and protection at rest |
| 09 | outbound API mechanics, retry contract, and credential-on-the-wire |
| 10 | code-quality convention and declared-contract enforcement |
| 11 | test-suite adequacy |

All four 15→N citations are to auth/API concepts that **do not exist** in the
current codebase. Note that (09) is itself on the deletion list (Phase 09
auditor report §7 — verdict: DELETE), so Phase 15's citation of (09) is doubly
void: it points to a phase that will be deleted and asserts an API contract
that never existed.

### Phases that CITE 15 (who references Phase 15)

| Phase | Line(s) | Current text | Disposition |
|---|---|---|---|
| 10 | 18 | `authorization (15)` | **remove** (15 deleted; Phase 10 is async-framed and on the rework/deletion list) |
| 03 auditor report | 163 | "Phase 15 — not yet read… assumes 'a standalone async CLI tool that fetches data from' the Mediascope API" | informational note; no citation edit needed (it is a recommendation note, not a cross-phase reference in a phase file) |

**Orchestration files:** `audit-multi-phase.md` enumerates phases by listing
the `phases/` directory, so deleting `15-audit-authorization.md` removes it
from the selectable set automatically — no hard-coded sequence to edit.

## 4. Severity Taxonomy Assessment

Phase 15's taxonomy (lines 86–106) opens with the **sound principle** ("grade
by effect and blast radius, not by mechanism name; rate what is true now, not
the worst consequence if triggered; an empty band is a valid outcome"). That
principle should be retained project-wide.

However, **every illustrative example is auth/credential/retry-framed and
therefore moot** for this codebase:

- **CRITICAL:** "a credential obtainable without any gate"; "an auth failure
  silently retried as transient"; "a 401 treated as retryable when a refreshed
  token could have succeeded first." → No credentials exist; no 401; no retry
  budget; no refresh hook.
- **HIGH:** "credential material or token values reaching logs"; "a 403
  misclassified as a transient network error"; "a declared token-refresh hook
  that is unimplemented." → No credentials, no 403, no logs of auth context,
  no declared refresh hook.
- **MEDIUM:** "scope mismatch not detected"; "an auth-failure message that is
  opaque or actionability-free"; "a refresh hook wired but never invoked." →
  No scope to mismatch; no 401 handler; no refresh hook.
- **LOW:** "documentation or comment drift about token lifetime, scope, or
  refresh"; "a declared refresh interval that is not enforced." → No token
  lifetime, scope, or refresh to drift about; the only stale-auth string is
  the `mko_get_mediascope_data` docstring in `app.py:2`, already recorded as
  LOW drift by Phase 02 §2 Block 2 (F10).

**Decision: the taxonomy does not survive.** Because the entire phase is
deleted, there is no surviving taxonomy to re-base. The generic concerns the
examples tried to name (credential safety, observable failure, scope
validation) cannot be populated — every band is empty by design, and the
underlying conditions (credentials, 401s, tokens, refresh, retry) do not exist
to populate them. Phase 10 (code-quality taxonomy) and Phase 05 (config
accuracy) already cover doc-vs-code drift on the real surface.

## 5. Report Output Assessment

| Item | Current (Phase 15 §Report Output) | Verdict |
|---|---|---|
| Findings path | `.ai/audit/15-authorization/findings.md` | **Void.** Directory confirmed absent (`Test-Path .ai\audit\15-authorization` → `False`). No findings were ever produced. With no auth/credential surface to audit, no findings path is needed. |
| ID prefix | `AUT-` | **Void.** Unique in the repo (no existing `AUT-` collision found in tracked findings), but unused — no findings will ever be minted. |
| problems-only | `true` (front matter + §116) | **Moot.** No problems exist; the only valid output would be the empty-state string, but the phase is deleted so even that output path goes with it. |
| per-pass lines | ≤100, incremental append (§115) | **Moot.** No findings to append. |
| Empty state | `No problems found in this phase.` (§118) | **Moot.** Phase deletion removes the phase entirely; the empty-state string is not needed. |
| Shipped-test-asserts-current-behaviour rule | §122: "a shipped test asserting current behaviour is a remediation blocker" | **Moot.** No auth/API tests exist; `pyproject.toml` has no async markers (the `--asyncio-mode=auto` line is commented out). |

**Decision: the report output does not survive.** There are no findings, no
output directory, and no ID prefix to carry forward. Deleting the phase file
removes all five output specifications with it.

## 6. Candidate Findings

**None.** No API exists, no credentials exist, no `AuthorizationError` exists,
no 401/403 handling exists, no `refresh_token_func` hook exists, no `connections/`
directory exists, no retry/backoff exists, no network exists, no scope
validation exists. Every block assesses to Fit: None with an entirely outdated
premise. There are no candidate findings to emit under the `AUT-` prefix, and
the empty state `No problems found in this phase.` is itself rendered moot by
the deletion recommendation.

## 7. Conclusion — Should Phase 15 Be Deleted Entirely?

**Yes — delete Phase 15 entirely. No re-basing.**

Rationale:

1. **No authorization surface exists.** The current architecture is synchronous
   and single-shot with no event loop, no network, no credentials, no
   `AuthorizationError`, no 401/403 handling, no `refresh_token_func` hook, and
   no `connections/` directory. A repo-wide grep for `asyncio`, `async def`,
   `asyncio.run`, `Semaphore`, `Queue`, `requests`, `httpx`, `aiohttp`,
   `token`, `credential`, and `api_key` across `src/` returns **zero code
   matches** (the only `credential`-adjacent hit is the vestigial
   `mko_get_mediascope_data` docstring in `app.py:2`, a stale-name drift
   with no runtime surface). `pyproject.toml` declares no HTTP/async/auth
   dependency. No `.env*` files exist. Phase 15's central subject — OAuth2
   authorization policy, 401 retry eligibility, token refresh/rotation, JSON
   credential files in `connections/`, scope validation, credential-on-the-
   wire, auth-failure logging — is **entirely fictional** for this codebase.

2. **All four blocks are Fit: None.** Every block's premise (auth-failure
   classification, token lifecycle, credential leakage, scope validation) has
   zero analog. The only genuinely portable concept (auth-failure
   classification) is already owned by Phase 04 (error serialisation), Phase 12
   (observability), and Phase 02 (credential policy — which confirmed zero
   credentials). Re-basing would produce a phase whose severity bands can
   **never** be populated — every band is empty by design, and the underlying
   conditions (credentials, 401s, tokens) do not exist.

3. **No surviving concept is unique to this phase.** Unlike Phase 01 Block 2
   (transformed "Singleton → Lazy Service Factory") or Phase 05 (re-themed
   template-conformance blocks to real Pydantic models), Phase 15 has **no
   synchronous analog** to any of its four blocks. The synchronous analog of
   "clean resource release on error" is already Phase 01 Block 6; "local file
   crash-safety" is Phase 04; "observability surface" is Phase 12; "doc-vs-code
   drift" is Phase 10. There is no gap left to fill.

4. **Phase 15 itself cites Phase 09, which is on the deletion list.** Phase 15
   line 26-27 defers "outbound API mechanics, retry contract, and
   credential-on-the-wire" to (09). The Phase 09 auditor report (§3)
   recommends deletion of Phase 09 entirely — so Phase 15's (09) citation is
   void. With both parent premises (09's API client and 15's auth policy)
   being entirely fictional for this codebase, keeping Phase 15 alive
   perpetuates a phantom authorization model that does not exist.

5. **Phase 10 — the sole consumer of the (15) citation — is itself async-framed
   and on the rework/deletion list.** Phase 10 line 18 cites
   `authorization (15)` as a scope boundary, but Phase 10's own Purpose (line
   14) is framed around an async model and its Block 10 area references the
   event-loop bridge (09). Phase 10 will be reworked or deleted independently;
   its removal of the `(15)` citation is a mechanical one-line edit that
   belongs to Phase 10's own rework session, not to Phase 15's deletion.

6. **No findings, no output, no precedent to preserve.** Phase 15 has
   `status: draft`, `validated: no`, produced **zero findings**
   (`Test-Path .ai\audit\15-authorization` → `False`). It carries no
   remediation blocker. The Phase 09 auditor report (§7) and Phase 03 auditor
   report (same deletion logic applied to the async-concurrency phase) both
   establish the precedent: when an audit phase's entire premise is
   async/API/credential-framed and the codebase is synchronous-local-only, the
   phase is deleted, not re-based.

**Recommended action:** Delete
`.kilo/commands/audit/phases/15-audit-authorization.md`. The sole downstream
citation to clean is in Phase 10 line 18 (`authorization (15)`), which is
deferred to Phase 10's own rework/deletion session (it shares the same
async-framed fiction). The findings output path
`.ai/audit/15-authorization/findings.md` and Finding-ID prefix `AUT-` are
correspondingly void.

**Effort to delete: trivial** (one file removal; the downstream Phase 10
citation edit is tracked under Phase 10's rework, not this deletion).
**Priority: recommended** — not mandatory, since Phase 15 has `status: draft`
and produced no findings, but safe to remove now and prevents auditors from
chasing a phantom Mediascope-API authorization model that does not exist.

---

## Appendix A — Evidence pedigree

- **Architecture source files read:** `src/mko_data_cleaner/app.py` (full,
  165 lines), `core/errors.py` (full, 30 lines), `core/app_service.py` (full,
  342 lines), `core/csv_service.py` (full, 375 lines), `core/db_service.py`
  (first 50 lines + grep hits), `core/dict_service.py`, `core/models.py`,
  `core/paths.py`, `core/utils.py`, `core/init_service.py`.
- **Dependency declaration read:** `pyproject.toml` (full, 183 lines) — no
  `requests`/`httpx`/`aiohttp`/`mediascope-api-lib`; `urllib3` listed but
  unused in `src/` (grep for `urllib` in `*.py` → zero matches in production).
- **Primitive grep:** `src/mko_data_cleaner/` for `asyncio|async def|
  asyncio.run|Semaphore|Queue|requests|httpx|aiohttp|token|credential|
  api_key` → **zero code matches**. `AuthorizationError` grep in `src/` →
  **zero matches** (absent from `core/errors.py`). `refresh_token` → **zero
  matches**. `connections` → **zero matches** in `src/`.
- **Environment files:** `glob **/.env*` → **0 files**.
- **Credential store:** `Test-Path src\mko_data_cleaner\connections` →
  **False** (directory does not exist).
- **Findings directory:** `Test-Path .ai\audit\15-authorization` → **False**
  (never created; no findings ever produced).
- **ID prefix collision:** grep for `AUT-` in `.ai/` → **no matches** in
  findings index; `AUT-` is unique but void (never minted).
- **Cross-phase citations:** grep over `.kilo/commands/audit/phases/` for
  `15|authorization \(15\)` → 1 citing phase confirmed (Phase 10 line 18) +
  Phase 15's own file. Grep over `.ai/reports/` for `Phase 15` → Phase 03
  auditor report line 163 (informational note, not a citation). Phase 15
  cites (02), (09), (10), (11) at lines 25–28; (09) itself is on the
  deletion list (Phase 09 auditor report §7).
- **Precedent:** Phase 09 auditor report
  (`.ai/reports/phase-09-auditor-report.md` — verdict: DELETE) and Phase 03
  auditor report (`.ai/reports/phase-03-auditor-report.md` — same deletion
  logic applied to the async-concurrency phase). Phase 02 auditor report §2
  Block 2 already deleted the parallel "credential seam" from Phase 02.
- **Docker runtime:** Not available in this environment (`docker` command not
  found on PATH). Not required — this audit is a static architecture-vs-doc
  comparison; the codebase has no runtime to start (no server, no API
  endpoint, no containerised service to probe). The grep evidence above is
  conclusive.
