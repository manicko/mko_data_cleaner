---
phase: "09"
phase_name: "External Integrations & API"
phase_file: ".kilo/commands/audit/phases/09-audit-external-api.md"
report_status: "draft"
recommendation: DELETE
---

# Research Recommendations — Phase 09 External Integrations & API

## Verdict

**DELETE Phase 09 entirely — all 8 blocks. No re-basing.**

The auditor report (`phase-09-auditor-report.md`) confirms the current `mko_data_cleaner`
is a synchronous, single-shot, local-only CLI + library. There is no outbound API, no
credentials, no network, no event loop, no retry/backoff, and no async/sync bridge
anywhere in the architecture. Phase 09's entire premise — a Mediascope API client,
`asyncio.to_thread` bridge, JSON credentials, exponential backoff with jitter, and
credential-on-the-wire logging — is fictional for this codebase. All eight blocks assess
to **Fit: None, Outdated: entirely**.

## 1. Proposed Final Block Set — Confirm DELETE (all 8 blocks)

Every block is deleted with the phase. None has a synchronous analog unique to this
phase; the portable concerns are already owned elsewhere:

| Block | Disposition | Where the concern lands instead |
|---|---|---|
| 1 — Credential seam | DELETE | No credentials exist. Phase 02 owns settings/secret policy. |
| 2 — Retry contract | DELETE | No outbound calls to retry. Phase 04 owns error serialisation. |
| 3 — Backoff/jitter | DELETE | No backoff strategy. Phase 13 owns retry-cost concerns. |
| 4 — Blocking-work bridge (`to_thread`) | DELETE | No event loop. Phase 01 Block 6 owns clean teardown (DBWorker context manager). |
| 5 — Dependency degradation | DELETE | No external deps to degrade. Phase 04 owns local crash-safety. |
| 6 — Degraded-result indistinguishability | DELETE | No fallbacks/breakers. Phase 02/05 own dead-config/defaulting drift. |
| 7 — Integration logging | DELETE | No integration surface. Phase 12 owns observability. |
| 8 — Declared dependency vs. code | DELETE (redundant) | Doc-vs-code drift is owned by Phase 10 (declared-contract) and Phase 02 (dead-config sweep). |

Unlike Phase 01 Block 2 (Singleton → Lazy Service Factory) or Phase 05 (template
conformance re-themed to real Pydantic models), Phase 09 has **no synchronous analog**
to any block. Re-basing would produce a phase whose severity bands could never be
populated.

## 2. Cross-Phase Citation Cleanup — Text to Remove When Each Phase Is Reworked

Four downstream phases cite (09). Each citation is mechanical to strip; the broader
async re-basing of Phases 10/12/13/15 is tracked as separate follow-on work.

### Phase 10 — `10-audit-code-quality.md` (line 17)
Cites (09) as the owner of "the event-loop bridge mechanism."

Current text (line 17):
> `...lifecycle correctness (05);` the event-loop bridge mechanism (09); test adequacy (11);`

Exact text to remove:
> `; the event-loop bridge mechanism (09)`

Resulting: `...lifecycle correctness (05); test adequacy (11);...`

### Phase 12 — `12-audit-production-ops.md` (line 25)
Cites (09) as the owner of "the outbound API call contract."

Current text (lines 24–26):
> `period resolution and data extraction (04); template validation (05);` the outbound API call contract (09); code-quality discipline (10); test-suite`

Exact text to remove:
> `; the outbound API call contract (09)`

Resulting: `...template validation (05); code-quality discipline (10);...`

### Phase 13 — `13-audit-performance.md` (lines 25–27)
Cites (09) twice: retry/backoff contract and blocking-work bridge.

Current text (lines 25–28):
> `acquisition and cancellation semantics (03);` the retry and backoff contract (09); period resolution and task generation (04);` the blocking-work bridge (09); CI/CD and observability (12);`

Exact text to remove (both occurrences):
> `; the retry and backoff contract (09)`
> `; the blocking-work bridge (09)`

Resulting: `...cancellation semantics (03); period resolution and task generation (04); CI/CD and observability (12);...`

### Phase 15 — `15-audit-authorization.md` (line 27)
Cites (09) as owner of outbound API mechanics, retry, and credential-on-the-wire.

Current text (lines 25–28):
> `lifecycle (acquisition, refresh, rotation). Owned elsewhere: credential sourcing,` validation, and protection at rest (02); outbound API mechanics, retry contract, and credential-on-the-wire (09); code-quality convention and declared-contract`

Exact text to remove:
> `; outbound API mechanics, retry contract, and credential-on-the-wire (09)`

Resulting: `...validation, and protection at rest (02); code-quality convention and declared-contract...`

## 3. Modern Practices / Angles — Confirm Inapplicable

The async/API analysis frame is inapplicable. There is no event loop, no HTTP client,
no async runtime, no credential store, no retry library. A repo-wide grep for
`asyncio | async def | asyncio.run | Semaphore | Queue | requests | httpx | aiohttp |
token | credential | api_key` across `src/mko_data_cleaner/` returns **zero code matches**
(the single `https://` hit in `db_service.py:435` is a SQLite documentation URL comment).
`pyproject.toml` declares no HTTP/async dependency; no `.env*` files exist.

The synchronous analogs of Phase 09's concerns are already owned:
- **Local filesystem / resource boundary** → Phase 01 (resource lifecycle, Block 6:
  `DBWorker` context manager with `checkpoint WAL + delete on __exit__`).
- **Local file I/O and pipeline crash-safety** → Phase 04 (data pipeline & error
  serialisation).
- **Observability surface** → Phase 12 (logs only local file/DB operations).
- **Doc-vs-code drift** → Phase 10 (declared-contract enforcement) and Phase 02
  (dead-config sweep). Phase 02 §2 Block 2 already recorded the only vestigial
  external-capability drift: the stale `mko_get_mediascope_data` name in `app.py`,
  `.gitignore`, `MANIFEST.in`, and Phase 12 line 86 — all string drift, zero runtime
  surface.

## 4. Quality / Brevity — Confirm No Numbering or Path-Drift Impact

- **Front matter:** Phase 09 file is `status: draft`, `validated: no` — deletion is safe;
  it produced no findings and carries no remediation blocker.
- **Findings directory:** `.ai/audit/09-external-api/` is **confirmed absent** (`Test-Path`
  → `False`). No findings were ever produced; no output path to clean.
- **ID prefix:** `EXT-` is unique in the repo but **void** — never minted as a finding ID
  anywhere in tracked findings (grep confirms references only in this report and a scratch
  copy of the phase file). No findings index or registry entry to remove.
- **Numbering/path-drift:** Phase 09 sits in a sparse sequence (01, 02, 04, 05, 09,
  10, 11, 12, 13, 15, 16, 17, 99). Deleting 09 leaves a gap that is already present
  (03, 06–08 absent). Orchestration files enumerate phases by directory listing
  (`audit-multi-phase.md`), so deleting the file removes it from the selectable set
  automatically — no hard-coded sequence to edit. `grep` over `.kilo/commands/audit/` for
  `09` returns matches only in this phase file and Phase 12 line 109's "HTTP 404"
  (unrelated HTTP-status notation, not a phase citation).
- **`report-output` block:** Deletion removes all five output specs with the file —
  findings path, ID prefix, `problems-only` flag, per-pass lines, and the empty-state
  string (`No problems found in this phase.`).

## 5. Edit Delta

**Perform now:**
- Delete `.kilo/commands/audit/phases/09-audit-external-api.md`.

**Deferred (cleaned when downstream phases are individually reworked):**
- Remove the `(09)` citation and surrounding async/API phrase from:
  - Phase 10 line 17 — `; the event-loop bridge mechanism (09)`
  - Phase 12 line 25 — `; the outbound API call contract (09)`
  - Phase 13 lines 25–27 — `; the retry and backoff contract (09)` ; `; the blocking-work bridge (09)`
  - Phase 15 line 27 — `; outbound API mechanics, retry contract, and credential-on-the-wire (09)`

Per project rule (English-only, production code is king, workspace cleanliness), these
downstream edits belong to the individual phase rework sessions for 10/12/13/15 — moving
their async fiction to the synchronous model — and should not be bundled into this
deletion.

## Reference

Auditor report: `.ai/reports/phase-09-auditor-report.md` (recommendation: DELETE, §7).
Precedent: Phase 03 auditor report applied identical deletion logic to the async-
concurrency phase; Phase 02 §2 Block 2 already deleted the parallel "credential seam"
block. Both establish the convention: when an audit phase's premise is async/API/
credential-framed and the codebase is synchronous-local-only with no re-baseable subject,
the phase is deleted, not re-based.
