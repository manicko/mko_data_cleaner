---
phase: "15"
phase_name: "Authorization & Credential Lifecycle"
phase_file: ".kilo/commands/audit/phases/15-audit-authorization.md"
report_status: "final"
recommendation: DELETE
---

# Research Recommendations — Phase 15 Authorization & Credential Lifecycle

## Verdict

**DELETE Phase 15 entirely — all 4 blocks. No re-basing.**

The auditor report (`phase-15-auditor-report.md`) confirms the current `mko_data_cleaner` is a
synchronous, single-shot, local-only CLI + library. There is no outbound API, no credentials, no
network, no event loop, no retry/backoff, and no auth-failure surface anywhere in the architecture.
Phase 15's entire premise — a Mediascope API client, OAuth2 authorization policy, 401 retry
eligibility, token refresh/rotation, JSON credentials in `connections/`, scope validation, and
credential-on-the-wire — is fictional for this codebase. All four blocks assess to **Fit: None,
Outdated: entirely**.

## 1. Proposed Final Block Set — Confirm DELETE (all 4 blocks)

Every block is deleted with the phase. None has a synchronous analog unique to this phase:

| Block | Disposition | Where the concern lands instead |
|---|---|---|
| 1 — Auth-failure classification & retry eligibility | DELETE | No 401/403 responses exist. Phase 04 owns error serialisation (`errors.py`); Phase 01 Block 6 owns clean teardown (`DBWorker` context manager). |
| 2 — Token & credential lifecycle (acquisition, refresh, rotation) | DELETE | No credentials exist. `pyproject.toml` declares no HTTP/async/auth dependency; no `.env*` files (`glob **/.env*` → 0 files); no `connections/` directory (confirmed absent). Phase 02 owns credential policy and confirmed zero credentials. |
| 3 — Credential leakage in failure paths | DELETE | No credential material exists at any layer. The only `credential`/`token` hit in `src/` is a vestigial docstring name (`mko_get_mediascope_data`) in `app.py`. Phase 12 owns observability for the real pipeline. |
| 4 — Scope & authority validation | DELETE | No API operations to scope against; pipeline reads local files and writes local SQLite. Phase 04 owns the data-pipeline scope; Phase 10 owns declared-contract drift. |

A repo-wide grep for `asyncio | async def | asyncio.run | Semaphore | Queue | requests | httpx |
aiohttp | token | credential | api_key` across `src/mko_data_cleaner/` returns **zero code matches**.
`AuthorizationError` grep returns **zero matches** (absent from `core/errors.py`, which defines only
five exception classes). `refresh_token` → **zero matches**. `connections` → **zero matches** in
`src/`. Unlike Phase 01 Block 2 (Singleton → Lazy Service Factory) or Phase 05 (template conformance
re-themed to real Pydantic models), Phase 15 has **no synchronous analog** to any block. Re-basing
would produce a phase whose severity bands can never be populated.

## 2. Confirmed Findings — Confirm None

**None.** No auth API exists, no credentials exist, no `AuthorizationError` exists, no 401/403
handling exists, no `refresh_token_func` hook exists, no `connections/` directory exists, no
retry/backoff exists, no network exists, no scope validation exists. Every block assesses to
Fit: None with an entirely outdated premise. There are no candidate findings to emit under the
`AUT-` prefix.

## 3. Cross-Phase Citation Cleanup

### Phases that 15 CITES (scope boundaries, lines 25–28)

| Cited phase | Property 15 defers to it | Status |
|---|---|---|
| 02 | credential sourcing, validation, and protection at rest | Void — no credentials exist |
| 09 | outbound API mechanics, retry contract, and credential-on-the-wire | Void — (09) on the deletion list; citation itself fictional |
| 10 | code-quality convention and declared-contract enforcement | Void — Phase 15 deleted |
| 11 | test-suite adequacy | Void — Phase 15 deleted |

Phase 15 cites (02) and (09). (09) is itself on the deletion list (Phase 09 auditor report §7 —
verdict: DELETE), so Phase 15's citation of (09) is doubly void: it points to a phase that will be
deleted and asserts an API contract that never existed. With Phase 15 deleted, all 15→N citations
are removed with the phase file.

### Phases that CITE 15 (who references Phase 15)

| Phase | Line | Current text | Disposition |
|---|---|---|---|
| 10 | 18 | `authorization (15)` | **remove** — deferred to Phase 10 rework (shares the same async-framed fiction) |

### Exact citation text to remove (when Phase 10 is reworked)

Current text (Phase 10 line 17, scope-boundary sentence, runs into line 18):

> `...lifecycle correctness (05); the event-loop bridge mechanism (09); test adequacy (11); pipeline and container posture (12); measurable performance consequences (13); authorization (15). This phase owns convention and consistency inside the code...`

Exact text to remove:
> `; authorization (15)`

Resulting: `...measurable performance consequences (13). This phase owns convention and...`

**Rationale for deferral:** Phase 10 line 17 is itself async-framed (cites the event-loop bridge (09))
and on the rework/deletion list. Per project rule (workspace cleanliness, single responsibility),
the Phase 10 citation edit belongs to Phase 10's own rework session — not bundled into Phase 15's
deletion. Phase 15 has no other downstream consumers (Phase 03 auditor report line 163 is an
informational note, not a citation; orchestration files enumerate phases by directory listing, so
deleting `15-audit-authorization.md` removes it from the selectable set automatically).

## 4. Modern Practices / Angles — Confirm Inapplicable

The auth/API analysis frame is inapplicable. There is no event loop, no HTTP client, no async
runtime, no credential store, no retry library, and no auth-failure surface. The synchronous analogs
of Phase 15's concerns are already owned:

- **Local resource boundary** → Phase 01 (resource lifecycle, Block 6: `DBWorker` context manager with
  `checkpoint WAL + delete on __exit__`).
- **Local file I/O and pipeline crash-safety** → Phase 04 (data pipeline & error serialisation;
  `errors.py` defines `NoReportFoundError`, `WrongDataSettings`, `DataValidationError`, `ConfigError`,
  `SettingsNotInitializedError` — no `AuthorizationError`).
- **Observability surface** → Phase 12 (logs only local file/DB operations — rows, chunks, timings).
- **Doc-vs-code drift** → Phase 10 (declared-contract enforcement) and Phase 02 (dead-config sweep).
  Phase 02 §2 Block 2 already recorded the only vestigial external-capability drift: the stale
  `mko_get_mediascope_data` name in `app.py`, `.gitignore`, and `MANIFEST.in` — all string drift,
  zero runtime surface.

**Auth-failure boundary:** the only real failure mode at the Phase 01 boundary is the DBWorker
resource being unavailable/non-mappable (file/DB-level), handled by Phase 04's error
serialisation — there is no auth failure to classify or log.

## 5. Quality / Brevety — Confirm No Impact

- **Front matter:** Phase 15 file is `status: draft`, `validated: no` — deletion is safe; it
  produced no findings and carries no remediation blocker.
- **Findings directory:** `.ai/audit/15-authorization/` is **confirmed absent**
  (`Test-Path .ai\audit\15-authorization` → `False`). No findings were ever produced; no output path
  to clean.
- **ID prefix:** `AUT-` is unique in the repo but **void** — grep over `.ai/` confirms references only
  in this report and the phase file's own scratch copy; never minted as a finding ID in any tracked
  findings index. No findings registry entry to remove.
- **Numbering/path-drift:** Phase 15 sits in a sparse sequence (01, 02, 04, 05, 09, 10, 11, 12, 13,
  15, 16, 17, 99). Deleting 15 leaves a gap already present (03, 06–08 absent). Orchestration files
  enumerate phases by directory listing (`audit-multi-phase.md`), so deleting the file removes it
  from the selectable set automatically — no hard-coded sequence to edit.
- **`report-output` block:** Deletion removes all five output specs with the file — findings path,
  ID prefix (`AUT-`), `problems-only` flag, per-pass lines, and the empty-state string
  (`No problems found in this phase.`).

## 6. Edit Delta

**Perform now:**
- Delete `.kilo/commands/audit/phases/15-audit-authorization.md`.

**Deferred (cleaned when downstream phases are individually reworked):**
- Remove the `(15)` citation from:
  - Phase 10 line 18 — `; authorization (15)`

**Void (no action required):**
- `.ai/audit/15-authorization/findings.md` — directory confirmed absent; never created.
- `AUT-` finding-ID prefix — void; never minted; no findings index entry.

Per project rule (workspace cleanliness, single responsibility), the downstream Phase 10 citation
edit belongs to Phase 10's own rework session (it shares the same async-framed fiction) and should
not be bundled into this deletion.

## Reference

Auditor report: `.ai/reports/phase-15-auditor-report.md` (recommendation: DELETE, §7).
Precedent: Phase 09 auditor report (`.ai/reports/phase-09-auditor-report.md` — verdict: DELETE) and
Phase 03 auditor report (same deletion logic applied to the async-concurrency phase). Phase 02 §2
Block 2 already deleted the parallel "credential seam" from Phase 02. All establish the convention:
when an audit phase's premise is auth/API/credential-framed and the codebase is synchronous-local-only
with no re-baseable subject, the phase is deleted, not re-based.
