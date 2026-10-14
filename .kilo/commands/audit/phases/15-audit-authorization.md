---
name: 15-authorization
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 15 — Authorization & Credential Lifecycle

## Purpose

Audits authorization policy, auth-failure classification, scope validation, and
credential/token lifecycle for a standalone async CLI tool that fetches data from
the Mediascope API. The outbound API call is a synchronous client wrapped in
`asyncio.to_thread`, with retries bounded by a max-attempts limit and exponential
backoff plus jitter. An `AuthorizationError` is handled as a distinct, non-retriable
401 path, and a `refresh_token_func` hook is declared as a planned extension
for token refresh — not yet implemented. Credentials enter via a JSON file
in `connections/`.

This file names what to examine and under which angle; the executing auditor
discovers the concrete artifacts. Owned here: authorization policy, auth-failure
classification and retry eligibility, scope validation, and the credential/token
lifecycle (acquisition, refresh, rotation). Owned elsewhere: credential sourcing,
validation, and protection at rest (02); outbound API mechanics, retry contract,
and credential-on-the-wire (09); code-quality convention and declared-contract
enforcement (10); test-suite adequacy (11).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Auth-failure classification and retry eligibility

Establish how authorization failures (401) are distinguished from transient or
permanent failures. Check whether a 401 or `AuthorizationError` is excluded from
retry — spinning the full backoff budget on an invalid credential — whether 403
is classified differently from 401, and whether the failure class is observable
in logs or metrics. Check whether a refreshable 401 is retried with a refreshed
token before the retry budget is exhausted.

Evidence: the retry-eligible failure-class set; one induced 401 and whether it is
retried; the failure-state counter or log for each auth class.

### 2. Token and credential lifecycle: acquisition, refresh, rotation

Establish the credential/token lifecycle: how a credential is acquired at session
start, whether a refresh mechanism exists and when it is triggered, and whether
expiry, rotation, or revocation is handled. Treat a declared-but-unimplemented
hook as a distinct state — a planned refresh path that is never wired into the
request flow.

Evidence: the acquisition path; one refresh attempt and its trigger; the refresh
hook's existence, scope, and implementation status.

### 3. Credential leakage in failure paths

Establish whether authorization context — the credential, token value, request
arguments, or headers — reaches a log line, an exception string, a serialised
error file, or a referrer. Examine the 401 handler and any debug output it
emits when a task fails to authenticate.

Evidence: per failure path — the fields logged or serialised and the sanitiser's
masking verdict; one path by which credential material could be observed.

### 4. Scope and authority validation

Establish whether the credential's scope or authority is validated against the
operations the tool performs — task build, send, poll, result extraction. Check
whether an under-scoped credential is detected and reported distinctly from an
outright auth failure, or whether a scope mismatch returns partial data without
warning.

Evidence: the scope-check inventory (present or absent); one under-scoped
credential and its observable outcome; one scope mismatch distinguishable from
a 401.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. An empty band is a valid outcome —
do not populate it with a hypothetical, and do not promote an item for sounding
alarming.

- **CRITICAL** — a credential obtainable without any gate, or an auth failure
  silently retried as transient, so a revoked or invalid credential spins through
  the full retry budget while masking the real problem; a 401 treated as
  retryable when a refreshed token could have succeeded first.
- **HIGH** — credential material or token values reaching logs, exception
  strings, or serialised error files; a 403 misclassified as a transient network
  error, so a permission failure is retried instead of surfaced; a declared
  token-refresh hook that is unimplemented, leaving expiry or rotation unhandled.
- **MEDIUM** — scope mismatch not detected, so an under-scoped credential returns
  partial data without warning; an auth-failure message that is opaque or
  actionability-free; a refresh hook wired but never invoked in the request path.
- **LOW** — documentation or comment drift about token lifetime, scope, or
  refresh, with no runtime consequence; a declared refresh interval that is not
  enforced.

## Report Output

- Findings path: `.ai/audit/15-authorization/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `AUT-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- Finding IDs use the declared bare prefix; a shipped test, configuration
  comment, or runbook may already carry an ID from a prior cycle, so check
  for a collision before minting a new one
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker
