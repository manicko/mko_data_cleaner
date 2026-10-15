---
name: 02-config-secrets
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 02 — Configuration & Secrets Management

## Purpose

Audit how configuration and credentials are declared, resolved, validated, and
kept separate in a standalone async CLI tool that fetches data from the
Mediascope API. Bundled YAML templates live in the package `settings/` tree and
are copied to the user config directory (`~/.config/mko_data_cleaner/`)
by the `init` command. All loaded configuration is validated through Pydantic
v2 models before use. The Mediascope API credential is a JSON file in
`connections/`.

Other phases own: process startup and import-time behaviour (01); the
blocking-work bridge and retry contract (09); fixed-value enum discipline
(10); packaging and CI/CD (12); the async pipeline (03, 04). This phase owns
the **values** themselves — how they are sourced, validated, and merged.

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced
behaviour, or a static proof that a stated property does not hold; the method
is the auditor's. A passing check is methodology, never a finding.

### 1. Settings surface & template distribution topology

Establish which configuration variant each running surface loads and how the
choice is made. Confirm or refute that the package templates, the user config
directory, and the defaults directory are each consumed by their intended reader.
Check whether user edits to copied templates are preserved across a
non-forced re-init, whether a variant exists with no consumer, and whether the
template copy operation is atomic with respect to partial writes.

Evidence: effective variant per surface; the template-to-user copy mechanism;
one user-edited file preserved or clobbered across re-init.

### 2. Credential seam: where the Mediascope secret enters

Derive the credential file and establish where the secret is injected into an
API request — path, query, header, or body. Treat absent, empty, and malformed
as three distinct states. Check whether the secret reaches a log, an exception
string, or a referrer; whether one process holds the credential that another
never needs; and whether a missing or invalid credential fails fast with an
actionable message that does not echo the secret.

Evidence: per call — where the credential enters, the three states, the
holding surface; one path by which the wire form could be observed.

### 3. Pydantic validation fidelity & defaults merge

Establish how the merged data-settings dict is constructed: the base defaults
from the defaults template, overlaid with the report-specific values, then
validated as a `Data` model. Check whether validation occurs *before* the
settings reach the API call, whether a type mismatch or missing required key
fails fast with a named field, and whether the merge preserves list-vs-dict
typing for every field the API consumer expects.

Evidence: the merge chain (defaults → overlay → validate); one induced type
mismatch and the resulting error; the validated model that the consumer reads.

### 4. Secret validation & fail-fast behaviour

For the credential guard, ask four questions: is a guard present, is it
unconditional, can any reachable value disable it, and is the failure
actionable yet free of the secret's value? Treat absent, empty, and malformed
as three distinct states. Where the guard is enforced in several places, check
the implementations agree on the condition, the skip rules, and the file they
expect.

Evidence: guard inventory with a scoping verdict; one controlled-environment
observation per guard; the failure text itself.

### 5. Dead, ineffective & unconsumed configuration

Ask three questions in one place: is each assigned setting name still read by
the code that loads it; does every declared value reach a reachable consumer;
is every environment branch reachable under some real selection. Where a value
is derived rather than read directly, check that the derived result satisfies
its consumer's contract, not merely that the derivation exists.

Evidence: per-name liveness verdicts; unread-name list; derived-value validity
check.

### 6. Environment separation & process parity

Determine what must not cross between the CLI, the library, and the module
entry surfaces. Check whether debug-level logging, credential exposure, and
config-variant selection are consistent across all three entry surfaces, and
whether a value written into one surface's config can leak into another.

Evidence: effective-configuration diff per entry surface; the honoured config
paths per surface.

### 7. Env-overridable production posture

Identify settings whose value can change the security or delivery posture of a
running report — the concurrency limit, the retry count, the export path, the
credentials file path. Check whether each such substitution is intended,
constrained, and re-pinned where a production variant is supposed to harden it.
Separate documented operator controls from configuration that looks like drift.

Evidence: the list of posture-affecting settings with the pinning status of
each.

---

## Severity Taxonomy

| Severity | Conditions |
|---|---|
| CRITICAL | A real credential obtainable by anyone who can read the file carrying it; a credential file that is empty, invalid, or shared across environments; a config gate that is bypassed or silently disabled |
| HIGH | A production control bypassable or silently disabled; secret values reaching logs, output, or error paths; a merged config value that reaches the API consumer without validation and causes silent data corruption |
| MEDIUM | Config-name drift; ineffective or unconsumed configuration; a setting name the validator no longer reads; leakage of config across entry surfaces |
| LOW | Placeholder and template gaps; undocumented switches; missing explanatory comments; a template copy that is non-atomic but never observed to interleave |

Severity is anchored to **present state**: rate what is true now, not the
worst consequence a defect would have if it were triggered.

---

## Report Output

- Findings path: `.ai/audit/02-config-secrets/findings.md`
- Template: `.ai/audit/templates/audit-findings.md`
- **Incremental append**, ≤100 lines per pass.
- Prefix every finding ID with `CFG-`.
- Record findings only and omit passing checks; every finding must carry
  reproducible runtime evidence and the exact consequence. If nothing is wrong
  in this phase, write exactly: `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker.
