---
name: 02-config
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 02 — Configuration Management

## Purpose

Audit how configuration is declared, resolved, validated, and consumed in a
synchronous, single-shot, local-only CLI tool and library that post-processes
Mediascope TV advertising exports received as files on disk (no network, no
credentials, no authentication). Bundled YAML templates live in the package
`settings/` tree and are copied to the user settings directory by the `init`
command. All loaded configuration is validated through Pydantic v2 models before
use.

Other phases own: process startup and import-time behaviour (01); fixed-value
enum discipline (10); packaging and CI/CD (12). This phase owns the **values**
themselves — how they are sourced, validated, and consumed — including the
`settings_dir` forwarding asymmetry between `init` and `run` surfaces and the
malformed-YAML passthrough defect.

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: a static proof that a stated property
does not hold, a reproduced validation error, or an observed silent pass; the
method is the auditor's. A passing check is methodology, never a finding.

### 1. Settings surface & template distribution topology

- Which config variant does each running surface consume, and via what reader?
- Are package templates copied to the user directory preserving edits, with an
  atomic write on re-init?
- Does `init` honor the same `settings_dir` that `run` honors, or does one ignore
  a caller override?

Evidence: effective variant per surface; the template-to-user copy mechanism; one
user-edited file preserved or clobbered across re-init.

### 3. Pydantic validation fidelity & config loading

- Is each YAML loaded directly (no overlay merge) and validated through a typed
  model before use?
- Does a missing required key fail fast with a named field on the validated model?
- Does a present-but-malformed file surface a cause-specific error at the gate, or
  does the loader return a blank that masks the syntax-failure cause?

Evidence: the load+validate chain; one induced parse error and its message; the
validated model that the consumer reads.

### 4. Settings gate: fail-fast fidelity

- Is the config existence gate unconditional and fail-fast on missing settings
  files?
- Does the gate catch malformed-but-present files, or only absence — and where does
  the malformed path surface instead?
- Is the failure actionable (names the file, points to remediation) and consistent
  across all entry surfaces?

Evidence: the gate inventory with a scoping verdict; one controlled-environment
observation per gate; the failure text itself.

### 5. Dead, ineffective & unconsumed configuration

- Is each assigned setting name still read by consumer code and every declared
  value reaching a reachable consumer?
- Are declared dependencies in the project manifest actually imported anywhere in
  source?
- Does any documented config key or logger exist in docs but not in the shipped
  file?

Evidence: per-name liveness verdicts; unread-name list; the derived-value
validity check.

### 6. Entry-surface parity & config forwarding

- Do the CLI, library API, and module entry load the same config and honor the same
  `settings_dir` identically?
- Does the bare package entry (`python -m <package>`) launch the CLI, or only the
  submodule form?
- Can a value for one surface be ignored by another due to divergent `settings_dir`
  handling?

Evidence: the effective-configuration diff per entry surface; the honoured config
paths per surface.

### 7. Config-driven operational posture

- Which settings change operational posture (output shape, format, DB location)
  rather than just defaults?
- Are posture-affecting settings env-overridable, or is there no env-override layer
  at all?
- Does the DB file location isolate per report, or live inside the report directory
  by default?

Evidence: the list of posture-affecting settings with the pinning status of each.

> Note: Block 2 was deleted — no credentials exist in this codebase.

---

## Severity Taxonomy

| Severity | Conditions |
|---|---|
| CRITICAL | Config gate bypassed or silently disabled — reversion to silent fallback on missing settings. **Empty today** (do not regress): the gate does raise. |
| HIGH | Malformed-but-present config passes the gate and surfaces as an opaque downstream error; an unvalidated config value reaches a consumer causing silent data corruption. |
| MEDIUM | Dead or unconsumed configuration (e.g., a helper with zero call sites); declared dependencies never imported; setting-name drift; leakage of config across entry surfaces. |
| LOW | Stale dev-only execution blocks in production modules; non-atomic template copy; missing `__main__.py` so the bare package entry fails; doc-code divergence on config/logger names. |

Severity is anchored to **present state**: rate what is true now, not the worst
consequence a defect would have if it were triggered.

---

## Report Output

- Findings path: `.ai/audit/02-config/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, distribution, cross-finding analysis, roadmap, rollout safety,
  appendices
- Finding-ID prefix: `CFG-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding must carry
  reproducible runtime evidence and the exact consequence. If nothing is wrong
  in this phase, write exactly: `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker.
