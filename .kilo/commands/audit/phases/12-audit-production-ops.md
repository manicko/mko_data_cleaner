---
name: 12-production-ops
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 12 — Production Operations, Security & Observability

## Purpose

What the shipped package is made of and what decides its content; what
determines which checks gate a release; what an operator is told when something
is wrong; and what a failure costs in output. This is a standalone async CLI
tool distributed via `uv` and PyPA packaging — there are no containers, no
database, no reverse proxy, no health probes, and no backup strategy. This file
names what to examine and under which angle — the executing auditor discovers
the concrete artifacts.

**Scope Boundaries** — taken in: CI/CD gates, packaging, and observability.
Deferred by this phase: import-time side effects and bootstrap (01); settings
values and credentials (02); Semaphore/Queue/worker-pool concurrency (03);
period resolution and data extraction (04); template validation (05); the
outbound API call contract (09); code-quality discipline (10); test-suite
adequacy (11); concurrency ceilings and retry cost (13). There is no edge tier,
no transport security boundary, no health-probe contract, and no backup to
this phase.

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others,
and each carries its own evidence. Where a property cannot be settled in the
environment at hand, establish it from what is observable and state plainly
what could not be verified and why. A passing check is methodology, never a
finding, and any sound evidence is admissible: nothing here gates a finding on
this phase's own list.

### 1. A control that runs, reports clean, and examines nothing

Take every automated control that emits a pass/fail signal — the linter, the
type checker, the test suite, a secret scan — and separate three questions per
control: does it exist, what scope does it declare, what did it actually
examine. Establish what is configured to be enforced and which suppressions are
sanctioned before judging any result; never infer a missing tool, and never
treat a non-zero issue count as a defect by itself. A control whose declared
and actual scope differ is a finding however green it is: characteristically
one whose paths resolve against the wrong working directory, so it examines
nothing, reports no issues, and exits successfully.

Evidence: per control — existence, declared scope, the file / item / series
count it actually examined, and whether any shipped guard would notice a change
in that scope.

### 2. CI/CD pipeline & gates

Establish whether a CI/CD pipeline exists at all and which checks it gates
behind a release. For each gate — lint, typecheck, tests — check whether it is
loaded, green, and what its scope excludes. Check whether an artifact can be
published regardless of a failing gate, whether credentials reach a pre-gate
step, and what overlapping or re-entrant runs do to shared state (e.g., the
user config directory). A gate whose configuration sits in the repository but
is never read from the directory it runs in is a finding.

Evidence: per gate — loaded or not, green or not, scope; the path by which an
artifact or release proceeds with the check failing or not having run.

### 3. What makes a check a gate vs. a report

Establish which checks can prevent a release and what triggers the automation,
what dependency makes a verdict reach the artifact, and whether the artifact can
be published regardless of a verdict. Check whether the same checks can be run
locally by a contributor, and whether a local run reaches the same
configuration and scope as the CI run.

Evidence: per check — trigger events, the dependency that would make it
blocking, and the path by which an artifact proceeds with the check failing or
not having run; whether it is locally reproducible.

### 4. Packaging & artifact identity

Establish what decides the package content: the `pyproject.toml` build
metadata, the setuptools backend, pinned dependency versions, and the entry
points (`mko-get-mediascope` console script, `python -m`). Check whether the
published artifact coordinates match what the install path expects, whether a
version is immutable after publish, and whether a rollback selects an artifact
identical to the forward path.

Evidence: the external-input inventory (base images, build tools, dependencies)
with a pin verdict and integrity verdict each; the three identifiers (pyproject
name, import name, console script) compared; the mutability of a revert target.

### 5. Observability: signals produced and consumed

Establish which signals the system emits and whether anything in the deployed
configuration consumes them. The logging pipeline is configured via `dictConfig`
at import time: check whether log file handles are created eagerly, whether a
failure state (retry exhaustion, task failure, validation error) has a counter
or log entry, or only an unlogged exception. Check whether every log path
reaches the same structured, configured output, and whether the logging
configuration is validated at load time or silently falls back.

Evidence: the signal inventory with produced/consumed verdicts; one failure
state with a counter, a log, or neither; the logging config load path and its
validation.

### 6. Runbooks & operational documentation

Establish whether the procedures a responder is told to follow would run as
written: required state, required credentials, required files, assumed tool
versions. Check whether each operational claim in the install, run, and incident
corpus — a control that is live, a cadence, a value, a path — is still true of
the system as shipped. Treat a comment, docstring, or runbook line as a claim to
be tested against the executing path: documentation describing a system other
than the one deployed is itself a finding.

Evidence: the operational-claim inventory with a true/false verdict each; one
procedure with the step that fails on the host and why.

### 7. Release & rollback

Establish what a release and a rollback require: version pinning in
`pyproject.toml`, the `uv` lock file, config schema compatibility. Check
whether a rollback restores the same package version and the same config
schema the forward path used, and whether a config-template change is
backward-compatible with an older code version. Check whether the
`init` command's template copy is idempotent and non-destructive to user
edits.

Evidence: the forward and revert paths compared; one config-template change
with its backward-compatibility verdict; the init command's behaviour on an
existing user directory.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered.

- **CRITICAL** — a control believed to be active that is not, on the path that
  decides whether a change ships or a release publishes, so that nothing
  distinguishes a vetted subject from an unvetted one for a reviewer or for
  the system's own records. An empty band is a valid outcome — do not populate
  it with a hypothetical, and do not promote an item for sounding alarming.
- **HIGH** — a state the system is in now where the failure is neither
  survivable nor observable: a failure state with no counter and no log, so a
  quiet tool is indistinguishable from a working one; a gate loaded and green
  while examining nothing; a rollback that does not restore the same package
  version or config schema the forward path used.
- **MEDIUM** — degraded operability with a workaround: a control present and
  effective in part; a documented procedure or claim that is inaccurate, but
  whose consequence an operator can work around; a gate that can be locally
  bypassed while CI enforces it; a config-template change that is not
  backward-compatible with a stated older version.
- **LOW** — documentation and hygiene drift with no runtime consequence
  today: a runbook line contradicting the shipped behaviour; a template copy
  that is non-atomic but never observed to interleave; a comment about the
  logging config that does not match the loaded file.

## Report Output

- Findings path: `.ai/audit/12-production-ops/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `OPS-` — already in use in shipped source, tests, and
  workflows from a prior cycle; check for a collision before minting an ID and
  report it rather than creating a second namespace
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence
- Empty state, exactly: `No problems found in this phase.`
