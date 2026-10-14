---
name: 16-rendered-surface
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 16 — Rendered-Surface & Output Conformance Measurement

## Purpose

Measures the rendered output of this tool — CSV files and serialised error files
— against a declared surface/output contract. The data pipeline converts each API
result to a `DataFrame`, prepares columns via a data strategy, then exports CSV
with locale-specific formatting: `decimal=","`, `sep=";"`, `encoding="utf-8-sig"`,
and `mode="x"` (exclusive create, silently skipping an existing file). Failed
tasks are serialised to a YAML error file. This phase does not audit the
mechanics of how the write happens — that is Phase 04's scope — but whether what
is written conforms to a declared output contract: schema, column types, format,
and presence.

This file names what to examine and under which angle; the executing auditor
discovers the concrete artifacts. Owned here: rendered-output conformance to a
declared surface contract. Owned elsewhere: export mechanics, file-existence
handling, and error-file location (04); template-to-model schema conformance
(05); fixed-value enum discipline (10); test-suite adequacy (11).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. CSV schema conformance against a declared contract

Establish whether the exported CSV's column set, order, and per-field types
conform to a declared output contract. Check whether a column the contract
requires is missing, whether a column's type on disk differs from the contract
(e.g., a numeric field rendered as text or vice versa), and whether extra
columns are tolerated or rejected. Check whether the contract is discoverable
and versioned alongside the export.

Evidence: the effective CSV schema (columns, order, types); one missing or
type-divergent column; the contract's discoverability and version linkage.

### 2. CSV format conformance: separators, decimals, encoding

Establish whether the exported CSV's format matches the declared contract: the
field separator, the decimal mark, the character encoding, the line endings, and
the presence or absence of a byte-order mark. Check whether a format divergence
— for example, a comma decimal mark where the contract expects a period, or
vice versa — would cause a downstream consumer to silently misread a value.

Evidence: per format dimension — the declared value and the rendered value; one
format mismatch and the downstream misinterpretation it enables.

### 3. Output presence and completeness

Establish whether every task that the pipeline reports as successful produces an
output file that conforms to the contract. Check whether a file skipped by
`mode="x"` (because it already exists) is counted as success without producing
conformant output, and whether an empty or partial result is indistinguishable
from a complete one.

Evidence: the success-to-output mapping for one run; one skipped file and
whether it is reported as success; one empty result and its disposition.

### 4. Error-file conformance and content

Establish whether the serialised error file conforms to a declared error-output
contract — its structure, fields, and serialised format. Check whether the
serialised task context (settings, interval, target, error message) is present
and correctly formatted, and whether the error file's naming is reproducible so
a re-run can detect a prior failure.

Evidence: one error file and its field inventory; the declared error contract
(if any); the naming reproducibility for one induced failure.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. An empty band is a valid outcome —
do not populate it with a hypothetical, and do not promote an item for sounding
alarming.

- **CRITICAL** — rendered output that conforms to no declared contract while being
  consumed as authoritative, so a downstream reader silently misreads a value or
  a malformed file is treated as valid; a locale-format mismatch on the wire
  (e.g., comma-decimal where the consumer expects a period) that changes a
  value's magnitude.
- **HIGH** — a CSV column whose type or name diverges from the declared surface,
  causing a downstream parse failure or a silent data substitution; a required
  column missing from the output, producing a structurally incomplete file
  consumed as complete; `mode="x"` skipping an existing file so a stale result
  is reported as fresh.
- **MEDIUM** — an encoding or line-ending mismatch that is locale-dependent and
  undeclared; a format deviation (separator, BOM) that a downstream consumer
  tolerates but should not; an error file whose structure or fields do not match
  the declared contract.
- **LOW** — documentation or comment drift about output format or schema, with
  no runtime consequence; an undeclared optional column in an otherwise
  conformant file; a non-reproducible error-file name that does not affect
  detection in practice.

## Report Output

- Findings path: `.ai/audit/16-rendered-surface/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `OUT-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- Finding IDs use the declared bare prefix; a shipped test, configuration
  comment, or runbook may already carry an ID from a prior cycle, so check
  for a collision before minting a new one
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker
