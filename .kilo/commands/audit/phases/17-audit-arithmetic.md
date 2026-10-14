---
name: 17-arithmetic
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 17 — Arithmetic & Derived-Quantity Verification

## Purpose

Verifies the arithmetic correctness of derived quantities produced by this tool:
aggregation semantics, floating-point error bounds, null/NaN handling, and
agreement between multiple implementations of the same derived value. The pipeline
converts API results to a `DataFrame`, applies data-preparation strategies that
pivot and aggregate — frequency-distribution pivoting with `aggfunc="first"` and
interval-based column sorting — then writes CSV with locale-specific rendering
(`decimal=","`, `sep=";"`). Where two implementations of one derived quantity
exist, Phase 10 records the duplication and this phase establishes whether they
agree on the arithmetic.

This file names what to examine and under which angle; the executing auditor
discovers the concrete artifacts. Owned here: numerical correctness of derived
quantities, aggregation semantics, float-error bounds, and null/NaN propagation.
Owned elsewhere: template-to-model field conformance (05); CSV format and schema
conformance (16); code-quality detection of duplicated implementations (10);
test-suite adequacy (11).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Aggregation semantics and their declared intent

Establish how each aggregation combines values: the pivot function used, the
interval boundaries (inclusive or exclusive), and whether a `first` aggregation
preserves a deterministic result when underlying order is not guaranteed. Check
whether the aggregation semantics match a declared contract or comment, and
whether an interval-boundary choice affects a count or sum by a small margin.

Evidence: per aggregation — the function, the boundary rule, and one computed
result; one boundary case where the inclusive/exclusive choice changes the
output; the declared semantics and whether the code matches it.

### 2. Floating-point error bounds and accumulation

Establish how floating-point values are accumulated through aggregation and
rendered to CSV. Check whether rounding occurs before or after aggregation,
whether intermediate sums can accumulate error beyond a declared bound, and
whether the locale-specific decimal mark (`decimal=","`) preserves full precision
or silently truncates. Check whether a tolerance or error bound is declared for
each derived quantity.

Evidence: the accumulation path (pre- or post-aggregation rounding); one
derived value with an observed error exceeding the declared bound; the declared
tolerance, if any, and its source.

### 3. Null and NaN propagation through aggregations

Establish how null, NaN, and None values propagate through each aggregation and
downstream rendering. Check whether a null is silently dropped in a total that
a consumer treats as complete, whether a NaN propagates without being caught or
reported, and whether a null is substituted as zero — indistinguishable from a
real zero. Check whether the behaviour is consistent across all aggregations.

Evidence: per aggregation — the null/NaN handling verdict; one null silently
dropped in a total; one NaN that propagates to output without a distinct
failure state.

### 4. Multi-implementation agreement

Establish whether two or more code paths compute the same derived quantity. For
each such pair, check whether they apply the same aggregation function, the same
boundary rules, and the same null/NaN handling. Where they disagree, determine
which implementation is canonical and whether the divergence is documented.

Evidence: the implementation inventory with a per-quantity count; one pair whose
results diverge on a representative input; the canonical verdict and the
discrepancy's propagation to output.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. An empty band is a valid outcome —
do not populate it with a hypothetical, and do not promote an item for sounding
alarming.

- **CRITICAL** — a derived quantity whose aggregation semantics silently produce
  a wrong value consumed as authoritative, so a total, rate, or pivot is
  incorrect without any error; null/NaN values silently dropped in a sum that
  a downstream reader treats as complete, producing a figure with no
  distinguishable failure state.
- **HIGH** — floating-point error that accumulates beyond a declared bound,
  changing a rounded value at a consumer boundary; a NaN that propagates to the
  rendered CSV without being caught or reported; two implementations of the
  same derived quantity that already disagree on a representative input.
- **MEDIUM** — an interval-boundary decision (inclusive vs. exclusive) in a pivot
  that affects a count or sum by a small margin; a null substituted as zero
  without being distinguishable from a real zero; a declared tolerance or error
  bound with no source or enforcement.
- **LOW** — a comment or docstring describing an aggregation that does not match
  the executed path; rounding applied at the wrong stage but within tolerance;
  an aggregation function whose result has no downstream consumer.

## Report Output

- Findings path: `.ai/audit/17-arithmetic/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `NUM-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- Finding IDs use the declared bare prefix; a shipped test, configuration
  comment, or runbook may already carry an ID from a prior cycle, so check
  for a collision before minting a new one
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker
