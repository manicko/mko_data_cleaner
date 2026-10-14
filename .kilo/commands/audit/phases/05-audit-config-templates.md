---
name: 05-config-templates
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 05 — Config Template Validation & Spec Conformance

## Purpose

Audits whether the bundled YAML templates — report definition files and default
data-settings files — conform to the Pydantic v2 models that consume them, and
whether the defaults-merge contract holds end to end. Report templates in
`settings/reports/` are validated against `ReportSettings` → `PeriodModel` →
`LastTimeModel`; default data templates in `settings/defaults/` are validated
against `DataDefaults`. A merged dict of base defaults overlaid with report
settings is then validated as a `Data` model before reaching the API call.
Mismatches between template field names and model attributes, or between the
declared schema and what the code actually reads, cause silent data loss or
validation failures at runtime.

This file names what to examine and under which angle; the executing auditor
discovers the concrete artifacts. Owned here: template-to-model conformance,
field-name drift, template-key survival through the merge, and the extra-field policy of
each model. Owned elsewhere: template distribution and credential sourcing
(02); the data pipeline and export correctness (04); fixed-value enum
discipline (10); import-time bootstrap (01).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: a static proof that a stated property
does not hold, a reproduced validation error, or an observed silent pass; the
method is the auditor's. A passing check is methodology, never a finding.

### 1. Template-to-Pydantic model conformance

Load every bundled YAML report template and every default data template and
validate each against its target Pydantic model. Check whether validation errors
name the specific key that failed, whether a template that fails can still be
loaded by a lenient parser at runtime, and whether the failing templates share
a common cause (e.g., field names in the template do not match the model
attributes).

Evidence: the pass/fail count per template type; one failing template with the
exact error; the common cause behind all failures.

### 2. Field-name drift between templates and models

Establish whether template field names match the Pydantic model attribute names
exactly. Check whether a key present in a template (e.g., a period subtype
field) is mapped to a different attribute name in the model, and whether
`extra="forbid"` on a nested model causes validation to reject otherwise-valid
data. Check whether `extra="allow"` on a parent model silently absorbs typos
in fields that should be validated.

Evidence: the name-mismatch inventory; one template field silently absorbed by
`extra="allow"`; the nested model whose `extra="forbid"` rejects a valid key.

### 3. Period / last_time model conformance

Establish whether the `PeriodModel` → `LastTimeModel` chain validates the
template's period specification. Check whether `date_filter` and `last_time`
are mutually exclusive when one is expected, whether `last_time`'s sub-fields
(`time_unit`, `unit_count`, `include_current`) are all present and correctly
typed, and whether a period that provides neither fails fast with a clear
message.

Evidence: the PeriodModel verdict for one template; one period with a missing
required sub-field; the failure mode when neither field is supplied.

### 4. Template-field conformance within the merge

Establish whether template keys survive the defaults-to-Data merge into the
correct `Data` model fields. Check whether a key present in a template is mapped
to the right attribute in the `Data` model after the overlay, and whether a
missing default file produces a usable configuration or a crash.

Evidence: the merged `Data` for one report; one template key and its surviving
field; one missing default file and its behaviour.

### 5. Extra-field policy and silent absorption

Establish the `extra` configuration of each Pydantic model in the validation
chain (`ReportSettings`, `PeriodModel`, `LastTimeModel`, `DataDefaults`,
`Data`). Check whether a typoed or unknown key at any level is silently ignored
(`extra="allow"` or `"ignore"`) or rejected (`extra="forbid"`), and whether a
key that is silently absorbed was intended to be consumed.

Evidence: per model — the `extra` setting; one unknown key silently absorbed;
one consumed key that is silently dropped.

### 6. Declared spec conformance against what the code reads

Establish whether every key that the data-preparation code reads from the
merged `Data` model is actually present in the validated template. Check
whether a comment or template key states a field that the code's strategy
never consults, and whether a constant duplicated between templates and code
has diverged.

Evidence: the read key inventory; one template key never consulted by code;
one duplicated value that disagrees.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered.

- **CRITICAL** — a report template that fails validation so the report cannot
  run at all, while a shipped example asserts it should work; a field-name
  mismatch that silently drops a required API parameter (e.g., `date_filter` or
  `kitId`), producing a report against the wrong data without error.
- **HIGH** — `extra="allow"` silently absorbing a typoed field name that the
  code never reads, so a misconfigured report runs with defaults the operator
  did not intend; a defaults-merge that drops a subtype-specific override,
  changing the data returned; a template that passes lenient parsing but fails
  strict validation with no runtime fallback.
- **MEDIUM** — a nested model with `extra="forbid"` that rejects a valid key
  only in certain configurations, creating an inconsistent validation surface;
  a duplicated constant (e.g., a kit ID or filter) between templates and code
  that has not yet diverged.
- **LOW** — documentation and drift with no runtime consequence today: a
  comment in a template contradicting the field it describes; a template key
  never consulted but harmlessly present; a default file with no consumer.

## Report Output

- Findings path: `.ai/audit/05-config-templates/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `TPL-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding
  needs a static proof or a reproduced validation error and the exact consequence
- Empty state, exactly: `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker
