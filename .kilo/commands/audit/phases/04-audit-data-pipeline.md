---
name: 04-data-pipeline
status: draft
validated: no
executor: auditor
problems-only: true
---

# Phase 04 — Data Pipeline & Report Lifecycle

## Purpose

Audits the data pipeline from period resolution through task generation, worker
execution, data extraction, CSV export, and error serialisation. A report
begins by resolving a date period (explicit `date_filter` or computed
`last_time`, clamped by the API's available period), then a Strategy generates
per-interval × per-audience tasks into a bounded `asyncio.Queue`. A pool of
workers consumes each task through a send → wait → extract → export loop,
calling the Mediascope API at each stage. Tasks that fail are serialised to a
YAML error file; successful results are written as CSV. There is no database
and no authentication lifecycle — the "lifecycle" here is the lifecycle of a
report task from period resolution to CSV output.

This file names what to examine and under which angle; the executing auditor
discovers the concrete artifacts. Owned here: period resolution, task generation,
worker loop ordering, data extraction, CSV export, and error serialisation.
Owned elsewhere: the Semaphore and Queue concurrency (03); the retry and
backoff contract (09); config defaults merge (02); performance
ceilings (13).

---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. Period resolution and API clamping

Establish how the report's date period is derived: an explicit `date_filter`
pair versus a computed `last_time` window, and how the API's available-period
clamps both bounds. Check whether an explicit period that extends past the
API's available range is truncated silently or with a warning, and whether the
`multiple_files` flag correctly slices a multi-month period into sub-intervals
at the configured frequency.

Evidence: the resolved interval for an explicit period and a computed period;
one out-of-range period and how it is clamped; the slice count for a
multi-file period.

### 2. Task generation via the Strategy pattern

Establish how the task-generation strategy produces per-interval × per-audience
tasks from the resolved period and target audiences. Check whether tasks
already present in the export folder are skipped (the `check_done` guard),
whether the generated task settings match the resolved period exactly, and
whether a report that would produce zero tasks fails fast or silently no-ops.

Evidence: the task count for one interval; one already-exported task skipped;
one zero-task report and its behaviour.

### 3. Worker loop: send → wait → extract ordering

Establish the per-task execution flow: build the task on the API, send it,
poll for completion, then extract the result. Check whether a task that fails
to send is still polled (wasting time), whether `wait_task` polls only on
terminal statuses and does not spin unbounded, and whether a task that reports
`status is False` is routed to error serialisation without attempting data
extraction.

Evidence: the send→wait→extract ordering; one failed send and whether it is
polled; the poll interval and terminal-status set.

### 4. API result extraction and dataframe conversion

Establish how the raw API result is converted to a `DataFrame` and prepared by
the data strategy. Check whether a `None` or empty dataframe is distinguished
from a real result, whether the prepared columns match the configured output
columns, and whether a `KeyError` in the strategy is logged with the
offending column names.

Evidence: the DataFrame shape and columns for one task; one empty/None result
and its disposition; the error logged on a KeyError.

### 5. CSV export safety and file-existence handling

Establish how results are written to CSV files. Check whether the export uses
`mode="x"` (exclusive create), which silently skips when the file already
exists — a silent skip means a task can be reported as done without producing
output. Check whether the export path resolves relative to the configured
export folder, whether the filename includes a timestamp when intended, and
whether a partially written file is left behind on a crash during write.

Evidence: the file-open mode and filename pattern; one existing file and
whether the task is skipped or overwritten; the path resolution base.

### 6. Error serialisation and task-to-yaml

Establish how failed tasks are recorded. Check whether `TVTask.to_yaml` writes
relative to the configured export folder or to the process working directory,
and whether the error file includes enough context (settings, interval, target,
error message) to retry the task. Check whether an error file is produced for
every failure class or only some, and whether the error file name is
reproducible (so a re-run can detect it).

Evidence: the error-file target directory; one induced failure and whether an
error file is produced; the fields included in a serialised task.

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now, not the worst consequence if triggered. An empty band is a valid outcome —
do not populate it with a hypothetical.

- **CRITICAL** — a report task whose result is silently skipped because the CSV
  file already exists (mode `"x"`), so the run reports success while no output is
  produced; error serialisation that writes to the working directory instead of
  the export folder, making failures impossible to locate.
- **HIGH** — a task that fails to send but is still polled in a tight loop,
  consuming API quota and worker time; a period that is clamped to the wrong
  API boundary, silently truncating data; a zero-task report that completes
  with no output and no error.
- **MEDIUM** — a silent skip of an already-exported task that should have been
  re-run with new data; an empty dataframe treated as success rather than a
  distinct failure; a KeyError in the strategy that is logged but not
  attributed to a specific column or task.
- **LOW** — observability drift with no runtime consequence today: an error file
  missing a context field; a comment about the export path that does not match
  the configured value; a serialised task whose timestamp makes the name
  non-reproducible.

## Report Output

- Findings path: `.ai/audit/04-data-pipeline/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front matter,
  summary, findings, distribution, cross-finding analysis, roadmap, rollout safety,
  appendices
- Finding-ID prefix: `PIPE-`
- Incremental append, ≤100 lines per pass
- `problems-only: true` — findings only, omit passing checks; every finding needs
  runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker
