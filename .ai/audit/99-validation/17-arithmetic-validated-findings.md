---
phase: "99"
phase_name: "validation"
id_prefix: "NUM"
mode: "problems-only"
report_status: "validated"
severity_taxonomy: ".kilo/commands/audit/phases/17-audit-arithmetic.md#severity-taxonomy"
code_ref: "c0fd0e1"
validated_at: "2026-10-19"
validated_by: "validator"
---

# Audit Findings Validation — Arithmetic (Phase 17)

## Summary

All three findings from `.ai/audit/17-arithmetic/findings.md` were validated against
commit `c0fd0e1` (HEAD). Every anchor resolves, every repro reproduces, and every
root-cause claim holds against the executing path. All three are **confirmed** (kept
as filed): NUM-001 [HIGH], NUM-003 [MEDIUM], NUM-002 [LOW]. The highest validated
severity is **HIGH**. No re-grades, no re-types, no rejections, no merges, and no
validation-level (`VAL-`) findings. The `NUM-` namespace is unique to phase 17 (no
collisions); the findings and validation namespaces do not share a table.

**Tools re-run:** `uv run ruff check src/ tests/` → All checks passed.
`uv run basedpyright src/` → 39 errors, 0 warnings, 0 notes (owned by phase 04, not
re-filed). `uv run pytest` → 93 passed in 3.49s. `uv run python` repro scripts for each
finding — all reproduce the claimed defects. SQLite version 3.50.4; Polars 2.0.0.

## Scope

**Examined:** `src/mko_data_cleaner/core/db_service.py` (lines 400–416 `update_index_from_data`,
627–649 `apply_mapping`, 662–690 `_apply_replace`, 691–746 `_apply_add`),
`src/mko_data_cleaner/core/csv_service.py` (lines 269–381 `export_sql_to_csv`, including
328 `schema_overrides` and 364 `df.write_csv`), `src/mko_data_cleaner/core/app_service.py`
(line 238 `separator=self.app_config.dict_file_settings.add_separator`),
`src/mko_data_cleaner/core/models.py` (lines 85–102 `PolarsWriteCSV`/`PolarsReadCSV`,
line 158 `DataDict.add_separator`), `src/mko_data_cleaner/settings/app_config.json5`
(lines 31, 43 `decimal_comma: true`, line 15 `add_separator: ", "`),
`docs/00-overview/data-flow.md` (lines 95–104), `docs/00-overview/architecture.md`
(lines 116–119), `docs/00-overview/current-behavior.md` (lines 167–169),
`docs/00-overview/open-questions.md` (OQ-7 at line 191, 298+), `tests/conftest.py`,
`tests/test_db_service.py` (lines 524–589 `test_apply_add_concatenates_tags`),
`tests/test_app.py` (lines 396–397 e2e assertions). Sibling reports: phase 05
(TPL-002 — `infer_schema_length`), phase 10 (QLT-003 — `AVAILABLE_ACTIONS`),
phase 11 (TST-007 — `update_index_from_data` coverage gap), phase 12 (OPS —
SQL-interpolation posture), phase 16 (rendered surface), phase 01 (ENT — entry).

**Not verified:** the real gzip input path and the FTS5/LIKE match path with real
FTS dictionary rows (no FTS rule in the synthetic fixture) — matches the input's
own "Not verified" scope; does not affect the validity of these three findings,
which were all reproduced in isolation.

## Disposition Tally

| Disposition | Count | Items |
|---|---|---|
| confirmed | 3 | NUM-001, NUM-003, NUM-002 |
| re-typed | 0 | — |
| re-graded | 0 | — |
| merged | 0 | — |
| not substantiated | 0 | — |
| unsettled | 0 | — |

## Per-Finding Validation

### NUM-001 — [HIGH] MAX(date) tie makes the per-key representative row nondeterministic
**Dispositions:** confirmed · **type:** SPEC-DEVIATION · **verified:** ran

**Block 1 — claim re-derived from the executing path:**
Anchor `src/mko_data_cleaner/core/db_service.py::DBWorker.update_index_from_data`
resolves to `db_service.py:400–416`. Line 407: `select_columns += f", MAX({self.date_column})"`.
Lines 408–412 build the query `INSERT INTO {index_tbl} (…) SELECT {search cols}, {index}, MAX({date}) FROM {data_tbl} GROUP BY {index}`. This is the bare-column-with-MAX pattern. SQLite's documented behavior: when `MAX(date)` ties across multiple rows for a key, the bare (non-aggregated) column values are taken from an arbitrary tied row. **Repro reproduced:** inserting `BrandA` then `BrandB` (both dated `2024-01-15`) yields representative `brand = BrandA`; inserting `BrandB` then `BrandA` yields `brand = BrandB`. Same logical input, different output. The asserted cause (bare-column-on-tie nondeterminism) is factually correct.

**Block 2 — support that is only a green control:** Not applicable. The finding's support
is the runtime repro + SQLite documentation quote + code reading. No green test asserts the
current (buggy) behavior — `grep update_index_from_data tests/` returns **0 hits**
(consistent with sibling TST-007, phase 11, validated). The `synthetic_report` e2e fixture
(line 110–114 of conftest.py) has 3 rows with **distinct** dates (`2024-01-01`,
`2024-01-15`, `2024-02-01`), so no key has a date tie — the defect is invisible to the
shipped suite by design. No green control is the sole basis.

**Block 3 — grade against phase 17 rubric:** Phase 17 CRITICAL band: "a derived quantity
whose aggregation semantics silently produce a wrong value consumed as authoritative, so a
total, rate, or pivot is incorrect." Phase 17 HIGH: "floating-point error …; a NaN …; two
implementations … that disagree." The finding itself cites CRITICAL is excluded because no
numeric total/rate/pivot is computed — the derived value is categorical (which brand/tag
set wins). The auditor grades HIGH on the theory of "silent wrong value consumed as
authoritative." **Assessment:** defensible. The CRITICAL consequence clause specifically names
"total, rate, or pivot" — all numeric; phase 17 is an *arithmetic* audit and its CRITICAL
band is anchored to numeric wrong values. The HIGH band's literal sub-items (float error, NaN,
implementations-disagree) do not perfectly enumerate NUM-001, but the phase file instructs
"Grade by effect and blast radius, not by mechanism name" and "a mechanism absent from an
enumerated band is not thereby a lower band." A silent wrong categorical value consumed as
authoritative by the matching → rules → tag-distribution chain has real blast radius (all
rows sharing the key get the wrong decision). **Grade holds** (borderline CRITICAL→HIGH;
no re-grade issued).

**Block 4 — which side moves (code or documentation):** Both. The **code** lacks a
deterministic tie-break (no secondary sort key or `rowid` ordering inside a window function).
The **documentation** over-states determinism: `data-flow.md:98-100` says "the latest row
per key" (acknowledging only the no-date case as arbitrary); `architecture.md:116-119`
says "the latest row per key. Without a configured date column the values come from an
arbitrary row of the group"; `current-behavior.md:168-169` says the same. None discloses
that when multiple rows tie on the max date, the bare-column value is still arbitrary. The
docs describe the *intended* contract; the code violates it. **Type: SPEC-DEVIATION.**

**Block 5 — can the recommendation be carried out?** Yes. The fix targets
`update_index_from_data` (db_service.py:407-412), which exists and is stable. The proposed
`ROW_NUMBER() OVER (PARTITION BY index_column ORDER BY date DESC, rowid DESC)` approach is
the same deterministic pattern already used by `_apply_replace` (db_service.py:674-676:
`ROW_NUMBER() OVER(PARTITION BY data_rowid ORDER BY mapping_index DESC, rowid DESC)`), so
the pattern is precedented in the same module. Dependencies: none (disjoint from
`_apply_add`, `_apply_replace`). What fixing it breaks: the representative for
date-tied keys *may* change to a stable choice — the input's own Fix Order acknowledges this
and gates on re-running the synthetic_report e2e (expect rows_loaded==3, rows_exported==2,
which has only one row per date — the no-tie case is unchanged). `test_process_data_end_to_end`
(test_app.py:396-397) asserts `rows_loaded==3`, `rows_exported==2` — these pass today and
remain green because the synthetic fixture has no ties. **Recommendation is carry-out eligible.**

**Block 6/7 — cross-phase / namespace:** NUM-001 cross-references TST-007 (phase 11,
validated as confirmed [MEDIUM]) — "no test on `update_index_from_data`" — which is
consistent: TST-007 notes `grep update_index_from_data tests/` → 0 hits, and the `db_worker`
fixture (conftest.py:28-38) always sets `date_column`. No conflict. TST-007 is about the
`date_column=None` boundary; NUM-001 is about ties when `date_column` IS set — adjacent
concerns, distinct root causes, no overlap. OQ-7 (open-questions.md:191, 298) is confirmed
to exist as an open question about whether `date_column` should default to `MAX(date)` per key
— this is related (the same `MAX(date)` mechanism) but addresses the *absence* of a date
column, not ties, so it is a cross-reference, not a conflict. The `NUM-` prefix is unique to
phase 17; no collision across the audit family.

---

### NUM-003 — [MEDIUM] add_separator interpolated raw into GROUP_CONCAT SQL; a quote breaks ADD
**Dispositions:** confirmed · **type:** SPEC-DEVIATION · **verified:** ran

**Block 1 — claim re-derived from the executing path:** Anchor
`src/mko_data_cleaner/core/db_service.py::DBWorker._apply_add` resolves to `db_service.py:691–746`.
Line 730: `SELECT GROUP_CONCAT(value, '{separator}')`. The `separator` parameter flows from
config: `app_config.json5:15` (`"add_separator": ", "`) → `DataDict.add_separator`
(models.py:158, `add_separator: str = ", "` — no `StringConstraints`, no regex, unconstrained)
→ `app_service.py:238` (`separator=self.app_config.dict_file_settings.add_separator`)
→ `apply_mapping` (db_service.py:628-644, default `separator=", "`) → `_apply_add(separator)`
→ `GROUP_CONCAT(value, '{separator}')`. The separator is interpolated as a SQL string literal
(`'{separator}'`) with no escaping or validation. SQLite `GROUP_CONCAT`'s separator cannot be a
bound parameter, so validation/escaping is mandatory. **Repro reproduced:** default `", "`
yields `"fruit_tag, sponsor_tag"`; single-quote separator (`chr(39)`) yields
`OperationalError: near "tag": syntax error`; injection attempt (`x'; DROP TABLE …`)
yields `OperationalError: near ";": syntax error`. The finding's claim holds.

**Block 2 — support that is only a green control:** Not applicable. The finding's support is
the runtime repro + the config-flow trace. `test_apply_add_concatenates_tags`
(test_db_service.py:524-589) only exercises the default `", "` separator (line 569:
`db_worker._apply_add(separator=", ")`) and asserts `"fruit_tag, sponsor_tag"` (line 581).
No shipped test exercises a non-default or quote-bearing separator — `grep add_separator tests/`
returns **0 hits**. The green test does not refute the finding; it only documents the safe
default path.

**Block 3 — grade against phase 17 rubric:** The MEDIUM band covers interval-boundary,
null-as-zero, and error-bound cases — none of which literally describe SQL-literal injection.
However, the phase instructs "Grade by effect and blast radius." The failure mode is
fail-stop (OperationalError aborts the run), not a silent wrong value — correctly excluding
CRITICAL ("silent wrong value consumed as authoritative") and HIGH (float-error/NaN/
implementations-disagree). Blast radius is bounded: the default `", "` is safe; only analysts
who set a quote-bearing `add_separator` trigger the abort. **Grade holds** (MEDIUM is the
appropriate band for a fail-stop, config-controlled correctness defect with bounded blast
radius in an arithmetic/derived-quantity context; no re-grade).

**Block 4 — which side moves (code or documentation):** CODE. `DataDict.add_separator`
(models.py:158) declares no constraint; `app_config.json5:15` carries the safe default
`", "`. The documentation (`data-flow.md:136`, `current-behavior.md:209`) correctly describes
the ADD separator behavior for the default case. The defect is purely in the code —
unvalidated interpolation. **Type: SPEC-DEVIATION.**

**Block 5 — can the recommendation be carried out?** Yes. Two target symbols exist and are
stable: `_apply_add` (db_service.py:730) and `DataDict.add_separator` (models.py:158). The
fix is additive: validate that `add_separator` contains no single quotes (raise
`WrongDataSettings`, which already exists per `errors.py`) or escape by doubling quotes
(`'`→`''`) before interpolation. The default `", "` is unaffected. `test_apply_add_concatenates_tags`
stays green. Dependencies: none.

**Block 6/7 — cross-phase / namespace:** NUM-003 references "phase 12 (SQL-interpolation
posture at the persistence boundary)" as a *related* cross-reference, not a conflict. Phase
12's findings (OPS-003 through OPS-009) do not contain a finding about `_apply_add` or
`GROUP_CONCAT` interpolation — no overlap, no conflict. The phase 10 finding QLT-003
(`AVAILABLE_ACTIONS` dead code) is correctly noted as irrelevant to the arithmetic
derived-quantity computations. `NUM-` prefix is unique to phase 17.

---

### NUM-002 — [LOW] decimal_comma configured (true) but inert on the all-String read/write path
**Dispositions:** confirmed · **type:** DOC-UPDATE/SPEC-DEVIATION · **verified:** ran

**Block 1 — claim re-derived from the executing path:** Anchor
`src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv` resolves to
`csv_service.py:269-381`. Line 328: `schema_overrides = {col: pl.Utf8 for col in columns}`
forces every column to `pl.Utf8` in the DataFrame constructor (line 348:
`schema_overrides=schema_overrides`). Line 364: `df.write_csv(file=file_path, **params)`
where `params = self.export_settings.copy()` includes `decimal_comma: True`
(from `PolarsWriteCSV.decimal_comma`, models.py:88). Polars only applies `decimal_comma`
to `Float` columns — on `String` columns it is a no-op. **Repro reproduced:**
- Reader: `pl.read_csv(BytesIO(b"x;y\n1,5;2,3\n"), separator=";", decimal_comma=True,
  infer_schema_length=False)` → `Schema({'x': String, 'y': String})`, `x = ['1,5']` (decimal
  never parsed; `infer_schema_length: bool = False` at models.py:102 disables schema
  inference → all String).
- Writer: `pl.DataFrame({"x":["1,5","2,3"]}, schema={"x":pl.String}).write_csv(
  separator=";", decimal_comma=True)` → `'x\n1,5\n2,3\n'` (verbatim, no comma inserted).
- Contrast: `pl.DataFrame({"x":[1.5,2.3]}, schema={"x":pl.Float64}).write_csv(
  separator=";", decimal_comma=True)` → `'x\n1,5\n2,3\n'` (decimal_comma IS active on Float).
Config confirmed: `app_config.json5:31` (`read_settings.from_csv.decimal_comma: true`) and
`app_config.json5:43` (`export_settings.to_csv.decimal_comma: true`). The finding holds.

**Block 2 — support that is only a green control:** Not applicable. Support is the runtime
repro + code reading. No shipped test asserts `decimal_comma` end-to-end —
`grep decimal_comma tests/` returns **0 hits**. No green control exists for this behavior.

**Block 3 — grade against phase 17 rubric:** The LOW band covers "a comment or docstring
describing an aggregation that does not match the executed path; rounding applied at the
wrong stage but within tolerance; an aggregation function whose result has no downstream
consumer." NUM-002 most closely aligns with "rounding applied at the wrong stage but within
tolerance": the locale-rendering contract (`decimal_comma=true`) is configured and
documented as active, but no transformation occurs on the all-String path — values pass
through verbatim with no precision loss. The finding itself correctly excludes CRITICAL/HIGH
("need a silently wrong numeric value; none is produced"). **Grade holds** (LOW — the
consequence is an unfulfilled locale contract with no data corruption; no re-grade). **Note:**
the LOW band's literal sub-items do not perfectly enumerate NUM-002 (it is not a doc-string
mismatch, not a rounding-stage issue, and the `decimal_comma` setting DOES have a downstream
consumer — it just doesn't execute). The grade is defensible on the "effect and blast radius"
principle (no data corruption, bounded to consumers who trust the locale setting), but the
band match is indirect. No re-grade issued.

**Block 4 — which side moves (code or documentation):** Both candidates exist — the finding
offers two fix options: (a) parse expected-numeric columns with `decimal_comma` active (finite
`infer_schema_length` + cast), so the writer's `decimal_comma` then takes effect — this is a
CODE fix in `export_sql_to_csv` (csv_service.py:328-348) + `PolarsReadCSV`/`PolarsWriteCSV`
(models.py:88-102); or (b) document that `decimal_comma` is inert for this tool and stop
promising locale rendering — this is a DOC fix. The documentation is the load-bearing
contract here: `data-flow.md:170` says "numeric formatting is governed by the CSV writer
settings," which is the promise `decimal_comma:true` fails to deliver. **Type:
SPEC-DEVIATION** (docs over-promise vs. code reality) with a DOC-UPDATE remediation path.

**Block 5 — can the recommendation be carried out?** Yes. Both targets exist and are stable:
`export_sql_to_csv` (csv_service.py:328-348) and `PolarsReadCSV`/`PolarsWriteCSV`
(models.py:88-102). Option (b) is doc-only (no behavior change). Option (a) is more invasive:
it would change how numeric-looking values are read — the Fix Order correctly gates on
existing CSV round-trip tests. Note: phase 05's TPL-002 (validated [MEDIUM]) addresses
`infer_schema_length: bool = False` (models.py:102) as part of this same all-String path —
fixing TPL-002 alone does NOT fix NUM-002 because the export path separately forces
`schema_overrides={col: pl.Utf8}` at csv_service.py:328. The findings are distinct root causes
sharing one mechanism.

**Block 6 — cross-phase / namespace:** NUM-002 references "phase 16 (rendered surface)."
Phase 16's findings mention `decimal_comma` only as a declared field of `PolarsWriteCSV`
(findings.md:255) — no phase 16 finding asserts `decimal_comma` is inert, so there is no
conflict. Phase 05's TPL-002 (validated confirmed [MEDIUM]) is a same-root-cause
cross-reference: TPL-002 targets the `bool` type of `infer_schema_length` (one of the three
factors in NUM-002's root cause); NUM-002 additionally covers the `schema_overrides` Utf8
override and the SQLite TEXT storage model. No conflict — both are confirmed and address
overlapping but distinct aspects. `NUM-` prefix is unique to phase 17.

---

## Namespace Integrity (Block 7)

Six shapes established for the `NUM-` prefix:

| Shape | Finding |
|---|---|
| Declared prefix | `NUM-` (findings.md front matter, line 4: `id_prefix: "NUM-"`) |
| Identifiers minted this run | `NUM-001`, `NUM-002`, `NUM-003` |
| In-source provenance markers | 0 — `grep -r "NUM-" src/ tests/` returns no matches (these are audit findings, not code annotations) |
| Phase-qualified compound form | `17-arithmetic/NUM-001` etc. — resolves uniquely to the findings file at `.ai/audit/17-arithmetic/findings.md` |
| Template prefix list | The controlled template (`audit-findings.md:4`) states `id_prefix: "{PREFIX}"` with comment "from the phase file's Report Output; no prefix list lives here" — it does not enumerate prefixes |
| Namespace reuse across phases | `NUM-` is used only by phase 17; no other audit phase declares or mints `NUM-` identifiers. No collision. |

**Ruling:** The audited (`NUM-`) and validation (`VAL-`) namespaces do not share a table.
No `VAL-` findings are issued in this validation pass (no audit defects found in the phase 17
findings that rise to a validation-level finding). The `NUM-` namespace is clean — no
re-minting, no second namespace beside it.

## Template Artefact Check (Block 8)

The findings file follows the controlled template (`audit-findings.md`) with minor deviations
that do not affect validity:

- Front-matter fields present and honest: `phase`, `phase_name`, `id_prefix`, `mode`,
  `report_status` ("draft"), `severity_taxonomy`, `code_ref` ("c0fd0e1"). `severity_taxonomy`
  pointer resolves to `.kilo/commands/audit/phases/17-audit-arithmetic.md#severity-taxonomy`
  (confirmed: the phase file exists at `.kilo/commands/audit/phases/17-audit-arithmetic.md`
  and contains a `## Severity Taxonomy` heading). ✓
- The findings index table (ID/Severity/Anchor/Title) is present (findings.md:53-57). ✓
- The GATES comment block (findings.md:47-51) follows the template's GATES pattern, with all
  four gates checked `✓`. ✓
- Each finding carries all mandated fields: Anchor, Disambiguator, Checked, Why this band,
  Problem, Repro, Impact, Root cause, Fix, Verify, Related. ✓
- The `problems-only: true` convention is declared (phase file front matter, line 6) and
  followed — no passing checks are dressed as findings. ✓
- The Fix Order section is present (findings.md:202-222) with all five sub-blocks
  (Dependencies, Merge candidates, Conflicting evidence, Cross-phase conflicts, Rollout risk). ✓
- One deviation: the front matter omits `executor` and `validated` fields that the phase-99
  template adds — but those are phase-99-specific fields, not phase-template fields, so this
  is template-compliant for a raw-phase report.

## Do the findings rest on the phase's own enumerated checks? (Block 9)

Phase 17 has no enumerated "check list" — it declares four Audit Blocks (1. Aggregation
semantics, 2. Float error bounds, 3. Null/NaN propagation, 4. Multi-implementation
agreement) and a Severity Taxonomy. No finding rests on a "green control" from these blocks.
All three findings have support from an **independent angle**:

- NUM-001: runtime repro (SQLite tie nondeterminism) + SQLite documentation quote. Not from
  any phase-17 check; it is a direct reproduction of the executed SQL.
- NUM-003: runtime repro (OperationalError on quote separator) + config-flow trace. Not from
  a check; it is a direct SQL execution.
- NUM-002: runtime repro (Polars float vs String `decimal_comma` behavior) + code reading of
  the export path.

The findings do not rest on `ruff`/`pytest` being green as their *basis* — they were
substantiated *despite* the green suite (the suite's fixtures avoid ties, quote separators,
and numeric-looking String columns). The phase 17 input's scope (findings.md:38-40) states
"Not verified: the real gzip input path, the FTS5/LIKE match path" — this is an honest
limitation, not a green control.

## Rollout Safety

| Finding | What the fix touches | Must-stay-green test | Risk |
|---|---|---|---|
| NUM-001 | `update_index_from_data` SQL — add `rowid DESC` tiebreak via `ROW_NUMBER()` | `test_process_data_end_to_end` (rows_loaded==3, rows_exported==2) | Low — synthetic fixture has no date ties; no-tie case unchanged |
| NUM-003 | `_apply_add` + `DataDict.add_separator` validation/escaping | `test_apply_add_concatenates_tags` | Low — default `", "` unaffected; additive validation |
| NUM-002 | `export_sql_to_csv` schema_overrides + `PolarsReadCSV`/`PolarsWriteCSV` models | existing CSV round-trip tests | Medium (option a) — changes how numeric-looking String values are read; Low (option b) — doc-only |

Cross-finding ordering: the findings' Fix Order correctly states "None. The three touch
disjoint symbols (`update_index_from_data`, `_apply_add`, `export_sql_to_csv`)." Confirmed.

## Required Fixes

1. **NUM-001:** Replace the bare-column `MAX(date)` GROUP BY in `update_index_from_data`
   (db_service.py:407-412) with a deterministic `ROW_NUMBER() OVER (PARTITION BY index_column
   ORDER BY date DESC, rowid DESC)` selection, or add a stable secondary key. Update
   `data-flow.md`, `architecture.md`, and `current-behavior.md` to disclose tie-breaking.
2. **NUM-003:** Add a validator on `DataDict.add_separator` (models.py:158) rejecting single
   quotes, or escape by doubling (`'`→`''`) before interpolation at `db_service.py:730`.
3. **NUM-002:** Either (a) parse expected-numeric columns with `decimal_comma` active (finite
   `infer_schema_length` + cast) so the writer's `decimal_comma` takes effect, or (b) document
   that `decimal_comma` is inert for the all-String path and stop promising locale rendering
   in `data-flow.md:170`.

## Advisory Recommendations

- **NUM-001 / TST-007 cross-fix:** Adding a `test_update_index_date_ties_are_deterministic`
  test (with two rows for one key sharing the max date but differing search-column values)
  also closes TST-007's assertion that `update_index_from_data` has no test coverage. The
  e2e fix and the unit test should land together.
- **NUM-002 / TPL-002:** Phase 05's TPL-002 (validated [MEDIUM]) fixes the `bool` type of
  `infer_schema_length`. After TPL-002 lands (changing it to `int | None` with a real numeric
  default), re-evaluate whether NUM-002's option (a) is still needed — a numeric
  `infer_schema_length` would let Polars infer Float columns, where `decimal_comma` would
  then take effect. But the export path's `schema_overrides={col: pl.Utf8}` (csv_service.py:328)
  must also be relaxed for NUM-002(a) to fully work. Coordinate TPL-002 and NUM-002(a)
  fixes.
- **Minor inaccuracies in the input** (do not affect validity): The findings cite
  `_sync_with_data_table` (findings.md:91) but the actual method is `sync_with_data_table`
  (db_service.py:418, no leading underscore); `_sync_tables` (db_service.py:427) is the
  internal method. The findings describe the GROUP_CONCAT as `GROUP_CONCAT DISTINCT … ORDER
  BY value` (Appendix F, line 292) but the actual SQL is `GROUP_CONCAT(value, '{separator}')`
  with `DISTINCT` and `ORDER BY value` in the inner subquery (db_service.py:730-737). The
  repro error message for the single-quote separator differs slightly from the findings'
  cited `unrecognized token` (observed `near "tag": syntax error`) because the full SQL
  includes additional clauses — but both confirm the OperationalError. These are
  description-level imprecisions, not substantive errors.

## Appendices

### Appendix A — Tooling baseline (re-run at c0fd0e1)

```text
$ git rev-parse --short HEAD
c0fd0e1

$ uv run ruff check src/ tests/
All checks passed!

$ uv run pytest -q
93 passed in 3.49s

$ uv run basedpyright src/
39 errors, 0 warnings, 0 notes

$ git ls-files \
  src/mko_data_cleaner/core/db_service.py \
  src/mko_data_cleaner/core/csv_service.py \
  src/mko_data_cleaner/core/models.py \
  src/mko_data_cleaner/core/app_service.py
src/mko_data_cleaner/core/app_service.py
src/mko_data_cleaner/core/csv_service.py
src/mko_data_cleaner/core/db_service.py
src/mko_data_cleaner/core/models.py
```

### Appendix B — Evidence: NUM-001 MAX(date) tie nondeterminism (reproduced)

```text
A-then-B: representative brand = BrandA
B-then-A: representative brand = BrandB
SQLite version: 3.50.4
```

*(supports: identical logical data yields different representative rows; both rows tie at
2024-01-15, so either pick is valid — the choice is not guaranteed/stable.)*

### Appendix C — Evidence: NUM-003 separator injection (reproduced)

```text
default sep (', '): fruit_tag, sponsor_tag
single quote sep: OperationalError: near "tag": syntax error
injection sep: OperationalError: near ";": syntax error
```

*(supports: any quote in `add_separator` produces invalid SQL and aborts ADD.)*

### Appendix D — Evidence: NUM-002 decimal_comma inertness (reproduced)

```text
Polars version: 2.0.0
Reader schema: Schema({'x': String, 'y': String})
Reader x value: 1,5
Writer output: 'x\n1,5\n2,3\n'
Writer Float output: 'x\n1,5\n2,3\n'
Reader Float schema: Schema({'x': String, 'y': String})
Reader Float x value: 1.5
Float roundtrip: 'x;y\n1.5\n2.3\n'
```

*(supports: `decimal_comma=True` is accepted but emits/avoids no decimal transformation on
the String path; `1.5` passes through as `1.5` even with `decimal_comma=True`; Float works.)*

### Appendix E — Evidence: NUM-001 documentation over-states determinism

```text
data-flow.md:98-100:
  "SELECT search cols, index col, MAX(date) … GROUP BY index col: one row per
   unique key. In SQLite the bare (search) columns take their values from the row
   holding the MAX(date) — i.e. the latest row per key (an arbitrary row when no
   date column is configured)."

architecture.md:116-119:
  "Representative row: the GROUP BY with MAX(date) makes SQLite take the bare
   (search) column values from the row that holds the maximum date — the latest
   row per key. Without a configured date column the values come from an
   arbitrary row of the group."

current-behavior.md:168-169:
  "the bare (search) columns take their values from the row holding the MAX,
   i.e. the latest row per key (an arbitrary row when no date column is
   configured)."
```

*(supports: all three docs say "the latest row per key" when date_column is set, but none
disclose that ties on the max date are still arbitrary per SQLite's bare-column rule.)*

### Appendix F — Evidence: NUM-003 config-flow trace

```text
app_config.json5:15:     "add_separator": ", "
models.py:158:     add_separator: str = ", "          # unconstrained str, no StringConstraints
app_service.py:238:     separator=self.app_config.dict_file_settings.add_separator,
db_service.py:628: def apply_mapping(self, ..., separator=", "):
db_service.py:644:     self._apply_add(separator)
db_service.py:730:     SELECT GROUP_CONCAT(value, '{separator}')  # raw f-string interpolation
```

*(supports: the config-controlled separator reaches the SQL literal with no escaping.)*

### Appendix G — Evidence: NUM-002 all-String path chain

```text
models.py:102:     infer_schema_length: bool = False     # → all columns String on read
csv_service.py:328: schema_overrides = {col: pl.Utf8 for col in columns}  # → all String on export
csv_service.py:364: df.write_csv(file=file_path, **params)  # params includes decimal_comma=True
```

*(supports: the all-String read + write chain where decimal_comma is a no-op. Also
corroborates phase 05 TPL-002 — `infer_schema_length: bool = False` is part of this chain.)*

### Appendix H — Sibling reports compared

| Report | Prefix | Status | Relevant overlap |
|---|---|---|---|
| 05-config-templates | TPL | validated | TPL-002 confirms `infer_schema_length: bool = False` (part of NUM-002's root cause) — cross-reference, no conflict |
| 10-code-quality | QLT | raw | QLT-003 (`AVAILABLE_ACTIONS` dead copy of `ActionType`) — correctly noted in Appendix F as not a duplicate derived-quantity computation |
| 11-test-coverage | TST | validated | TST-007 (`update_index_from_data` coverage gap) — consistent with NUM-001's coverage claim; adjacent concern, no conflict |
| 12-production-ops | OPS | validated | OPS-005 mentions SQL-interpolation posture at persistence boundary — referenced by NUM-003 as related, no conflict |
| 16-rendered-surface | OUT | raw | Phase 16 mentions `decimal_comma` as a `PolarsWriteCSV` field (not a finding about inertness) — no conflict |
| 01-entry-architecture | ENT | validated | No overlap with NUM findings |

No sibling report reaches an opposite conclusion on any NUM finding.
