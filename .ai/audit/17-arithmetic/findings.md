---
phase: "17"
phase_name: "arithmetic"
id_prefix: "NUM-"
mode: "problems-only"
report_status: "draft"
severity_taxonomy: ".kilo/commands/audit/phases/17-audit-arithmetic.md#severity-taxonomy"
code_ref: "c0fd0e1"
---

# Audit Findings — Arithmetic

## Summary

Three findings, highest severity **HIGH**. The tool has no numeric
totals/rates/pivots — its derived quantities are categorical: a per-key
"representative row" (`MAX(date)…GROUP BY index`), a REPLACE winner
(`ROW_NUMBER()`), and an ADD tag list (`GROUP_CONCAT`). Two of those are
silently defective: the per-key representative is non-reproducible when rows
for a key tie on the date (SQLite picks an arbitrary tied row), and the ADD tag
aggregation interpolates the analyst-configurable separator raw into SQL so a
single quote in `add_separator` aborts the run. The third is an inert
`decimal_comma` setting whose locale-rendering contract is never applied on the
all-text read/write path. The REPLACE winner and the ADD tag ordering are
deterministic and not flagged.

## Scope

**Examined:** `core/db_service.py — ` `DBWorker.update_index_from_data`
(`GROUP BY index_column … MAX(date)`), `DBWorker._apply_add`
(`GROUP_CONCAT(value, '{separator}')`), `DBWorker._apply_replace`
(`ROW_NUMBER() … ORDER BY mapping_index DESC, rowid DESC`); `core/csv_service.py
— `CSVWorker.export_sql_to_csv` (`schema_overrides={col: pl.Utf8}`) and
`CSVWorker._read_csv_in_chunks` (`scan_csv` under reader settings);
`core/models.py — ` `PolarsReadCSV.infer_schema_length`,
`PolarsWriteCSV.decimal_comma`, `DataDict.add_separator`.

**Tools:** `uv run ruff check src/ tests/` (clean), `uv run basedpyright src/`
(39 errors — owned by phase 04, not re-filed), `uv run pytest` (93 passed),
`uv run python` repros, `git ls-files`, `grep`, SQLite 3.50.4.

**Not verified:** the real gzip input path, the FTS5/LIKE match path (no FTS
rule in fixtures), and downstream consumer expectations of comma-decimals
(end-to-end on the shipped `synthetic_report` only covers tags/brands, not
decimals).

<!-- GATES — run before appending; each falsifies a fabricated finding:
     1. every cited path appears in `git ls-files`  ✓ (Appendix A)
     2. the symbol exists in that file, disambiguator token inside it  ✓
     3. no `:<digits>` in any locator  ✓ (locators use ::Symbol)
     4. every "ran" claim has output pasted in a fence below  ✓ -->

| ID | Severity | Anchor | Title |
|---|---|---|---|
| NUM-001 | HIGH | `src/mko_data_cleaner/core/db_service.py::DBWorker.update_index_from_data` | MAX(date) tie makes the per-key representative row nondeterministic |
| NUM-003 | MEDIUM | `src/mko_data_cleaner/core/db_service.py::DBWorker._apply_add` | add_separator interpolated raw into GROUP_CONCAT SQL; a quote breaks ADD |
| NUM-002 | LOW | `src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv` | decimal_comma configured (true) but inert on the all-String read/write path |

#### NUM-001 — [HIGH] MAX(date) tie makes the per-key representative row nondeterministic

**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker.update_index_from_data`
**Disambiguator:** `MAX({self.date_column})`
**Checked:** ran
**Why this band:** the per-key representative row is a derived quantity consumed
as authoritative (its search-column values drive matching → rules → the tags
distributed to every row sharing the key) and its selection is silently
non-reproducible with no error. CRITICAL is not used: no numeric
total/rate/pivot is computed (the derived value is categorical — which brand/tag
set wins), and blast radius is bounded to keys that carry ≥2 rows on the same
date. The phase's HIGH band ("a silent wrong value consumed as authoritative")
fits: the same input yields different outputs depending on physical row order.
**Problem:** `update_index_from_data` (db_service.py:407-412) resolves one row
per `index_column` key via
`SELECT <search cols>, <index>, MAX(<date>) … GROUP BY <index>`. Per SQLite's
own docs, when `MAX(date)` ties across multiple rows for a key, the bare
(non-aggregated) column values are taken from an arbitrary one of the tied rows,
and that choice is storage-order-dependent (and may even differ per bare
column). The repo docs state only "the latest row per key"
(data-flow.md:98-100, architecture.md:116-117, current-behavior.md:168-169)
with no disclosure of ties, so the declared contract over-states
determinism. (The no-date case — arbitrary row by design — is documented as
intended per data-flow.md:101 and is out of scope here.)
**Repro:** insert two rows for key `K1` both dated `2024-01-15` but with
different `brand` (`BrandA`, then `BrandB`); run
`SELECT brand, AdId, MAX(researchDate) FROM data GROUP BY AdId` → representative
`brand = BrandA`. Rebuild the identical data inserting `BrandB` first →
representative `brand = BrandB`. Both rows tie on the max date; SQLite returns a
bare column from whichever tied row its scan lands on, so the same logical input
produces different outputs.
**Impact:** the chosen representative's search-column values feed the LIKE/FTS
match step; the winning DELETE/REPLACE/ADD rules then depend on it, and
`_sync_with_data_table` copies that decision to **all** data rows sharing the
key. For any key with duplicate `(key, date)` rows — common in per-ad daily
Mediascope exports — the entire cleaning outcome (which tags/brands land in
`clean_data`) is silently reproducible-inconsistent across runs/environments,
with no error and no log. The analyst consuming `clean_data` cannot know which
representative won.
**Root cause:** per-key resolution relies on SQLite's bare-column-with-MAX
optimization, which is explicitly *arbitrary on ties*; no deterministic
tie-break (secondary sort key or `rowid` tiebreak inside a subquery/window
function) is applied, and the docs do not document the limitation.
**Fix:** make tie-breaking explicit and stable. Replace the bare-column MAX with
a deterministic selection, e.g.
`ROW_NUMBER() OVER (PARTITION BY index_column ORDER BY date DESC, rowid DESC)`
over the data table, then `INSERT … WHERE rn = 1`; or keep `MAX(date)` but add a
stable secondary key. Target: `update_index_from_data` (db_service.py:407-412).
**Verify:** new test needed: `test_update_index_date_ties_are_deterministic`
(two rows for one key sharing the max date but differing search-column values;
assert the chosen bare columns are stable across repeated runs and equal the
rowid-DESC tiebreak winner). `grep -rn update_index_from_data tests/` → 0 hits,
so no shipped test covers it (cross-ref TST-007).
**Related:** TST-007 (date branch / `update_index_from_data` coverage gap),
`architecture.md` "Per-Key Rule Resolution" representative-row note, OQ-7.

#### NUM-003 — [MEDIUM] add_separator interpolated raw into GROUP_CONCAT SQL; a quote breaks ADD

**Anchor:** `src/mko_data_cleaner/core/db_service.py::DBWorker._apply_add`
**Disambiguator:** `GROUP_CONCAT(value, '{separator}')`
**Checked:** ran
**Why this band:** the ADD tag list is a string-aggregation derived quantity
produced by `GROUP_CONCAT`, and its control input (`add_separator`) is
config-controlled with no validation, so a legitimately-configurable value can
corrupt the aggregation. Failure is fail-stop (an `OperationalError` that aborts
the run) rather than a silent wrong value, and the default separator is safe —
bounding blast radius to analysts who set a quote-bearing `add_separator`.
**Problem:** `_apply_add` (db_service.py:730) emits
`GROUP_CONCAT(value, '{separator}')` where `separator` is the
f-string-interpolated `dict_file_settings.add_separator` (models.py:158, default
`", "`, unconstrained `str`). It is placed inside single quotes with no escaping
or validation; SQLite `GROUP_CONCAT`'s separator cannot be a bound parameter, so
the value must be validated/escaped as a literal.
**Repro:** set `add_separator = "'"` (a single quote) or
`"x'; DROP TABLE data_table;--"`. The generated
`GROUP_CONCAT(value, '\\'')` / `GROUP_CONCAT(value, 'x'; DROP…')` is invalid SQL.
Observed: `OperationalError: unrecognized token: "''') FROM …"` and
`OperationalError: near ";": syntax error`. The default `", "` produces
`fruit_tag, sponsor_tag` (covered by shipped
`test_apply_add_concatenates_tags`).
**Impact:** any quote in `add_separator` silently disables the entire ADD step
— no ADD tags are written and the run aborts with a late SQL error rather than a
clear configuration error at the settings boundary.
**Root cause:** config-controlled SQL-literal input is interpolated, not escaped
or validated; `DataDict.add_separator` declares no constraint.
**Fix:** validate `add_separator` rejects single quotes (raise
`WrongDataSettings`) or escape by doubling quotes (`'`→`''`) before
interpolation. Target: `_apply_add` (db_service.py:730) and/or a validator on
`DataDict.add_separator` (models.py:158).
**Verify:** new test needed: `test_apply_add_rejects_quote_separator` (assert a
quote-bearing `add_separator` raises a documented error or is escaped). No
shipped test exercises a non-default separator.
**Related:** phase 12 (SQL-interpolation posture at the persistence boundary);
`current-behavior.md` ADD semantics.

#### NUM-002 — [LOW] decimal_comma configured (true) but inert on the all-String read/write path

**Anchor:**
`src/mko_data_cleaner/core/csv_service.py::CSVWorker.export_sql_to_csv`
**Disambiguator:** `schema_overrides`
**Checked:** ran
**Why this band:** `decimal_comma` is `true` on both read
(`PolarsReadCSV`, models.py:99/102) and write (`PolarsWriteCSV`,
models.py:88/99) per the shipped `app_config.json5` (json5:31,43), and docs say
the writer governs numeric formatting (data-flow.md:124). But the read path sets
`infer_schema_length=false` (→ all columns `String`) and the export path forces
`schema_overrides={col: pl.Utf8}` (csv_service.py:328,348); Polars only applies
`decimal_comma` to `Float` columns, so on a string-only path values pass through
verbatim — no precision loss, but the locale-rendering contract is unfulfilled
(`1.5` stays `1.5`, never `1,5`). CRITICAL/HIGH need a silently wrong numeric
value; none is produced, so LOW.
**Problem:** the configured `decimal_comma:true` never exercises.
**Repro:**
(1) reader:
`pl.read_csv(io.BytesIO(b"x;y\n1,5;2,3\n"), separator=";", decimal_comma=True, infer_schema_length=False)`
→ schema `Schema({'x': String, 'y': String})`, `x = ['1,5']` (decimal never
parsed);
(2) writer:
`pl.DataFrame({"x":["1,5","2,3"]}, schema={"x":pl.String}).write_csv(separator=";", decimal_comma=True)`
→ `'x\n1,5\n2,3\n'` (verbatim);
(3) export path: `schema_overrides = {col: pl.Utf8 for col in columns}`
(csv_service.py:328) then `df.write_csv(file_path, **params)` (csv_service.py:364).
**Impact:** the locale contract is silently unfulfilled; a downstream consumer
trusting `decimal_comma:true` (e.g. Excel set to comma decimals) receives
dot-decimals, while source comma-decimals pass through unparsed. No value is
corrupted, but the configured locale cannot be relied upon.
**Root cause:** the all-text storage model (SQLite `TEXT` columns +
`schema_overrides` `Utf8` + `infer_schema_length=false`) routes every value
through `String`, where `decimal_comma` is a no-op; config/docs were not
co-designed with the text-only path.
**Fix (pick one):** (a) parse only expected-numeric columns with
`decimal_comma` active (finite `infer_schema_length` + cast), so the writer's
`decimal_comma` then takes effect; or (b) document that `decimal_comma` is inert
for this tool (values are re-exported verbatim) and stop promising locale
rendering. Target: `export_sql_to_csv` (csv_service.py:328-348) +
`PolarsReadCSV`/`PolarsWriteCSV` (models.py:88-102).
**Verify:** new test needed: `test_decimal_comma_roundtrip` (run a
numeric-looking value through `export_sql_to_csv` with `decimal_comma=true` and
assert the contract is either enforced or explicitly verbatim). No shipped test
asserts `decimal_comma` end-to-end.
**Related:** `data-flow.md` "Provenance and Preservation"; phase 16
(rendered surface).

## Fix Order

- **Dependencies:** None. The three touch disjoint symbols
  (`update_index_from_data`, `_apply_add`, `export_sql_to_csv`).
- **Merge candidates:** None share a root cause. NUM-002 and NUM-003 both sit in
  the rule-aggregation/export string-handling neighborhood but are distinct.
- **Conflicting evidence:** None.
- **Cross-phase conflicts:** NUM-001's "no test on `update_index_from_data`" is
  consistent with TST-007 (phase 11). NUM-003's SQL-interpolation angle should be
  filed under phase 12 (persistence-boundary injection) in addition — not a
  conflict.
- **Rollout risk:**
  - NUM-001: switching to an explicit tie-break changes the representative for
    date-tied keys — outputs *may* differ for ties only. Gate: re-run the
    `synthetic_report` e2e (expect rows_loaded==3, rows_exported==2) and assert
    the no-tie case is unchanged.
  - NUM-003: validation/escaping is additive (default `", "` unaffected). Gate:
    `test_apply_add_concatenates_tags` stays green.
  - NUM-002: option (b) is doc-only (no behavior change); option (a) could
    change how numeric-looking values are read — gate on existing CSV round-trip
    tests + a new decimal test.

## Appendices

### Appendix A — Tooling baseline + cited-path gate

```text
$ git ls-files src/mko_data_cleaner/core/db_service.py src/mko_data_cleaner/core/csv_service.py src/mko_data_cleaner/core/models.py
src/mko_data_cleaner/core/csv_service.py
src/mko_data_cleaner/core/db_service.py
src/mko_data_cleaner/core/models.py

$ uv run ruff check src/ tests/
All checks passed!

$ uv run pytest -q
93 passed in 3.59s

$ uv run basedpyright src/
39 errors, 0 warnings, 0 notes
```

### Appendix B — Evidence: NUM-001 tie nondeterminism (ran)

```text
== NUM-001: MAX(date) tie -> bare columns from an arbitrary tied row ==
  insert A-then-B (A first): representative brand = BrandA  ...
  insert B-then-A (B first): representative brand = BrandB  ...
```
*(supports: identical logical data yields different representative rows; both
rows tie at `2024-01-15`, so either pick is valid — the choice is not
guaranteed/stable.)*

### Appendix C — Evidence: NUM-001 SQLite bare-column-on-tie contract

Quote (SQLite `lang_select.html` / forum):
> "If the same minimum or maximum value occurs on two or more rows, then bare
> values might be selected from any of those rows. The choice is arbitrary."

And:
> "The choice of which min() or max() aggregate determines the selection of bare
> column values is arbitrary. The choice might be different for different bare
> columns within the same query."

*(supports: tie → arbitrary bare-column selection, not "the latest row.")*

### Appendix D — Evidence: NUM-003 separator injection (ran)

```text
  default separator ', ' -> None            (empty tags table in this repro branch; shipped test asserts 'fruit_tag, sponsor_tag')
  separator "'": OperationalError: unrecognized token: "''') FROM (SELECT value FROM data_table..."
  separator "x'; DROP TABLE data_table;--": OperationalError: near ";": syntax error
```
*(supports: any quote in `add_separator` produces invalid SQL and aborts ADD.)*

### Appendix E — Evidence: NUM-002 decimal_comma inertness (ran)

```text
write decimal_comma=True on Utf8 column: 'x\n1,5\n2,3\n'
read infer_schema_length=False schema: Schema({'x': String, 'y': String}) vals: ['1,5']
```
*(supports: `decimal_comma=true` is accepted but emits/avoids no decimal
transformation on the String path; `1.5` would pass through as `1.5`.)*

### Appendix F — Block 4: multi-implementation inventory (not filed — single implementation)

Each derived quantity has exactly one implementation; no divergence to verify:
- representative row (per-key, latest by `MAX(date)`) — `update_index_from_data` (db_service.py:400-416) only;
- REPLACE winner (highest `mapping_index`, `rowid` tiebreak) — `_apply_replace`
  (db_service.py:662-689) only;
- ADD tag list (`GROUP_CONCAT DISTINCT … ORDER BY value`) — `_apply_add`
  (db_service.py:691-746) only.

No pandas/polars pivot (`aggfunc`/`pivot`/`groupby` absent from `src/` — grep
returns only the SQLite `GROUP BY`/`MAX` in `db_service.py:412/407`). Phase 10's
only relevant duplication (QLT-003 `AVAILABLE_ACTIONS`) is a dead constant copy
of `ActionType`, not a duplicate derived-quantity computation.
