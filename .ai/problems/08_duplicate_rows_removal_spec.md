---
id: duplicate-rows-removal
domain: problem
tags:
  - specification
  - deduplication
  - data-quality
  - cli
related:
  - current-behavior
  - architecture
  - data-flow
  - open-questions
---

# Duplicate Rows Removal — Analytical Specification

**Status:** ready for planning
**Source request:** [P01.md](P01.md)
**Owner interview:** 2026-11-20
**Research date:** 2026-11-20
**Supersedes:** the "separate CLI command" wording of `P01.md` (see [D16](#product-owner-decisions))

---

## 1. Problem Statement

Mediascope exports delivered into `raw_data/` contain rows that are **identical across
every column**. They arise from overlapping export windows, repeated manual exports, and
concatenated portal downloads. Today every such row is imported into SQLite, participates
in rule resolution, and is written to `clean_data/`.

Consequences for the analyst:

- inflated row counts in the cleaned dataset;
- duplicated facts in downstream reports;
- manual de-duplication outside the tool, which is not reproducible and not documented.

The tool already has a *per-key* mechanism (`index_column`, default `AdId`) but it only
collapses rows **for rule resolution**; it never removes rows from the exported data
([architecture.md](../00-overview/architecture.md), "Per-Key Rule Resolution"). Exact
duplicate rows therefore survive into the output.

**Goal.** Provide an optional, deterministic, pre-rule de-duplication step that keeps one
row per identical group, controlled by `app_config.json5`, overridable from the CLI and
from the public Python API, and reported through explicit row-count metrics.

---

## 2. Scope

### In scope

- A boolean `deduplicate` setting in the Pydantic settings model and in the shipped
  `app_config.json5` template.
- A `--deduplicate` flag on the existing `run` CLI command.
- A `deduplicate: bool | None = None` override on `process_data(...)`.
- A global de-duplication operation over all raw files, executed before cleaning rules.
- Three row-count metrics on `ReportResult`.
- Unit, end-to-end and precedence tests.
- Documentation updates in `README.md` and `docs/00-overview/*`.

### Out of scope

See [§12](#12-out-of-scope).

---

## 3. Confirmed Requirements

### 3.1 Functional requirements

| ID | Requirement | Acceptance criterion |
|---|---|---|
| **FR-1** | A duplicate is two or more rows equal across **all source columns** (every column present in the raw CSV, after header cleaning, before extra columns are added). | Test D1: two identical rows collapse to one. |
| **FR-2** | Equality is **exact Polars value equality**: case-sensitive, whitespace-sensitive, `null` is not equal to `""`. | Tests D11a–D11c. |
| **FR-3** | De-duplication runs **after the raw rows are read and before any cleaning rule** (DELETE/REPLACE/ADD), and **globally across every file** in `raw_data/`. | Test D2 (two files) and D3 (chunk boundary). |
| **FR-4** | Exactly one row per identical group survives; all other copies are removed. | Test D5 (all identical → one row). |
| **FR-5** | The surviving row is the **first occurrence in input order**; input order is the sorted file order followed by row order inside each file. | Test D12; `self.data_files` is sorted deterministically. |
| **FR-6** | Raw input files are **never modified**; de-duplication is in-memory only. | Test D16 (byte-compare before/after). |
| **FR-7** | The mechanism is Polars' built-in `DataFrame.unique()` / `LazyFrame.unique()` with `keep="first"` and `maintain_order=True`. | Code review + tests D1/D12. |
| **FR-8** | New setting `deduplicate: bool = False` at the **top level** of `DataSettings`, sibling of `clear_export_folder` and `force`. | Model + template + test T1-T2. |
| **FR-9** | Existing user configs that lack the key continue to work and resolve to `false`. | Test T1-4 (legacy config). |
| **FR-10** | CLI flag `--deduplicate` on `run`; absent → config value; present → `true`. There is **no** `--no-deduplicate`. | Tests D7–D9, D17. |
| **FR-11** | `process_data(..., deduplicate=None)`; `None` → config, `True`/`False` → explicit override. | Tests D10a–D10d. |
| **FR-12** | `ReportResult` gains `rows_removed_duplicates` and `rows_resulted`; `rows_resulted = rows_loaded − rows_removed_duplicates`. | Test D18 in every scenario. |
| **FR-13** | `rows_loaded` keeps its current meaning: **raw rows read before de-duplication**. | Test D1: `rows_loaded == 4` with one duplicate removed. |
| **FR-14** | Cleaning-rule semantics are unchanged by de-duplication. | Test D14a–D14c. |
| **FR-15** | The non-mapped (`null_data`) export is derived from post-de-duplication rows; no separate handling. | Test D15a–D15b. |
| **FR-16** | When de-duplication is disabled, pipeline behaviour is **byte-identical** to today. | Existing suite unchanged; test D4/D17. |
| **FR-17** | All new user-visible strings, log messages and docstrings are English. | Code review. |

### 3.2 Non-functional requirements

| ID | Requirement | Target / evidence |
|---|---|---|
| **NFR-1** | De-duplication of 2 000 000 realistic rows (10 text columns, ~2.5 % duplicates) completes in **< 5 s** including the SQLite write. | Measured 1.77 s. |
| **NFR-2** | Peak RSS for the same workload stays **< 600 MB**. | Measured 214 MB. |
| **NFR-3** | No per-row Python loops in the de-duplication path. | Code review. |
| **NFR-4** | Deterministic output: repeated runs on the same input produce the same row order. | Test D12. |
| **NFR-5** | Full test suite runtime stays **≤ 20 s**. | Measured baseline 10–13 s; new tests ≤ 1 s. |
| **NFR-6** | `ruff check`, `ruff format --check`, `basedpyright` clean. | Commands in §13. |

---

## 4. Product Owner Decisions

Numbering continues the register in
[open-questions.md](../00-overview/open-questions.md) (last decision D15).

| ID | Decision | Rationale | Status |
|---|---|---|---|
| **D16** | **CLI shape:** a single `--deduplicate` flag on the existing `run` command. There is **no** separate CLI command and **no** `--no-deduplicate` counterpart. This **supersedes** the "отдельная cli команда" wording in `P01.md`. | PO decision 2026-11-20: "just flag `--deduplicate`". Keeps one command surface and matches the existing `--force` pattern. | Confirmed |
| **D17** | **Stage:** de-duplication runs after raw load and **before** cleaning rules. | PO answer Q1=A. Cheapest point; identical raw rows necessarily receive identical rule results, so output is unchanged relative to post-rule dedup for exact duplicates. | Confirmed |
| **D18** | **Definition:** rows identical across **all** columns; no "keep-strategy" ambiguity because only fully identical rows are affected. | PO answers Q2=A, Q3 ("must be identical"). | Confirmed |
| **D19** | **Default:** `deduplicate: false`. | PO answer Q4=A. Silent row removal must be opt-in. | Confirmed |
| **D20** | **Configuration shape:** one top-level boolean, no nested section, no `StrEnum`. | PO answer Q5=A; a boolean is not a fixed value set (project rule 10 does not apply). | Confirmed |
| **D21** | **Global scope, non-destructive:** de-duplicate across all `raw_data/` files; never rewrite raw files. | PO answer Q8=A. | Confirmed |
| **D22** | **Mechanism:** Polars built-in `.unique()` with `keep="first"`, `maintain_order=True`. | PO answer Q9. Implemented as one global lazy plan over the sorted file list, not per chunk. | Confirmed |
| **D23** | **Metrics:** `rows_loaded` (raw, as today), `rows_removed_duplicates`, `rows_resulted = rows_loaded − rows_removed_duplicates`. | PO answer Q7. | Confirmed |
| **D24** | **Scale target:** hundreds of thousands to a few million rows per report. | PO answer Q10=B. | Confirmed |
| **D25** | **CLI feedback:** the existing summary line is preserved and, when de-duplication is active, a `… · N duplicate(s) removed` segment is appended. | PO answer Q7 requires visible feedback; appending preserves the existing `"3 loaded → 2 clean"` substring asserted by tests. | Confirmed |
| **D26** | **`timings` contract unchanged:** no new `timings` key; de-duplication time is included in `timings["import"]`. | Avoids churn on the asserted `{import, mapping, export}` key set; de-duplication is part of loading. | Confirmed (engineering) |
| **D27** | **Determinism:** `CSVWorker.data_files` is sorted once at discovery, for both the de-duplication and the non-de-duplication path. | `Path.glob` order is filesystem-dependent; "first occurrence" must not be. | Confirmed (engineering) |

---

## 5. Conceptual Development Tasks

Each task is atomic and independently reviewable. "Owner" is the agent role that should
execute it.

### T1 — Settings model and template

**Purpose.** Declare the new setting and ship its default.

**Changes.**
- `src/mko_data_cleaner/core/models.py`: add `deduplicate: bool = False` to
  `DataSettings` after `force` (`models.py:178`).
- `src/mko_data_cleaner/settings/app_config.json5`: add `"deduplicate": false` after
  `"force": false` (line 49), preserving CRLF and the absence of a trailing newline.
- No `model_config`, validator, or enum is required.

**Acceptance criteria.**
- `DataSettings(**config_without_key).deduplicate is False`.
- `DataSettings(**config_with_true).deduplicate is True`.
- Shipped template parses and yields `deduplicate is False`.
- Unknown-key tolerance is unchanged (`extra="ignore"`).

**Owner.** Implementor. **Depends on.** none.

### T2 — Deterministic file ordering

**Purpose.** Make "first occurrence" reproducible.

**Changes.** `src/mko_data_cleaner/core/csv_service.py`, `_set_files_params`
(`csv_service.py:60-69`): sort `self.data_files` after discovery.

**Acceptance criteria.**
- Two files discovered in reverse creation order are processed in sorted path order.
- Existing behaviour is unchanged apart from ordering.

**Owner.** Implementor. **Depends on.** none.

### T3 — Global de-duplication primitive

**Purpose.** Add one method that reads all raw files, de-duplicates globally and returns
the frame plus counts.

**Changes.** `src/mko_data_cleaner/core/csv_service.py`:
```python
def load_deduplicated(self, col_names: list[str]) -> tuple[pl.DataFrame, int, int]:
    """Read all raw files, drop exact duplicate rows, return (df, rows_read, rows_removed)."""
```
Implementation shape (research R1, Approach 1):
- one `pl.scan_csv(self.data_files, new_columns=col_names, **self.reader_settings)`;
- `.unique(subset=None, keep="first", maintain_order=True)`;
- `.collect()`;
- `rows_removed = rows_read - df.height`.

Must handle the empty result (header-only input / zero rows) without calling
`write_database` on an empty frame.

**Acceptance criteria.**
- Two files with one shared row → `rows_removed == 1`.
- Duplicate separated by more than `CHUNK_SIZE` → removed (test D3).
- `null` and `""` remain distinct; comparison is case-sensitive (tests D11a–D11c).
- 1 000 000 rows de-duplicate in < 1 s (primitive-level measurement).

**Owner.** Implementor. **Depends on.** T1, T2.

### T4 — Pipeline integration and metrics

**Purpose.** Wire the primitive into the run and produce the three metrics.

**Changes.**
- `src/mko_data_cleaner/core/app_service.py`
  - `AppService.__init__(..., deduplicate: bool | None = None)` → `self._deduplicate`.
  - `_resolved_deduplicate(self) -> bool` mirroring `_resolved_clear_export_folder`
    (`app_service.py:56-61`).
  - `_import_data(db_worker, csv_worker) -> tuple[int, int]`: when de-duplication is
    active, call `load_deduplicated`, write the frame once via ADBC, commit, then
    `update_index_from_data()`; return `(rows_loaded, rows_removed)`. When inactive,
    keep the existing chunk loop and return `(rows_count, 0)`.
  - `run_report`: `rows_loaded, rows_removed = ...`;
    `rows_resulted = rows_loaded - rows_removed`; pass both new fields to
    `ReportResult`.
  - `get_app_service(..., deduplicate: bool | None = None)` threaded to `AppService`.
- `src/mko_data_cleaner/core/models.py`: `ReportResult` gains
  `rows_removed_duplicates: NonNegativeInt = 0` and
  `rows_resulted: NonNegativeInt = 0`.
- Logging: one `logger.info` line naming removed / loaded / resulted counts.

**Acceptance criteria.**
- `rows_resulted == rows_loaded − rows_removed_duplicates` in every scenario.
- `rows_loaded` equals the raw file row count when de-duplication is on.
- With de-duplication off, `rows_removed_duplicates == 0` and
  `rows_resulted == rows_loaded`.
- `set(result.timings.keys()) == {"import", "mapping", "export"}` is preserved.
- `update_index_from_data` produces the same distinct table with and without
  de-duplication for exact duplicates (structural guarantee; covered by D14).

**Owner.** Implementor. **Depends on.** T3.

### T5 — CLI and public API override

**Purpose.** Expose the override on both entry points.

**Changes.**
- `src/mko_data_cleaner/app.py`
  - `run`: add
    `deduplicate: bool | None = typer.Option(None, "--deduplicate", help="Remove duplicate rows (identical across all columns) before cleaning.")`
    after `force` (`app.py:86-90`); forward to `get_app_service(...)`.
  - `process_data(..., deduplicate: bool | None = None)` with docstring; forward.
  - Summary line: append ` · {rows_removed_duplicates:,} duplicate(s) removed` when
    de-duplication is active (existing substring preserved).
- `src/mko_data_cleaner/__init__.py` re-exports are unchanged.

**Acceptance criteria.**
- `--deduplicate` present → de-duplication on even when config is `false`.
- flag absent + config `false` → off.
- flag absent + config `true` → on.
- `process_data(deduplicate=False)` overrides config `true`.
- `--no-deduplicate` is rejected by Typer (exit code 2) — documented, not a defect.

**Owner.** Implementor. **Depends on.** T4.

### T6 — Test suite

**Purpose.** Prove behaviour, precedence, edge cases and non-regression.

**Changes.**
- `tests/conftest.py`: add a `make_report` factory fixture (files, rows, dict lines);
  do **not** modify `synthetic_report` (its `3/2` contract is a no-dedup regression
  guard).
- `tests/test_csv_service.py`: `load_deduplicated` unit tests (within file, across
  files, null/empty/case semantics, empty input).
- `tests/test_app.py`: end-to-end matrix D1–D18, CLI precedence, `process_data`
  precedence, raw-files-untouched, metrics invariant.
- No `pyproject.toml` marker changes; the scale test is a plain test.

**Acceptance criteria.** All checks in [§13](#13-requirements-traceability-and-dod).

**Owner.** Implementor + Validator. **Depends on.** T4, T5.

### T7 — Documentation

**Purpose.** Keep documentation truthful (project rule 14).

**Changes.**
- `README.md`: `ReportResult` field table, Python example, CLI options, feature step.
- `docs/00-overview/current-behavior.md`: entry-point signatures, main scenario step,
  configuration table, outputs.
- `docs/00-overview/architecture.md`: public interface, sequence diagram, side effects.
- `docs/00-overview/data-flow.md`: transformation pipeline, duplicate-handling table.
- `docs/00-overview/open-questions.md`: record D16–D27.
- `docs/00-overview/product-overview.md`: optional concept + success criteria.

**Acceptance criteria.** Every new config key, flag, parameter and result field appears
in the docs; no stale statement remains; all new text is English.

**Owner.** Doc-specialist. **Depends on.** T1–T5.

---

## 6. Research Summary

Three read-only investigations were run on 2026-11-20. All claims below are backed by
`file:line` citations, executed commands, or measurements from the research reports.

### R1 — Insertion point and algorithm

**Architecture facts.**
- Raw rows enter via `CSVWorker.get_data_chunks` → `_read_csv_in_chunks`
  (`csv_service.py:159-197`), streamed with `collect_batches(chunk_size=10000)`
  (`csv_service.py:28,180`) and inserted per chunk (`app_service.py:236-243`).
- `rows_loaded` is the number of rows handed to SQLite (`app_service.py:232-249`).
- `{data_table}_distinct` is built with `GROUP BY index_column`
  (`db_service.py:409-424`); rules run against `target_table`
  (`db_service.py:101-105`), so exact duplicates collapse to one key regardless.
- `sync_with_data_table` (`db_service.py:426-484`), `build_non_mapped`
  (`db_service.py:805-829`) and `export_sql_to_csv` (`csv_service.py:294-406`) operate
  downstream and require no change.
- Verified experimentally: with and without de-duplication the distinct table is
  identical, therefore rule semantics are unchanged.

**Verified capabilities (polars 2.0.0, installed).**
- `unique(subset=None, *, keep="first", maintain_order=True)`; `subset=None` = all columns.
- `keep="first"` keeps the first input occurrence (verified with distinguishable
  payloads across two files).
- `maintain_order=True` preserves input row order and, contrary to the 2.0.0 docstring,
  still runs on the streaming engine (verified across `streaming` / `in-memory` / `auto`).
- `pl.scan_csv([file1, file2], ...)` produces a single `UNION` plan — genuinely global.
- `null` and `""` are **not** equal; an unquoted empty CSV field is read as `null`
  (`empty_string_is_null=True` by default).
- gzip input, schema-identical multi-file input, and header-only files behave correctly.

**Measurements (fresh process per row, 10 text columns).**

| Approach | Rows in | Rows out | Wall | Peak RSS |
|---|---|---|---|---|
| `collect()` only (no dedup) | 2 040 000 | 2 040 000 | 0.48 s | 384 MB |
| `scan_csv(list).unique(keep="first", maintain_order=True)` | 2 040 000 | 50 000 | 1.11 s | **194 MB** |
| same, `engine="in-memory"` | 2 040 000 | 50 000 | 1.10 s | 953 MB |
| full `read_csv` + `df.unique()` | 1 100 000 | — | 0.78 s | 543 MB |
| per-chunk unique + Python seen-set + ADBC | 1 100 000 | — | 5.24 s | 345 MB |
| SQLite `CREATE TABLE AS SELECT DISTINCT` | 1 100 000 | — | 13.24 s | 364 MB |
| `sqlite3` + `SELECT DISTINCT` | 1 100 000 | — | 12.18 s | 385 MB |

End-to-end de-duplication + ADBC write: 2 040 000 rows → 50 000 in **1.77 s / 214 MB**.

**Correctness proof against the naive approach.** Per-chunk `unique()` on a 4-row frame
split 2+2 yielded 3 rows instead of 4 (one duplicate lost). A 25-row frame with a
duplicate straddling a chunk boundary yielded 25 rows (per-chunk) vs 24 (global).

**Recommendation (accepted).** One global `scan_csv` over the sorted file list, single
`.unique(keep="first", maintain_order=True)`, one `collect()`, one `write_database`.
Rejected: per-chunk unique (incorrect), per-chunk + seen-set (slower, more complex),
SQLite `DISTINCT` (violates the `.unique` constraint and is slowest).

**Worst case identified.** Near-unique data (no duplicates) is the memory worst case:
1 000 000 rows × 391 MB → **1414 MB** peak with `maintain_order=True` streaming. This is
inherent to global exact de-duplication (the distinct set must be held). Accepted for
the stated scale; documented as a risk.

### R2 — Configuration, CLI and API conventions

- `DataSettings` (`models.py:170-178`) has no `model_config`; Pydantic's default
  `extra="ignore"` applies, so a new field with a default is backward compatible and old
  configs parse unchanged. Verified at runtime.
- **Critical trap:** because unknown keys are ignored, adding `deduplicate` to
  `app_config.json5` *without* adding the Pydantic field silently does nothing. The
  model change and the template change must land together.
- Existing boolean precedents: `clear_export_folder` (`models.py:177`),
  `force` (`models.py:178`), both `bool = False`.
- Override pattern: `AppService._resolved_clear_export_folder` /
  `_resolved_force` (`app_service.py:56-64`); `get_app_service` threads overrides
  (`app_service.py:493-517`).
- Typer 0.24.1 verified behaviour for `bool | None = typer.Option(None, "--deduplicate")`:
  absent → `None`; present → `True`; `--deduplicate false` → exit 2;
  `--no-deduplicate` → exit 2. This is exactly the required tri-state.
- `ReportResult` consumers that construct the model: `tests/test_app.py:270-282`,
  `301-313`, `537-551`. Defaulting the new fields to `0` avoids breaking them.
- Documentation impact checklist produced for README, `current-behavior.md`,
  `architecture.md`, `data-flow.md`, `open-questions.md`, `product-overview.md`.

### R3 — Test strategy

- Baseline suite: **133 collected, 129 passed, 4 failed, ~10–13 s**. The 4 failures are
  pre-existing and unrelated: `tests/test_notebook.py:14-19` hardcodes
  `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb`, which was renamed to
  `02-clean_data.ipynb` (commit `a1edda3`).
- `CHUNK_SIZE` is patchable via
  `monkeypatch.setattr(CSVWorker, "CHUNK_SIZE", 2)`; verified that chunks become
  `[2, 2, 2, 1]` and that `process_data` results are unchanged at 2 / 5000 / 10000.
- Full-pipeline scale is **not** testable in CI: 20 000 rows ≈ 30 s, 100 000 rows
  > 400 s (superlinear rule engine). The de-duplication primitive itself is cheap:
  1 000 000 rows in 0.148 s.
- Recommended: a dedicated `make_report` factory instead of extending `synthetic_report`
  (which is asserted as `rows_loaded == 3`, `rows_exported == 2` in ~30 places).
- `--strict-markers` is active and no `markers` key exists, so any custom marker would
  break collection. The plan uses no custom marker.

**Research verdict:** actionable, internally consistent, and sufficient to specify.
No follow-up research is required. The only residual uncertainty (memory ceiling for
pathological near-unique multi-million-row inputs) is recorded as a risk, not a blocker.

---

## 7. Assumptions

| ID | Assumption | Basis | Risk if wrong |
|---|---|---|---|
| A1 | `rows_loaded` means raw rows read before de-duplication. | PO: "rows loaded (as now)". | Metrics become meaningless if it silently becomes post-dedup. |
| A2 | Exact Polars equality is the intended definition; `null` and `""` are different values. | PO Q9 ("built-in `.unique`"). | Empty cells spelled differently may or may not collapse. |
| A3 | Input file order may be sorted without changing business behaviour. | Order currently filesystem-dependent; only affects tie-breaking and export row order. | A consumer relying on the old arbitrary order would see a diff. |
| A4 | Analysts run on a machine with enough RAM for the distinct set (scale target ≤ a few million rows). | PO Q10=B; measured 214 MB at 2 M realistic rows. | Near-unique multi-million-row reports may need a DB-side fallback later. |
| A5 | De-duplication is a run-local, in-memory step; raw files remain the source of truth. | PO Q8=A. | Any expectation of in-place file cleaning is unmet. |
| A6 | No new `timings` key is required by downstream consumers. | Existing contract asserted in three tests. | A pipeline expecting `timings["deduplicate"]` would need a follow-up. |
| A7 | The 4 pre-existing `test_notebook.py` failures are unrelated and may remain. | Verified root cause (renamed notebook). | A red suite could mask new failures; Validator must compare against baseline. |

---

## 8. Constraints

| ID | Constraint | Source |
|---|---|---|
| C1 | All code, comments, docstrings, log and error messages must be English. | Project rule 1 |
| C2 | Production code is king: fix or remove conflicting tests, never distort production code. | Project rule 2 |
| C3 | Strict separation of concerns: orchestration in `AppService`, I/O in `CSVWorker`, models in `models.py`. | Project rule 3, architecture |
| C4 | No `print()` for logic output; use `logging`. | Project rule 12 |
| C5 | Pydantic v2 for settings; `StrEnum` only for fixed value sets (not applicable to a boolean). | Project rules 10–11 |
| C6 | Small, focused modules/functions; no new abstraction without justification. | Project rules 5, 7, 15 |
| C7 | No new runtime dependency; Polars ≥2.0.0,<3.0.0 already pinned. | `pyproject.toml:22` |
| C8 | Documentation must be updated in the same change. | Project rule 14 |
| C9 | `app_config.json5` formatting (CRLF, `//` comments, no trailing newline) must be preserved to keep the diff minimal. | `settings/app_config.json5` |
| C10 | The public API is `process_data` / `initialize_settings`; `core/*` stays internal. | D3, architecture |

---

## 9. Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R-1 | **Silent no-op:** `deduplicate` added to the template but not to `DataSettings` → Pydantic ignores it, every test fails with "0 removed". | Medium | High | Land model + template in the same task; add an explicit test `DataSettings(**template).deduplicate is False`. |
| R-2 | **`rows_loaded` double-discounting:** if `_import_data` returns post-dedup rows while `rows_resulted` subtracts again. | Medium | High | `_import_data` returns `(raw_rows_read, removed)` explicitly; test D18 invariant in every scenario. |
| R-3 | **Per-chunk de-duplication:** passes small tests, silently leaves cross-chunk duplicates. | Low (if spec followed) | High | Mandatory chunk-boundary test D3 with `CHUNK_SIZE=2`. |
| R-4 | **Memory blow-up on near-unique data** (1 M rows → ~1.4 GB measured). | Medium | Medium | Document the ceiling; keep `maintain_order=True` streaming; revisit with a DB-side fallback if real reports exceed the target. |
| R-5 | **Nondeterministic "first occurrence"** due to `Path.glob` order. | Medium | Medium | Sort `data_files` (T2); test D12 asserts determinism. |
| R-6 | **Silent rule-semantics change** if de-duplication is inserted after rule application. | Low | High | Placement is fixed pre-rule; tests D14a–D14c compare exports with/without dedup. |
| R-7 | **Docs drift** across six documents. | Medium | Low | Use the R2 checklist in T7; Validator verifies every new identifier appears. |
| R-8 | **Pre-existing red tests** mask new failures. | High | Low | Record the baseline (129 passed / 4 failed) and require no *additional* failures. |
| R-9 | **Polars behaviour change in a future 2.x** (e.g. `maintain_order` disabling streaming). | Low | Medium | Pin `<3.0.0`; add a primitive-level scale test; document the version assumption. |
| R-10 | **`null` vs `""` ambiguity** when empty fields are quoted differently across files. | Medium | Low | Document exact-equality semantics; add tests D11a–D11c; do not normalize. |

---

## 10. Open Questions (non-blocking)

| ID | Question | Working assumption until answered |
|---|---|---|
| OQ-A | Should the CLI show the removed-duplicate count even when it is `0`? | Yes, whenever de-duplication is active (auditable). |
| OQ-B | Should `rows_resulted` be the post-dedup count or the final exported count? | Post-dedup (`loaded − removed`); `rows_exported` remains the final count. |
| OQ-C | Should a near-unique multi-million-row report trigger a warning or a hard failure? | Neither; document the memory characteristic only. |
| OQ-D | Should the project version be bumped for this feature? | Not part of this specification; handled at release time. |
| OQ-E | Should the de-duplicated `null_data` export be treated as a data-quality signal? | No; unchanged from D14 (dictionary feedback only). |

None of these blocks planning or implementation.

---

## 11. Assumptions → Requirements Traceability

| Requirement | Task(s) | Test(s) |
|---|---|---|
| FR-1, FR-2 | T3 | D1, D11a–D11c |
| FR-3, FR-4, FR-5 | T2, T3 | D2, D3, D5, D12 |
| FR-6 | T3, T4 | D16 |
| FR-7 | T3 | Code review, D1, D12 |
| FR-8, FR-9 | T1 | T1-1…T1-4 |
| FR-10, FR-11 | T5 | D7–D10, D17 |
| FR-12, FR-13 | T4 | D18, D1 |
| FR-14 | T3, T4 | D14a–D14c |
| FR-15 | T4 | D15a–D15b |
| FR-16 | T4, T5 | D4, D17, existing suite |
| FR-17 | all | Code review |
| NFR-1, NFR-2 | T3 | Primitive benchmark (manual + bounded unit test) |
| NFR-4 | T2, T3 | D12 |
| NFR-5 | T6 | `uv run pytest -q` |
| NFR-6 | T6 | ruff / basedpyright |

---

## 12. Out of Scope

- A separate CLI command (superseded by D16).
- `--no-deduplicate` or any way to disable a config-enabled de-duplication from the CLI.
- De-duplication on a configurable column subset.
- Fuzzy / normalized matching (trim, case folding, Unicode normalization).
- De-duplication after rule application, or de-duplication of rows that become
  identical only because of REPLACE/ADD.
- Rewriting or cleaning the raw input files in place.
- Changes to the rule engine, the SQLite schema, or the `{data_table}_distinct` design.
- A separate export of removed duplicates.
- A `timings["deduplicate"]` key or per-rule statistics.
- A full-pipeline scale test in CI (manual benchmark only).
- Changes to the 4 pre-existing `test_notebook.py` failures.

---

## 13. Requirements Traceability and Definition of Done

### Test matrix (from R3, condensed)

| ID | Scenario | Expected L / R / Res / E |
|---|---|---|
| D1 | Duplicate within one file | 4 / 1 / 3 / 2 |
| D2 | Duplicate across two files | 4 / 1 / 3 / 2 |
| D3 | Duplicate across a chunk boundary (`CHUNK_SIZE=2`) | 7 / 1 / 6 / 4 |
| D4 | No duplicates | 3 / 0 / 3 / 2 |
| D5 | All rows identical | 3 / 2 / 1 / 1 |
| D6 | Header-only / single row | 0 / 0 / 0 / 0 and 1 / 0 / 1 / 1 |
| D7 | Config `true`, no flag | 4 / 1 / 3 / 2 |
| D8 | Config `false` + `--deduplicate` | 4 / 1 / 3 / 2 |
| D9 | Config `true`, flag absent (fallback) | 4 / 1 / 3 / 2 |
| D10 | `process_data(deduplicate=True/False/None)` × config `true/false` | 4 / 1 / 3 / 2 or 4 / 0 / 4 / 3 |
| D11 | `null` collapses; `""` ≠ `null`; case-sensitive | per scenario |
| D12 | First occurrence kept deterministically | surviving order = first occurrences |
| D13 | Same `index_column`, different other columns | 2 / 0 / 2 / 2 |
| D14 | De-duplication + DELETE / ADD / REPLACE | exact expected export counts |
| D15 | `null_data` derived post-dedup | file present, counts consistent |
| D16 | Raw files untouched | byte-identical |
| D17 | Flag absent + config `false` | 4 / 0 / 4 / 3 |
| D18 | Metric invariant | `Res == L − R` in all scenarios |

### Definition of Done

**Code**
- [ ] `deduplicate: bool = False` on `DataSettings`; template updated; old configs parse.
- [ ] `CSVWorker.load_deduplicated` uses one global `scan_csv` + `.unique(keep="first", maintain_order=True)`.
- [ ] `CSVWorker.data_files` sorted at discovery.
- [ ] `_resolved_deduplicate` implemented; `_import_data` returns `(rows_loaded, rows_removed)`.
- [ ] `--deduplicate` on `run`; `deduplicate` on `process_data`; `get_app_service` threaded.
- [ ] `ReportResult` has `rows_removed_duplicates` and `rows_resulted` (default `0`).
- [ ] Log line emitted; CLI summary extended per D25.
- [ ] No `print()`; all strings English.

**Tests**
- [ ] D1–D18 implemented and passing.
- [ ] Existing suite: **no additional failures** beyond the 4 pre-existing `test_notebook.py` failures.
- [ ] `uv run pytest -q --ignore=tests/test_notebook.py` → all pass.
- [ ] Suite runtime ≤ 20 s.

**Quality gates**
- [ ] `uv run ruff check src/ tests/`
- [ ] `uv run ruff format --check src/ tests/`
- [ ] `uv run basedpyright src/`

**Documentation**
- [ ] README `ReportResult` table, CLI section, Python example, feature description.
- [ ] `current-behavior.md` entry points, scenario, config table, outputs.
- [ ] `architecture.md` interface + sequence diagram.
- [ ] `data-flow.md` pipeline + duplicate-handling row.
- [ ] `open-questions.md` D16–D27 recorded.
- [ ] No stale statement; all new text English.

### Commands

```powershell
uv run pytest -q
uv run pytest -q --ignore=tests/test_notebook.py
uv run pytest -k "dedup or duplicate" -v
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/
uv run basedpyright src/
```

---

## 14. Definition of Ready

This specification is ready for planning when all of the following hold:

- [x] Problem statement and business goal are explicit.
- [x] Scope and non-goals are explicit.
- [x] All gray areas from the initial analysis are resolved by PO decisions D16–D27.
- [x] Confirmed requirements are individually testable (FR-1…FR-17, NFR-1…NFR-6).
- [x] Conceptual tasks T1–T7 have an owner, dependencies and acceptance criteria.
- [x] Research evidence exists for the insertion point, the algorithm, the
      configuration/CLI conventions and the test strategy.
- [x] Risks have mitigations; open questions are non-blocking.
- [x] Definition of Done and verification commands are stated.
- [x] Traceability from requirement → task → test exists (§11).

**Planning status:** ready.
**Implementation must not start before** the PO confirms D16–D27 (already confirmed in
the interview of 2026-11-20) and the planner has sequenced T1–T7.

---

## 15. Related Documents

- [P01.md](P01.md) — original request
- [current-behavior.md](../00-overview/current-behavior.md)
- [architecture.md](../00-overview/architecture.md)
- [data-flow.md](../00-overview/data-flow.md)
- [open-questions.md](../00-overview/open-questions.md)
- [product-overview.md](../00-overview/product-overview.md)