---
id: open-questions
domain: overview
tags:
  - decisions
  - gaps
  - questions
related:
  - product-overview
  - current-behavior
  - data-flow
  - architecture
  - doc-maintenance-rules
---

## Purpose

Single register for (a) decisions already confirmed by the product owner,
(b) discrepancies between code, documentation and owner intent, (c)
assumptions that still require validation, and (d) open questions for the
product owner. Answers are preserved with their rationale; nothing here is
deleted when resolved.

Owner decisions D1–D11 below come from the analytical specification
[`01_pip_installable_jupyter_pipeline_step_spec.md`](../../.ai/problems/01_pip_installable_jupyter_pipeline_step_spec.md)
(PO interview of 2026-10-14). Discrepancies are numbered **D-** locally in
this register and referenced from the other overview documents. Open
questions are numbered **OQ-**.

## 1. Owner-Confirmed Decisions (status: confirmed)

| # | Decision | Rationale (as recorded) | Status |
|---|---|---|---|
| D1 | First release installs from a pinned git tag (`%pip install "git+…@v0.1.0"`); PyPI is a later phase, TestPyPI as dry-run | Avoids irreversible PyPI publication before verification | Confirmed 2026-10-14 |
| D2 | Missing settings raise `SettingsNotInitializedError` (subclass of `FileNotFoundError`) naming the missing file and remediation; no silent auto-init | Explicit failures beat surprises in a pipeline; `auto_init` may be added later as opt-in only | Confirmed; **implemented** |
| D3 | Configuration location: platformdirs default + optional `settings_dir` parameter on both public functions | Portable across platforms (Windows has no config env var) | Confirmed; **implemented on the run path** (`initialize_settings` does not forward it yet — D-6) |
| D4 | License: BSD-3-Clause with the author's copyright line; SPDX `license` expression replaces the MIT classifier | Legal/business correctness before publishing | Confirmed; **not implemented** (D-8) |
| D5 | `list_settings` is repointed to the user settings directory, not removed | Keeps a diagnostic command working (fixed the broken `reports` path) | Confirmed; **implemented** |
| D6 | No compatibility shim: the internal `core.app_service` import path disappears; README documents the migration | Pre-1.0 SemVer | Confirmed; **implemented in code** (README still stale — D-3) |
| D7 | `process_data` input: directory only; misleading "file not found" wording corrected | Matches the report-folder workflow | Confirmed; **implemented** |
| D8 | Result object scope: paths (report, import/export dirs, db, `null_data` files) + timestamps + clean row counts; no per-rule or non-mapped statistics | Gives the pipeline a contract without over-building | Confirmed; **not implemented** (D-6) |
| D9 | Example notebook under `examples/notebooks/` with a tiny (~8 row) synthetic sample exercising DELETE/REPLACE/ADD; no 43 MB reference | Demonstrates the workflow without repo bloat | Confirmed; **not implemented** |
| D10 | CI on push/PR: tests → build → `twine check` → wheel contents → fresh-venv install+import smoke | Catches packaging regressions (the original crash would have been caught) | Confirmed; **not implemented** |
| D11 | First release version `0.1.0`, tag `v0.1.0`; 0.x SemVer conventions | Reproducible install target for the notebook | Confirmed; **not implemented** |

Requirements R1–R10 of the same specification are the derived work items;
their implementation status is tracked in
[product-overview.md](product-overview.md).

### Decisions confirmed during the documentation review (2026-10-15)

These extend D1–D11 (same numbering convention) and answer the open questions
OQ-1…OQ-4 raised by this investigation:

| # | Decision | Rationale | Status |
|---|---|---|---|
| D12 | The base README example must match the base configuration: `term` is read from dictionary column 4, the intermediate column 3 is shown in the example. Column positions are user-specific — `col_indexes` in `app_config.yaml` is the knob; the README documents how to adapt a dictionary without the intermediate column | The owner's own dictionaries carry intermediate data in column 3, but users' dictionaries may not; rather than guessing, the config position is the single source of truth and the example mirrors the shipped default | Confirmed; **README example updated** |
| D13 | Intended semantics of the extra column: it starts empty; REPLACE writes the base value; ADD merges the tags of all matching ADD rules; a sequential REPLACE-then-ADD on one column yields the REPLACE value merged with the ADD tags (`1` + `2` → `1 2`) | Owner's explicit example of the expected sequence | **Confirmed intent; implementation gap** — current ADD overwrites the REPLACE value (verified 2026-10-15), tracked as D-2 |
| D14 | `null_data` is dictionary-improvement feedback only. It is not a data-quality report, a warning, or an error signal | Owner: "только для словаря и не является какой-либо проверкой качества данных" | Confirmed; **documentation updated** |
| D15 | Per-key rule resolution is intended design: rows are grouped by the unique key (`index_column`), rules are applied once per unique key, then extra columns and deletions are joined back to the main table by the key | Owner: explicit performance optimization ("позволяет значительно сократить количество операций") | Confirmed; **architecture re-described** |

## 2. Discrepancies and Gaps (current state vs. documentation/intent)

| # | Discrepancy | Evidence | Impact on product | Status |
|---|---|---|---|---|
| D-1 | ~~README/base-config dictionary layout mismatch~~ — the shipped `app_config.yaml` reads the search **term** from dictionary column index 4 and ignores column 3, while the README's example table showed the term at index 3 | [models.py `DictColumnsIndexes`](../../src/mko_data_cleaner/core/models.py), [settings template](../../src/mko_data_cleaner/settings/app_config.yaml), [README dictionary section](../../README.md); verified at runtime: a README-shaped dictionary searched the wrong column | A dictionary built from the old README never matched anything | **Resolved** — owner decision D12; README example updated to match the base config |
| D-2 | ADD writes the merged tag list into the extra column instead of merging into a value previously written by REPLACE | [`_apply_add`](../../src/mko_data_cleaner/core/db_service.py); runtime-verified 2026-10-15: REPLACE wrote `REPLACED`, two ADD rules produced `TAG_A, TAG_B` — the REPLACE value is lost | Owner-confirmed intent (D13) is REPLACE-then-ADD → `1 2` (merged); the implementation yields `2` (only ADD tags). ADD-on-ADD merging itself works as documented | **Confirmed gap** — intent confirmed, code fix pending (not fixed by this documentation task) |
| D-3 | README documented the removed `core.app_service.app_service` singleton | [README](../../README.md), [app_service.py](../../src/mko_data_cleaner/core/app_service.py) (singleton replaced by `get_app_service`) | `ImportError` for anyone following the README (D6 said README documents the migration) | **Resolved** — section removed; README now shows `process_data` |
| D-4 | `_detect_date_column` docstring claims `DD/MM/YYYY`, `DD.MM.YYYY`, `DD-MM-YYYY` support; the implementation regex matches only `YYYY-MM-DD`-style values | [`_is_date_column` / `DATE_REGEX`](../../src/mko_data_cleaner/core/csv_service.py) | Day-first date columns are never auto-detected | Open (OQ-7) |
| D-5 | User-facing run messages are Russian (`Обработка стартовала …`), while the project rule requires English for all user-visible strings; newer CLI messages are English | [app_service.py `run_report`](../../src/mko_data_cleaner/core/app_service.py), [app.py](../../src/mko_data_cleaner/app.py), [project rule 1 (English Only)](../../.kilo/rules/project.md) | Mixed-language UX; violates the house rule | Open (OQ-10) |
| D-6 | `process_data` returns `None`; the owner-confirmed result object (paths + timings + row counts, D8) is not implemented; `initialize_settings(settings_dir=…)` ignores the parameter | [app.py](../../src/mko_data_cleaner/app.py) (NOTE comment); runtime-verified | A pipeline cannot depend on the run's outputs programmatically | Owner-confirmed target work (spec T4), not started |
| D-7 | ~~Undocumented collapsing~~ — per-key rule resolution is **intended design** | [update_index_from_data](../../src/mko_data_cleaner/core/db_service.py), [sync_with_data_table](../../src/mko_data_cleaner/core/db_service.py); runtime-verified and re-studied 2026-10-15 | Owner-confirmed (D15): rows are grouped by the unique key, rules are applied once per unique key (performance optimization), then results are joined back by the key. Now correctly described in [architecture.md](architecture.md) and [data-flow.md](data-flow.md) | **Resolved — intended behavior, now documented** |
| D-8 | Metadata/hygiene bundle (detailed below) | [pyproject.toml](../../pyproject.toml), [LICENSE.txt](../../LICENSE.txt), [app.py:2](../../src/mko_data_cleaner/app.py), [log_config.yaml](../../src/mko_data_cleaner/settings/log_config.yaml) | Blocks trustworthy publishing; house-rule violations | Owner-confirmed target work (D4, R9) |
| D-9 | The repository's example fixtures use an older dictionary format (`upd` action, term at index 3) that the current code does not accept as-is; `data/raw_data/example_data.csv` is a placeholder containing a sentence | [data/dict/example_dict.csv](../../data/dict/example_dict.csv), [data/raw_data/example_data.csv](../../data/raw_data/example_data.csv) | Misleading for new users; spec F12 records the 43 MB sample is also not the documented shape | Open (OQ-9) |
| D-10 | `_apply_delete` executes the identical DELETE statement twice | [`_apply_delete`](../../src/mko_data_cleaner/core/db_service.py) | Harmless today (second pass matches zero rows), but wasteful and misleading; full description in [`01-apply-delete-double-execution.md`](../../.ai/audit/problems/01-apply-delete-double-execution.md) | Noted defect; **not fixed** (task forbids code changes) |

### D-8 explained (metadata/hygiene items, one by one)

1. **`rich` is imported but not declared as a dependency.** `app.py` executes
   `from rich.console import Console`, but `rich` does not appear in
   `[project].dependencies` ([pyproject.toml](../../pyproject.toml)). Today it
   arrives *transitively* through `typer` (which depends on `rich`). If a
   future `typer` release drops or renames that dependency, importing the
   package breaks — a fragile, indirect coupling. Fix: declare `rich`
   explicitly.
2. **License metadata contradicts the LICENSE file.** `pyproject.toml`
   declares the classifier `License :: OSI Approved :: MIT License`, while
   [LICENSE.txt](../../LICENSE.txt) contains the BSD-3-Clause text — and its
   copyright line names *"Audrey Roy Greenfeld and individual contributors"*,
   a cookiecutter-template author, not this project. Anyone installing the
   package would see "MIT License" in the metadata, which is wrong. Modern
   setuptools (PEP 639) also deprecates classifiers in favor of
   `license = "BSD-3-Clause"` + `license-files`. Owner decision **D4**:
   BSD-3-Clause with the project author's copyright line.
3. **No `__version__`.** The version exists only in `pyproject.toml`
   (`0.0.1`). There is no runtime way to ask the installed package which
   version it is — but the notebook workflow (spec R4/T4) is supposed to
   print the version, and a guard test is supposed to assert it. Fix: expose
   `__version__` from the top-level package (or read
   `importlib.metadata.version("mko_data_cleaner")`).
4. **The top-level `__init__.py` is empty.** Consequently
   `from mko_data_cleaner import process_data` fails; only the longer
   `from mko_data_cleaner.app import process_data` works. Owner-confirmed
   target (R4): top-level re-exports of the two public functions with
   `__all__`.
5. **Stale project name `mko_get_mediascope_data`.** Still present in the
   `app.py` module docstring (line 2) and in `.gitignore` (lines 224–225) —
   leftovers from the abandoned predecessor project. Cosmetic, but
   confusing and against the "remove stale references" requirement (R9).
6. **Stale loggers in `log_config.yaml`.** The logging configuration still
   defines logger entries `tests` and `telebot` (lines 51–60) for modules
   that do not exist in this project — residue from a Telegram-bot project.
   They do no harm, but they pollute the settings template that every user
   copies on `init`.

## 3. Assumptions Requiring Validation

| # | Assumption | Basis | Risk if wrong |
|---|---|---|---|
| INV-1 | The examples under `data/` are stale samples, not the canonical current format | Spec F12/A5; format mismatch with code | Users copy a broken dictionary |
| INV-2 | `index_column` (`AdId`) is present in every real export | Template default; sample columns | Without it the distinct/synchronization path never runs |
| INV-3 | Exports are upper-case by convention, so ASCII-only `COLLATE NOCASE` matching is sufficient | README ("строго UPPERCASE"); sample data | Mixed-case Cyrillic data would silently not match |
| INV-4 | The processing pipeline itself is functionally adequate (only packaging/API work needed) | Spec A1, auditor-aligned | Unknown functional gaps would surface only under real data |
| INV-5 | The 43 MB sample is for manual runs only and will not be shipped | Spec A7/C5 | Repo size / distribution bloat |

Spec assumptions A1–A7 are recorded in
[`01_pip_installable_jupyter_pipeline_step_spec.md`](../../.ai/problems/01_pip_installable_jupyter_pipeline_step_spec.md)
and are not repeated here except where they affect product behavior.

## 4. Open Questions for the Product Owner

The investigation could not answer these from the repository. Each entry
states what the code/docs reveal, what is unclear, why it matters, the
question in plain language, and the alternatives. **No answers have been
assumed**; the questions are ordered by impact.

### OQ-1 — The ignored dictionary column — ANSWERED 2026-10-15 (decision D12)

1. **What the code shows:** the default configuration reads `action` (0),
   `match` (1), `search` (2), `term` (4); column 3 is never read. Columns
   from index 5 onward become the analyst output columns.
2. **What is unclear:** what column 3 is meant to hold (the legacy example
   dictionaries have a `value` column there), and whether the README's table
   (term at index 3) or the shipped configuration is authoritative.
3. **Why it matters:** a dictionary built from the README never matches
   anything; a dictionary built for the config contains a mystery column.
   This determines whether the product is usable without trial and error.
4. **Question:** What does the column between "search column index" and
   "term" contain in your working dictionaries, and which layout should the
   documentation describe?
5. **Alternatives:** (a) the config's layout is correct and the README table
   must gain the missing column; (b) the README layout is correct and the
   default `term` index should become 3; (c) both layouts exist in practice
   and the tool should detect which one it received.

**Answer (2026-10-15, owner):** "в реальных данных, как в конфиге" — the
owner's working dictionaries follow the config (term at index 4; column 3
holds intermediate data). The intermediate column is specific to the owner's
data and may not exist for users, so the owner was unsure how to document
it, then decided: **"Давай поменяем базовый пример"** — the base README
example must match the base configuration. **Rationale:** rather than hiding
the difference, the example mirrors the shipped default and the README
documents the `col_indexes` knob, so users whose dictionaries lack the
intermediate column know exactly what to change. **Action taken:** README
dictionary table + CSV examples updated to include the column at index 3;
a note explains that `term` position is configurable.

### OQ-2 — What ADD should do to an existing value — ANSWERED 2026-10-15 (decision D13)

1. **Code shows:** ADD writes the merged, sorted, de-duplicated list of all
   matching ADD values into the extra column, **replacing** whatever was
   there (including REPLACE results or source data).
2. **Unclear:** whether the intended behavior is append-to-existing (as the
   README states) or replace-with-tag-list (as implemented), and what should
   happen when REPLACE and ADD both write the same column for one row.
3. **Why it matters:** analysts combine a "clean value" (REPLACE) with
   "extra tags" (ADD); the current behavior silently drops one of them.
4. **Question:** When one row gets both a replaced value and added tags in
   the same column, what should the final cell contain?
5. **Alternatives:** (a) always merge old value + new tags; (b) replace (keep
   current behavior) and document it; (c) forbid both actions on one column.

**Answer (2026-10-15, owner):** The extra column is created empty and filled
by sequentially adding tags; the result "looks like a merge" (in Russian
there is no distinction between merge and add) — the documentation should say
the tags are **merged**. For a row receiving both a REPLACE value and ADD
tags, the owner's stated expectation is the strict sequence: REPLACE first
("все предыдущие значения заменятся"), then ADD — "r = 1, a = 2 → итог 12".

**Verification result (reported back to the owner):** the implementation
does **not** produce `12`. Runtime-verified 2026-10-15: with
`r`→`REPLACED` and two `a` rules on the same row/column, the exported cell
contains `TAG_A, TAG_B` — the REPLACE value is lost
([`_apply_add`](../../src/mko_data_cleaner/core/db_service.py) writes the
`GROUP_CONCAT` of the ADD tags, not a merge with the cell's previous
content). ADD-on-ADD merging (two `a` rules → `TAG_A, TAG_B`) works exactly
as documented. **Status:** owner intent confirmed (D13); the REPLACE→ADD
sequence is a confirmed implementation gap (D-2). Not fixed in this
documentation task; the README now carries an explicit limitation note.

### OQ-3 — Purpose and scope of the non-mapped (`null_data`) output — ANSWERED 2026-10-15 (decision D14)

1. **Code shows:** distinct combinations of search columns + extra columns
   where any extra column is empty, exported to the report root; empty ⇒ no
   file.
2. **Unclear:** whether it is only a dictionary-improvement loop, or also a
   data-quality report for the analyst; whether rows matched by some rules
   but not others should be distinguishable.
3. **Why it matters:** it defines what "the tool worked correctly" means for
   a run and what the pipeline should do with unmatched rows.
4. **Question:** What is `null_data` for — improving the dictionary, or
   reporting rows that still need attention? Should the run treat unmatched
   rows as an error, a warning, or normal output?
5. **Alternatives:** (a) dictionary feedback only (current); (b) also a
   data-quality report with counts per column; (c) configurable policy
   (warn/fail).

**Answer (2026-10-15, owner):** "Это только для словаря и не является
какой-либо проверкой качества данных" — `null_data` exists **only** for
dictionary improvement; it is not a data-quality check, warning, or error
signal. **Rationale:** keeps the product's meaning of "successful run"
simple: unmatched rows are normal output of an iteratively grown dictionary,
never a failure. **Action taken:** purpose wording updated in
[current-behavior.md](current-behavior.md), [data-flow.md](data-flow.md) and
[product-overview.md](product-overview.md).

### OQ-4 — Per-advertisement collapsing via `index_column` — ANSWERED 2026-10-15 (decision D15)

1. **Code shows:** with `index_column` configured, rules match and apply on
   one distinct row per ad id (`MAX(date)` for the date column), then results
   are synchronized back to all data rows with that ad id.
2. **Unclear:** whether this is intended product behavior (one decision per
   advertisement) or an implementation shortcut; what should happen when two
   rows share an ad id but differ in a searched column.
3. **Why it matters:** it decides whether rule results are per row or per
   advertisement, which changes the meaning of every output.
4. **Question:** Should the cleaning decision be made once per advertisement
   (current behavior) or independently per row?
5. **Alternatives:** (a) per advertisement (current); (b) per row; (c) per
   advertisement but raise a warning when rows with the same ad id disagree.

**Answer (2026-10-15, owner):** per-key resolution is **intended**. The
owner's description of the mechanism: "Там есть специальный уникальный ключ.
Если он задан, то применение идет по ключу. Т.е. сначала данные группируются
по ключу, а затем для каждого уникального ключа идет их очистка. А потом уже
снова Join в основную таблицу по ключу. Это позволяет значительно сократить
количество операций." The owner also asked for the architecture to be
re-studied and described as correctly as possible.

**Action taken:** the mechanism was re-verified in code and re-described
correctly in [architecture.md](architecture.md) ("Per-key rule resolution
(intended design)") and [data-flow.md](data-flow.md) (transformation
pipeline). The verified mechanics: `update_index_from_data` builds
`data_table_distinct` with
`SELECT search cols, index col, MAX(date) … GROUP BY index col` — in SQLite,
bare columns take their values from the row holding the `MAX`, i.e. the
latest row per key (an arbitrary row when no date column is configured);
matching and rule application then run once per unique key against that
table; `sync_with_data_table` copies the resulting extra columns back to all
data rows with the same key and deletes data rows whose key was deleted. The
defensive INSERT branch of `_sync_tables` cannot fire in the standard flow
(distinct is a subset of the data keys) and is documented as such.

### OQ-5 — Should rules ever rewrite original source columns?

1. **Code shows:** REPLACE/ADD only ever write the analyst-defined extra
   columns; source columns are never modified.
2. **Unclear:** whether analysts need to correct source values (e.g. fix a
   misspelled brand in place) or whether "add new columns only" is the
   intended product rule.
3. **Why it matters:** shapes the dictionary's purpose and the export's
   trustworthiness (original values preserved vs. corrected).
4. **Question:** Should the tool be allowed to change original Mediascope
   values, or must originals always stay untouched with cleaning happening
   only in added columns?
5. **Alternatives:** (a) extra columns only (current); (b) allow REPLACE on
   any column including source columns; (c) keep source columns plus a
   "corrections applied" flag column.

### OQ-6 — Standalone tool or pipeline step (follow-ups to D1)

1. **Confirmed:** the product should be usable as one step of a Jupyter
   pipeline (D1, R4).
2. **Unclear:** whether the report-directory layout (`raw_data/`,
   `dict/merged_dictionary.csv`) remains the only supported interface, or
   the pipeline eventually needs explicit input/output paths and multiple
   sequential cleaning passes.
3. **Why it matters:** determines the shape of the public API after the
   result object lands (D8) and whether a second pass can reuse the first
   pass output as input.
4. **Question:** Is the report-directory layout the long-term interface for
   the pipeline step, or do you foresee needing explicit input/output
   locations and chained passes?
5. **Alternatives:** (a) report directory stays the contract; (b) add
   optional explicit paths; (c) support chaining passes over the same
   dictionary.

### OQ-7 — Case and date-format expectations in real exports

1. **Code shows:** LIKE matching is upper-case with ASCII-only
   `COLLATE NOCASE`; date auto-detection recognizes only `YYYY-MM-DD`-shaped
   values (despite the docstring's day-first claim).
2. **Unclear:** whether real exports are guaranteed upper-case and
   ISO-formatted, and what should happen for mixed-case Cyrillic values or
   day-first dates.
3. **Why it matters:** silent non-matching is the worst failure mode for a
   cleaning tool; unrecognized dates lose the date column entirely.
4. **Question:** Are your Mediascope exports always upper-case with
   ISO-style dates, or should matching and date detection handle other
   forms?
5. **Alternatives:** (a) keep upper-case-only matching and document the
   requirement; (b) add case-insensitive Cyrillic folding; (c) support
   day-first date detection.

### OQ-8 — Dictionary lifecycle and merging

1. **Code shows:** if `merged_dictionary.csv` exists it is used as-is; only
   when it is missing are the dictionary-folder files merged (deduplicated,
   sorted by the literal `search_column_idx` column name).
2. **Unclear:** whether analysts maintain one merged file or fragments, and
   whether a merged file should ever be refreshed automatically.
3. **Why it matters:** determines whether edits to fragment files take
   effect and how dictionary versions are managed.
4. **Question:** Do you maintain one merged dictionary file, or keep
   fragments that should be re-merged on each run?
5. **Alternatives:** (a) merged file is authoritative (current); (b) always
   re-merge from fragments; (c) re-merge only when fragments are newer.

### OQ-9 — Freshness of the shipped examples

1. **Code/docs show:** `data/dict/example_dict.csv` uses an older action
   vocabulary (`upd`) and a different column layout; `data/raw_data/
   example_data.csv` contains a placeholder sentence.
2. **Unclear:** whether the owner wants working, current examples in the
   repository (the confirmed plan produces a synthetic sample for the
   notebook, D9) and what should happen to the old files.
3. **Why it matters:** new users copy the examples first; stale examples
   produce confusing failures.
4. **Question:** Should the old example files be replaced with
   README-shaped ones, or removed once the notebook sample exists?
5. **Alternatives:** (a) replace with current-format examples; (b) delete
   them and point to the notebook; (c) keep as historical reference with a
   README warning.

### OQ-10 — Language of user-facing output

1. **Code shows:** `run_report` prints Russian start/finish messages and a
   synchronization notice; the newer CLI messages are English; the project
   rule requires English-only user-visible strings.
2. **Unclear:** which language the end users of the CLI actually need.
3. **Why it matters:** the CLI is the primary interface for the analyst
   today; changing language affects usability.
4. **Question:** Should all user-facing messages be English, or is the CLI
   Russian-facing for now?
5. **Alternatives:** (a) full English (house rule); (b) keep Russian and
   amend the rule for this repo; (c) bilingual by configuration.

## 5. Investigation Limitations

- The pytest suite (35 tests) was run locally on 2026-10-15 and passed; tests
  cover the settings gate, CLI wiring, dictionary pattern building, DB table/
  index/FTS creation and export naming — not the full rule engine end-to-end.
- End-to-end verification used small synthetic reports (2–5 rows) in scratch
  directories that were removed afterwards. Verified runs covered: DELETE,
  REPLACE, ADD (full and partial match), ADD-on-ADD merging, the
  REPLACE-then-ADD sequence on one column (gap D-2), non-mapped export,
  synchronization, cleanup, the settings gate, and the ragged-CSV abort.
- **Not executed:** the 43 MB real sample in `data/example/` (its dictionary
  uses an older, incompatible format — see D-9), any FTS5 rule path, and any
  gzip input path. These are documented from code reading only.
- The per-key (`index_column`) architecture was re-studied in code on
  2026-10-15 and re-described per owner decision D15.
- The review questions OQ-1…OQ-4 were answered by the owner on 2026-10-15 and
  are recorded as decisions D12–D15 in §1; the remaining questions OQ-5…OQ-10
  are still **open** and no answers have been assumed for them.
- No application code, configuration, dependency or test file was modified by
  the investigation; changes were limited to documentation (this register,
  the other overview docs, README) and the new audit problem note
  ([`01-apply-delete-double-execution.md`](../../.ai/audit/problems/01-apply-delete-double-execution.md)).
