---
phase: "16"
phase_name: "Rendered-Surface & Output Conformance Measurement"
phase_file: ".kilo/commands/audit/phases/16-audit-rendered-surface.md"
report_status: "draft"
code_ref: "b390d04"
scope: >-
  rework assessment of Phase 16 against a synchronous, single-shot,
  local-only CLI + library with no event loop, no API, no network, no
  credentials, and no retry/backoff. The actual export surface is two
  gzip-compressed CSV files: clean-data exports (`data_table_*.csv.gz` to
  `clean_data/`) and null-data exports (`null_data_*.csv.gz` to the report
  root), both produced by `CSVWorker.export_sql_to_csv()` from SQLite tables
  via Polars DataFrames. No YAML error files, no `mode="x"`, no API results.
recommendation: REBASE (reframe, keep concept of output conformance)
---

# Audit Report — Phase 16 Rendered-Surface (Rework Assessment)

## 1. High-Level Architecture Context Relevant to This Phase

`mko_data_cleaner` is a **local, synchronous, single-shot** CLI tool and Python
library that post-processes Mediascope exports received as **files on disk**.
There is **no outbound API, no network, no credentials, no asyncio, no retry/backoff,
and no event-loop bridge** anywhere in the current architecture.

The actual export lifecycle (one in-process call chain
`app.py → AppService.run_report → CSVWorker.export_sql_to_csv`,
`app_service.py:110-290`):

```
report_dir (data_path)
  → CSVWorker.get_dictionary()        # Polars → mapping rules (DELETE/REPLACE/ADD)
  → CSVWorker.check_date_column()     # regex sample-based date detection
  → with DBWorker(...) as db:         # sqlite3 + adbc_driver_sqlite; WAL, disposable
      create_table_with_index()       # data_table + *_distinct index + FTS5 + triggers
      _import_data()                  # Polars read_csv_batched (10k chunks) → ADBC → SQLite
      apply_mapping()                 # DELETE → REPLACE → ADD via generate_rules_blocks
      build_non_mapped()              # rows with NULL extra columns → temp table
      export_sql_to_csv(..., "null_data", base_path)   # → <report_root>/null_data_*.csv.gz
      sync_with_data_table()          # UPDATE/DELETE index↔data
      export_sql_to_csv(..., "data_table", export_path) # → <report_root>/clean_data/data_table_*.csv.gz
  → ReportResult (timestamps, durations, row counts, timings)
  → DBWorker.__exit__ → close() → delete DB files (always, even on error)
```

**Actual export parameters** (from `PolarsWriteCSV` model + `app_config.yaml`,
passed verbatim to Polars `df.write_csv`):

| Parameter | Phase 16 Purpose claims | Reality (code) |
|---|---|---|
| Decimal mark | `decimal=","` (pandas-style string) | `decimal_comma=True` (Polars boolean, `models.py:88`; `csv_service.py:358-361`) |
| Separator | `sep=";"` | `separator=";"` (`app_config.yaml:44`; `models.py:86`; `csv_service.py:358`) |
| Encoding | `encoding="utf-8-sig"` | **No such parameter** in Polars `write_csv` (verified Polars 1.38.1). UTF-8 BOM is controlled by `include_bom=True` (`app_config.yaml:43`), absorbed by `extra="allow"` on `PolarsWriteCSV` (`models.py:84-90`), survives `model_dump()`, passed to Polars. With `compression="gzip"`, the output is binary — the BOM sits inside the gzip stream. |
| Compression | Not mentioned | `compression="gzip"` → `.csv.gz` (`app_config.yaml:48`; `models.py:90`; `csv_service.py:238`) |
| Exclusive create | `mode="x"` (skip existing file) | **No `mode="x"` exists.** Files named via `get_file_name()` with `%Y%m%d-%H%M%S` + `file_index` (`csv_service.py:237-240`). Same-second re-runs produce identical filenames and **overwrite** (no collision guard, `csv_service.py:356-361`). |
| Error serialisation | "Failed tasks serialised to YAML error file" | **No YAML error files, no `TVTask.to_yaml`, no serialised task context.** Errors are Python exceptions (`errors.py`: 5 classes — `NoReportFoundError`, `WrongDataSettings`, `DataValidationError`, `ConfigError`, `SettingsNotInitializedError`). Grep for `to_yaml\|TVTask\|error_file\|yaml_error` in `src/` → **zero matches**. |

**What is absent (per task brief + code-verified):**

- No API results — data originates from local CSV files imported into SQLite,
  then exported from SQLite tables via `sqlite3.Cursor.fetchmany()` →
  `pl.DataFrame(schema_overrides={col: pl.Utf8})` (`csv_service.py:338-346`).
- No `mode="x"` — file existence is handled by timestamped names, not exclusive-create.
- No YAML error serialisation — there is no per-task error file or `TVTask` model.
- No `Decimal` type — all source columns are stored as SQLite `TEXT` and exported
  as text; `decimal_comma=True` is a rendering-only flag on the Polars writer.

**Precedent:** Phase 04 (REBASE) and Phase 13 (REBASE) both established the
reframing pattern for async/API-framed phase specs against the synchronous
codebase. Phase 15 (DELETE) established the deletion precedent for phases whose
entire premise is fictional (authorization surface). Phase 16 falls on the
Phase 10/Phase 13 side — the underlying concern (rendered-output conformance)
is valid and alive, only the *framing* (API results, mode="x", YAML error files,
utf-8-sig) is obsolete.

---

## 2. Per-Block Assessment (1–4)

| # | Current block | Fit | Outdated (what is wrong) | Replacement (sync analog) |
|---|---|---|---|---|
| 1 | CSV schema conformance against declared contract | **Partial** | "API result → DataFrame" (Purpose line 14); no declared output schema contract exists (columns derive from source CSV headers, cleaned via `clean_names`/`make_valid`; no machine-readable manifest). | Audit column-name survival: `source_headers` → `clean_names`/`make_valid` (utils.py:81-103) → SQLite TEXT all (`db_service.py:134-193`) → `export_sql_to_csv` cursor→`pl.DataFrame` with `schema_overrides={col: pl.Utf8}` (`csv_service.py:325, 341-346`). The "declared contract" = source CSV header + dictionary extra-column contract (documented in data-flow.md:38-64), not a versioned schema manifest. |
| 2 | CSV format conformance: separators, decimals, encoding | **Partial** | `decimal=","` (pandas) vs `decimal_comma=True` (Polars boolean); `encoding="utf-8-sig"` (pandas) — no such parameter in Polars `write_csv`; `sep=";"` correct as `separator=";"`. | Audit the real `PolarsWriteCSV` settings: `separator=";"` (`app_config.yaml:44`), `decimal_comma=True` (`app_config.yaml:46`), `compression="gzip"` (`app_config.yaml:48`), `include_bom=True` (`app_config.yaml:43`, absorbed by `extra="allow"` `models.py:84-90`). Check whether `include_bom=True` inside gzip is observable to any consumer; whether a comma-decimal vs period mismatch would mislead downstream. |
| 3 | Output presence & completeness | **Partial** | `mode="x"` does not exist (Purpose line 16-17); files use `%Y%m%d-%H%M%S` + `file_index` (`csv_service.py:237-240`); same-second re-runs **overwrite** (opposite of "silently skipping"). | Replace `mode="x"` with same-second overwrite via second-granular timestamp + `file_index` chunking. Audit: (a) does every successful `export_sql_to_csv` produce a conformant file? (b) Does the empty-table early-return (`return 0`, `csv_service.py:334-336`) write no file and is it reported as success? (c) Is a truncated/partial file (non-atomic write, `csv_service.py:356-361`) indistinguishable from complete? |
| 4 | Error-file conformance & content | **None** | "Failed tasks serialised to YAML error file" (Purpose line 18) — entirely fictional. No `to_yaml`, no `TVTask`, no YAML error files. Grep `to_yaml\|TVTask\|error_file\|yaml_error` in `src/` → **zero matches**. | Repurpose to **crash-safety / non-atomic writes**: audit whether `df.write_csv` writes directly to the final path with no temp+rename (`csv_service.py:356-361`); whether `DBWorker.__exit__` → `close()` deletes DB files unconditionally even on exception (`db_service.py:786-788, 762-781`); whether `ReportResult` carries an error/status field (it does not — `models.py:170-195`). Overlaps with Phase 04 Block 5/6; Phase 16 should own *output-readiness* (is the file safe to consume?), not *cleanup* (Phase 04). |

### Detailed per-block analysis

#### Block 1 — CSV schema conformance against a declared contract

- **Fit: Partial.** The concept of auditing the exported CSV's column set, order,
  and per-field types applies — the exports are real CSV files written from SQLite
  tables. But the "declared output contract" premise is only partially grounded:
  there is **no machine-readable, versioned schema manifest**. Columns are derived
  from the source CSV headers (cleaned via `clean_names`/`make_valid`,
  `utils.py:81-103`) → SQLite `TEXT` for all columns (`db_service.py:134-193`) →
  exported as `pl.DataFrame` with `schema_overrides={col: pl.Utf8}`
  (`csv_service.py:325, 341-346`). The schema is effectively "all text, column
  names match the cleaned source headers + dictionary extra columns."
- **Outdated:** The Purpose (line 14) states "the data pipeline converts each API
  result to a DataFrame" — no API exists. Data flows SQLite cursor →
  `pl.DataFrame` (`csv_service.py:338-346`). The block text itself (lines 38-48)
  does not name the API, but its evidence-seeking ("the contract's discoverability
  and version linkage") finds no versioned contract.
- **Replacement:** Audit column-name survival through the full chain:
  `source_headers` (from `pl.read_csv(n_rows=0)`, `csv_service.py:77-86`) →
  `set_data_tbl_columns` + `clean_names`/`make_valid` (`db_service.py:107-110`,
  `utils.py:73-103`) → SQLite `create_table` with all `TEXT` (`db_service.py:134-193`)
  → `export_sql_to_csv` cursor→`pl.DataFrame` with `schema_overrides={col: pl.Utf8}`
  (`csv_service.py:325, 341-346`). The "declared contract" maps to the source CSV
  header contract + the dictionary's extra-column contract documented in
  `data-flow.md:38-64` (prose, not machine-readable). Check: is a source column
  the dictionary expects silently dropped after cleaning? Are extra columns from
  the dictionary correctly appended? Are entirely empty dictionary columns dropped
  before export (`dict_service.py _drop_empty_columns`)?

#### Block 2 — CSV format conformance: separators, decimals, encoding

- **Fit: Partial.** The concept is fully applicable — the exports are real CSV
  files with a separator, decimal mark, compression, BOM, and encoding. But the
  Purpose (line 16) names wrong parameters.
- **Outdated:**
  - `decimal=","` (pandas parameter name/value) — the actual code uses
    `decimal_comma=True` (a boolean in Polars, `models.py:88`,
    `app_config.yaml:45`, passed at `csv_service.py:358-361`).
  - `encoding="utf-8-sig"` (pandas parameter) — **no such parameter exists** in
    Polars `write_csv` (verified against Polars 1.38.1 signature:
    `write_csv(self, file, *, include_bom, compression, ..., decimal_comma,
    ...)` — there is no `encoding` argument). The UTF-8 BOM is controlled by
    `include_bom=True` (`app_config.yaml:43`), which is **not a declared field** of
    `PolarsWriteCSV` (`models.py:84-90`) — it is absorbed by `extra="allow"` and
    survives `model_dump()` (Phase 05 gap #4). Two implicit contracts are in play.
  - `sep=";"` is correct as `separator=";"` (`models.py:86`, `csv_service.py:358`).
- **Replacement:** Audit the real `PolarsWriteCSV` export settings
  (`app_config.yaml:40-48` + `models.py:84-90`):
  - `separator=";"` — matches source/destination convention
  - `decimal_comma=True` — Polars-native decimal mark (comma)
  - `compression="gzip"` — output is `.csv.gz` (binary); encoding is governed by
    gzip internals, not a CSV `encoding` parameter
  - `include_bom=True` — absorbed by `extra="allow"`; check whether the BOM is
    observable inside the gzip stream and whether a consumer that decompresses and
    parses could be misled by it
  - `include_header=True` — header row present
  - `chunk_size: 100000` — from config, overrides model default `10000`
    (`csv_service.py:312`)
- **Evidence:** the effective format dimensions (separator, decimal mark,
  compression, BOM, header) — declared in config vs. rendered; one mismatch and
  the downstream misinterpretation it enables.

#### Block 3 — Output presence and completeness

- **Fit: Partial.** The concept survives — the question "does every successful task
  produce a conformant output file?" is valid. But the mechanism named (`mode="x"`)
  is entirely fictional and the real risk is **inverted**.
- **Outdated:** The Purpose (lines 16-17) claims `mode="x"` (exclusive create,
  "silently skipping an existing file"). **No `mode="x"` exists** in the codebase.
  Grep for `mode.*=.*['\"]x['\"]` in `src/mko_data_cleaner/` → zero matches. The
  actual file naming is `get_file_name()` (`csv_service.py:237-240`):
  `f"{name_prefix}_{strftime('%Y%m%d-%H%M%S')}_{name_suffix}{ext}"`, with
  `file_index` starting at 1 per run (`csv_service.py:314`). A same-second re-run
  produces identical filenames and **overwrites** the prior file — the opposite of
  "silently skipping." There is no collision guard.
- **Replacement:** Replace `mode="x"` with **same-second overwrite via
  second-granular timestamp + `file_index` chunking**. Audit:
  - Does every successful `export_sql_to_csv` call produce a conformant output
    file? Two calls per run: `null_data` (→ `base_path`, `app_service.py:246-251`)
    and `data_table`/`clean_data` (→ `export_path`, `app_service.py:264-268`).
  - Does the empty-table early-return write no file? `export_sql_to_csv` returns
    `0` for an empty table (`csv_service.py:334-336`) — `df.write_csv` is never
    called, no file is written (`current-behavior.md:293` confirms: "Empty
    non-mapped set → No `null_data` file written").
  - Is a partial/truncated file (from crash) indistinguishable from a complete one?
    `df.write_csv` writes directly to the final path (`csv_service.py:356-361`) —
    non-atomic, no temp+rename.
- **Evidence:** the success-to-output mapping for one run; one same-second
  overwrite; one empty result and its disposition (no file, returns 0).

#### Block 4 — Error-file conformance and content

- **Fit: None.** The concept of auditing a serialised error file does not apply —
  **no error files exist**.
- **Outdated: Entire premise.** "Failed tasks are serialised to a YAML error file"
  (Purpose line 18); "the serialised task context (settings, interval, target,
  error message) is present and correctly formatted" (block text, lines 74-76);
  "the error file's naming is reproducible so a re-run can detect a prior failure"
  (line 77-78). **None of this exists.** Grep for `to_yaml|TVTask|error_file|
  yaml_error` in `src/mko_data_cleaner/` → **zero matches**. `errors.py` defines
  five plain `Exception` subclasses (no serialization method). `ReportResult`
  (`models.py:170-195`) has no `error` or `status` field. The `DBWorker` context
  manager (`db_service.py:786-788`) deletes the DB file unconditionally on exit
  even on exception — so there is no persisted error state to audit.
- **Replacement:** Repurpose to **crash-safety & output-readiness of exported
  files**. The synchronous analog of "error-file conformance" is "crash-safety
  of the write and the recoverability of state":
  - Does `df.write_csv` write directly to the final path (`csv_service.py:356-361`)
    with no temp+rename? A mid-write crash leaves a truncated `.csv.gz` that a
    downstream consumer may read as complete.
  - Does `DBWorker.__exit__` → `close()` → `_delete_base_files` destroy diagnostic
    state even on failure (`db_service.py:786-788, 762-781`)?
  - Does `ReportResult` carry an error/status field (it does not —
    `models.py:170-195`)?
  - Does `run_report` distinguish a full export from a zero-row export
    (`rows_exported == 0` is indistinguishable from a full report —
    `app_service.py:278-290`)?
  - **Ownership note:** Phase 04 Block 5 (crash-safety) and Block 6 (failure mode)
    already own the *write* and *cleanup* questions. Phase 16 (re-based) should own
    the *output-readiness* question: "Is the file on disk safe for a downstream
    consumer to read?" — which requires atomic writes (temp+rename) + a size or
    checksum footer. No other phase currently audits this.

---

## 3. Phase Purpose & Scope Assessment

### Purpose text (lines 11–21) — Assessment: rewrite

**Current text:**

> "Measures the rendered output of this tool — CSV files and serialised error
> files — against a declared surface/output contract. The data pipeline converts
> each API result to a `DataFrame`, prepares columns via a data strategy, then
> exports CSV with locale-specific formatting: `decimal=","`, `sep=";"`,
> `encoding="utf-8-sig"`, and `mode="x"` (exclusive create, silently skipping an
> existing file). Failed tasks are serialised to a YAML error file. This phase
> does not audit the mechanics of how the write happens — that is Phase 04's
> scope — but whether what is written conforms to a declared output contract:
> schema, column types, format, and presence."

**Every substantive claim is wrong:**

| Phase 16 Purpose claim | Reality (code) |
|---|---|
| "serialised error files" | No error files exist — see §2 Block 4 |
| "converts each API result to a DataFrame" | Data flows SQLite cursor → `pl.DataFrame` (`csv_service.py:338-346`); no API |
| `decimal=","` (pandas) | `decimal_comma=True` (Polars boolean, `models.py:88`) |
| `sep=";"` | Correct as `separator=";"` (`models.py:86`) |
| `encoding="utf-8-sig"` (pandas) | No such parameter in Polars `write_csv`; BOM is `include_bom=True` (`app_config.yaml:43`, absorbed by `extra="allow"`) |
| `mode="x"` (exclusive create, skip existing) | No `mode="x"`; timestamped names, same-second **overwrites** (`csv_service.py:237-240`) |
| "Failed tasks serialised to YAML error file" | No YAML serialization, no `TVTask` (`errors.py`: 5 plain exceptions) |

**Rewritten Purpose (recommended):**

> "Measures the rendered CSV output of this tool — `clean_data/data_table_*.csv.gz`
> and `null_data_*.csv.gz` — against the output surface declared in configuration
> and documentation. The data pipeline exports SQLite table contents to gzip-
> compressed CSV via Polars: columns originate from cleaned source CSV headers
> (`clean_names`/`make_valid`), all values are rendered as text (`schema_overrides=
> {col: pl.Utf8}`), and the format is governed by `PolarsWriteCSV` settings
> (`separator=";"`, `decimal_comma=True`, `compression="gzip"`, `include_bom=True`).
> This phase audits whether what is written conforms to the declared surface: schema
> (column set, order, types), format (separator, decimal mark, compression, BOM),
> output presence (every successful export produces a conformant file; empty
> results produce none), and crash-safety (whether a non-atomic write can leave a
> truncated file that is consumed as complete). This phase does not audit the
> mechanics of how the write happens — that is Phase 04's scope — but whether what
> is written is safe and conformant for a downstream consumer."

### Scope boundaries (lines 25–27) — Assessment: update citations

**Current text:**

> "Owned here: rendered-output conformance to a declared surface contract. Owned
> elsewhere: export mechanics, file-existence handling, and error-file location
> (04); template-to-model schema conformance (05); fixed-value enum discipline
> (10); test-suite adequacy (11)."

| Citation | Phase exists? | Status | Action |
|---|---|---|---|
| (04) | Yes | Re-based to sync | **KEEP, reword** — "export mechanics" and "file-existence handling" are valid. Remove "error-file location" (no error files exist). Reword to: "crash-safety / non-atomic writes." The boundary is sharp: Phase 04 owns the *write path* (how the CSV is written, DB cleanup); Phase 16 owns *output-readiness* (is the rendered file safe to consume?). |
| (05) | Yes | Re-based to sync | **KEEP** — `template-to-model schema conformance` is valid; Phase 05 owns config↔Pydantic↔Polars conformance (e.g., `include_bom` absorbed by `extra="allow"`). |
| (10) | Yes | Re-based to sync | **KEEP** — `fixed-value enum discipline` is valid. No async framing. |
| (11) | Yes | Exists (sync) | **KEEP** — `test-suite adequacy` is valid. No async framing. |

**Proposed reworked scope boundary:**

> "Owned here: rendered-output conformance to the declared surface contract — the
> schema, format, presence, and crash-safety-readiness of the exported CSV files.
> Owned elsewhere: export mechanics and crash cleanup (04); template-to-model
> schema conformance (05); fixed-value enum discipline (10); test-suite adequacy
> (11)."

---

## 4. Severity Taxonomy Assessment

**Verdict: Keep the principle; re-ground the examples.**

Phase 16's taxonomy (lines 85–109) opens with the **sound principle** ("Grade by
effect and blast radius, not by mechanism name; rate what is true now, not the
worst consequence if triggered. An empty band is a valid outcome"). This principle
should be **retained verbatim** — it matches Phase 04 §4 and Phase 13 §4.

However, **every illustrative example is framed around the fictional
API/mode="x"/YAML-error-file model**:

- **CRITICAL** (lines 92–96): "rendered output that conforms to no declared
  contract while being consumed as authoritative, so a downstream reader silently
  misreads a value" — **valid concept** for the sync case (a downstream consumer
  of `clean_data/data_table_*.csv.gz` reading a truncated file as complete, or a
  comma-decimal where the consumer expects a period that changes a value's
  magnitude). Keep and re-ground.
- **HIGH** (lines 97–101): "a CSV column whose type or name diverges from the
  declared surface" — valid concept (column-name drift through `clean_names`).
  "a required column missing from the output" — valid. BUT `mode="x" skipping an
  existing file` — **wrong** (no mode="x"); replace with same-second overwrite.
- **MEDIUM** (lines 102–105): "an encoding or line-ending mismatch that is
  locale-dependent and undeclared" — **partially valid** but
  `encoding="utf-8-sig"` doesn't exist; the real dimension is `include_bom`
  (absorbed by `extra="allow"`, a Phase 05 finding). "a format deviation
  (separator, BOM) that a downstream consumer tolerates but should not" — valid.
  "an error file whose structure or fields do not match the declared error
  contract" — **wrong** (no error files).
- **LOW** (lines 106–109): "documentation or comment drift about output format or
  schema" — valid. "an undeclared optional column" — valid. "a non-reproducible
  error-file name" — **wrong** (no error files); replace with "non-reproducible
  filename (second-granular timestamp, `csv_service.py:239`)."

### Recommended re-grounding

| Band | Current (async/API/error-file) example | Reworded (sync) example |
|---|---|---|
| CRITICAL | "locale-format mismatch on the wire" | Keep — "comma-decimal where the consumer expects a period changes a value's magnitude"; add "non-atomic write leaves a truncated `.csv.gz` consumed as complete" |
| HIGH | "mode='x' skipping existing file" | "same-second re-run overwrites prior output (`get_file_name`, `%Y%m%d-%H%M%S`, `csv_service.py:239`) — a stale result is reported as fresh" |
| HIGH | "required column missing" | Keep |
| HIGH | "CSV column type/name diverges" | Keep (column-name drift through `clean_names`) |
| MEDIUM | "error file structure mismatch" | *remove* — no error files; Phase 04 owns failure-mode observability |
| MEDIUM | "encoding/line-ending mismatch" | "BOM inside gzip is unobservable to decompressing consumers (`include_bom=True` absorbed by `extra='allow'`, `app_config.yaml:43`, `models.py:84-90`)" |
| MEDIUM | "format deviation (separator, BOM)" | Keep |
| LOW | "non-reproducible error-file name" | *remove* — no error files; Phase 04 covers reproducibility |
| LOW | "doc/comment drift about output format/schema" | Keep |
| LOW | "undeclared optional column" | Keep |

---

## 5. Report Output Assessment

| Item | Current (Phase 16 §Report Output) | Verdict | Justification |
|---|---|---|---|
| Findings path | `.ai/audit/16-rendered-surface/findings.md` | **Keep** | Matches `.ai/audit/NN-name/findings.md` convention (cf. Phase 04: `.ai/audit/04-data-pipeline/findings.md`, Phase 05: `.ai/audit/05-config-templates/findings.md`). Directory does not yet exist — no prior findings to reconcile. |
| Template | `.ai/audit/templates/audit-findings.md` | **Keep** | Standard findings template. |
| Finding-ID prefix | `OUT-` | **Keep, no collision** | Repo-wide grep for `OUT-` (excluding `.venv*`, `.git*`) returns **1 match**: the Phase 16 phase file itself (`16-audit-rendered-surface.md:117`). Zero `OUT-NNN` finding identifiers in any findings document, test file, or shipped source. Unique. |
| problems-only | `true` (front matter + §119) | **Keep** | All candidate findings are code-verified defects (non-atomic writes, stale docs, same-second overwrite, no output contract). |
| per-pass lines | ≤100, incremental append (§118) | **Keep** | Template constraint. |
| Empty state | `No problems found in this phase.` (§121) | **Keep** | Verbatim. |
| Shipped-test-asserts-current-behaviour | §125: "a shipped test asserting current behaviour is a remediation blocker" | **Keep** | See candidate findings — no test asserts `mode="x"`, `encoding="utf-8-sig"`, or YAML error files (these don't exist in code). The tests that DO exist assert the actual behavior (`test_csv_service.py`, `test_app.py:463-472`) and would need to be updated if the export format changes (e.g., if `include_bom` is corrected — see F3). |

---

## 6. Candidate Findings (with code refs)

These are code-verified starting points the re-based Phase 16 blocks should
develop into findings (with runtime evidence). Severity bands follow Phase 16's
own taxonomy, re-grounded to synchronous reality.

| Finding | Location | Severity | Ownership |
|---|---|---|---|
| **F1. Stale doc: `data-flow.md:178` claims "repeated runs never overwrite previous outputs."** The filename uses `%Y%m%d-%H%M%S` (`csv_service.py:239`) — second-granular. A re-run within the same second produces identical filenames and **overwrites** (`file_name.format(file_index=file_index)`, `csv_service.py:356-361`). No collision guard exists. This is a doc-vs-code divergence with real data-loss implications. | `data-flow.md:178` (doc); `csv_service.py:237-240, 356-361` (code) | HIGH | Phase 16 (output conformance); Phase 04 Block 5 already found the code-side overwrite — Phase 16 owns the doc-vs-rendered divergence. |
| **F2. Non-atomic CSV writes — no temp+rename.** `df.write_csv(file=file_path, **params)` writes directly to the final path (`csv_service.py:356-361`). A mid-write crash (process kill, disk full) leaves a truncated `.csv.gz` that a consumer reads as a complete file. No checksum, no size footer, no atomic rename. | `csv_service.py:356-361` | HIGH | Phase 16 (output-readiness); Phase 04 Block 5 owns the write mechanics. |
| **F3. `include_bom=True` is not a declared `PolarsWriteCSV` field — two implicit contracts.** `include_bom` appears in `app_config.yaml:43` but is absent from the `PolarsWriteCSV` model (`models.py:84-90`); it is absorbed by `extra="allow"` and survives `model_dump()` to reach Polars `write_csv`. With `compression="gzip"`, the BOM is embedded inside the gzip stream — invisible to any consumer that simply decompresses. No code or test verifies the BOM's presence or absence in the rendered output. | `app_config.yaml:43`; `models.py:84-90`; `csv_service.py:358-361` | MEDIUM | Phase 16 (format conformance); Phase 05 records the config-model gap (gap #4). |
| **F4. No declared output schema contract — columns are implicit.** There is no machine-readable, versioned schema manifest for the exported CSV. Column set/order derive from source CSV headers (`csv_service.py:77-86`) → `clean_names`/`make_valid` (`utils.py:73-103`) → SQLite TEXT all (`db_service.py:134-193`). A downstream consumer must reconstruct the schema from `data-flow.md` prose (lines 38-64). No automated check ensures the rendered header matches the declared contract. | `csv_service.py:77-86, 325`; `utils.py:73-103`; `db_service.py:134-193`; `data-flow.md:38-64` | MEDIUM | Phase 16 (schema conformance); Phase 05 (template-to-model) does not cover output-schema discoverability. |
| **F5. Empty-table export silently produces no file but reports success.** `export_sql_to_csv` returns `0` for an empty table (`csv_service.py:334-336`) — `df.write_csv` is never called, no file is written. `run_report` still constructs a `ReportResult` with `rows_exported=0` (`app_service.py:278-290`), indistinguishable from a successful full export. The `null_data` export (non-mapped rows) can legitimately be empty (`current-behavior.md:293`), but the consumer cannot tell. | `csv_service.py:334-336`; `app_service.py:246-251, 264-268, 278-290`; `models.py:170-195` | HIGH | Phase 16 (output presence); Phase 04 Block 6 owns `ReportResult` field design. |
| **F6. No `error`/`status` field on `ReportResult` — failure is silent.** `ReportResult` (`models.py:170-195`) carries `rows_loaded`/`rows_exported` but no `error` or `status` field. A run that fails mid-pipeline raises and never returns (DB is deleted by `__exit__`→`close()`, `db_service.py:786-788`); a run that succeeds with `rows_exported == 0` is indistinguishable from one that produced a full report. The `null_data` export can produce files that a consumer might mistake for errors. | `models.py:170-195`; `app_service.py:278-290`; `db_service.py:786-788` | MEDIUM | Phase 16 (output completeness/crash-safety); Phase 04 Block 6 owns the result-field design. |

> These do not depend on changing any source files. They are the *findings* the
> re-based Phase 16 is built to elicit. F1 and F2 are already partially surfaced by
> Phase 04 (§5 Block 5, §6 Block 6) — Phase 16 should cross-file (cite Phase 04)
> rather than re-derive, and add the *output-readiness* angle Phase 04 does not
> cover (F2: no checksum/integrity footer on the rendered file).

---

## 7. Cross-Phase Citation Inventory

### 7.1 Phases that Phase 16 CITES (scope boundary, lines 25–27)

Phase 16's scope boundary defers to:

| Cited phase | Current citation text | Phase status | Action |
|---|---|---|---|
| (04) | "export mechanics, file-existence handling, and error-file location" | Re-based (sync) | **KEEP, reword** — "export mechanics" and "file-existence handling" are valid. Remove "error-file location" (no error files exist). Reword to: "crash-safety / non-atomic writes." The boundary is sharp: Phase 04 owns the *write path* (how the CSV is written, DB cleanup); Phase 16 owns *output-readiness* (is the rendered file safe to consume?). |
| (05) | "template-to-model schema conformance" | Re-based (sync) | **KEEP** — valid. Phase 05 owns config↔Pydantic↔Polars conformance (e.g., `include_bom` absorbed by `extra="allow"`). |
| (10) | "fixed-value enum discipline" | Re-based (sync) | **KEEP** — valid. No async framing. |
| (11) | "test-suite adequacy" | Exists (sync) | **KEEP** — valid. No async framing. |

### 7.2 Phases that CITE (16) — who references Phase 16

| Phase | Line(s) | Citation text | Phase 16 status | Action |
|---|---|---|---|---|
| 11 | 19 | "What the rendered outcome is, and what a derived quantity equals, are 16's and 17's" | REBASE | **KEEP** — valid. Phase 11 defers rendered-outcome measurement to Phase 16; Phase 17 owns arithmetic. No async framing. |
| 12 | 26 | (does not cite 16) | — | N/A |
| 13 | 18 | "a `data_table` export naming and same-second overwrite" | (reference) | Already documented — Phase 13 references Phase 16's future scope; Phase 04 already covers same-second overwrite code-side. |

**Orchestration files:** `audit-multi-phase.md` enumerates phases by listing the
`phases/` directory, so the phase file itself is always discoverable. No
hard-coded sequence needs editing when Phase 16 is re-based.

**Precedent:** Phase 04 (REBASE) and Phase 13 (REBASE) both established the
reframing pattern for async/API-framed phase specs against the synchronous
codebase. Phase 16 falls on the Phase 10/Phase 13 side — the underlying concerns
(rendered-output conformance, format divergence, crash-safety-readiness) are valid
and live, only the *framing* (API results, mode="x", YAML error files) is
obsolete.

---

## 8. Proposed New Block Set (Re-based, Reframed)

The 4 blocks should be reframed from API/error-file/mode="x" to the synchronous
output-conformance surface. Each maps 1:1 to an original block and preserves its
evidence-seeking intent.

| # | Original block | New title (sync reframing) | Core question |
|---|---|---|---|
| 1 | CSV schema conformance vs declared contract | **CSV schema conformance against the source-column contract** | Does the exported CSV's column set, order, and types match the cleaned source headers + dictionary extra columns? Is there a machine-readable, versioned schema manifest? Column-name survival through `clean_names` → SQLite TEXT → `export_sql_to_csv` cursor→`pl.DataFrame`. |
| 2 | CSV format conformance: separators, decimals, encoding | **CSV format conformance: separator, decimal mark, compression, BOM** | Does the rendered format match the `PolarsWriteCSV` settings? `separator=";"`, `decimal_comma=True`, `compression="gzip"`, `include_bom=True` (absorbed by `extra="allow"`). Is the BOM observable inside gzip? Does a comma-decimal/period mismatch risk downstream misinterpretation? |
| 3 | Output presence & completeness | **Output presence & completeness (same-second overwrite, not mode="x")** | Does every successful export produce a conformant file? Replace `mode="x"` with second-granular timestamp + `file_index` chunking — same-second re-runs **overwrite**. Does the empty-table early-return (`return 0`, `csv_service.py:334-336`) write no file and is it reported as success? Is a partial file indistinguishable from complete? |
| 4 | Error-file conformance | **Crash-safety & output-readiness of exported files** | Does `df.write_csv` write directly to the final path (non-atomic, `csv_service.py:356-361`)? Is there a checksum/integrity marker? Does `DBWorker.__exit__` delete the DB unconditionally on error (`db_service.py:786-788`)? Does `ReportResult` carry an error/status field (no — `models.py:170-195`)? **Ownership:** Phase 04 owns the write/cleanup path; Phase 16 owns whether the rendered file is safe to consume. |

---

## 9. Conclusion — Verdict: REBASE

**REBASE (reframe, don't delete).**

Phase 16's underlying concern — does the rendered CSV output conform to a declared
surface contract (schema, format, presence, crash-safety-readiness)? — is
**valid and alive** for the synchronous, local-only, single-shot codebase. The
exports are real gzip-compressed CSV files (`clean_data/data_table_*.csv.gz` and
`null_data_*.csv.gz`) produced by `CSVWorker.export_sql_to_csv()` from SQLite
tables via Polars DataFrames.

However, **every framing premise in the Purpose (lines 13–18) is entirely wrong**
for this codebase:

1. **No API.** Data does not come from "API results" — it is exported from
   SQLite tables via `sqlite3.Cursor.fetchmany()` → `pl.DataFrame(schema_overrides=
   {col: pl.Utf8})` (`csv_service.py:338-346`).
2. **No `mode="x"`.** Files are named via `get_file_name()` with
   `%Y%m%d-%H%M%S` + `file_index` (`csv_service.py:237-240`); same-second re-runs
   overwrite, they do not silently skip.
3. **No YAML error files.** No `TVTask.to_yaml`, no serialised task context;
   errors are Python exceptions (`errors.py`: 5 classes).
4. **No `encoding="utf-8-sig"` parameter.** Not a pandas project — it is Polars.
   The UTF-8 BOM is `include_bom=True` (`app_config.yaml:43`), absorbed by
   `extra="allow"` on `PolarsWriteCSV` (`models.py:84-90`).
5. **No `decimal=","`.** It is `decimal_comma=True` (Polars boolean, `models.py:88`).

The required rework is **moderate**:
- Rewrite the Purpose (1 paragraph) to remove API/error-file/mode="x" framing and
  describe the real SQLite→Polars→CSV export path.
- Reframe Block 3: replace `mode="x"` with same-second overwrite
  (`csv_service.py:237-240, 356-361`).
- Repurpose Block 4: from "error-file conformance" to "crash-safety &
  output-readiness of exported files" (non-atomic writes,
  `csv_service.py:356-361`; DB-deletion-on-crash, `db_service.py:786-788`;
  `ReportResult` fields, `models.py:170-195`).
- Reword the Scope-boundary (04) citation: remove "error-file location" (no error
  files); clarify the Phase 04 vs Phase 16 ownership boundary (write path vs
  output-readiness).
- Re-ground the severity taxonomy examples: remove `mode="x"` (HIGH), "error file"
  (MEDIUM/LOW), `encoding="utf-8-sig"` (MEDIUM); reword to same-second overwrite,
  non-atomic writes, `include_bom` inside gzip, no schema manifest.
- Keep report output unchanged: findings path
  `.ai/audit/16-rendered-surface/findings.md` (valid convention), `OUT-` prefix
  (zero collisions — repo-wide grep confirms only Phase 16's own line 117 matches),
  `problems-only: true`, empty state `No problems found in this phase.`

**Candidate findings: 6** (F1–F6) are documented in §6 with full code references,
severity bands, and ownership attribution. They span all four re-based blocks and
the Report Output section. F1 and F2 are already partially surfaced by Phase 04
(code-side) — Phase 16 should cross-file (cite Phase 04) and add the
*output-readiness* angle Phase 04 does not cover (F2: no checksum/integrity
footer on the rendered file).

**No source files are modified** by this report — it audits the phase *spec* and
the production code behaviour, per the auditor role. The findings F1–F6 are the
work the re-based Phase 16 surfaces when executed against the synchronous
codebase.

---

## Appendix A — Evidence pedigree

- **Phase file read:** `.kilo/commands/audit/phases/16-audit-rendered-surface.md`
  (full, 126 lines) — Purpose (lines 11-21), 4 Audit Blocks (lines 31-81),
  Severity Taxonomy (lines 85-109), Report Output (lines 111-126), Scope
  boundary citations (lines 25-27), Finding-ID prefix `OUT-` (line 117).
- **Architecture source files read:**
  `src/mko_data_cleaner/core/csv_service.py` (full, 375 lines) — `CSVWorker`,
  `get_file_name` (lines 237-240), `export_sql_to_csv` (lines 269-375),
  `get_files_suffix` (lines 242-263), `df.write_csv` direct-to-final-path
  (lines 356-361); `src/mko_data_cleaner/core/app_service.py` (full, 342 lines) —
  `run_report` (lines 110-290), null_data export to `base_path` (line 249),
  clean_data export to `export_path` (line 267), `ReportResult` construction
  (lines 278-290); `src/mko_data_cleaner/core/db_service.py` (lines 50-134,
  330-410, 630-643, 745-788) — `DBWorker.__exit__`→`close()`→`_delete_base_files`
  (lines 786-788, 762-781), `target_table` property (lines 75-79),
  `build_non_mapped` predicate (lines 745-755), `create_table` all-TEXT
  (lines 134-193), PRAGMAs (lines 116-123); `src/mko_data_cleaner/core/models.py`
  (full, 195 lines) — `PolarsWriteCSV` (lines 84-90), `ReportResult` (lines
  170-195), `WorkingPaths` (lines 60-67), StrEnums (lines 106-117);
  `src/mko_data_cleaner/core/utils.py` (lines 1-50, 73-103) — `clean_names`/
  `make_valid`; `src/mko_data_cleaner/core/errors.py` (full, 30 lines) — 5
  exception classes, no serialization;
  `src/mko_data_cleaner/settings/app_config.yaml` (full, 50 lines) —
  `export_settings.to_csv` block (lines 40-49).
- **Documentation read:** `docs/00-overview/data-flow.md` (full, 191 lines) —
  output format description (lines 265-270), "repeated runs never overwrite"
  claim (lines 177-178, stale), source-column semantics (lines 38-64);
  `docs/00-overview/current-behavior.md` (lines 263-276, 292-294) — CSV format
  (line 267), empty non-mapped → no file (line 293); `docs/00-overview/
  architecture.md` (lines 225-252) — DB deletion on exception (lines 229-230),
  double-DELETE note (line 252).
- **Polars API verification:** Polars 1.38.1 `DataFrame.write_csv` signature
  verified locally (`import polars as pl; help(pl.DataFrame.write_csv)`) —
  parameters include `include_bom`, `compression`, `separator`,
  `decimal_comma`, `include_header`, `line_terminator` etc.; **no `encoding`
  parameter**, **no `mode` parameter**. `compression` accepts only
  `uncompressed`/`gzip`/`zstd` (not `bz2`/`zip`/`xz`/`zstd` from the model's
  broader `Literal`).
- **Phase files read (cross-reference):** `04-audit-data-pipeline.md` (full,
  211 lines — Block 5 crash-safety, Block 6 failure mode);
  `05-audit-config-templates.md` (lines 72-84 — gap #4 `include_bom` not a
  declared `PolarsWriteCSV` field); `10-audit-code-quality.md` (full, 132 lines
  — scope boundary line 18); `11-audit-test-coverage.md` (full, 132 lines —
  scope boundary line 19); `16-audit-rendered-surface.md` (full, 126 lines —
  the subject).
- **Prior auditor reports read:** `phase-04-auditor-report.md` (full, 253 lines
  — REBASE; §5 candidate findings F1-F8; Block 5 crash-safety, Block 6
  failure mode); `phase-13-auditor-report.md` (full, 594 lines — REBASE;
  §6 candidate findings F1-F7; severity re-grounding pattern);
  `phase-15-auditor-report.md` (full, 330 lines — DELETE; format precedent for
  this report type).
- **Test files read:** `tests/test_csv_service.py` (full, 75 lines) —
  `test_export_sql_to_csv_returns_row_count` (lines 34-55),
  `test_export_sql_to_csv_returns_zero_for_empty_table` (lines 58-74),
  `test_get_file_name` (lines 22-31); `tests/test_app.py` (lines 438-472) —
  asserts `result.export_dir` exists, `result.null_data_files` is a list,
  `result.timings.keys() == {"import", "mapping", "export"}`. **No test
  asserts `mode="x"`, `encoding="utf-8-sig"`, or YAML error files** — these
  concepts exist only in the Phase 16 spec, not in code or tests.
- **Prefix grep:** repo-wide (excluding `.venv*`, `.git*`) for `OUT-` →
  **1 match**: `.kilo/commands/audit/phases/16-audit-rendered-surface.md:117`
  (the phase file's own Report Output section). **Zero** `OUT-NNN` finding
  identifiers in any findings document, test file, or shipped source. No
  collision.
- **Negative grep — API/error-file/mode:** `src/mko_data_cleaner/` for
  `to_yaml|TVTask|error_file|yaml_error|mode.*=.*['\"]x['\"]|async def|
  asyncio|requests|httpx|aiohttp|token|credential|api_key` → **zero code
  matches** for all async/API/error-file patterns. `errors.py` defines exactly 5
  exception classes — no serialization method.
- **Docker runtime:** Not started — this audit is a static architecture-vs-doc
  comparison against a local-only CLI with no server, API endpoint, or
  containerised service to probe. The grep and code-reading evidence above is
  conclusive for all four blocks.