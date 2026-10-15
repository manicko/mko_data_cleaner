---
name: 16-rendered-surface
status: draft
validated: no
executor: auditor
problems-only: true
code_ref: "b390d04"
severity_taxonomy: ".kilo/commands/audit/phases/16-audit-rendered-surface.md#severity-taxonomy"
---

# Phase 16 — Rendered-Surface & Output Conformance Measurement

## Purpose

Measures the rendered CSV output of this tool — `clean_data/data_table_*.csv.gz`
and `null_data_*.csv.gz` — against the output surface declared in configuration
and documentation. The pipeline exports SQLite table contents to gzip-compressed
CSV via Polars: columns originate from cleaned source CSV headers
(`clean_names`/`make_valid`, `utils.py:73-101`), all values are rendered as text
(`schema_overrides={col: pl.Utf8}`, `csv_service.py:325, 341-346`), governed by
`PolarsWriteCSV` settings (`separator=";"`, `decimal_comma=True`,
`compression="gzip"`, `include_bom=True` absorbed by `extra="allow"`,
`app_config.yaml:42-48`; `models.py:84-90`). Audits: schema, format, output
presence, crash-safety-readiness — not how the write happens (Phase 04), but
whether the output is safe for a downstream consumer.

**Scope:** schema, format, presence, crash-safety-readiness of exported
CSVs. **Out of scope:** export mechanics and non-atomic writes (04);
conformance (05); enum (10); tests (11).
---

## Audit Blocks

Each block is independent — execute any one with no knowledge of the others.
Evidence is a class, not a check list: observed output, a reproduced behaviour,
or a static proof that a stated property does not hold; the method is the
auditor's. A passing check is methodology, never a finding.

### 1. CSV schema conformance against the source-column contract

Establish whether the exported CSV's column set, order, and per-field types
conform to the source-column contract.

- **Evidence (chain):** headers from `pl.read_csv(n_rows=0)` (`csv_service.py:77-86`)
  → `clean_names`/`make_valid` (`utils.py:73-101`, `db_service.py:107-110, 154-156`)
  → SQLite `TEXT` for all columns (`db_service.py:181`; full method
  `db_service.py:134-193`)
  → `export_sql_to_csv` reads `data_cursor.description` (`csv_service.py:323`)
  → `pl.DataFrame` with `schema_overrides={col: pl.Utf8}` (`csv_service.py:341-346`).
  No type information survives.
- **Evidence (extra columns & gap):** Dictionary-derived extras appended before
  export; entirely empty ones dropped (`dict_service.py:53` `_drop_empty_columns`,
  called at `dict_service.py:111`). **Gap (OUT-005):** the only contract is prose
  in `data-flow.md:38-64`; no machine-readable schema manifest is emitted.

### 2. CSV format conformance: separator, decimal mark, compression, BOM

Establish whether the rendered format matches the `PolarsWriteCSV` settings
declared in `app_config.yaml:42-48` and consumed at `csv_service.py:358-361`.

- **Evidence (separator & decimal):** `separator=";"` (`app_config.yaml:44`,
  `models.py:86`) and `decimal_comma=True` (Polars boolean, `app_config.yaml:46`,
  `models.py:87`) — both applied via `**params` at `csv_service.py:358`. The phase
  spec's `decimal=","` is pandas syntax; grep `import pandas` in `src/` → zero.
- **Evidence (compression):** `compression="gzip"` → `.csv.gz` —
  `app_config.yaml:48`, `models.py:90`, suffix via `get_files_suffix`
  (`csv_service.py:242-263`). `write_csv` has no `encoding` parameter (Polars
  1.38.1).
- **Evidence (BOM):** `include_bom=True` — `app_config.yaml:43`. **Not a declared
  field** of `PolarsWriteCSV` (`models.py:84-90`); absorbed by `extra="allow"`
  (`models.py:85`) and survives `model_dump()` (`app_service.py:133`) →
  `**params` → `df.write_csv` (`csv_service.py:358`). With gzip the BOM is the
  first 3 bytes of the *uncompressed* stream — invisible to decompressing
  consumers. No test asserts it.
- **Evidence (header & chunk size):** `include_header=True` (`app_config.yaml:47`,
  `models.py:88`); `chunk_size: 100000` overrides model default `10000`
  (`models.py:89`), popped at `csv_service.py:312` as `fetchmany` bound
  (`csv_service.py:338`).

### 3. Output presence & completeness (same-second overwrite, not mode="x")

Establish whether every successful export produces a conformant file, and
whether an empty result is indistinguishable from a complete one.

- **Evidence (naming & overwrite):** `get_file_name()` (`csv_service.py:237-240`)
  builds `f"{name_prefix}_{strftime('%Y%m%d-%H%M%S')}_{name_suffix}{ext}"` with
  `file_index` per run (`csv_service.py:314`) — timestamp is **second-granular**.
  A re-run within the same second produces an identical filename;
  `file_name.format(file_index=file_index)` (`csv_service.py:356`) then
  `df.write_csv(file=file_path, **params)` (`csv_service.py:358-361`) writes
  directly to that path, **overwriting**. `mode="x"` does not exist (grep
  `mode.*=.*['"]x['"]` in `src/` → zero; not a Polars parameter).
- **Evidence (two exports & empty-table):** Two exports per run: `null_data`
  to `base_path` (`app_service.py:246-251`, glob at `app_service.py:252`);
  `data_table` to `export_path` (`app_service.py:264-268`). `export_sql_to_csv`
  returns `0` for empty table (`csv_service.py:334-336`) — no file written;
  `run_report` reports `rows_exported=0` (`app_service.py:289`) and constructs
  `ReportResult` (`app_service.py:278-290`). `current-behavior.md:293`:
  "Empty non-mapped set → No `null_data` file written." `test_csv_service.py:58-74`
  locks the return value but not file presence.

### 4. Crash-safety & output-readiness of exported files

Establish whether a crash leaves a truncated file consumed as complete, and
whether diagnostic state is recoverable.

- **Evidence (non-atomic write):** `df.write_csv(file=file_path, **params)` writes
  **directly to the final path** (`csv_service.py:356-361`). No temp+rename, no
  checksum. A mid-write crash leaves a truncated `.csv.gz` consumed as complete.
- **Evidence (DB deletion on crash):** `DBWorker.__exit__(exc_type, exc, tb)` →
  `self.close()` (`db_service.py:786-788`) runs **unconditionally** on exception.
  `close()` → `_delete_base_files()` (`db_service.py:769-781, 762-767`) deletes
  the DB + `-wal`/`-shm` sidecars regardless of `exc_type`.
  `current-behavior.md:294`: "Unexpected exception → `close()` still runs and
  deletes the DB."
- **Evidence (no error/status field, no serialisation, ownership):** `errors.py`
  defines 5 plain `Exception` subclasses (lines 4-20); grep `to_yaml|TVTask|error_file`
  in `src/` → zero. `ReportResult` (`models.py:170-195`) has `rows_loaded`/
  `rows_exported` but **no `error` or `status` field** — `rows_exported == 0` is
  indistinguishable from a full export. **Ownership:** Phase 04 owns the write
  & cleanup path; Phase 16 owns *output-readiness* — "Is the file safe to read?"

---

## Severity Taxonomy

Grade by **effect and blast radius**, not by mechanism name; rate what is true
now. An empty band is a valid outcome — do not populate it with a hypothetical,
and do not promote an item for sounding alarming.

- **CRITICAL** — rendered output consumed as authoritative while conforming to no
  declared contract, so a downstream reader silently misreads a value or a
  malformed file is treated as valid. *Empty today* — format matches
  `PolarsWriteCSV` in `app_config.yaml:42-48`.
- **HIGH** — a CSV column whose type/name diverges from the declared surface;
  a required column missing; a **same-second re-run overwrites** prior output
  (`get_file_name`, `csv_service.py:239`; second-granular, no guard at
  `csv_service.py:356-361`) — **OUT-001**; a **non-atomic write** leaving a
  truncated `.csv.gz` consumed as complete (`csv_service.py:356-361`) —
  **OUT-002**; an **empty-table export writing no file** while reporting
  success (`csv_service.py:334-336`) — **OUT-003**.
- **MEDIUM** — a declared model field absent from the Pydantic schema but
  silently absorbed by `extra="allow"` (e.g. `include_bom: True` at
  `app_config.yaml:43`, undeclared in `PolarsWriteCSV` `models.py:84-90`) —
  **OUT-004**; **no machine-readable schema manifest** (contract is prose only,
  `data-flow.md:38-64`) — **OUT-005**; **no `error`/`status` field on
  `ReportResult`** (`models.py:170-195`) so `rows_exported == 0` is
  indistinguishable from a full export — **OUT-006**.
- **LOW** — documentation drift about output format with no runtime consequence;
  a stale doc claim (`data-flow.md:178` "repeated runs never overwrite") with no
  live data loss in the current test suite.

## Report Output

- Findings path: `.ai/audit/16-rendered-surface/findings.md`
- Template: `.ai/audit/templates/audit-findings.md` — follow it for front
  matter, summary, findings, distribution, cross-finding analysis, roadmap,
  rollout safety, appendices
- Finding-ID prefix: `OUT-` — repo-wide grep for `OUT-NNN` finding identifiers
  returns zero in the current tree (`b390d04`); retain with no collision
- `problems-only: true` — findings only, omit passing checks; every finding
  needs runtime evidence and the exact consequence; empty state, exactly:
  `No problems found in this phase.`
- If a shipped test asserts the current behaviour as intended, still file the
  finding and record the test as a remediation blocker
