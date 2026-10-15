---
id: current-behavior
domain: overview
tags:
  - behavior
  - usage
  - cli
  - api
related:
  - product-overview
  - data-flow
  - architecture
  - open-questions
---

## Purpose

Describe how MKO Data Cleaner actually works **today**: entry points, the main
processing scenario step by step, the rule engine semantics, outputs, error
behavior, and configuration that changes behavior. Every significant claim
cites its source (file, function, or test). Items marked
**[runtime-verified]** were executed by the documentation investigation in a
scratch copy and then cleaned up; **[tested]** means a repository test covers
the behavior (the full suite — 35 tests — was run and passed on 2026-10-15);
everything else is **[code-verified]** by reading the implementation.

## Entry Points and Ways to Run

| Entry point | Location | Notes |
|---|---|---|
| CLI `mko-data-cleaner init [--force]` | [app.py `init`](../../src/mko_data_cleaner/app.py) | Copies settings templates into the per-user settings directory; reachable without prior settings since the import fix |
| CLI `mko-data-cleaner run <report_dir> [-v] [--settings-dir DIR]` | [app.py `run`](../../src/mko_data_cleaner/app.py) | `<report_dir>` must exist (`typer` `exists=True`); any exception prints `❌ Ошибка: …` and exits with code 1 |
| CLI `mko-data-cleaner list-settings [--settings-dir DIR]` | [app.py `list_settings`](../../src/mko_data_cleaner/app.py) | Lists YAML files in the user settings directory; prints a hint if the directory does not exist (does not raise) |
| Python `process_data(report_path, settings_dir=None)` | [app.py](../../src/mko_data_cleaner/app.py) | Synchronous; raises `FileNotFoundError` if the path is not a directory; raises `SettingsNotInitializedError` if settings are missing |
| Python `initialize_settings(force=False, settings_dir=None)` | [app.py](../../src/mko_data_cleaner/app.py) | **Known gap:** `settings_dir` is accepted but not forwarded to `init_project`; settings are always written to the default platformdirs location [runtime-verified] |
| Console script | [pyproject.toml `[project.scripts]`](../../pyproject.toml) | `mko-data-cleaner = "mko_data_cleaner.app:app"` |

Network access, databases and external services are not involved: the only
external resources are the local file system and the packaged YAML templates.

## First-Run Behavior (settings gate)

- `import mko_data_cleaner.app` performs **no** filesystem side effects and
  does not configure logging; the service is constructed lazily by
  `get_app_service()` ([app_service.py `get_app_service`](../../src/mko_data_cleaner/core/app_service.py)).
- `get_app_service()` first validates that `app_config.yaml` **and**
  `log_config.yaml` exist in the user settings directory and otherwise raises
  `SettingsNotInitializedError` (a subclass of `FileNotFoundError`) whose
  message names the missing files and the remediation
  `initialize_settings()` ([errors.py](../../src/mko_data_cleaner/core/errors.py)).
  [tested: tests/test_app.py] [runtime-verified]
- Only after the gate does the factory apply `logging.config.dictConfig`.

## Where Settings Live

- Package templates: `src/mko_data_cleaner/settings/app_config.yaml` and
  `log_config.yaml`, shipped inside the wheel/sdist via
  `[tool.setuptools.package-data]` ([pyproject.toml](../../pyproject.toml),
  [MANIFEST.in](../../MANIFEST.in)).
- User settings directory: `platformdirs.user_config_dir("mko_data_cleaner")`
  + `/settings`. On Windows 11 this resolved to
  `C:\Users\<user>\AppData\Local\mko_data_cleaner\mko_data_cleaner\settings`
  [runtime-verified]. `init` copies the templates there
  ([init_service.py `_copy_templates`](../../src/mko_data_cleaner/core/init_service.py)).
- `run`/`process_data` accept `--settings-dir` / `settings_dir` to override
  the base directory; the YAMLs are then expected at
  `<settings_dir>/settings/` ([build_app_paths](../../src/mko_data_cleaner/core/app_service.py)).

## Expected Input: The Report Directory

Default layout (all paths relative to the report directory; configured in
`data_paths`, [models.py `WorkingPaths`](../../src/mko_data_cleaner/core/models.py)):

```
<report_dir>/
├── raw_data/                    # import_folder — one or more .csv or .gz exports
├── dict/merged_dictionary.csv   # dict_file — the rules dictionary
├── data_base/db_example.db      # db_file — temporary, DELETED after the run
├── clean_data/                  # export_folder — created if missing; output
└── null_data_<timestamp>_N.csv.gz  # written at the report root
```

- Raw files must match the configured extension (`data_file_settings.extension`,
  default template value `gz`; the repository example uses `csv`).
  If none are found, `WrongDataSettings` aborts the run before any processing
  ([csv_service.py `_set_files_params`](../../src/mko_data_cleaner/core/csv_service.py))
  [runtime-verified].
- The **first** raw file found is used to read the column headers
  ([get_csv_headers](../../src/mko_data_cleaner/core/csv_service.py));
  headers must therefore be identical across files.
- Headers are transliterated/stripped to valid SQL identifiers
  (`ARTICLE LIST3` → `ARTICLELIST3`, `COST RUB` → `COSTRUB`), with a warning
  per invalid name ([utils.py `clean_names`/`make_valid`](../../src/mko_data_cleaner/core/utils.py))
  [runtime-verified]. Non-ASCII header values are transliterated via `unidecode`.
- `index_column` (template default `AdId`) and `date_column` (template default
  `researchDate`) are looked up **after** cleaning, so case and shape must
  match the cleaned header (`AdId` matched `ADID` in verification because
  SQLite identifiers are case-insensitive) [runtime-verified].
- If the configured `date_column` is absent, an error is logged and the tool
  scans for a column matching `^\d{4}[-/.]\d{2}[-/.]\d{2}$` (YYYY-MM-DD-style)
  in a 500-row sample; if none matches, `date_column` becomes `None`
  ([check_date_column](../../src/mko_data_cleaner/core/csv_service.py))
  [runtime-verified]. Note: the docstring of `_detect_date_column` also
  mentions `DD/MM/YYYY` support, which the regex does **not** implement —
  see [open-questions.md](open-questions.md) D-4.

## The Dictionary (rule set)

- The dictionary is read from `dict_file`; if it does not exist, all CSV files
  in the dictionary folder are merged, de-duplicated and sorted by the literal
  column `search_column_idx` into it ([get_merged_dictionary](../../src/mko_data_cleaner/core/csv_service.py)).
- `dict_file_settings.col_indexes` defines which dictionary columns hold the
  rule settings. The shipped template is:

  | Setting | Index | Meaning |
  |---|---|---|
  | `action` | 0 | `a` (ADD), `r` (REPLACE), `d` (DELETE) |
  | `match` | 1 | `f`, `p`, `s`, `e`, `fts` |
  | `search` | 2 | 0-based index of the **data** column to search (for `fts`: several indexes separated by `|`) |
  | `term` | 4 | the search term / FTS query parts |

- All columns at indexes **after** the maximum configured index (i.e. index 5
  and later with the template) become the **extra columns** — the
  analyst-defined output columns. Fully empty columns are dropped
  ([dict_service.py `_initialize_mapping`](../../src/mko_data_cleaner/core/dict_service.py)).
- **Important:** with the shipped configuration the search **term** is read
  from dictionary column index **4**, and column index **3 is not used by the
  rules** (the owner's own dictionaries carry intermediate data there). The
  README example now mirrors the base configuration, and documents that the
  positions come from `col_indexes` in `app_config.yaml` — a user whose
  dictionary has no intermediate column sets `term: 3` instead. See
  [open-questions.md](open-questions.md) D-1 / decision D12
  [code-verified + runtime-verified].

## Main Scenario: `run` / `process_data`

Trace of `AppService.run_report`
([app_service.py](../../src/mko_data_cleaner/core/app_service.py)); each step
lists the participating component:

1. **Resolve paths** — `base_path` = report directory; ensures the DB parent
   directory and the export directory exist.
2. **Read the dictionary** — `CSVWorker.get_dictionary()` → `MappingDict`
   (Polars) splits rules into LIKE rules and FTS rules, builds LIKE patterns
   and FTS queries ([build_mapping](../../src/mko_data_cleaner/core/dict_service.py)).
3. **Detect the date column** — `CSVWorker.check_date_column()` (see above).
4. **Open SQLite** — `DBWorker` opens two connections to the DB file
   (`sqlite3` for DDL/queries, ADBC for bulk writes) and applies
   `PRAGMA journal_mode=WAL`, `synchronous=OFF`, `temp_store=MEMORY`,
   `cache_size=-100000` ([db_service.py `_init_base`](../../src/mko_data_cleaner/core/db_service.py)).
5. **Create schema** — data table (all columns `TEXT`), the distinct index
   table, indexes, the match tables; if FTS rules exist, an FTS5 virtual
   table with insert/delete/update triggers ([create_table_with_index](../../src/mko_data_cleaner/core/db_service.py),
   [link_search_table](../../src/mko_data_cleaner/core/db_service.py)).
6. **Import raw data** — streamed in chunks of 10 000 rows through Polars and
   appended via ADBC ([`_import_data`](../../src/mko_data_cleaner/core/app_service.py));
   then the distinct index table is populated with
   `SELECT search cols, index col, MAX(date) … GROUP BY index col`
   ([update_index_from_data](../../src/mko_data_cleaner/core/db_service.py)).
7. **Compute matches** — FTS matches via the FTS5 `MATCH` join
   ([insert_matches_from_fts](../../src/mko_data_cleaner/core/db_service.py));
   LIKE matches via one `data.col LIKE rules.pattern COLLATE NOCASE` query per
   search column ([insert_matches](../../src/mko_data_cleaner/core/db_service.py)).
   **Per-key rule resolution (intended design, owner decision D15):** when
   `index_column` is configured, matching and rule application run against the
   distinct index table (`DBWorker.target_table`), i.e. **once per unique key
   value**, not per row. The distinct table is built with
   `SELECT search cols, index col, MAX(date) … GROUP BY index col`; in SQLite
   the bare (search) columns take their values from the row holding the
   `MAX`, i.e. the latest row per key (an arbitrary row when no date column
   is configured). The extra columns start as `NULL` in that table and are
   filled by the rules. Afterwards the results are joined back to the data
   table (step 11). This is a deliberate performance optimization
   [code-verified + runtime-verified].
8. **Apply rules in fixed order** — for each action block
   (`generate_rules_blocks` → DELETE, REPLACE, ADD) the rules are written to
   `mapping_table` and applied by [apply_mapping](../../src/mko_data_cleaner/core/db_service.py).
9. **Collect non-mapped rows** — distinct combinations of search + extra
   columns where any extra column is NULL ([build_non_mapped](../../src/mko_data_cleaner/core/db_service.py)).
10. **Export non-mapped** — `null_data_<timestamp>_1.csv.gz` at the report root
    ([export_sql_to_csv](../../src/mko_data_cleaner/core/csv_service.py)).
11. **Synchronize** — the extra columns resolved per unique key are copied
    from the distinct index table back to **all** data rows sharing that key
    (`UPDATE … FROM … WHERE key = key`); data rows whose key was removed by
    DELETE rules are deleted from the data table; the INSERT branch of
    `_sync_tables` (index value present in the distinct table but missing in
    the data table) cannot fire in the standard flow, because the distinct
    table is built from the data table ([sync_with_data_table](../../src/mko_data_cleaner/core/db_service.py))
    [code-verified + runtime-verified].
12. **Export clean data** — the data table is written in chunks to
    `clean_data/data_table_<timestamp>_N.csv.gz`, all values as text
    ([export_sql_to_csv](../../src/mko_data_cleaner/core/csv_service.py)).
13. **Cleanup** — on context-manager exit: WAL checkpoint, close both
    connections, and **delete the database file** (and its `-wal`/`-shm`
    sidecars) ([close](../../src/mko_data_cleaner/core/db_service.py))
    [runtime-verified: no `data_base/` remained after a run].

## Rule Engine Semantics

Order is always **DELETE → REPLACE → ADD**
([generate_rules_blocks](../../src/mko_data_cleaner/core/dict_service.py)).

### Actions

| Action | Code | Behavior | Evidence |
|---|---|---|---|
| DELETE | `d` | Removes matched rows from the target (distinct) table, so after synchronization all data rows with that index value disappear | [`_apply_delete`](../../src/mko_data_cleaner/core/db_service.py) [runtime-verified] |
| REPLACE | `r` | Overwrites the extra columns with the rule's values. When several REPLACE rules match the same row, the one with the **highest `mapping_index`** wins (`ROW_NUMBER() … ORDER BY mapping_index DESC`) | [`_apply_replace`](../../src/mko_data_cleaner/core/db_service.py) [runtime-verified] |
| ADD | `a` | Collects the values of **all matching ADD rules** for a row into the tags table, then writes them into the extra column as a de-duplicated, alphabetically sorted list joined by `dict_file_settings.add_separator` (default `", "`) | [`_apply_add`](../../src/mko_data_cleaner/core/db_service.py) [runtime-verified] |

**ADD semantics (owner-confirmed, decision D13):** the extra column is
created empty; ADD **merges** the tags of all matching ADD rules into it as a
de-duplicated, alphabetically sorted list joined by
`dict_file_settings.add_separator` (default `", "`). Merging of tags from
several ADD rules is verified: two ADD rules on one row produced
`TAG_A, TAG_B` [runtime-verified 2026-10-15].

**ADD nuance (confirmed gap, decision D13 / D-2):** the intended sequence
REPLACE-then-ADD on the same column is `1` + `2` → `1 2` (merged). The
current implementation does **not** merge: `_apply_add` writes only the
`GROUP_CONCAT` of the ADD tags and does not read the cell's previous content,
so a value previously written by REPLACE (or present in the source data) is
overwritten. Runtime-verified 2026-10-15: with `r` → `REPLACED` and two ADD
rules on the same row/column, the exported cell contains `TAG_A, TAG_B` — the
REPLACE value is lost. Not fixed by this documentation task; the README
carries an explicit limitation note. See
[open-questions.md](open-questions.md) D-2.

### Match types (LIKE family)

| match | LIKE pattern | Meaning |
|---|---|---|
| `f` | `TERM` | full match (exact equality under `COLLATE NOCASE`) |
| `p` | `%TERM%` | contains |
| `s` | `TERM%` | starts with |
| `e` | `%TERM` | ends with |
| `fts` | FTS5 query | full-text search over the search columns |

- Patterns are built in Polars and **upper-cased**
  ([`_build_search_like_pattern`](../../src/mko_data_cleaner/core/dict_service.py));
  tests assert the upper-casing and the pattern shapes
  ([tests/test_dict_service.py](../../tests/test_dict_service.py)).
- Matching uses SQLite `COLLATE NOCASE`, which is **ASCII-only**: Cyrillic
  case folding does not apply. Exports are upper-case by convention, so this
  normally does not matter [code-verified].
- DELETE rules also generate a LIKE pattern; a DELETE rule therefore only
  removes rows whose search column actually matches its term
  [tested: test_pattern_generation asserts the pattern exists].

### FTS rules (`fts`)

- `search_column_idx` holds several 0-based indexes separated by `|`
  (`fts_separator`), paired positionally with the term parts; each value is
  wrapped as `"col_2:(\"TERM\")"` etc. and joined
  ([build_match_query](../../src/mko_data_cleaner/core/dict_service.py)).
- Invalid FTS rows (non-numeric index, unpaired values) are skipped with a
  warning log, not fatal ([`_build_fts_query`](../../src/mko_data_cleaner/core/dict_service.py)).
- An FTS5 virtual table with content = the distinct index table and triggers
  keeps the index in sync ([link_search_table](../../src/mko_data_cleaner/core/db_service.py)).
- The FTS path was **not** exercised by the verification runs (no FTS rule in
  the synthetic sample) — see [Investigation Limitations](#investigation-limitations).

## Outputs

| Output | Location | Format |
|---|---|---|
| Clean data | `<report>/clean_data/data_table_<timestamp>_<N>.csv.gz` | `;`-separated, header, gzip, all values as text; one file per `export_settings.to_csv.chunk_size` rows |
| Non-mapped data | `<report>/null_data_<timestamp>_<N>.csv.gz` | distinct search+extra combinations where any extra column is NULL; if the non-mapped set is empty, no file is written. **Purpose (owner-confirmed D14):** dictionary-improvement feedback only — not a data-quality report, warning, or error signal [runtime-verified] |
| Database | `<report>/data_base/db_example.db` | deleted at the end of the run |
| Logs | user settings directory `log.log` (RotatingFileHandler, 2 MB × 2) + console | via `log_config.yaml` |

Note: `process_data` currently returns `None`; the result object (paths +
timings + row counts) is owner-confirmed but not implemented — see
[product-overview.md](product-overview.md) and
[open-questions.md](open-questions.md) D-6 [runtime-verified].

## Errors, Warnings and Edge Cases

| Situation | Behavior | Source |
|---|---|---|
| Import without settings | Succeeds, no side effects | [tested] [runtime-verified] |
| `run`/`process_data` without settings | `SettingsNotInitializedError` (catchable as `FileNotFoundError`); CLI prints the error and exits 1 | [app.py `run`](../../src/mko_data_cleaner/app.py) [tested] |
| `process_data` with a file path or missing dir | `FileNotFoundError("Directory not found: …")` | [app.py `process_data`](../../src/mko_data_cleaner/app.py) |
| No raw files with the configured extension | `WrongDataSettings` with a "check settings" message | [`_set_files_params`](../../src/mko_data_cleaner/core/csv_service.py) [runtime-verified] |
| Ragged CSV row (more fields than the header) | Polars `ComputeError` aborts the run (no `truncate_ragged_lines` configured) | [runtime-verified] |
| Invalid `app_config.yaml` contents | Pydantic validation error wrapped in `ConfigError` | [app.py `app_config` property](../../src/mko_data_cleaner/core/app_service.py) |
| Missing/invalid YAML file read by `yaml_to_dict` | Returns `{}` (defense in depth) | [utils.py `yaml_to_dict`](../../src/mko_data_cleaner/core/utils.py) [tested] |
| Header names that are not valid SQL identifiers | Cleaned (transliterated, alphanumerics kept) with a warning | [`clean_names`](../../src/mko_data_cleaner/core/utils.py) [runtime-verified] |
| Configured `date_column` missing | ERROR log, then auto-detection; if none found, no date column is used | [runtime-verified] |
| Invalid FTS dictionary row | Warning, rule skipped | [`_build_fts_query`](../../src/mko_data_cleaner/core/dict_service.py) |
| Duplicate dictionary rows | Deduplicated when the merged dictionary is generated (`unique()`); **not** deduplicated when reading an existing `merged_dictionary.csv` | [get_merged_dictionary](../../src/mko_data_cleaner/core/csv_service.py) |
| Dictionary column entirely empty | Column dropped (not exported) | [`_drop_empty_columns`](../../src/mko_data_cleaner/core/dict_service.py) [runtime-verified] |
| Empty non-mapped set | No `null_data` file written | [`export_sql_to_csv` early return](../../src/mko_data_cleaner/core/csv_service.py) [runtime-verified] |
| Unexpected exception during `run_report` | Propagates out of the `with DBWorker(...)` block; `close()` still runs (context manager) and deletes the DB | [db_service.py `__exit__`](../../src/mko_data_cleaner/core/db_service.py) |

## Configuration That Affects Behavior

`app_config.yaml` ([template](../../src/mko_data_cleaner/settings/app_config.yaml),
[Pydantic models](../../src/mko_data_cleaner/core/models.py)):

| Key | Effect |
|---|---|
| `data_paths.*` | Import/export folders, dictionary and DB file locations (relative to the report directory) |
| `data_file_settings.extension` | Which raw files are read (`csv` / `gz`) |
| `data_file_settings.index_column` | Enables the distinct index table, deduplication by that column, and the synchronization step |
| `data_file_settings.date_column` | Optional date column; only `MAX(date)` per index value survives into the distinct table |
| `dict_file_settings.col_indexes` | Meaning of the dictionary columns (see above) |
| `dict_file_settings.add_separator` | Separator used when ADD writes multiple values |
| `dict_file_settings.fts_separator` | Separator for multi-column FTS rules |
| `database_settings.table_name` | Data table name (`data_table`) |
| `read_settings.from_csv.*` | Polars CSV reader options (separator `;`, decimal comma, …) |
| `export_settings.to_csv.*` | Polars CSV writer options (chunk size, compression, …) |

`log_config.yaml` configures the logging handlers/formatters and is applied by
the factory at service construction. It still contains the unused `tests` and
`telebot` logger entries (leftover from another project) — see
[open-questions.md](open-questions.md) D-8.

## Investigation Limitations

- The full pytest suite (35 tests) was executed locally on 2026-10-15 and
  passed; tests cover the settings gate, CLI wiring, dictionary pattern
  building, DB table/index/FTS creation and CSV export naming.
- End-to-end runs were executed on **synthetic 5-row reports** in a scratch
  directory (since deleted), covering DELETE, REPLACE, ADD (full and partial
  match), non-mapped export, synchronization and cleanup.
- **Not executed:** the 43 MB real sample in `data/example/` (its dictionary
  uses an older, incompatible format — see D-1), any FTS5 rule path, and any
  gzip input path. These are documented from code reading only.
- No claim of runtime verification is made for scenarios not executed.
