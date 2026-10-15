# Example notebook: `mko_data_cleaner_pipeline_example.ipynb`

A ready-to-run, **valid** (nbformat 4.5+, unique cell ids) example notebook that
demonstrates `mko_data_cleaner` as one step in a Jupyter pipeline. It generates a
tiny synthetic Mediascope-style report **in-cell** (no 43 MB sample file is
referenced or required) and runs the full clean → map → export cycle.

> The notebook is **input-only**: no committed output cells. Outputs are
> produced at execution time (the repository stays clean).

## Prerequisites

- Python >= 3.12 (developed and verified on Python 3.14 + uv 0.9.26).
- The package itself. Choose one:
  - From a **clean kernel**, the notebook's install cell installs the pinned
    release tag:
    `%pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.1.0"`
    (the `v0.1.0` tag is cut in a later release step; until then use the local
    install below).
  - **Local dev install** (no network needed): from the repo root,
    `uv sync` installs the project (editable) plus notebook tooling
    (`nbformat`, `nbclient`, `ipykernel`) into the dev group — the notebook's
    import cell is a no-op in this environment.

## How to run

1. `uv sync` (installs the package and the dev-group notebook tooling).
2. Open `examples/notebooks/mko_data_cleaner_pipeline_example.ipynb` in Jupyter
   Lab / Notebook, or run it headlessly with `jupytext`/`nbclient`.
3. Run all cells top-to-bottom.

## Cell flow

| # | Type | Tag | Purpose |
|---|------|-----|---------|
| 1 | markdown | | Title + purpose + prerequisites |
| 2 | code | `install` | Import-guard `%pip install ...@v0.1.0` (no-op in dev venv) |
| 3 | markdown | | "Import the library and public API" |
| 4 | code | | Imports + `__version__` + `process_data` docstring |
| 5 | markdown | | "Initialize settings" |
| 6 | code | | `initialize_settings(settings_dir=tempdir)` → `app_config.yaml` + `log_config.yaml` |
| 6b | code | | Config override: `index_column=ADID`; `date_column` key dropped (see Troubleshooting) |
| 7 | markdown | | "Expected report directory layout" |
| 8 | code | | Print the layout tree |
| 9 | markdown | | "Create a tiny synthetic report (DELETE / REPLACE / ADD)" |
| 10 | code | | Write the 7-row gzip CSV + 3-rule dictionary |
| 11 | markdown | | "Run the pipeline" |
| 12 | code | | `process_data(report_dir, settings_dir=...)` → `ReportResult` |
| 13 | markdown | | "Inspect the result" |
| 14 | code | | Consume `rows_loaded`/`rows_exported`/`timings`/`export_dir`/`null_data_files` + read the gzip CSVs with polars |
| 15 | markdown | | Notes & troubleshooting |

## Synthetic sample

Data header (README uppercase Mediascope family, `;`-separated, gzipped):

```
YEAR;MONTH;ADVERTISERS;BRANDS;ADID;MEDIA;COST_RUB;QUANTITY
```

7 data rows (`id1`–`id7`): 3× Apple, 2× Brand2, 1× Brand1, 1× Orange, 1× Pear.

Dictionary (`action;match;search;value;term;tag`, `;`-separated, plain text).
The template `col_indexes` are `action=0, match=1, search=2, term=4`; column 5
(`tag`) is the extra mapping column. All three rules target `search=3`, i.e. the
`BRANDS` data-table column (data layout after `clean_names`:
`YEAR=0, MONTH=1, ADVERTISERS=2, BRANDS=3, ADID=4, MEDIA=5, COST_RUB=6,
QUANTITY=7, tag=8`):

| action | match | search | term   | extra `tag` | effect |
|--------|-------|--------|--------|-------------|--------|
| `d`    | `f`   | `3`    | Brand2 | —           | DELETE matching rows |
| `r`    | `f`   | `3`    | Apple  | fruit       | REPLACE `tag` = fruit |
| `a`    | `f`   | `3`    | Brand1 | added_by_brand | ADD `tag` value |

`value` (dictionary column 3) is unused — confirmed by the README and the
template (only `action/match/search/term` are mapped; `term` is the search
string and the extra `tag` column holds the replacement/add value).

### Config override (cell 6b)

The shipped template hardcodes `index_column: AdId` (camelCase) while the sample
uses the uppercase `ADID` header. Cell 6b overrides `index_column=ADID` so the
index lookup matches deterministically (SQLite column-name resolution is
case-insensitive, but we avoid relying on that).

## Verified outputs

Observed by executing the notebook top-to-bottom in the dev venv with
`nbclient` (install cell removed). The `ReportResult`:

- `rows_loaded = 7`
- `rows_exported = 5`
- `timings` keys = `{import, mapping, export}`, all `>= 0`
- `export_dir` = `<report>/clean_data/` (contains `data_table_*.csv.gz`)
- `null_data_files` = `[<report>/null_data_<timestamp>_1.csv.gz]`

Clean-data export (`clean_data/data_table_*.csv.gz`, `;`-separated) — 5 rows:

| BRANDS | tag            | (from 7 rows, Brand2×2 deleted) |
|--------|----------------|---------------------------------|
| Apple  | fruit          | |
| Apple  | fruit          | |
| Brand1 | added_by_brand | |
| Orange | null           | unmatched |
| Pear   | null           | unmatched |

Null-data extract (`null_data_*.csv.gz`) — 2 rows (`BRANDS;tag`): `Orange;null`,
`Pear;null`.

> **Deviation from the original task narrative:** the design note in the plan
> mentioned `rows_exported=3` (counting only the 3 *matched* rows). The actual
> pipeline exports the **entire** `data_table` (7 loaded − 2 `Brand2` rows
> deleted = 5 rows), which includes the 2 unmatched-but-not-deleted rows
> (`Orange`, `Pear`) whose `tag` stays `null`. The `null_data_*` file is a
> separate extract of exactly those unmatched rows. The notebook code prints the
> true `result.rows_exported` (5); this README documents the true counts.

## Troubleshooting

- **`v0.1.0` tag not found.** The install cell pins `git+...@v0.1.0`, which is
  only cut in a later release step. Until then, use the local dev install
  (`uv sync`) and run the notebook in that environment — the import guard makes
  the install cell a no-op.
- **`AttributeError: 'OutStream' object has no attribute 'reconfigure'` (should not occur).** Fixed in the package: `mko_data_cleaner.app` guards the import-time `sys.stdout.reconfigure(line_buffering=True)` call with `hasattr(sys.stdout, "reconfigure")`, so ipykernel's `OutStream` is skipped and the package imports under a real kernel with **no notebook shim** required (line buffering is irrelevant inside a kernel).
- **`SettingsNotInitializedError` / `FileNotFoundError: app_config.yaml`.** Run
  cell 6 (`initialize_settings`) before cell 12 (`process_data`).
- **`DataSettings` validation error on `date_column`.** The model field
  `date_column: str = None` is not `Optional`; an explicit YAML `null` raises a
  `ValidationError`. Cell 6b **drops** the `date_column` key (letting the
  default `None` apply) instead of assigning `None`, which yields the intended
  "no date column" behavior without triggering validation.
  - **Reading the CSVs.** The exported CSVs are `;`-separated (per config).
    `pl.read_csv(path, separator=";")` yields the full column layout; the notebook's
    quick `.head()` reads use the same separator.
- **`ProgrammingError: This API requires PyArrow to be installed`.** This occurs
  when `pyarrow` is present on disk (a `find_spec` lookup succeeds) but cannot be
  imported — a broken or partial installation (e.g. leftover user-site files or a
  failed native-extension install). polars and `adbc-driver-manager` disagree on
  pyarrow availability, so the failure surfaces deep inside
  `DataFrame.write_database(engine="adbc")`. The package detects this state early
  and raises `PyArrowInstallationError` — fix with
  `pip install --force-reinstall pyarrow` in a clean venv.
