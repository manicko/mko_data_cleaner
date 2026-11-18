# Example notebook: `02-clean_data.ipynb`

`examples/notebooks/02-clean_data.ipynb` is a working notebook for running
`mko_data_cleaner.process_data` against local report folders. It is **not**
self-contained: you must provide initialized settings, a report workspace, and a
dictionary file on disk before running the processing cells.

> The notebook is **input-only**: keep outputs cleared before committing.

## Prerequisites

- Python >= 3.12 and a Jupyter environment (`jupyterlab`, `notebook`, or VS Code).
- `mko_data_cleaner` installed in the active kernel environment.
- `polars` available in the notebook environment (used for inspecting exported CSVs).
- Initialized settings directory containing:
  - `settings/app_config.json5`
  - `settings/log_config.json5`
- Local report data and a dictionary CSV accessible from the paths configured in the notebook.

## Install cell

The first code cell can install/update the package from GitHub:

- `REPO_URL`: source repository (`https://github.com/manicko/mko_data_cleaner.git`)
- `GIT_REF`: ref to install (`master` by default; can be changed to a commit SHA or tag)
- `FORCE_REINSTALL`: when `True`, reinstalls even if the package is already present

If `FORCE_REINSTALL=True` or the package is missing, the cell runs a `pip install --upgrade`
from the selected Git ref and prints a reminder to restart the kernel if packages/imports changed.

## Notebook flow

| Cell | Type | Purpose |
|---|---|---|
| 1 | markdown | Placeholder/title cell (empty). |
| 2 | code | Optional install/update of `mko_data_cleaner` from a Git ref. |
| 3 | markdown | Import section header. |
| 4 | code | Imports `Path`, `polars`, `process_data`, `ReportResult`; prints package version and `process_data` summary. |
| 5 | markdown | Path/report configuration header. |
| 6 | code | Defines `WORKSPACE_ROOT`, `SETTINGS_WORKSPACE`, `REPORT_PATH`, and `DICT_PATH`; prints resolved paths. |
| 7 | code | Minimal completeness checks for required settings/report/dictionary paths. |
| 8 | markdown | Run section header. |
| 9 | code | Iterates subdirectories in `REPORT_PATH` and runs `process_data(...)` for each; catches and prints exceptions per folder. |
| 10 | markdown | Result fields overview (`ReportResult`). |
| 11 | code | Empty placeholder cell. |
| 12 | code | Prints result fields and previews exported `clean_data` / `null_data` CSVs with Polars. |

## Path configuration

The notebook uses portable defaults derived from the current working directory:

```python
WORKSPACE_ROOT = Path.cwd()
SETTINGS_WORKSPACE = WORKSPACE_ROOT
REPORT_PATH = WORKSPACE_ROOT.parent / "theiss"
DICT_PATH = REPORT_PATH / "merged_dictionary.csv"
```

Adjust these values to match your environment before running:

- `SETTINGS_WORKSPACE` must contain `settings/app_config.json5` and `settings/log_config.json5`
  (create them with `initialize_settings(...)` or `mko-data-cleaner init --settings-dir ...`).
- `REPORT_PATH` is expected to contain report subdirectories to process.
- `DICT_PATH` is passed as `dict_path=...` to `process_data`; it must point to an existing dictionary file.

## Processing behavior

For each directory entry under `REPORT_PATH`, the run cell:

1. Prints the folder name.
2. Calls `process_data(report_dir, settings_dir=SETTINGS_WORKSPACE, dict_path=DICT_PATH)`.
3. Prints the returned `ReportResult`.
4. On failure, prints `RUN STATUS: FAILED - no success claim.` and the exception type/message.

`process_data` resolves report paths and export behavior according to the active `app_config.json5`.
The returned `ReportResult` includes fields such as `rows_loaded`, `rows_exported`, `timings`,
`export_dir`, `null_data_files`, `import_dir`, `report_dir`, and `db_path`.

## Result inspection

The final cell assumes a `result` variable is available and reads:

- `result.rows_loaded`, `result.rows_exported`, `result.timings`
- `result.export_dir`, `result.null_data_files`
- the first `*.csv.gz` file in `export_dir`
- the first `null_data` file, if present

It previews CSVs with `pl.read_csv(..., separator=";")`. If the processing loop did not produce
a usable `result` for the current kernel state, run/configure the earlier cells and re-run the
inspection cell after a successful `process_data` call.

## Notes

- `initialize_settings()` is not called by this notebook; settings must already exist at
  `SETTINGS_WORKSPACE/settings/`.
- The notebook passes `dict_path` explicitly. If you prefer the default dictionary resolution,
  remove the `dict_path=...` argument and ensure the report layout matches your config.
- Do not commit notebook outputs, local paths, credentials, or generated data artifacts.
