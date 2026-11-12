# 02-clean local run scripts

Local Windows scripts for running the 02-clean report processing workflow from
`examples/notebooks/02-clean/` without notebook installation or preview cells.
They mirror the notebook processing flow (path defaults, per-folder
`process_data(...)` calls, and failure status messages) with CLI-only output.

## Files

- `run_report.py`: Python entrypoint.
- `run_report.bat`: Windows Command Prompt launcher.
- `run_report.ps1`: PowerShell launcher.

## Prerequisites

- Python environment with `mko_data_cleaner` installed and importable by the
  selected launcher (`py -3`, `python`, or `uv run python`).
- Existing settings files in `<script_dir>/settings/`:
  - `app_config.json5`
  - `log_config.json5`
- Report workspace and dictionary at the default paths (or update
  `run_report.py` for a different layout).

## Defaults and behavior

`run_report.py` resolves paths relative to its own directory:

- `WORKSPACE_ROOT = <script_dir>`
- `SETTINGS_WORKSPACE = <script_dir>`
- `REPORT_PATH = <script_dir>/../theiss` (i.e. `examples/notebooks/theiss`)
- `DICT_PATH = REPORT_PATH/merged_dictionary.csv` (passed to `process_data` as an
  absolute path)

On startup the script prints resolved paths and performs missing-path checks for:

- `<SETTINGS_WORKSPACE>/settings/app_config.json5`
- `<SETTINGS_WORKSPACE>/settings/log_config.json5`
- `REPORT_PATH`
- `DICT_PATH`

If required paths are missing, it prints a clear failure status and exits with
code `1` without claiming success.

If `REPORT_PATH` exists, the script iterates subdirectories in sorted name order
and, for each folder:

1. Prints the folder/run header.
2. Calls
   `process_data(report_dir, settings_dir=SETTINGS_WORKSPACE, dict_path=DICT_PATH.resolve())`.
3. On success, prints `RUN STATUS: OK` and key `ReportResult` fields:
   `report_dir`, `rows_loaded`, `rows_exported`, `timings`, `export_dir`,
   `null_data_files`.
4. On failure, prints `RUN STATUS: FAILED - no success claim.` with the exception
   type/message, then continues to the next folder.

The script prints a final summary and exits with:

- `0` when all processed folders complete without failures (including the case
  where no report subdirectories are found).
- `1` when required setup/data is missing or one or more folders fail.

The scripts do not install dependencies, do not modify notebooks, and do not
print DataFrame/CSV previews.

## Running

From `examples/notebooks/02-clean/`:

```bat
run_report.bat
```

```powershell
.\run_report.ps1
```

```bash
py -3 run_report.py
# or: python run_report.py
# or: uv run python run_report.py
```

`run_report.bat` and `run_report.ps1` set the current directory to the script
directory, try Python launchers in order (`py -3`, `python`, `uv run python`),
keep output visible, pause before closing on success or failure, and propagate or
report the script exit code.

## Notes

- Core library output (stdout progress/banners and logging) may interleave with
  script messages.
- `process_data` resolves report/export behavior according to the active
  `app_config.json5`; export-folder conflicts and per-report data issues follow
  existing library behavior.
- Do not commit generated logs, notebook outputs, or local data artifacts.
