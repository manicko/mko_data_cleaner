"""Run 02-clean notebook workflow from this folder (CLI, no installation)."""

from __future__ import annotations

import sys
from pathlib import Path

from mko_data_cleaner import process_data


def _print_missing_paths(missing: list[Path]) -> None:
    if missing:
        print("Missing required paths:")
        for path in missing:
            print(f"  - {path}")
    else:
        print("Missing required paths: none")


def main() -> int:
    workspace_root = Path(__file__).resolve().parent
    settings_workspace = workspace_root
    report_path = workspace_root.parent / "theiss"
    dict_path = report_path / "merged_dictionary.csv"

    print(f"WORKSPACE_ROOT: {workspace_root}")
    print(f"SETTINGS_WORKSPACE: {settings_workspace}")
    print(f"REPORT_PATH: {report_path}")
    print(f"DICT_PATH: {dict_path}")

    required_paths = [
        settings_workspace / "settings" / "app_config.json5",
        settings_workspace / "settings" / "log_config.json5",
        report_path,
        dict_path,
    ]
    missing = [path for path in required_paths if not path.exists()]
    _print_missing_paths(missing)
    if missing:
        print("RUN STATUS: FAILED - required paths are missing. No success claim.")
        return 1

    report_dirs = sorted(
        (path for path in report_path.iterdir() if path.is_dir()),
        key=lambda path: path.name,
    )
    if not report_dirs:
        print("No report subdirectories found in REPORT_PATH.")
        print("RUN STATUS: COMPLETED - no report folders to process, no failures.")
        return 0

    failures = 0
    processed = 0

    for report_dir in report_dirs:
        print()
        print(f"=== Processing report folder: {report_dir.name} ===")
        print(f"report_dir: {report_dir}")
        try:
            result = process_data(
                report_dir,
                settings_dir=settings_workspace,
                dict_path=dict_path.resolve(),
            )
        except Exception as exc:
            failures += 1
            print("RUN STATUS: FAILED - no success claim.")
            print(f"{type(exc).__name__}: {exc}")
            continue

        processed += 1
        print("RUN STATUS: OK")
        print(f"report_dir: {result.report_dir}")
        print(f"rows_loaded: {result.rows_loaded}")
        print(f"rows_exported: {result.rows_exported}")
        print(f"timings: {result.timings}")
        print(f"export_dir: {result.export_dir}")
        print(f"null_data_files: {result.null_data_files}")

    print()
    print("=== Summary ===")
    print(f"Folders processed successfully: {processed}")
    print(f"Folders failed: {failures}")
    if failures:
        print("RUN STATUS: FAILED - one or more folders failed. No success claim.")
        return 1

    print("RUN STATUS: COMPLETED - all report folders processed without failures.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
