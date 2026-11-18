#!/usr/bin/env python3
"""Minimal helper driver for examples/02-clean-data/02-run-cleaning.ps1.

This helper is external to src/mko_data_cleaner and exists only to support the
example workflow reliably (settings/config preflight, safe initialization,
process_data execution, and structured result capture).
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from pathlib import Path
from typing import Any


def _emit(payload: dict[str, Any], exit_code: int = 0) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    sys.stdout.flush()
    return exit_code


def _error_payload(exc: BaseException, action: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "action": action,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
    }
    # Preserve useful structured details when available.
    for attr in ("file_path", "expected_min_columns", "actual_columns", "hint", "step"):
        if hasattr(exc, attr):
            payload[attr] = getattr(exc, attr)
    return payload


def action_probe(_args: argparse.Namespace) -> int:
    info: dict[str, Any] = {
        "ok": False,
        "action": "probe",
        "executable": sys.executable,
        "python_version": sys.version.split()[0],
    }
    try:
        import mko_data_cleaner  # type: ignore

        info["ok"] = True
        info["package_version"] = getattr(mko_data_cleaner, "__version__", None)
        info["module_file"] = getattr(mko_data_cleaner, "__file__", None)
    except Exception as exc:  # noqa: BLE001 - reported to caller
        info.update(_error_payload(exc, "probe"))
        return _emit(info, 1)
    return _emit(info, 0)


def action_settings_init(_args: argparse.Namespace) -> int:
    try:
        from mko_data_cleaner.app import initialize_settings  # type: ignore
        from mko_data_cleaner.core.paths import APP_PATHS  # type: ignore

        settings_dir = APP_PATHS.user_settings_dir
        app_cfg = settings_dir / "app_config.json5"
        log_cfg = settings_dir / "log_config.json5"
        pre = {
            "dir_exists": settings_dir.exists(),
            "app_config_exists": app_cfg.exists(),
            "log_config_exists": log_cfg.exists(),
        }
        path = initialize_settings(force=False)
        post = {
            "dir_exists": settings_dir.exists(),
            "app_config_exists": app_cfg.exists(),
            "log_config_exists": log_cfg.exists(),
        }
        payload = {
            "ok": True,
            "action": "settings-init",
            "executable": sys.executable,
            "settings_path": str(path),
            "pre": pre,
            "post": post,
            "created_app_config": (not pre["app_config_exists"]) and post["app_config_exists"],
            "created_log_config": (not pre["log_config_exists"]) and post["log_config_exists"],
        }
        return _emit(payload, 0)
    except Exception as exc:  # noqa: BLE001
        payload = _error_payload(exc, "settings-init")
        payload["executable"] = sys.executable
        payload["traceback"] = traceback.format_exc()
        return _emit(payload, 1)


def action_config_info(_args: argparse.Namespace) -> int:
    try:
        from mko_data_cleaner.core.paths import APP_PATHS  # type: ignore
        import mko_data_cleaner.core.utils as utils  # type: ignore

        settings_dir = APP_PATHS.user_settings_dir
        app_config_path = APP_PATHS.app_config
        log_config_path = APP_PATHS.log_config
        missing = []
        if not app_config_path.is_file():
            missing.append(app_config_path.name)
        if not log_config_path.is_file():
            missing.append(log_config_path.name)
        if missing:
            return _emit(
                {
                    "ok": False,
                    "action": "config-info",
                    "executable": sys.executable,
                    "settings_path": str(settings_dir),
                    "app_config_path": str(app_config_path),
                    "log_config_path": str(log_config_path),
                    "missing_files": missing,
                    "error_type": "SettingsNotInitializedError",
                    "error_message": "Default settings are missing required file(s): " + ", ".join(missing),
                },
                1,
            )

        config = utils.config_to_dict(app_config_path)
        data_paths = config.get("data_paths") or {}
        data_file_settings = config.get("data_file_settings") or {}
        dict_file_settings = config.get("dict_file_settings") or {}
        import_folder = str(data_paths.get("import_folder", "raw_data"))
        export_folder = str(data_paths.get("export_folder", "clean_data"))
        db_file = str(data_paths.get("db_file", "data_base/db_example.db"))
        dict_file = str(data_paths.get("dict_file", "dict/merged_dictionary.csv"))
        extension = str(data_file_settings.get("extension", "gz"))
        dict_extension = str(dict_file_settings.get("extension", "gz"))

        payload = {
            "ok": True,
            "action": "config-info",
            "executable": sys.executable,
            "settings_path": str(settings_dir),
            "app_config_path": str(app_config_path),
            "log_config_path": str(log_config_path),
            "import_folder": import_folder,
            "export_folder": export_folder,
            "db_file": db_file,
            "dict_file": dict_file,
            "data_extension": extension,
            "dict_extension": dict_extension,
            "clear_export_folder_default": bool(config.get("clear_export_folder", False)),
            "force_default": bool(config.get("force", False)),
        }
        return _emit(payload, 0)
    except Exception as exc:  # noqa: BLE001
        payload = _error_payload(exc, "config-info")
        payload["executable"] = sys.executable
        payload["traceback"] = traceback.format_exc()
        return _emit(payload, 1)


def _parse_bool(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")


def action_run(args: argparse.Namespace) -> int:
    try:
        from mko_data_cleaner import __version__ as package_version  # type: ignore
        from mko_data_cleaner.app import process_data  # type: ignore

        report_dir = Path(args.report_dir).expanduser()
        dict_path = Path(args.dict_path).expanduser()
        clear_export_folder = bool(args.clear_export_folder)
        force = bool(args.force)

        result = process_data(
            report_path=report_dir,
            dict_path=dict_path,
            clear_export_folder=clear_export_folder,
            force=force,
        )

        # ReportResult is a Pydantic model in current API.
        if hasattr(result, "model_dump"):
            result_data = result.model_dump(mode="json")
        else:
            result_data = dict(result)  # type: ignore[arg-type]

        payload = {
            "ok": True,
            "action": "run",
            "executable": sys.executable,
            "package_version": package_version,
            "result": result_data,
        }
        return _emit(payload, 0)
    except Exception as exc:  # noqa: BLE001
        payload = _error_payload(exc, "run")
        payload["executable"] = sys.executable
        payload["traceback"] = traceback.format_exc()
        return _emit(payload, 1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Helper driver for 02-run-cleaning.ps1")
    parser.add_argument(
        "--action",
        required=True,
        choices=["probe", "settings-init", "config-info", "run"],
        help="Action to perform.",
    )
    parser.add_argument("--report-dir", help="Resolved report directory for run action.")
    parser.add_argument("--dict-path", help="Absolute dictionary file path for run action.")
    parser.add_argument(
        "--clear-export-folder",
        type=_parse_bool,
        help="Explicit clear_export_folder boolean for run action.",
    )
    parser.add_argument(
        "--force",
        type=_parse_bool,
        help="Explicit force boolean for run action.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.action == "probe":
            return action_probe(args)
        if args.action == "settings-init":
            return action_settings_init(args)
        if args.action == "config-info":
            return action_config_info(args)
        if args.action == "run":
            missing = []
            if not args.report_dir:
                missing.append("--report-dir")
            if not args.dict_path:
                missing.append("--dict-path")
            if args.clear_export_folder is None:
                missing.append("--clear-export-folder")
            if args.force is None:
                missing.append("--force")
            if missing:
                return _emit(
                    {
                        "ok": False,
                        "action": "run",
                        "error_type": "ArgumentError",
                        "error_message": "Missing required argument(s): " + ", ".join(missing),
                    },
                    2,
                )
            return action_run(args)

        return _emit(
            {
                "ok": False,
                "action": str(args.action),
                "error_type": "ArgumentError",
                "error_message": f"Unsupported action: {args.action}",
            },
            2,
        )
    except Exception as exc:  # noqa: BLE001
        payload = _error_payload(exc, getattr(args, "action", "unknown"))
        payload["traceback"] = traceback.format_exc()
        return _emit(payload, 1)


if __name__ == "__main__":
    raise SystemExit(main())
