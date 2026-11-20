"""Tests for the app-level public API (B1/T1 + B4/T4)."""

import gzip
import logging
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import json5
import polars as pl
import pytest
import typer
from pydantic import ValidationError
from typer.testing import CliRunner

from mko_data_cleaner.app import app, initialize_settings, process_data
from mko_data_cleaner.core.app_service import AppService, get_app_service
from mko_data_cleaner.core.csv_service import CSVWorker
from mko_data_cleaner.core.errors import (
    DictionaryStructureError,
    ExportFolderConflictError,
    SettingsNotInitializedError,
)
from mko_data_cleaner.core.models import DataFileExtension, ReportResult
from mko_data_cleaner.core.paths import APP_DIR, APP_NAME, AppPaths
from mko_data_cleaner.core.utils import config_to_dict
from mko_data_cleaner.notebook import check_notebook_environment

# Path to the package's shipped settings templates (app_config.json5 / log_config.json5)
TEMPLATE_SETTINGS_DIR = APP_DIR / "settings"


# --------------------------------------------------------------------------- #
# Shared fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def populated_settings(tmp_path: Path) -> Path:
    """A platformdirs-style user dir whose ``settings/`` is populated with templates."""
    user_dir = tmp_path / "userdir"
    settings_subdir = user_dir / "settings"
    settings_subdir.mkdir(parents=True)
    for name in ("app_config.json5", "log_config.json5"):
        shutil.copy2(TEMPLATE_SETTINGS_DIR / name, settings_subdir / name)
    return user_dir


@pytest.fixture
def empty_settings_dir(tmp_path: Path) -> Path:
    """A user dir with no ``settings/`` subdir (settings not initialized)."""
    return tmp_path / "empty_cfg"


@pytest.fixture
def empty_report_dir(tmp_path: Path) -> Path:
    """An existing but empty report directory."""
    report_dir = tmp_path / "report"
    report_dir.mkdir()
    return report_dir


@pytest.fixture(autouse=True)
def _isolate_logging():
    """Restore global logging state changed by ``dictConfig`` so tests don't leak."""
    import logging

    root = logging.getLogger()
    saved_root_handlers = root.handlers[:]
    saved_root_level = root.level
    saved_loggers: dict[str, tuple[list, int, bool, bool]] = {}
    for name in list(root.manager.loggerDict):
        lg = logging.getLogger(name)
        saved_loggers[name] = (lg.handlers[:], lg.level, lg.disabled, lg.propagate)
    yield
    root.handlers[:] = saved_root_handlers
    root.level = saved_root_level
    for name, (handlers, level, disabled, propagate) in saved_loggers.items():
        lg = logging.getLogger(name)
        lg.handlers[:] = handlers
        lg.setLevel(level)
        lg.disabled = disabled
        lg.propagate = propagate


# --------------------------------------------------------------------------- #
# 1. Import-time side effects
# --------------------------------------------------------------------------- #
def test_import_app_does_not_create_settings_dir(tmp_path: Path):
    """Importing ``mko_data_cleaner.app`` must not create the settings directory."""
    target = str(tmp_path)
    script = (
        "import platformdirs, os, sys\n"
        "platformdirs.user_config_dir = lambda *a, **k: sys.argv[1]\n"
        "settings_dir = os.path.join(sys.argv[1], 'settings')\n"
        "import mko_data_cleaner.app as _app_module\n"
        "from mko_data_cleaner.app import process_data, initialize_settings\n"
        "if os.path.isdir(settings_dir):\n"
        "    print('FAIL: settings dir created', file=sys.stderr)\n"
        "    sys.exit(1)\n"
        "print('OK')\n"
        "sys.exit(0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, target],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_import_survives_stdout_without_reconfigure():
    """Importing must not crash when sys.stdout lacks reconfigure() (ipykernel).

    ``mko_data_cleaner.app`` calls ``sys.stdout.reconfigure(line_buffering=True)``
    at import time. Jupyter/ipykernel replace ``sys.stdout`` with an ``OutStream``
    that has no ``reconfigure``, which previously raised ``AttributeError`` and
    broke ``import mko_data_cleaner`` inside any kernel — the project's primary
    "Jupyter pipeline step" entry point. Run in a subprocess with a stdout that
    lacks ``reconfigure`` to avoid import-state pollution of the test process.
    """
    script = (
        "import sys\n"
        "class _NoReconStream:\n"
        "    encoding = 'utf-8'\n"
        "    def write(self, _s): pass\n"
        "    def flush(self): pass\n"
        "sys.stdout = _NoReconStream()\n"
        "import mko_data_cleaner  # noqa: F401\n"
        "import mko_data_cleaner.app  # noqa: F401\n"
        "sys.stderr.write('RECONFIG_OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "RECONFIG_OK" in result.stderr


# --------------------------------------------------------------------------- #
# 2-4. process_data without settings
# --------------------------------------------------------------------------- #
def test_process_data_without_settings_raises_settings_error(
    empty_report_dir: Path, empty_settings_dir: Path
):
    with pytest.raises(SettingsNotInitializedError):
        process_data(empty_report_dir, settings_dir=empty_settings_dir)


def test_process_data_error_is_file_not_found(
    empty_report_dir: Path, empty_settings_dir: Path
):
    with pytest.raises(SettingsNotInitializedError) as exc_info:
        process_data(empty_report_dir, settings_dir=empty_settings_dir)
    assert isinstance(exc_info.value, FileNotFoundError)
    assert not isinstance(exc_info.value, ValidationError)


def test_process_data_error_names_initialize_settings(
    empty_report_dir: Path, empty_settings_dir: Path
):
    with pytest.raises(SettingsNotInitializedError) as exc_info:
        process_data(empty_report_dir, settings_dir=empty_settings_dir)
    message = str(exc_info.value)
    assert "initialize_settings()" in message
    assert "app_config.json5" in message


# --------------------------------------------------------------------------- #
# 5. CLI run without settings
# --------------------------------------------------------------------------- #
def test_run_cli_without_settings_raises_settings_error(
    empty_report_dir: Path, empty_settings_dir: Path
):
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", str(empty_report_dir), "--settings-dir", str(empty_settings_dir)],
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, SettingsNotInitializedError)
    assert "settings" in str(result.exception).lower()


# --------------------------------------------------------------------------- #
# 6-7. list_settings
# --------------------------------------------------------------------------- #
def test_list_settings_lists_when_dir_exists(populated_settings: Path):
    runner = CliRunner()
    result = runner.invoke(
        app, ["list-settings", "--settings-dir", str(populated_settings)]
    )
    assert result.exit_code == 0
    assert "app_config.json5" in result.output
    assert "log_config.json5" in result.output


def test_list_settings_does_not_raise_when_dir_missing(tmp_path: Path):
    runner = CliRunner()
    missing_dir = tmp_path / "missing"
    result = runner.invoke(app, ["list-settings", "--settings-dir", str(missing_dir)])
    assert result.exit_code == 0
    assert result.exception is None


# --------------------------------------------------------------------------- #
# 8. init CLI creates settings in a tmp dir
# --------------------------------------------------------------------------- #
def test_init_cli_creates_settings_in_tmp_dir(tmp_path: Path, monkeypatch):
    tmp_app_paths = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=tmp_path)
    monkeypatch.setattr("mko_data_cleaner.core.init_service.APP_PATHS", tmp_app_paths)

    runner = CliRunner()
    result = runner.invoke(app, ["init", "--force"])
    assert result.exit_code == 0, result.output

    dest = tmp_path / "settings"
    assert (dest / "app_config.json5").exists()
    assert (dest / "log_config.json5").exists()


# --------------------------------------------------------------------------- #
# 9-10. Settings present: factory constructs / process_data past the gate
# --------------------------------------------------------------------------- #
def test_get_app_service_constructs_after_initialization(populated_settings: Path):
    service = get_app_service(settings_dir=populated_settings)
    # Verify config parsed from shipped template, not just non-None
    assert service.app_config.data_file_settings.index_column == "AdId"
    assert service.app_config.database_settings.table_name == "data_table"
    assert service.app_config.data_file_settings.extension == DataFileExtension.gz
    assert service.log_config.version == 1
    assert service.app_config.data_paths.import_folder == Path("raw_data")
    assert service.app_config.data_paths.export_folder == Path("clean_data")


def test_process_data_past_gate_does_not_raise_settings_error(
    populated_settings: Path, synthetic_report: Path
):
    """Verify process_data passes the settings gate and completes with valid data."""
    result = process_data(synthetic_report, settings_dir=populated_settings)
    assert isinstance(result, ReportResult)
    assert result.rows_loaded >= 0
    assert result.rows_exported >= 0
    assert result.duration > 0.0
    assert set(result.timings.keys()) == {"import", "mapping", "export"}


# --------------------------------------------------------------------------- #
# 11-12. config_to_dict defense-in-depth
# --------------------------------------------------------------------------- #
def test_config_to_dict_returns_empty_dict_on_missing_file(tmp_path: Path):
    result = config_to_dict(tmp_path / "does_not_exist.json5")
    assert result == {}


def test_config_to_dict_returns_empty_dict_on_invalid_json5(tmp_path: Path):
    bad_file = tmp_path / "bad.json5"
    bad_file.write_text("key: value: extra\n", encoding="utf8")
    result = config_to_dict(bad_file)
    assert result == {}


# ------------------------------------------------------------------- #
# B4: ReportResult model contract
# ------------------------------------------------------------------- #
def test_report_result_model_construction_and_serialization():
    """ReportResult accepts all 11 fields and serializes via model_dump()."""
    result = ReportResult(
        report_dir=Path("/tmp/report"),
        import_dir=Path("/tmp/report/raw_data"),
        export_dir=Path("/tmp/report/clean_data"),
        db_path=Path("/tmp/report/data_base/db.db"),
        null_data_files=[],
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        finished_at=datetime(2024, 1, 1, 12, 0, 5),
        duration=5.0,
        timings={"import": 1.0, "mapping": 3.0, "export": 1.0},
        rows_loaded=100,
        rows_exported=95,
    )
    assert isinstance(result.report_dir, Path)
    assert isinstance(result.null_data_files, list)
    assert isinstance(result.timings, dict)
    assert result.rows_loaded == 100
    assert result.rows_exported == 95
    assert result.duration == 5.0
    dumped = result.model_dump()
    assert isinstance(dumped["started_at"], datetime)
    assert dumped["rows_loaded"] == 100


# ------------------------------------------------------------------- #
# B4: process_data returns ReportResult (mock get_app_service)
# ------------------------------------------------------------------- #
def test_process_data_returns_report_result(
    monkeypatch, populated_settings: Path, empty_report_dir: Path
):
    """process_data returns the ReportResult from run_report."""
    fake_result = ReportResult(
        report_dir=empty_report_dir,
        import_dir=empty_report_dir / "raw_data",
        export_dir=empty_report_dir / "clean_data",
        db_path=empty_report_dir / "data_base/db_example.db",
        null_data_files=[],
        started_at=datetime.now(),
        finished_at=datetime.now(),
        duration=0.5,
        timings={"import": 0.1, "mapping": 0.2, "export": 0.2},
        rows_loaded=42,
        rows_exported=40,
    )
    fake_service = MagicMock()
    fake_service.run_report.return_value = fake_result
    monkeypatch.setattr(
        "mko_data_cleaner.app.get_app_service",
        lambda settings_dir=None, dict_path=None, clear_export_folder=None, force=None, deduplicate=None: (
            fake_service
        ),
    )
    result = process_data(empty_report_dir, settings_dir=populated_settings)
    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 42
    assert result.rows_exported == 40
    fake_service.run_report.assert_called_once_with(empty_report_dir)


# ------------------------------------------------------------------- #
# B4: settings_dir threading — initialize_settings
# ------------------------------------------------------------------- #
def test_initialize_settings_threads_settings_dir(tmp_path: Path):
    """initialize_settings(settings_dir=...) writes to settings_dir / settings."""
    user_dir = tmp_path / "myapp"
    result = initialize_settings(settings_dir=user_dir)
    assert result == user_dir / "settings"
    assert (result / "app_config.json5").exists()
    assert (result / "log_config.json5").exists()


def test_initialize_settings_force_overwrites(tmp_path: Path):
    """initialize_settings(force=True, settings_dir=...) overwrites existing files."""
    user_dir = tmp_path / "myapp"
    dest = user_dir / "settings"
    dest.mkdir(parents=True)
    (dest / "app_config.json5").write_text("stale")

    result = initialize_settings(force=True, settings_dir=user_dir)
    assert (result / "app_config.json5").exists()
    # The template should have overwritten the stale file
    assert "data_paths" in (result / "app_config.json5").read_text()


# ------------------------------------------------------------------- #
# B4: settings_dir threading — CLI init --settings-dir
# ------------------------------------------------------------------- #
def test_init_cli_with_settings_dir(tmp_path: Path):
    """CLI `init --settings-dir X --force` writes to X / settings."""
    user_dir = tmp_path / "myapp"
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["init", "--settings-dir", str(user_dir), "--force"],
    )
    assert result.exit_code == 0, result.output
    dest = user_dir / "settings"
    assert (dest / "app_config.json5").exists()
    assert (dest / "log_config.json5").exists()


# ------------------------------------------------------------------- #
# B4: top-level re-exports
# ------------------------------------------------------------------- #
def test_top_level_exports():
    """import mko_data_cleaner exposes exactly the 6 public names."""
    import mko_data_cleaner

    expected = {
        "check_notebook_environment",
        "ReportResult",
        "SettingsNotInitializedError",
        "__version__",
        "initialize_settings",
        "process_data",
    }
    assert set(mko_data_cleaner.__all__) == expected
    assert isinstance(mko_data_cleaner.__version__, str)
    assert len(mko_data_cleaner.__version__) > 0
    assert mko_data_cleaner.process_data is process_data
    assert mko_data_cleaner.initialize_settings is initialize_settings
    assert mko_data_cleaner.ReportResult is ReportResult
    assert mko_data_cleaner.SettingsNotInitializedError is SettingsNotInitializedError
    assert mko_data_cleaner.check_notebook_environment is check_notebook_environment


# ------------------------------------------------------------------- #
# B4: minimal end-to-end pipeline (bridge to B5)
# ------------------------------------------------------------------- #
def test_process_data_end_to_end(populated_settings: Path, synthetic_report: Path):
    """Run a tiny synthetic report and verify the ReportResult fields."""
    report_dir = synthetic_report

    result = process_data(report_dir, settings_dir=populated_settings)

    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 3
    assert result.rows_exported == 2  # Brand2 row deleted
    assert result.rows_exported <= result.rows_loaded
    assert result.export_dir == report_dir / "clean_data"
    assert result.import_dir == report_dir / "raw_data"
    assert result.report_dir == report_dir
    assert (result.export_dir).exists()
    assert len(list(result.export_dir.glob("*.gz"))) > 0
    assert isinstance(result.null_data_files, list)
    assert result.duration > 0.0
    assert set(result.timings.keys()) == {"import", "mapping", "export"}
    assert all(v >= 0.0 for v in result.timings.values())

    # ── Verify DELETE and ADD rules by inspecting exported CSV content ──
    export_files = sorted(result.export_dir.glob("*.csv.gz"))
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)

    # Collect all string cell values
    all_cells = []
    for col in merged.columns:
        for val in merged[col].to_list():
            if val is not None:
                all_cells.append(str(val))

    # DELETE rule: Brand2 must be absent from all cells
    assert "Brand2" not in all_cells

    # Brand1 must be present (2 surviving rows)
    assert all_cells.count("Brand1") == 2

    # ADD rule: tag column populated with "added_by_brand" value for Brand1 rows
    assert "tag" in merged.columns
    tag_values = merged["tag"].drop_nulls().to_list()
    assert len(tag_values) > 0
    assert "added_by_brand" in tag_values


# ------------------------------------------------------------------- #
# B4: missing index_column — warn and continue cleaning full dataset
# ------------------------------------------------------------------- #
def test_process_data_missing_index_column_warns_and_continues(
    populated_settings: Path,
    missing_index_column_report: Path,
):
    """When the configured ``index_column`` (e.g. ``AdId``) is absent from the
    CSV data, ``process_data`` must log a WARNING, set ``index_column`` to
    ``None``, and still clean/export the full dataset (no crash).
    """
    report_dir = missing_index_column_report

    # Collect "app_service" log records. Attach the handler directly to that
    # logger because process_data() reconfigures logging via dictConfig (which
    # resets root handlers), so pytest's caplog cannot capture these records.
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    app_logger = logging.getLogger("app_service")
    collector = _Collector(level=logging.DEBUG)
    app_logger.addHandler(collector)
    try:
        result = process_data(report_dir, settings_dir=populated_settings)
    finally:
        app_logger.removeHandler(collector)

    # process completed successfully and exported data without the melt step
    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 3
    assert result.rows_exported == 2  # Brand2 row deleted by dictionary rule
    assert result.rows_exported <= result.rows_loaded
    assert result.export_dir == report_dir / "clean_data"
    assert (result.export_dir).exists()
    assert len(list(result.export_dir.glob("*.gz"))) > 0

    # the missing index column produced a warning
    messages = [record.getMessage() for record in records]
    assert any(
        "Index column 'AdId' not found in data columns" in msg for msg in messages
    ), messages
    assert any(
        "Proceeding without index/melt" in msg for msg in messages
    ), messages

    # the dictionary rules still applied: Brand2 deleted, Brand1 survived
    export_files = sorted(result.export_dir.glob("*.csv.gz"))
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)

    all_cells = [
        str(v)
        for col in merged.columns
        for v in merged[col].to_list()
        if v is not None
    ]
    assert "Brand2" not in all_cells  # DELETE rule applied without index table
    assert all_cells.count("Brand1") == 2


# ------------------------------------------------------------------- #
# B4: nonexistent directory raises FileNotFoundError
# ------------------------------------------------------------------- #
def test_process_data_nonexistent_directory_raises_filenotfound(populated_settings):
    """process_data raises FileNotFoundError for a path that doesn't exist."""
    nonexistent = Path("/nonexistent/path/that/does/not/exist")
    with pytest.raises(FileNotFoundError, match="Directory not found"):
        process_data(nonexistent, settings_dir=populated_settings)


# ------------------------------------------------------------------- #
# B4: CLI run success (end-to-end via CliRunner)
# ------------------------------------------------------------------- #
def test_run_cli_success(populated_settings, synthetic_report):
    """CLI run command succeeds with populated settings and valid data."""
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", str(synthetic_report), "--settings-dir", str(populated_settings)],
    )
    assert result.exit_code == 0, result.output
    assert "Report complete" in result.output
    assert "3 loaded → 2 clean" in result.output


# ------------------------------------------------------------------- #
# B4: ReportResult rejects negative row counts (NonNegativeInt validation)
# ------------------------------------------------------------------- #
def test_report_result_rejects_negative_row_counts():
    """ReportResult rejects negative row counts with ValidationError."""
    common = dict(
        report_dir=Path("/tmp/report"),
        import_dir=Path("/tmp/raw_data"),
        export_dir=Path("/tmp/clean_data"),
        db_path=Path("/tmp/test.db"),
        null_data_files=[],
        started_at=datetime(2026, 1, 1, 12, 0, 0),
        finished_at=datetime(2026, 1, 1, 12, 0, 5),
        duration=5.0,
        timings={"import": 1.0, "mapping": 2.0, "export": 2.0},
    )
    with pytest.raises(ValidationError):
        ReportResult(rows_loaded=-1, rows_exported=95, **common)
    with pytest.raises(ValidationError):
        ReportResult(rows_loaded=100, rows_exported=-5, **common)


# ------------------------------------------------------------------- #
# B1: separator mismatch raises DictionaryStructureError (integration)
# ------------------------------------------------------------------- #
def test_process_data_separator_mismatch_raises(
    separator_mismatch_report: Path, populated_settings: Path
):
    """process_data on a comma-separated-dict report raises DictionaryStructureError
    mentioning the merged dictionary file and a separator hint."""
    with pytest.raises(DictionaryStructureError) as exc_info:
        process_data(separator_mismatch_report, settings_dir=populated_settings)

    msg = str(exc_info.value)
    assert "merged_dictionary.csv" in msg
    assert "separator" in msg
    assert "reading dictionary" in msg


def test_run_cli_separator_mismatch(
    separator_mismatch_report: Path, populated_settings: Path
):
    """CLI run on a separator-mismatch report exits 1 and prints the error."""
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            str(separator_mismatch_report),
            "--settings-dir",
            str(populated_settings),
        ],
    )
    assert result.exit_code == 1
    assert "Ошибка" in result.output
    assert "merged_dictionary.csv" in result.output
    assert "separator" in result.output


# ------------------------------------------------------------------- #
# B5: dict_path override feature
# ------------------------------------------------------------------- #
def test_process_data_with_dict_path_override(
    populated_settings: Path,
    synthetic_report: Path,
    external_dict_file: Path,
):
    """An external abs dict_path is read and applied (Brand2 deleted, Brand1 tagged)."""
    result = process_data(
        synthetic_report,
        settings_dir=populated_settings,
        dict_path=external_dict_file,
    )
    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 3
    assert result.rows_exported == 2  # Brand2 deleted

    export_files = sorted(result.export_dir.glob("*.csv.gz"))
    assert export_files
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)

    all_cells = [
        str(v) for col in merged.columns for v in merged[col].to_list() if v is not None
    ]
    assert "Brand2" not in all_cells
    assert all_cells.count("Brand1") == 2
    assert "added_by_brand" in merged["tag"].drop_nulls().to_list()


def test_process_data_without_dict_path_uses_default(
    populated_settings: Path,
    synthetic_report: Path,
):
    """Without dict_path the config default <report_dir>/dict/merged_dictionary.csv is used."""
    # Direct property-level guard: no override → default path resolution.
    service = get_app_service(settings_dir=populated_settings)
    assert service._dict_path is None
    service.base_path = synthetic_report
    default_dict = synthetic_report / "dict" / "merged_dictionary.csv"
    assert service.dict_path == default_dict
    assert service.dict_path.is_file()

    # End-to-end behavior is unchanged by the feature.
    result = process_data(synthetic_report, settings_dir=populated_settings)
    assert result.rows_loaded == 3
    assert result.rows_exported == 2


def test_process_data_dict_path_missing_raises_filenotfound(
    populated_settings: Path,
    empty_report_dir: Path,
):
    """A non-existent override dict_path raises FileNotFoundError with the resolved path."""
    missing = empty_report_dir / "does_not_exist.csv"
    with pytest.raises(FileNotFoundError) as exc_info:
        process_data(
            empty_report_dir,
            settings_dir=populated_settings,
            dict_path=missing,
        )
    msg = str(exc_info.value)
    assert "Dictionary file not found" in msg
    assert str(missing) in msg
    assert str(missing.resolve()) in msg


def test_run_cli_with_dict_path(
    populated_settings: Path,
    synthetic_report: Path,
    external_dict_file: Path,
):
    """CLI run with --dict-path points to the external dictionary file."""
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            str(synthetic_report),
            "--settings-dir",
            str(populated_settings),
            "--dict-path",
            str(external_dict_file),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Report complete" in result.output
    assert "3 loaded → 2 clean" in result.output


def test_dict_path_relative_resolves_against_report_dir(
    populated_settings: Path,
    synthetic_report: Path,
):
    """A relative dict_path resolves against the report directory (Q1) and is applied."""
    # Place a custom dictionary in the report root with rules that differ from
    # the default (DELETE Brand1, add tag to Brand2) so a fallback to the
    # report-dir default dict would produce different output.
    custom = synthetic_report / "custom_dict.csv"
    custom.write_text(
        "action;match;search;value;term;tag\n"
        "d;f;4;NULL;Brand1;\n"
        "a;f;4;NULL;Brand2;kept_by_brand\n",
        encoding="utf-8",
    )

    # Property-level check: relative path resolves against base_path (report dir).
    service = get_app_service(
        settings_dir=populated_settings, dict_path="custom_dict.csv"
    )
    service.base_path = synthetic_report
    assert service.dict_path == custom
    assert service.dict_path.is_file()

    # End-to-end: the override (not the default) dict is applied.
    result = process_data(
        synthetic_report,
        settings_dir=populated_settings,
        dict_path="custom_dict.csv",
    )
    assert result.rows_loaded == 3
    assert result.rows_exported == 1  # both Brand1 rows deleted by override dict

    export_files = sorted(result.export_dir.glob("*.csv.gz"))
    assert export_files
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    all_cells = [
        str(v) for col in merged.columns for v in merged[col].to_list() if v is not None
    ]
    assert "Brand1" not in all_cells  # deleted by the override dictionary
    assert all_cells.count("Brand2") == 1
    assert "kept_by_brand" in merged["tag"].drop_nulls().to_list()


# ------------------------------------------------------------------- #
# B5: REPLACE rules without extra value columns are pruned, not crashed
# ------------------------------------------------------------------- #
def test_process_data_replace_rules_without_extra_cols(
    populated_settings, replace_without_extra_cols_report
):
    """A dictionary with REPLACE rules but no extra value columns must be
    pruned with a warning, not crash with a SQL syntax error.

    Regression test for the bug where _apply_replace received an empty
    extra_cols list and generated invalid SQL (dangling comma in SELECT,
    empty SET clause).
    """
    # Must not raise sqlite3.OperationalError
    result = process_data(
        replace_without_extra_cols_report, settings_dir=populated_settings
    )

    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 3
    # DELETE rule: Brand2 row removed → 2 rows exported
    assert result.rows_exported == 2
    assert result.duration > 0.0
    assert set(result.timings.keys()) == {"import", "mapping", "export"}

    # Verify DELETE rule worked (Brand2 absent, Brand1 ×2)
    export_files = sorted(result.export_dir.glob("*.csv.gz"))
    assert export_files
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)

    all_cells = [
        str(v) for col in merged.columns for v in merged[col].to_list() if v is not None
    ]
    assert "Brand2" not in all_cells  # DELETE rule applied
    assert all_cells.count("Brand1") == 2  # Brand1 rows survived


# --------------------------------------------------------------------------- #
# BLOCK 3 (T8): Export folder cleanup — truth table, override precedence,
#               interactive prompts, CLI flags, and pattern variations.
#
# Tests are grouped by concern (G1–G6) per the execution plan. All tests use
# the canonical ``populated_settings`` fixture (shipped ``app_config.json5``
# template with defaults clear_export_folder=false, force=false) and either the
# ``synthetic_report`` fixture (3 rows → 2 clean) or a hand-built report with a
# stale export file in ``clean_data/``.
# --------------------------------------------------------------------------- #

# --- Helper functions ---


def _create_stale_export_file(
    export_dir: Path,
    table_name: str = "data_table",
    suffix: str = ".csv.gz",
) -> Path:
    """Create a stale export file matching the tool's output pattern.

    The tool's output file naming convention is::

        <table_name>_<YYYYMMDD-HHMMSS>_<N><export_suffix>

    e.g. ``data_table_20240101-120000_1.csv.gz``.  The file is detected by the
    ``*<export_suffix>`` glob and deleted by the ``<table_name>_*<export_suffix>``
    clear pattern in ``_handle_export_folder``.
    """
    export_dir.mkdir(parents=True, exist_ok=True)
    stale_file = export_dir / f"{table_name}_20240101-120000_1{suffix}"
    stale_file.write_bytes(b"stale export data")
    return stale_file


def _set_config_keys(settings_dir: Path, **keys) -> None:
    """Set top-level keys in ``app_config.json5``.

    Example::

        _set_config_keys(populated_settings, clear_export_folder=True)
    """
    config_path = settings_dir / "settings" / "app_config.json5"
    config = json5.loads(config_path.read_text("utf-8"))
    for key, value in keys.items():
        config[key] = value
    config_path.write_text(json5.dumps(config, indent=2), encoding="utf-8")


def _set_config_nested(settings_dir: Path, section: str, key: str, value) -> None:
    """Set a nested key inside a top-level config section.

    Example::

        _set_config_nested(populated_settings, "database_settings", "table_name", "my_table")
    """
    config_path = settings_dir / "settings" / "app_config.json5"
    config = json5.loads(config_path.read_text("utf-8"))
    config[section][key] = value
    config_path.write_text(json5.dumps(config, indent=2), encoding="utf-8")


def _make_report(
    report_dir: Path,
    table_name: str = "data_table",
    compression: str = "gzip",
) -> Path:
    """Create a minimal synthetic report dir with raw_data and dict.

    Mirrors the ``synthetic_report`` fixture in conftest.py but allows specifying
    a custom table_name and compression for variation tests.
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


# --- G1: Non-interactive four-scenario truth table (via process_data) ---
# CliRunner / pytest captured stdout → sys.stdout.isatty() == False →
# non-interactive path in _is_interactive().


def test_export_folder_scenario_1_noninteractive_aborts(
    populated_settings: Path, synthetic_report: Path
):
    """Scenario 1: clear=F, force=F, non-interactive stale files → abort."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    with pytest.raises(ExportFolderConflictError, match="non-interactive|cancelled"):
        process_data(synthetic_report, settings_dir=populated_settings)
    assert stale.exists()  # not cleared on abort


def test_export_folder_scenario_2_clears_noninteractive(
    populated_settings: Path, synthetic_report: Path
):
    """Scenario 2: clear=T, force=F, non-interactive → clears stale + proceeds."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    result = process_data(
        synthetic_report, settings_dir=populated_settings, clear_export_folder=True
    )
    assert not stale.exists()
    assert result.rows_exported == 2


def test_export_folder_scenario_3_force_proceeds_without_clear(
    populated_settings: Path, synthetic_report: Path
):
    """Scenario 3: clear=F, force=T, non-interactive → no clear, proceeds."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    result = process_data(synthetic_report, settings_dir=populated_settings, force=True)
    assert stale.exists()  # not cleared
    assert result.rows_exported == 2


def test_export_folder_scenario_4_clears_with_force(
    populated_settings: Path, synthetic_report: Path
):
    """Scenario 4: clear=T, force=T, non-interactive → clears + proceeds."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    result = process_data(
        synthetic_report,
        settings_dir=populated_settings,
        clear_export_folder=True,
        force=True,
    )
    assert not stale.exists()
    assert result.rows_exported == 2


# --- G2: Empty/missing folder ---


def test_export_folder_missing_or_empty_proceeds(
    populated_settings: Path, synthetic_report: Path, tmp_path: Path
):
    """Missing or empty clean_data dir → proceeds normally (no abort).

    Case 1 (missing): synthetic_report does not create clean_data/ →
    ensure_dir() creates it, _handle_export_folder sees empty dir → return.

    Case 2 (empty): pre-create clean_data/ with no files →
    _handle_export_folder sees empty dir → return.
    """
    # Case 1: clean_data does not exist
    report_missing = synthetic_report
    assert not (report_missing / "clean_data").exists()
    result1 = process_data(report_missing, settings_dir=populated_settings)
    assert isinstance(result1, ReportResult)
    assert result1.rows_exported == 2

    # Case 2: clean_data exists but is empty
    report_empty = _make_report(tmp_path / "report_empty")
    (report_empty / "clean_data").mkdir(parents=True)
    assert not any((report_empty / "clean_data").iterdir())
    result2 = process_data(report_empty, settings_dir=populated_settings)
    assert isinstance(result2, ReportResult)
    assert result2.rows_exported == 2


# --- G3: Override precedence (None → config, explicit → override) ---


def test_config_clear_export_folder_applied_when_no_override(
    populated_settings: Path, synthetic_report: Path
):
    """Config clear_export_folder=true with no override → clears stale (scenario 2)."""
    _set_config_keys(populated_settings, clear_export_folder=True)
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    result = process_data(synthetic_report, settings_dir=populated_settings)
    assert not stale.exists()
    assert result.rows_exported == 2


def test_override_false_takes_precedence_over_config_true(
    populated_settings: Path, synthetic_report: Path
):
    """Config clear=True, override clear_export_folder=False → does NOT clear.

    Config has both flags set to True; the Python API explicitly passes
    ``clear_export_folder=False``.  The override wins: effective clear=False,
    force=True (from config) → scenario 3 → no clear, proceeds.
    """
    _set_config_keys(populated_settings, clear_export_folder=True, force=True)
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    result = process_data(
        synthetic_report,
        settings_dir=populated_settings,
        clear_export_folder=False,
    )
    assert stale.exists()  # not cleared (override False takes precedence)
    assert result.rows_exported == 2


def test_config_force_applied_when_no_override(
    populated_settings: Path, synthetic_report: Path
):
    """Config force=true with no override → proceeds without clearing (scenario 3)."""
    _set_config_keys(populated_settings, force=True)
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    result = process_data(synthetic_report, settings_dir=populated_settings)
    assert stale.exists()  # not cleared
    assert result.rows_exported == 2


# --- G4: Interactive scenarios (monkeypatch _is_interactive + typer.confirm) ---


def test_interactive_scenario_1_user_declines_aborts(
    monkeypatch, populated_settings: Path, synthetic_report: Path
):
    """Scenario 1 interactive: clear=F, force=F, interactive, confirm→False → abort."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    monkeypatch.setattr(AppService, "_is_interactive", lambda self: True)
    monkeypatch.setattr(typer, "confirm", lambda *a, **kw: False)
    with pytest.raises(ExportFolderConflictError, match="cancelled"):
        process_data(synthetic_report, settings_dir=populated_settings)
    assert stale.exists()


def test_interactive_scenario_1_user_accepts_proceeds_without_clearing(
    monkeypatch, populated_settings: Path, synthetic_report: Path
):
    """Scenario 1 interactive: clear=F, force=F, interactive, confirm→True → proceed, no clear."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    monkeypatch.setattr(AppService, "_is_interactive", lambda self: True)
    monkeypatch.setattr(typer, "confirm", lambda *a, **kw: True)
    result = process_data(synthetic_report, settings_dir=populated_settings)
    assert stale.exists()  # not cleared (scenario 1: proceed without clearing)
    assert result.rows_exported == 2


def test_interactive_scenario_2_user_accepts_clears(
    monkeypatch, populated_settings: Path, synthetic_report: Path
):
    """Scenario 2 interactive: clear=T, force=F, interactive, confirm→True → clears + proceeds."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    monkeypatch.setattr(AppService, "_is_interactive", lambda self: True)
    monkeypatch.setattr(typer, "confirm", lambda *a, **kw: True)
    result = process_data(
        synthetic_report, settings_dir=populated_settings, clear_export_folder=True
    )
    assert not stale.exists()  # cleared
    assert result.rows_exported == 2


# --- G5: CLI tests (CliRunner, non-interactive) ---


def test_cli_clear_export_folder_force_clears_stale_files(
    populated_settings: Path, synthetic_report: Path
):
    """CLI --clear-export-folder --force → clears stale files, exit 0."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            str(synthetic_report),
            "--settings-dir",
            str(populated_settings),
            "--clear-export-folder",
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output
    assert not stale.exists()


def test_cli_no_flags_aborts_with_stale_files(
    populated_settings: Path, synthetic_report: Path
):
    """CLI no flags, stale files → exit 1, 'non-interactive' in output."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", str(synthetic_report), "--settings-dir", str(populated_settings)],
    )
    assert result.exit_code == 1
    assert "non-interactive" in result.output.lower()
    assert stale.exists()  # not cleared on abort


def test_cli_force_proceeds_without_clearing(
    populated_settings: Path, synthetic_report: Path
):
    """CLI --force only, stale files → proceeds, stale file remains (scenario 3)."""
    stale = _create_stale_export_file(synthetic_report / "clean_data")
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            str(synthetic_report),
            "--settings-dir",
            str(populated_settings),
            "--force",
        ],
    )
    assert result.exit_code == 0, result.output
    assert stale.exists()  # not cleared (scenario 3: proceed without clearing)


# --- G6: Variations (table_name, compression, non-matching files) ---


def test_clear_pattern_follows_custom_table_name(
    populated_settings: Path, synthetic_report: Path
):
    """Custom table_name: only <custom_table>_* cleared; data_table_* preserved."""
    _set_config_nested(
        populated_settings, "database_settings", "table_name", "my_table"
    )
    clean_dir = synthetic_report / "clean_data"
    custom_stale = _create_stale_export_file(clean_dir, table_name="my_table")
    default_stale = _create_stale_export_file(clean_dir, table_name="data_table")
    result = process_data(
        synthetic_report, settings_dir=populated_settings, clear_export_folder=True
    )
    assert not custom_stale.exists()  # cleared (matches my_table_*.csv.gz)
    assert default_stale.exists()  # preserved (does not match my_table_*.csv.gz)
    assert result.rows_exported == 2


def test_suffix_detection_follows_compression_setting(
    populated_settings: Path, synthetic_report: Path
):
    """Compression=zstd: only .csv.zst files detected/cleared; .csv.gz ignored.

    Uses 'zstd' instead of 'zip' because polars 2.x ``write_csv`` only supports
    'uncompressed', 'gzip', and 'zstd' compression.  The suffix derivation via
    ``CSVWorker.get_files_suffix`` is the aspect under test, not the compression
    library.
    """
    config_path = populated_settings / "settings" / "app_config.json5"
    config = json5.loads(config_path.read_text("utf-8"))
    config["export_settings"]["to_csv"]["compression"] = "zstd"
    config_path.write_text(json5.dumps(config, indent=2), encoding="utf-8")

    clean_dir = synthetic_report / "clean_data"
    stale_zst = _create_stale_export_file(clean_dir, suffix=".csv.zst")
    stale_gz = _create_stale_export_file(clean_dir, suffix=".csv.gz")
    result = process_data(
        synthetic_report, settings_dir=populated_settings, clear_export_folder=True
    )
    assert not stale_zst.exists()  # cleared (matches *.csv.zst)
    assert stale_gz.exists()  # not detected (does not match *.csv.zst)
    assert result.rows_exported == 2


def test_non_matching_files_dont_trigger_warning(
    populated_settings: Path, synthetic_report: Path
):
    """Non-matching extension (.txt) → proceeds normally, file preserved.

    Only ``<table_name>_*<export_suffix>`` files are detected and cleared.
    Files with other extensions (e.g. ``.txt``) are invisible to the check.
    """
    clean_dir = synthetic_report / "clean_data"
    clean_dir.mkdir(parents=True, exist_ok=True)
    (clean_dir / "notes.txt").write_text("not an export file", encoding="utf-8")
    result = process_data(synthetic_report, settings_dir=populated_settings)
    assert result.rows_exported == 2
    assert (clean_dir / "notes.txt").exists()  # non-matching file preserved


# --------------------------------------------------------------------------- #
# B3 (T4): pipeline integration of de-duplication + row-count metrics.
# --------------------------------------------------------------------------- #


def _make_duplicate_report(report_dir: Path) -> Path:
    """Create a report whose raw data contains one exact duplicate row.

    Mirrors ``synthetic_report`` (3 distinct rows, dictionary deletes Brand2)
    but repeats the first row, so raw rows = 4 and post-dedup rows = 3. After
    the dictionary DELETE of Brand2, exactly 2 rows are exported.
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id1;2024-01-01;2024;01;Brand1",  # exact duplicate of the row above
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def _make_header_only_report(report_dir: Path) -> Path:
    """Create a report whose raw CSV contains only a header (zero data rows)."""
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("AdId;researchDate;YEAR;MONTH;BRAND\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def _exported_rows(report_dir: Path) -> list[tuple]:
    """Read the exported CSV files back as sorted row tuples for comparison."""
    export_files = sorted((report_dir / "clean_data").glob("*.csv.gz"))
    assert export_files, "expected at least one exported CSV file"
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    rows = [tuple(row) for row in merged.iter_rows()]
    return sorted(rows, key=lambda r: tuple(str(v) for v in r))


def test_dedup_on_reports_metrics(populated_settings: Path, tmp_path: Path):
    """De-duplication ON: raw rows loaded, one duplicate removed, result derived.

    De-duplication is enabled through ``app_config.json5`` (``deduplicate=true``)
    because the public ``process_data`` override is added in B4/T5; this block
    (B3/T4) wires the pipeline and metrics only.
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_duplicate_report(tmp_path / "report_dedup")
    result = process_data(report_dir, settings_dir=populated_settings)

    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 4  # raw rows before de-duplication
    assert result.rows_removed_duplicates == 1  # one exact duplicate
    assert result.rows_resulted == result.rows_loaded - 1
    assert result.rows_resulted == 3
    assert result.rows_exported == 2  # Brand2 deleted by the dictionary rule
    assert set(result.timings.keys()) == {"import", "mapping", "export"}


def test_dedup_off_reports_metrics(populated_settings: Path, synthetic_report: Path):
    """De-duplication OFF (shipped default): no rows removed, result equals loaded."""
    result = process_data(synthetic_report, settings_dir=populated_settings)

    assert result.rows_loaded == 3
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == result.rows_loaded
    assert result.rows_exported == 2
    assert set(result.timings.keys()) == {"import", "mapping", "export"}


def test_dedup_off_on_duplicate_fixture_reports_no_removal(
    populated_settings: Path, tmp_path: Path
):
    """De-duplication OFF keeps exact duplicates: nothing removed, nothing lost."""
    report_dir = _make_duplicate_report(tmp_path / "report_nodedup")
    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == result.rows_loaded
    assert result.rows_exported == 3  # both Brand1 duplicate rows exported
    assert set(result.timings.keys()) == {"import", "mapping", "export"}


def test_dedup_on_empty_input_reports_zero(populated_settings: Path, tmp_path: Path):
    """Header-only input with de-duplication ON: no crash, zero counts (D6)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_header_only_report(tmp_path / "report_empty")
    result = process_data(report_dir, settings_dir=populated_settings)

    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 0
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == 0
    assert result.rows_exported == 0
    assert set(result.timings.keys()) == {"import", "mapping", "export"}


def test_dedup_preserves_exported_row_set(populated_settings: Path, tmp_path: Path):
    """For exact duplicates, the exported row set is identical with/without dedup.

    Two separate user settings dirs are used so the same config value is not
    mutated between the two runs.
    """
    dedup_settings = tmp_path / "userdir_dedup"
    plain_settings = tmp_path / "userdir_plain"
    for user_dir in (dedup_settings, plain_settings):
        settings_subdir = user_dir / "settings"
        settings_subdir.mkdir(parents=True)
        for name in ("app_config.json5", "log_config.json5"):
            shutil.copy2(TEMPLATE_SETTINGS_DIR / name, settings_subdir / name)
    _set_config_keys(dedup_settings, deduplicate=True)

    dedup_report = _make_duplicate_report(tmp_path / "report_dedup_set")
    plain_report = _make_duplicate_report(tmp_path / "report_plain_set")

    result_dedup = process_data(dedup_report, settings_dir=dedup_settings)
    result_plain = process_data(plain_report, settings_dir=plain_settings)

    assert result_dedup.rows_removed_duplicates == 1
    assert result_plain.rows_removed_duplicates == 0
    # The distinct dataset reaching the export is identical in both runs; the
    # non-dedup run merely repeats the duplicated row.
    assert set(_exported_rows(dedup_report)) == set(_exported_rows(plain_report))


def test_resolved_deduplicate_override_precedence(populated_settings: Path):
    """Explicit override wins over config; None falls back to config (default False)."""
    service = get_app_service(settings_dir=populated_settings)
    # Shipped template default is false.
    assert service._resolved_deduplicate() is False

    service._deduplicate = True
    assert service._resolved_deduplicate() is True

    service._deduplicate = False
    assert service._resolved_deduplicate() is False

    # Config true + no override → resolved true.
    _set_config_keys(populated_settings, deduplicate=True)
    service_from_config = get_app_service(settings_dir=populated_settings)
    assert service_from_config._resolved_deduplicate() is True

    # Explicit False overrides config true.
    service_override = get_app_service(
        settings_dir=populated_settings, deduplicate=False
    )
    assert service_override._resolved_deduplicate() is False


# --------------------------------------------------------------------------- #
# B4 (T5): CLI flag + public API override for de-duplication.
#
# The ``deduplicate`` option is a single tri-state flag (D16): absent → ``None``
# (config decides), present → ``True``. There is deliberately no
# ``--no-deduplicate`` counterpart; passing it makes Typer exit with code 2.
# --------------------------------------------------------------------------- #


def test_cli_deduplicate_flag_enables_when_config_false(
    populated_settings: Path, tmp_path: Path
):
    """--deduplicate turns de-duplication ON even when the config says false (D8)."""
    _set_config_keys(populated_settings, deduplicate=False)
    report_dir = _make_duplicate_report(tmp_path / "report_flag_on")

    result = CliRunner().invoke(
        app,
        [
            "run",
            str(report_dir),
            "--settings-dir",
            str(populated_settings),
            "--deduplicate",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Report complete" in result.output
    assert "4 loaded → 2 clean" in result.output
    normalized = " ".join(result.output.split())
    assert "1 duplicate(s) removed" in normalized


def test_cli_deduplicate_absent_with_config_false_is_off(
    populated_settings: Path, tmp_path: Path
):
    """Flag absent + config false → de-duplication OFF (D17)."""
    _set_config_keys(populated_settings, deduplicate=False)
    report_dir = _make_duplicate_report(tmp_path / "report_flag_absent_off")

    result = CliRunner().invoke(
        app,
        ["run", str(report_dir), "--settings-dir", str(populated_settings)],
    )

    assert result.exit_code == 0, result.output
    assert "4 loaded → 3 clean" in result.output
    assert "duplicate(s) removed" not in result.output


def test_cli_deduplicate_absent_with_config_true_is_on(
    populated_settings: Path, tmp_path: Path
):
    """Flag absent + config true → de-duplication ON (D7/D9)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_duplicate_report(tmp_path / "report_flag_absent_on")

    result = CliRunner().invoke(
        app,
        ["run", str(report_dir), "--settings-dir", str(populated_settings)],
    )

    assert result.exit_code == 0, result.output
    assert "4 loaded → 2 clean" in result.output
    # Rich wraps long lines at the console width; normalize whitespace so the
    # removal segment is matched regardless of where the wrap falls.
    normalized = " ".join(result.output.split())
    assert "1 duplicate(s) removed" in normalized


def test_process_data_deduplicate_false_overrides_config_true(
    populated_settings: Path, tmp_path: Path
):
    """process_data(deduplicate=False) overrides a config value of true (D10)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_duplicate_report(tmp_path / "report_override_off")

    result = process_data(
        report_dir, settings_dir=populated_settings, deduplicate=False
    )

    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == 4
    assert result.rows_exported == 3  # duplicate row survives


def test_process_data_deduplicate_true_overrides_config_false(
    populated_settings: Path, tmp_path: Path
):
    """process_data(deduplicate=True) overrides a config value of false (D10)."""
    _set_config_keys(populated_settings, deduplicate=False)
    report_dir = _make_duplicate_report(tmp_path / "report_override_on")

    result = process_data(report_dir, settings_dir=populated_settings, deduplicate=True)

    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 1
    assert result.rows_resulted == 3
    assert result.rows_exported == 2


def test_cli_no_deduplicate_is_rejected(tmp_path: Path, synthetic_report: Path):
    """--no-deduplicate is not a supported option (D16): Typer exits with code 2.

    The assertion is specific to exit code 2 (Typer's usage-error code), so a
    settings error — which exits 1 — cannot satisfy it.
    """
    result = CliRunner().invoke(
        app,
        [
            "run",
            str(synthetic_report),
            "--settings-dir",
            str(tmp_path / "no_settings"),
            "--no-deduplicate",
        ],
    )

    assert result.exit_code == 2


def test_cli_summary_line_without_dedup_keeps_legacy_substring(
    populated_settings: Path, synthetic_report: Path
):
    """De-duplication off → the legacy ``3 loaded → 2 clean`` substring remains (D25)."""
    result = CliRunner().invoke(
        app,
        ["run", str(synthetic_report), "--settings-dir", str(populated_settings)],
    )

    assert result.exit_code == 0, result.output
    assert "3 loaded → 2 clean" in result.output
    assert "duplicate(s) removed" not in result.output


def test_cli_summary_line_with_dedup_shows_removed_segment(
    populated_settings: Path, tmp_path: Path
):
    """De-duplication active with a duplicate → the removal segment is appended (D25)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_duplicate_report(tmp_path / "report_summary_dedup")

    result = CliRunner().invoke(
        app,
        ["run", str(report_dir), "--settings-dir", str(populated_settings)],
    )

    assert result.exit_code == 0, result.output
    assert "4 loaded → 2 clean" in result.output
    normalized = " ".join(result.output.split())
    assert "1 duplicate(s) removed" in normalized


# --------------------------------------------------------------------------- #
# B5 (T6): D1–D18 coverage matrix — end-to-end gaps not covered above.
#
# Each test maps to a row of spec §13. Tests already covered by
# ``test_dedup_on_reports_metrics`` / ``test_dedup_off_reports_metrics`` /
# ``test_dedup_preserves_exported_row_set`` / the CLI precedence tests above
# are intentionally NOT duplicated here. This block adds only the D rows that
# lack an explicit end-to-end assertion: D3 (chunk boundary), D4 (dedup-on, no
# duplicate), D5 (all rows identical), D6 single-row, D12 (first-occurrence
# determinism), D13 (same index column, different other columns), D14 REPLACE /
# ADD variants, D15 (null_data derived post-dedup), D16 (raw files untouched)
# and D18 (metric invariant ``rows_resulted == rows_loaded - rows_removed``).
# --------------------------------------------------------------------------- #


def _make_all_identical_report(report_dir: Path) -> Path:
    """Three identical rows → de-duplication keeps one, one row is exported (D5).

    The dictionary deletes Brand2 (absent here) and adds a tag to Brand1, so
    the single surviving Brand1 row is exported with the tag.
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id1;2024-01-01;2024;01;Brand1",
        "id1;2024-01-01;2024;01;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def _make_single_row_report(report_dir: Path) -> Path:
    """One data row → de-duplication removes nothing (D6 single-row case).

    The dictionary deletes Brand2 (absent) and adds a tag to Brand1, so the
    single Brand1 row is exported.
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def _make_replace_rule_report(report_dir: Path) -> Path:
    """Report with one exact duplicate and a REPLACE rule (D14 REPLACE variant).

    Raw rows = 4, post-dedup = 3. The dictionary REPLACEs Brand2 with
    ``replaced_tag`` and ADDs ``added_by_brand`` to Brand1. No DELETE rule, so
    all three post-dedup rows are exported (Brand1×2 tagged, Brand2 replaced).
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "r;f;4;NULL;Brand2;replaced_tag",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def _make_add_only_rule_report(report_dir: Path) -> Path:
    """Report with one exact duplicate and an ADD-only rule (D14 ADD variant).

    Raw rows = 4, post-dedup = 3. The dictionary only ADDs
    ``added_by_brand`` to Brand1; no rows are deleted, so all three post-dedup
    rows are exported.
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def _make_two_file_duplicate_report(report_dir: Path) -> Path:
    """Two raw files sharing one exact row → de-duplication collapses it (D2).

    ``a.csv`` holds id1 (Brand1) and id2 (Brand2); ``b.csv`` holds id2 (Brand2,
    duplicate of a.csv's second row) and id3 (Brand1). Raw rows = 4, post-dedup
    = 3; the dictionary deletes Brand2 → 2 rows exported.
    """
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    file_a = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
    ]
    file_b = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id2;2024-01-15;2024;01;Brand2",  # exact duplicate of file_a's last row
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "a.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(file_a) + "\n")
    with gzip.open(raw_data_dir / "b.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(file_b) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir


def test_dedup_e2e_two_files_one_duplicate(populated_settings: Path, tmp_path: Path):
    """D2: one duplicate spread across two raw files collapses to one (4/1/3/2)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_two_file_duplicate_report(tmp_path / "report_two_file")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 1
    assert result.rows_resulted == 3
    assert result.rows_exported == 2
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates


def test_dedup_e2e_chunk_boundary_duplicate(
    populated_settings: Path, tmp_path: Path, monkeypatch
):
    """D3: a duplicate straddling a ``CHUNK_SIZE=2`` boundary is removed (7/1/6/4).

    Seven raw rows with one exact duplicate (id1 at the first and last row) and
    two Brand2 rows that the dictionary deletes. Patching ``CSVWorker.CHUNK_SIZE``
    to 2 proves the de-duplication is global (one plan), not per chunk.
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = tmp_path / "report_chunk_boundary"
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-01-16;2024;01;Brand2",
        "id4;2024-02-02;2024;02;Brand1",
        "id5;2024-02-03;2024;02;Brand1",
        "id6;2024-02-04;2024;02;Brand1",
        "id1;2024-01-01;2024;01;Brand1",  # duplicate of the first data row
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    monkeypatch.setattr(CSVWorker, "CHUNK_SIZE", 2)
    try:
        result = process_data(report_dir, settings_dir=populated_settings)
    finally:
        monkeypatch.setattr(CSVWorker, "CHUNK_SIZE", 10000)

    assert result.rows_loaded == 7
    assert result.rows_removed_duplicates == 1
    assert result.rows_resulted == 6
    assert result.rows_exported == 4
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates

    # The two Brand2 rows are deleted; the surviving Brand1 rows are exported.
    exported_ids = _exported_rows(report_dir)
    brands = [row[4] for row in exported_ids]
    assert "Brand2" not in brands
    assert brands.count("Brand1") == 4


def test_dedup_e2e_dedup_on_no_duplicates(
    populated_settings: Path, synthetic_report: Path
):
    """D4: de-duplication ON with no duplicates removes nothing (3/0/3/2)."""
    _set_config_keys(populated_settings, deduplicate=True)

    result = process_data(synthetic_report, settings_dir=populated_settings)

    assert result.rows_loaded == 3
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == 3
    assert result.rows_exported == 2  # Brand2 deleted by the dictionary rule
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates


def test_dedup_e2e_all_rows_identical(populated_settings: Path, tmp_path: Path):
    """D5: three identical rows collapse to one survivor, one exported (3/2/1/1)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_all_identical_report(tmp_path / "report_all_identical")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 3
    assert result.rows_removed_duplicates == 2
    assert result.rows_resulted == 1
    assert result.rows_exported == 1
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates

    exported = _exported_rows(report_dir)
    assert len(exported) == 1
    # The survivor is the Brand1 row carrying the added tag.
    assert exported[0][4] == "Brand1"
    assert exported[0][5] == "added_by_brand"


def test_dedup_e2e_single_row_input(populated_settings: Path, tmp_path: Path):
    """D6 (single-row): one data row removes nothing (1/0/1/1)."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_single_row_report(tmp_path / "report_single_row")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 1
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == 1
    assert result.rows_exported == 1
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates


def test_dedup_e2e_keeps_first_occurrence_order(
    populated_settings: Path, tmp_path: Path
):
    """D12: the surviving row is the first occurrence in input (sorted-file) order.

    Two rows share ``AdId=idB`` but differ in another column, so they are NOT
    exact duplicates and both survive; the row order in the export must follow
    first-occurrence order (idB before idA before idC).
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = tmp_path / "report_first_occurrence"
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "idB;2024-01-01;2024;01;Brand1",
        "idA;2024-01-15;2024;01;Brand2",
        "idB;2024-02-01;2024;02;Brand1",  # same AdId, different other columns
        "idC;2024-03-01;2024;03;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "a;f;4;NULL;Brand1;t",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    result = process_data(report_dir, settings_dir=populated_settings)

    # idB, idA, idB, idC are all distinct rows (idB differs in other columns),
    # so nothing is removed; all four are exported in input order.
    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == 4

    export_files = sorted((report_dir / "clean_data").glob("*.csv.gz"))
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    assert merged["AdId"].to_list() == ["idB", "idA", "idB", "idC"]


def test_dedup_e2e_same_index_column_different_columns(
    populated_settings: Path, tmp_path: Path
):
    """D13: same ``index_column`` with different other columns → both kept (2/0/2/2).

    Two rows share ``AdId`` but differ in every other column, so they are not
    exact duplicates and de-duplication keeps both. The per-key distinct table
    collapses them for rule resolution, but the exported data retains both.
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = tmp_path / "report_same_index"
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "idX;2024-01-01;2024;01;Brand1",
        "idX;2024-02-02;2024;02;Brand3",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "a;f;4;NULL;Brand1;alpha_tag",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 2
    assert result.rows_removed_duplicates == 0
    assert result.rows_resulted == 2
    assert result.rows_exported == 2
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates

    # Both distinct rows survive in the export (different BRAND values).
    export_files = sorted((report_dir / "clean_data").glob("*.csv.gz"))
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    assert sorted(merged["BRAND"].to_list()) == ["Brand1", "Brand3"]


def test_dedup_e2e_with_replace_rule(populated_settings: Path, tmp_path: Path):
    """D14 (REPLACE): de-duplication composes with a REPLACE rule (4/1/3/3).

    The dictionary REPLACEs Brand2 with ``replaced_tag`` and ADDs a tag to
    Brand1. No DELETE rule, so all three post-dedup rows are exported.
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_replace_rule_report(tmp_path / "report_replace")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 1
    assert result.rows_resulted == 3
    assert result.rows_exported == 3
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates

    export_files = sorted((report_dir / "clean_data").glob("*.csv.gz"))
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    # Brand2 row replaced its tag; Brand1 rows got the added tag.
    assert "replaced_tag" in merged["tag"].to_list()
    assert merged["tag"].to_list().count("added_by_brand") == 2


def test_dedup_e2e_with_add_rule(populated_settings: Path, tmp_path: Path):
    """D14 (ADD): de-duplication composes with an ADD-only rule (4/1/3/3).

    The dictionary only ADDs ``added_by_brand`` to Brand1; no rows are deleted,
    so all three post-dedup rows are exported.
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_add_only_rule_report(tmp_path / "report_add")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 4
    assert result.rows_removed_duplicates == 1
    assert result.rows_resulted == 3
    assert result.rows_exported == 3
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates

    export_files = sorted((report_dir / "clean_data").glob("*.csv.gz"))
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    # Brand1 rows get the tag; the Brand2 row has no tag.
    assert merged["tag"].to_list().count("added_by_brand") == 2


def test_dedup_e2e_null_data_derived_post_dedup(
    populated_settings: Path, tmp_path: Path
):
    """D15: the non-mapped (``null_data``) export is derived from post-dedup rows.

    One exact duplicate collapses (3→2), and a row that matches no rule
    (UnknownBrand) is exported to ``null_data`` with its BRAND/tag columns; the
    duplicate does not produce a second null_data row.
    """
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = tmp_path / "report_null_data"
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id1;2024-01-01;2024;01;Brand1",  # exact duplicate
        "id9;2024-03-03;2024;03;UnknownBrand",  # matches no rule → null_data
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    result = process_data(report_dir, settings_dir=populated_settings)

    assert result.rows_loaded == 3
    assert result.rows_removed_duplicates == 1
    assert result.rows_resulted == 2
    # id1 (Brand1) and id9 (UnknownBrand) are both exported to clean_data.
    assert result.rows_exported == 2
    assert result.rows_resulted == result.rows_loaded - result.rows_removed_duplicates

    # null_data export is present and contains the single unmapped row (post-dedup).
    assert result.null_data_files, "expected a null_data export file"
    null_path = result.null_data_files[0]
    assert null_path.exists()
    null_df = pl.read_csv(null_path, separator=";")
    assert null_df.height == 1
    assert null_df["BRAND"].to_list() == ["UnknownBrand"]


def test_dedup_e2e_raw_files_untouched(populated_settings: Path, tmp_path: Path):
    """D16: raw input files are byte-identical after a de-duplication run."""
    _set_config_keys(populated_settings, deduplicate=True)
    report_dir = _make_duplicate_report(tmp_path / "report_raw_untouched")
    raw_file = report_dir / "raw_data" / "sample.csv.gz"
    before = raw_file.read_bytes()

    process_data(report_dir, settings_dir=populated_settings)

    assert raw_file.read_bytes() == before


def test_dedup_metric_invariant_all_scenarios(populated_settings: Path, tmp_path: Path):
    """D18: ``rows_resulted == rows_loaded - rows_removed_duplicates`` in every scenario.

    Runs the same duplicate fixture under three de-duplication configurations
    (off, on-via-config, on-via-override) and asserts the metric invariant holds
    in each, with the expected removal count.
    """
    # Scenario 1: de-duplication OFF (config default false).
    off_settings = tmp_path / "userdir_off"
    settings_subdir = off_settings / "settings"
    settings_subdir.mkdir(parents=True)
    for name in ("app_config.json5", "log_config.json5"):
        shutil.copy2(TEMPLATE_SETTINGS_DIR / name, settings_subdir / name)
    off_report = _make_duplicate_report(tmp_path / "report_inv_off")
    result_off = process_data(off_report, settings_dir=off_settings)
    assert result_off.rows_removed_duplicates == 0
    assert result_off.rows_resulted == result_off.rows_loaded - 0
    assert result_off.rows_resulted == result_off.rows_loaded

    # Scenario 2: de-duplication ON via config.
    on_settings = tmp_path / "userdir_on"
    settings_subdir = on_settings / "settings"
    settings_subdir.mkdir(parents=True)
    for name in ("app_config.json5", "log_config.json5"):
        shutil.copy2(TEMPLATE_SETTINGS_DIR / name, settings_subdir / name)
    _set_config_keys(on_settings, deduplicate=True)
    on_report = _make_duplicate_report(tmp_path / "report_inv_on")
    result_on = process_data(on_report, settings_dir=on_settings)
    assert result_on.rows_removed_duplicates == 1
    assert result_on.rows_resulted == result_on.rows_loaded - 1
    assert result_on.rows_resulted == 3

    # Scenario 3: de-duplication ON via explicit override over config false.
    override_report = _make_duplicate_report(tmp_path / "report_inv_override")
    result_override = process_data(
        override_report, settings_dir=off_settings, deduplicate=True
    )
    assert result_override.rows_removed_duplicates == 1
    assert (
        result_override.rows_resulted
        == result_override.rows_loaded - result_override.rows_removed_duplicates
    )
    assert result_override.rows_resulted == 3
