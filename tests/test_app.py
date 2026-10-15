"""Tests for the app-level public API (B1/T1 + B4/T4)."""

import gzip
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from mko_data_cleaner.app import app, initialize_settings, process_data
from mko_data_cleaner.core.app_service import get_app_service
from mko_data_cleaner.core.errors import SettingsNotInitializedError
from mko_data_cleaner.core.init_service import init_project
from mko_data_cleaner.core.models import ReportResult
from mko_data_cleaner.core.paths import APP_DIR, APP_NAME, AppPaths
from mko_data_cleaner.core.utils import yaml_to_dict

# Path to the package's shipped settings templates (app_config.yaml / log_config.yaml)
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
    for name in ("app_config.yaml", "log_config.yaml"):
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
    assert "app_config.yaml" in message


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
    assert isinstance(result.exception, SettingsNotInitializedError)


# --------------------------------------------------------------------------- #
# 6-7. list_settings
# --------------------------------------------------------------------------- #
def test_list_settings_lists_when_dir_exists(populated_settings: Path):
    runner = CliRunner()
    result = runner.invoke(
        app, ["list-settings", "--settings-dir", str(populated_settings)]
    )
    assert result.exit_code == 0
    assert "app_config.yaml" in result.output
    assert "log_config.yaml" in result.output


def test_list_settings_does_not_raise_when_dir_missing(tmp_path: Path):
    runner = CliRunner()
    missing_dir = tmp_path / "missing"
    result = runner.invoke(
        app, ["list-settings", "--settings-dir", str(missing_dir)]
    )
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
    assert (dest / "app_config.yaml").exists()
    assert (dest / "log_config.yaml").exists()


# --------------------------------------------------------------------------- #
# 9-10. Settings present: factory constructs / process_data past the gate
# --------------------------------------------------------------------------- #
def test_get_app_service_constructs_after_initialization(populated_settings: Path):
    service = get_app_service(settings_dir=populated_settings)
    assert service.app_config is not None
    assert service.log_config is not None
    assert service.app_config.data_paths.import_folder is not None


def test_process_data_past_gate_does_not_raise_settings_error(
    populated_settings: Path, empty_report_dir: Path
):
    try:
        process_data(empty_report_dir, settings_dir=populated_settings)
    except SettingsNotInitializedError:
        pytest.fail("process_data raised SettingsNotInitializedError despite settings present")
    except Exception:
        # Operational errors from the empty report dir are acceptable here;
        # we only assert the settings gate is not the source of failure.
        pass


# --------------------------------------------------------------------------- #
# 11-12. yaml_to_dict defense-in-depth
# --------------------------------------------------------------------------- #
def test_yaml_to_dict_returns_empty_dict_on_missing_file(tmp_path: Path):
    result = yaml_to_dict(tmp_path / "does_not_exist.yaml")
    assert result == {}


def test_yaml_to_dict_returns_empty_dict_on_invalid_yaml(tmp_path: Path):
    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text("key: value: extra\n", encoding="utf8")
    result = yaml_to_dict(bad_file)
    assert result == {}


# ------------------------------------------------------------------- #
# B4: ReportResult model contract
# ------------------------------------------------------------------- #
def test_report_result_model_construction_and_serialization():
    """ReportResult accepts all 10 fields and serializes via model_dump()."""
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
    fake_service = SimpleNamespace(run_report=lambda path: fake_result)
    monkeypatch.setattr(
        "mko_data_cleaner.app.get_app_service",
        lambda settings_dir=None: fake_service,
    )
    result = process_data(empty_report_dir, settings_dir=populated_settings)
    assert isinstance(result, ReportResult)
    assert result.rows_loaded == 42
    assert result.rows_exported == 40
    assert result.report_dir == empty_report_dir


# ------------------------------------------------------------------- #
# B4: settings_dir threading — initialize_settings
# ------------------------------------------------------------------- #
def test_initialize_settings_threads_settings_dir(tmp_path: Path):
    """initialize_settings(settings_dir=...) writes to settings_dir / settings."""
    user_dir = tmp_path / "myapp"
    result = initialize_settings(settings_dir=user_dir)
    assert result == user_dir / "settings"
    assert (result / "app_config.yaml").exists()
    assert (result / "log_config.yaml").exists()


def test_initialize_settings_force_overwrites(tmp_path: Path):
    """initialize_settings(force=True, settings_dir=...) overwrites existing files."""
    user_dir = tmp_path / "myapp"
    dest = user_dir / "settings"
    dest.mkdir(parents=True)
    (dest / "app_config.yaml").write_text("stale")

    result = initialize_settings(force=True, settings_dir=user_dir)
    assert (result / "app_config.yaml").exists()
    # The template should have overwritten the stale file
    assert "data_paths" in (result / "app_config.yaml").read_text()


# ------------------------------------------------------------------- #
# B4: settings_dir threading — init_project
# ------------------------------------------------------------------- #
def test_init_project_with_settings_dir(tmp_path: Path):
    """init_project(force=True, settings_dir=...) writes to settings_dir / settings."""
    user_dir = tmp_path / "myapp"
    result = init_project(force=True, settings_dir=user_dir)
    assert result == user_dir / "settings"
    assert (result / "app_config.yaml").exists()
    assert (result / "log_config.yaml").exists()


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
    assert (dest / "app_config.yaml").exists()
    assert (dest / "log_config.yaml").exists()


# ------------------------------------------------------------------- #
# B4: settings_dir threading — init_project with None uses APP_PATHS
# ------------------------------------------------------------------- #
def test_init_project_none_uses_app_paths(tmp_path: Path, monkeypatch):
    """init_project(force, settings_dir=None) uses monkeypatchable APP_PATHS."""
    tmp_app_paths = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=tmp_path)
    monkeypatch.setattr("mko_data_cleaner.core.init_service.APP_PATHS", tmp_app_paths)
    result = init_project(force=True, settings_dir=None)
    assert result == tmp_path / "settings"
    assert (result / "app_config.yaml").exists()


# ------------------------------------------------------------------- #
# B4: top-level re-exports
# ------------------------------------------------------------------- #
def test_top_level_exports():
    """import mko_data_cleaner exposes exactly the 5 public names."""
    import mko_data_cleaner

    expected = {
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


def test_top_level_no_import_side_effects(tmp_path: Path):
    """importing the top-level package must not create a settings dir."""
    target = str(tmp_path)
    script = (
        "import platformdirs, os, sys\n"
        "platformdirs.user_config_dir = lambda *a, **k: sys.argv[1]\n"
        "import mko_data_cleaner\n"
        "print(mko_data_cleaner.__version__)\n"
        "if os.path.isdir(os.path.join(sys.argv[1], 'settings')):\n"
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


# ------------------------------------------------------------------- #
# B4: minimal end-to-end pipeline (bridge to B5)
# ------------------------------------------------------------------- #
def test_process_data_end_to_end(populated_settings: Path, tmp_path: Path):
    """Run a tiny synthetic report and verify the ReportResult fields."""
    report_dir = tmp_path / "report"
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    # Data CSV matching app_config.yaml: index_column=AdId, date_column=researchDate
    # Extension is 'gz' (from template config), so files must be .csv.gz
    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    # Dictionary CSV — columns: action(0) match(1) search(2) value(3) term(4) tag(5)
    # search=4 means "search in BRAND column" (data table column index 4:
    #   AdId=0, researchDate=1, YEAR=2, MONTH=3, BRAND=4, tag=5)
    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

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
