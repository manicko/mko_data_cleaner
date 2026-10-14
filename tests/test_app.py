"""Tests for the lazy, settings-gated app initialization (B1/T1)."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from mko_data_cleaner.app import app, process_data
from mko_data_cleaner.core.app_service import get_app_service
from mko_data_cleaner.core.errors import SettingsNotInitializedError
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
