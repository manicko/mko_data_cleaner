#!/usr/bin/env python
"""Install-time smoke test (B6/T6) — run in a fresh venv with the wheel pip-installed.

Validates that the pip-installed package:
  (i)   imports without crashing (F1/F2 regression gate)
  (ii)  process_data without settings raises SettingsNotInitializedError
  (iii) initialize_settings() ships YAML templates (F3 regression gate)
  (iv)  CLI commands work (--help, init, list-settings)
  (v)   end-to-end process_data on a tiny synthetic report (F8)
  (vi)  __version__ resolves from installed package metadata

Designed to run in a clean venv that has ONLY the wheel installed — no dev
dependencies. Uses ``settings_dir`` parameter for isolation (no env-var
patching needed), ``shutil.which`` for CLI discovery, and
``tempfile.mkdtemp()`` for temporary directories.
"""

import gzip
import importlib.metadata
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import mko_data_cleaner
from mko_data_cleaner import (
    SettingsNotInitializedError,
    initialize_settings,
    process_data,
)

# Reconfigure stdout/stderr to UTF-8 so Unicode progress-bar output
# (block chars, arrows) does not crash on legacy Windows code pages
# (cp1251, cp1252). On Linux/macOS the default is UTF-8, so this is
# a no-op there. Guard with hasattr for kernel/pyodide environments.
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

# Ensure the venv's bin/Scripts directory is on PATH so that shutil.which
# can discover the ``mko-data-cleaner`` console-script entry point, regardless
# of whether the venv was activated before launching this script.
_bin_dir = Path(sys.executable).parent
os.environ["PATH"] = str(_bin_dir) + os.pathsep + os.environ.get("PATH", "")

# ---------------------------------------------------------------------------
# (i) Import + version from installed metadata
# ---------------------------------------------------------------------------
_version = importlib.metadata.version("mko_data_cleaner")
assert mko_data_cleaner.__version__ == _version, (
    f"__version__ ({mko_data_cleaner.__version__}) != "
    f"installed metadata version ({_version})"
)
print(f"[PASS] (i) import OK, __version__ = {_version}")

# ---------------------------------------------------------------------------
# (ii) process_data WITHOUT settings → SettingsNotInitializedError (F1/F2)
# ---------------------------------------------------------------------------
_empty_settings = Path(tempfile.mkdtemp())
_report_dir = Path(tempfile.mkdtemp())
_report_dir.mkdir(exist_ok=True)
try:
    process_data(_report_dir, settings_dir=_empty_settings)
except SettingsNotInitializedError as exc:
    _msg = str(exc)
    assert "initialize_settings()" in _msg, (
        f"error message missing 'initialize_settings()': {_msg}"
    )
    assert "app_config.yaml" in _msg, (
        f"error message missing 'app_config.yaml': {_msg}"
    )
    print("[PASS] (ii) SettingsNotInitializedError raised for missing settings")
else:
    sys.exit("FAIL: process_data did not raise SettingsNotInitializedError")

# ---------------------------------------------------------------------------
# (iii) initialize_settings creates templates (F3)
# ---------------------------------------------------------------------------
_tmp_config = Path(tempfile.mkdtemp())
_settings_path = initialize_settings(settings_dir=_tmp_config)
assert (_settings_path / "app_config.yaml").exists(), "app_config.yaml not created"
assert (_settings_path / "log_config.yaml").exists(), "log_config.yaml not created"
print(f"[PASS] (iii) settings initialized at {_settings_path}")

# ---------------------------------------------------------------------------
# (iv) CLI smoke (--help, init, list-settings)
# ---------------------------------------------------------------------------
_cli = shutil.which("mko-data-cleaner")
assert _cli is not None, "mko-data-cleaner CLI not found in venv PATH"
# Ensure the subprocess uses UTF-8 for stdout/stderr so rich's Unicode
# console output (emojis, box-drawing) does not crash on Windows cp1251.
_cli_env = dict(os.environ)
_cli_env["PYTHONIOENCODING"] = "utf-8"
subprocess.run([_cli, "--help"], check=True, capture_output=True, env=_cli_env)
subprocess.run(
    [_cli, "init", "--settings-dir", str(_tmp_config), "--force"],
    check=True,
    capture_output=True,
    env=_cli_env,
)
subprocess.run(
    [_cli, "list-settings", "--settings-dir", str(_tmp_config)],
    check=True,
    capture_output=True,
    env=_cli_env,
)
print("[PASS] (iv) CLI smoke (--help, init, list-settings)")

# ---------------------------------------------------------------------------
# (v) End-to-end on synthetic sample
#
# DUPLICATED — keep in sync with:
#   - tests/test_app.py::test_process_data_end_to_end
#   - tests/test_app.py::test_null_data_files_excludes_stale
#   (both use tests/conftest.py::synthetic_report fixture)
# Shared assertions: rows_loaded==3, rows_exported==2, export_dir/import_dir/
# report_dir resolved, export dir exists with .gz files, null_data_files is
# list, duration>0, timings keys == {import, mapping, export}.
# ---------------------------------------------------------------------------
_e2e = Path(tempfile.mkdtemp())
_raw = _e2e / "raw_data"
_raw.mkdir(parents=True)
_ddir = _e2e / "dict"
_ddir.mkdir(parents=True)

# Header matches app_config.yaml template: index_column=AdId, date_column=researchDate
csv_lines = [
    "AdId;researchDate;YEAR;MONTH;BRAND",
    "id1;2024-01-01;2024;01;Brand1",
    "id2;2024-01-15;2024;01;Brand2",
    "id3;2024-02-01;2024;02;Brand1",
]
with gzip.open(_raw / "sample.csv.gz", "wt", encoding="utf-8") as f:
    f.write("\n".join(csv_lines) + "\n")

# Dictionary: DELETE Brand2 (row deleted), ADD tag for Brand1
# search=4 → BRAND column (0-based: AdId=0, researchDate=1, YEAR=2,
# MONTH=3, BRAND=4, tag=5-after-merge)
dict_lines = [
    "action;match;search;value;term;tag",
    "d;f;4;NULL;Brand2;",
    "a;f;4;NULL;Brand1;added_by_brand",
]
(_ddir / "merged_dictionary.csv").write_text(
    "\n".join(dict_lines) + "\n", encoding="utf-8"
)

_result = process_data(_e2e, settings_dir=_tmp_config)
assert _result.rows_loaded == 3, (
    f"expected 3 rows loaded, got {_result.rows_loaded}"
)
assert _result.rows_exported == 2, (
    f"expected 2 rows exported, got {_result.rows_exported}"
)
assert _result.export_dir == (_e2e / "clean_data").resolve()
assert _result.import_dir == (_e2e / "raw_data").resolve()
assert _result.report_dir == _e2e.resolve()
assert _result.export_dir.exists()
assert len(list(_result.export_dir.glob("*.gz"))) > 0
assert isinstance(_result.null_data_files, list)
assert _result.duration > 0.0
assert set(_result.timings.keys()) == {"import", "mapping", "export"}
print(
    f"[PASS] (v) end-to-end: loaded={_result.rows_loaded}, "
    f"exported={_result.rows_exported}"
)

# ---------------------------------------------------------------------------
# (vi) __version__ matches installed metadata (already asserted in (i))
# ---------------------------------------------------------------------------
print(f"[PASS] (vi) __version__ resolved from installed metadata: {_version}")

print("\n✅ All smoke tests passed")
