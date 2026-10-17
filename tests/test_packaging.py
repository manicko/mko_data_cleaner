"""Packaging tests (B6/T6): package-data regression gate (F3),

MANIFEST.in hygiene (F10), and metadata guards (F5, D4).

These tests run from a source checkout (pythonpath=["src"]) — no wheel
build required. They complement the CI build job's wheel-content assertion
by testing the source-level invariants that must hold before a wheel is
ever built.
"""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = ROOT / "pyproject.toml"
MANIFEST = ROOT / "MANIFEST.in"


def _load_pyproject():
    """Parse pyproject.toml once for metadata assertions."""
    with open(PYPROJECT, "rb") as f:
        return tomllib.load(f)


def test_template_json5_accessible_as_package_data():
    """Both JSON5 settings templates are accessible via importlib.resources.

    At pre-fix HEAD (B2 not applied), the wheel had 18 entries and
    zero settings/*.json5. This test fails on such code because
    importlib.resources.files() cannot locate non-package data
    without [tool.setuptools.package-data].
    """
    import importlib.resources as resources

    pkg = resources.files("mko_data_cleaner")
    assert (pkg / "settings" / "app_config.json5").is_file(), (
        "app_config.json5 not accessible as package data"
    )
    assert (pkg / "settings" / "log_config.json5").is_file(), (
        "log_config.json5 not accessible as package data"
    )


def test_template_json5_parse():
    """Shipped JSON5 templates parse as valid JSON5 with expected top-level keys."""
    import importlib.resources as resources

    import json5

    pkg = resources.files("mko_data_cleaner")
    app_cfg = json5.loads(
        (pkg / "settings" / "app_config.json5").read_text(encoding="utf-8")
    )
    log_cfg = json5.loads(
        (pkg / "settings" / "log_config.json5").read_text(encoding="utf-8")
    )
    assert "data_paths" in app_cfg
    assert "handlers" in log_cfg


def test_manifest_in_no_stale_project_name():
    """MANIFEST.in contains no stale mko_get_mediascope_data references (F10)."""
    assert "mko_get_mediascope_data" not in MANIFEST.read_text()


def test_manifest_in_includes_json5_templates():
    """MANIFEST.in has recursive-include for settings/*.json5 (sdist)."""
    content = MANIFEST.read_text()
    assert "recursive-include" in content
    assert "*.json5" in content
    assert "settings" in content


def test_pyproject_no_stale_license_classifier():
    """pyproject.toml has no deprecated License classifiers (F5)."""
    metadata = _load_pyproject()["project"]
    classifiers = metadata.get("classifiers", [])
    assert not any("License ::" in c for c in classifiers), classifiers


def test_pyproject_no_stale_project_name():
    """pyproject.toml project name is not the stale mko_get_mediascope_data (F10)."""
    metadata = _load_pyproject()["project"]
    assert metadata["name"] != "mko_get_mediascope_data"


def test_pyproject_uses_spdx_license():
    """pyproject.toml uses PEP 639 SPDX license expression (B3/D4)."""
    metadata = _load_pyproject()["project"]
    assert metadata["license"] == "BSD-3-Clause"
    assert metadata["license-files"] == ["LICENSE.txt"]


def test_pyproject_declares_package_data():
    """pyproject.toml declares [tool.setuptools.package-data] for JSON5 (B2)."""
    setuptools = _load_pyproject()["tool"]["setuptools"]
    package_data = setuptools.get("package-data", {})
    json5_patterns = package_data.get("mko_data_cleaner", [])
    assert "settings/*.json5" in json5_patterns, json5_patterns
