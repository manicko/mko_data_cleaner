"""Tests for package-level metadata (__version__)."""

import tomllib
from pathlib import Path
from importlib.metadata import PackageNotFoundError, version

import pytest

import mko_data_cleaner

ROOT = Path(__file__).resolve().parent.parent


def test_version_is_non_empty_string():
    """__version__ resolves to a non-empty string in all execution modes."""
    assert isinstance(mko_data_cleaner.__version__, str)
    assert len(mko_data_cleaner.__version__) > 0
    # Cross-check against pyproject.toml version
    with open(ROOT / "pyproject.toml", "rb") as f:
        pyproject = tomllib.load(f)
    expected_version = pyproject["project"]["version"]
    assert mko_data_cleaner.__version__ == expected_version, (
        f"__version__ ({mko_data_cleaner.__version__}) != "
        f"pyproject.toml version ({expected_version})"
    )


def test_version_matches_installed_metadata_when_available():
    """When installed, __version__ must agree with importlib.metadata.version."""
    try:
        installed = version("mko_data_cleaner")
    except PackageNotFoundError:
        pytest.skip("Running from source; package metadata not installed")
    assert mko_data_cleaner.__version__ == installed
