"""Tests for package-level metadata (__version__)."""

from importlib.metadata import PackageNotFoundError, version

import pytest

import mko_data_cleaner


def test_version_is_non_empty_string():
    """__version__ resolves to a non-empty string in all execution modes."""
    assert isinstance(mko_data_cleaner.__version__, str)
    assert len(mko_data_cleaner.__version__) > 0


def test_version_matches_installed_metadata_when_available():
    """When installed, __version__ must agree with importlib.metadata.version."""
    try:
        installed = version("mko_data_cleaner")
    except PackageNotFoundError:
        pytest.skip("Running from source; package metadata not installed")
    assert mko_data_cleaner.__version__ == installed
