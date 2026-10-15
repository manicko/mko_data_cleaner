"""Package-level version, sourced from installed package metadata."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mko_data_cleaner")
except PackageNotFoundError:
    # Fallback for source checkout / pythonpath=src (not pip-installed).
    # NOTE: must track the top-level ``version`` in pyproject.toml.
    __version__ = "0.0.1"
