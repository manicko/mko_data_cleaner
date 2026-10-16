"""Public API for mko_data_cleaner: initialize_settings, process_data, and result types."""

from importlib.metadata import PackageNotFoundError, version

from mko_data_cleaner.app import initialize_settings, process_data
from mko_data_cleaner.core.errors import SettingsNotInitializedError
from mko_data_cleaner.core.models import ReportResult

try:
    __version__ = version("mko_data_cleaner")
except PackageNotFoundError:
    # Fallback for source checkout / pythonpath=src (not pip-installed).
    # NOTE: must track the top-level ``version`` in pyproject.toml.
    __version__ = "0.2.0"

__all__ = [
    "ReportResult",
    "SettingsNotInitializedError",
    "__version__",
    "initialize_settings",
    "process_data",
]
