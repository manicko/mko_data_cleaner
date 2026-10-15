from pathlib import Path


class NoReportFoundError(Exception):
    pass


class WrongDataSettings(Exception):
    pass


class DataValidationError(Exception):
    pass


class ConfigError(Exception):
    pass


class SettingsNotInitializedError(FileNotFoundError):
    """Raised when user settings (app_config.yaml / log_config.yaml) are missing."""

    def __init__(self, user_settings_dir: Path, missing_files: list[str]):
        missing = ", ".join(missing_files)
        super().__init__(
            f"User settings are not initialized in {user_settings_dir}.\n"
            f"Missing file(s): {missing}.\n"
            "Run mko_data_cleaner.app.initialize_settings() to generate default "
            "settings."
        )


class PyArrowInstallationError(RuntimeError):
    """Raised when pyarrow is present on disk (findable by find_spec) but
    cannot be imported — i.e. a broken/partial installation.

    This state is detected before the ADBC write path because polars and
    adbc-driver-manager disagree on pyarrow availability: polars uses
    ``find_spec`` (succeeds for a broken module layout) while adbc-driver-manager
    performs a real ``import pyarrow``. The mismatch surfaces deep inside
    ``conn.adbc_get_info()`` as a cryptic
    ``ProgrammingError: This API requires PyArrow to be installed``.
    """

    _MESSAGE = (
        "pyarrow is installed on disk but cannot be imported — your pyarrow "
        "installation is broken or partial (e.g. leftover user-site files or a "
        "failed native-extension install). "
        "Fix with: python -m pip install --force-reinstall pyarrow"
    )

    def __init__(self, message: str | None = None):
        super().__init__(message or self._MESSAGE)
