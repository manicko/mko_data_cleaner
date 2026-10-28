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
    """Raised when user settings (app_config.json5 / log_config.json5) are missing."""

    def __init__(self, user_settings_dir: Path, missing_files: list[str]):
        missing = ", ".join(missing_files)
        super().__init__(
            f"User settings are not initialized in {user_settings_dir}.\n"
            f"Missing file(s): {missing}.\n"
            "Run mko_data_cleaner.app.initialize_settings() to generate default "
            "settings."
        )


class DictionaryStructureError(Exception):
    """Raised when the dictionary DataFrame structure doesn't match the
    ``col_indexes`` configuration.

    This typically happens when the CSV separator configured in
    ``read_settings.from_csv.separator`` doesn't match the dictionary file's
    actual separator, causing Polars to read it as a 1-column DataFrame.
    """

    def __init__(
        self,
        file_path: Path | str | None = None,
        expected_min_columns: int = 0,
        actual_columns: list[str] | None = None,
        hint: str | None = None,
        step: str | None = None,
    ):
        self.file_path = file_path
        self.expected_min_columns = expected_min_columns
        self.actual_columns = actual_columns or []
        self.hint = hint
        self.step = step
        parts = ["Dictionary structure validation failed"]
        if file_path:
            parts.append(f"File: {file_path}")
        if step:
            parts.append(f"Step: {step}")
        parts.append(
            f"Dictionary has {len(self.actual_columns)} column(s) but "
            f"col_indexes references column index {expected_min_columns - 1} "
            f"(needs at least {expected_min_columns} columns)."
        )
        if self.actual_columns:
            parts.append(f"Actual columns: {', '.join(self.actual_columns)}")
        if hint:
            parts.append(f"Hint: {hint}")
        super().__init__("\n  ".join(parts))


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
