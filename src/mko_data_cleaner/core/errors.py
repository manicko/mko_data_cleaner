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
