import shutil
from pathlib import Path

from mko_data_cleaner.core.paths import APP_DIR, APP_NAME, APP_PATHS, AppPaths


def _copy_templates(destination: Path, force: bool = False) -> None:
    """
    Copy template files from package settings directory to user settings directory.
    """

    source_dir = Path(APP_PATHS.app_settings_dir)
    destination.mkdir(parents=True, exist_ok=True)

    for item in source_dir.rglob("*"):
        relative_path = item.relative_to(source_dir)
        target_path = destination / relative_path

        if item.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue

        if target_path.exists() and not force:
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target_path)


def init_project(force: bool = False, settings_dir: Path | None = None) -> Path:
    """
    Initialize configuration directory with default templates.

    Args:
        force: Overwrite existing files.
        settings_dir: Optional user base directory. When provided,
            settings are written to ``settings_dir / "settings"``
            instead of the platformdirs-default location.

    Returns:
        Path to the directory where settings were written.
    """

    if settings_dir is None:
        user_settings_dir = APP_PATHS.user_settings_dir
    else:
        app_paths = AppPaths(
            app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir)
        )
        user_settings_dir = app_paths.user_settings_dir

    _copy_templates(user_settings_dir, force)
    return user_settings_dir


if __name__ == "__main__":
    init_project(force=True)
