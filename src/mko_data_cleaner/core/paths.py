from pathlib import Path

from platformdirs import user_config_dir
from pydantic import BaseModel

APP_NAME: str = __package__.split(".")[0]
APP_DIR: Path = Path(__file__).resolve().parent.parent
USER_DIR: Path = Path(user_config_dir(APP_NAME))


class PathResolver:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir

    def resolve(self, path: Path | str) -> Path:
        path = Path(path).expanduser()
        if not path.is_absolute():
            path = self.base_dir / path
        return path.resolve()

    @staticmethod
    def ensure_dir(path: Path) -> Path:
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def ensure_file_parent(path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


class AppPaths(BaseModel):
    app_dir: Path
    app_name: str
    user_dir: Path

    @property
    def user_settings_dir(self) -> Path:
        return Path(self.user_dir / "settings")

    @property
    def app_settings_dir(self) -> Path:
        return Path(self.app_dir / "settings")

    @property
    def app_config(self) -> Path:
        return Path(self.user_settings_dir, "app_config.yaml")

    @property
    def log_config(self) -> Path:
        return Path(self.user_settings_dir, "log_config.yaml")


APP_PATHS = AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=USER_DIR)
