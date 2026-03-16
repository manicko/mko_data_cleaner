from re import match, sub
import logging
import time
from os import PathLike
from pathlib import Path
from typing import Any, Literal

from unidecode import unidecode

from mko_data_cleaner.core.errors import WrongDataSettings
from mko_data_cleaner.core.models import ActionType, MatchType
import yaml

from pandas import DataFrame

logger = logging.getLogger(__name__)

# pattern used to check validity of column and table name before adding them
ALLOWED_PATTERN = f"^[a-zA-Z_][a-zA-Z0-9_]*$"


def progress_bar(message: str, current: int, total: int) -> None:
    """
    Displays a dynamic progress bar by overwriting the previous line.

    Args:
        message: Message to display (e.g. "Processing files", "Reading")
        current: Current progress step
        total: Total number of steps
    """
    if total <= 0:
        return

    percent = 100 if current >= total else int(current / total * 100)
    filled = percent // 5
    bar = "█" * filled + "░" * (20 - filled)

    print(f"\r[{bar}] {percent:3d}% ({current:,}/{total:,}) — {message} ",
          end="", flush=True)
    # new line after finishing the progress
    if current==total:
        print()



def parse_action(value: str) -> ActionType | None:
    """Safely parse Action enum."""
    try:
        return ActionType(value)
    except ValueError:
        return None


def parse_match(value: str) -> MatchType | None:
    """Safely parse Match enum."""
    try:
        return MatchType(value)
    except ValueError:
        return None



def get_names_index(names: list[str], index: list[int]) -> dict[int, str]:
    index = list(map(int, index))
    return {i: names[i] for i in index}


def is_valid_name(name: str, pattern: str = ALLOWED_PATTERN) -> bool:
    """
     Check whether provided name or list of names are valid
    (to be precise corresponds to ALLOWED_PATTERN) to use as table or column names
    :param name: str, list of names to be checked in string format
    :param pattern: str, regex pattern for name validation. If omitted global ALLOWED_PATTERN is used
    :return: bool, True or False
    """
    if not isinstance(name, str):
        raise ValueError(f"Invalid name: {name}, should be a string, {type(name)} is given")

    if not match(pattern, str(name)):
        logger.warning(f"The name: '{str(name)}' is not valid, "
              f"use the allowed pattern '{ALLOWED_PATTERN}'.")
        return False
    return True

def validate_names(*names: str) -> None:
    """Validate table and column names."""
    invalid = [name for name in names if not is_valid_name(name)]
    if invalid:
        raise WrongDataSettings(
            f"Invalid names: {', '.join(invalid)}"
        )

def make_valid(name: str) -> str:
    # 1. translate
    name = unidecode(name)
    # 2. clean
    name = sub(r'[^a-zA-Z0-9_]', '', name)
    return name


def clean_names(*names: str) -> list[str]:
    """
     Check whether provided name or list of names are valid
    (to be precise corresponds to ALLOWED_PATTERN) to use as table or column names
    :param names: str, list of names to be checked in string format
    :return: list[str], list of valid names
    """
    valid_names = []
    seen = set()
    for i, n in enumerate(names):
        name = n if is_valid_name(n) else make_valid(n)
        if not name:
            name = f'col_{i}'
        if name in seen:
            idx = 1
            while name + '_' + str(idx) in seen:
                idx += 1
            name = name + '_' + str(idx)
        valid_names.append(name)
        seen.add(name)
    return valid_names


def get_dir_content(path: str | PathLike, ext: str = 'yaml', subfolders=True):
    try:
        subfolders = '**/' if subfolders else ''
        files = Path(path).glob(f'{subfolders}*.{ext}')
    except Exception as err:
        raise err
    else:
        return files


def get_files_suffix(compression: str | dict = None):
    """Возвращает полное расширение файла с учётом сжатия.
    Всегда нормализует gzip → .gz (стандартное и надёжное расширение).
    """
    base = ".csv"

    if compression is None or compression == "infer" or not compression:
        return base

    # Получаем метод сжатия
    if isinstance(compression, dict):
        method = compression.get("method", "").lower().strip()
    else:
        method = str(compression).lower().strip()

    # Нормализация gzip (самая частая проблема)
    if method in ("gzip", ".gzip", "gz", ".gz"):
        return base + ".gz"

    # Другие популярные сжатия
    elif method in ("bz2", "bzip2"):
        return base + ".bz2"
    elif method in ("xz",):
        return base + ".xz"
    elif method in ("zip",):
        return base + ".zip"
    elif method in ("zstd",):
        return base + ".zst"

    else:
        clean = method.strip(".")
        return base + "." + clean


def csv_to_file(
        data_frame: DataFrame,
        csv_path_out: PathLike,
        file_prefix: str = "",
        compression: (
                Literal["infer", "gzip", "bz2", "zip", "xz", "zstd", "tar"]
                | None
                | dict[str, Any]
        ) = "infer",
        add_time: bool = True,
        *args,
        **kwargs,
):
    time_str = ""
    if add_time:
        time_str = "_" + time.strftime("%Y%m%d_%H%M%S")
    ext = get_files_suffix(compression)
    out_file = Path(csv_path_out, f"{file_prefix}{time_str}{ext}")

    try:
        encoding = kwargs.pop("encoding", "utf-8-sig")
        data_frame.to_csv(
            *args,
            path_or_buf=out_file,
            index=False,
            mode="x",
            decimal=",",
            sep=";",
            encoding=encoding,
            compression=compression,
            **kwargs,
        )
    except FileExistsError:
        logger.warning(f"File report {out_file} already exists. Skip it.")


def list_files_in_directory(
        path: str | PathLike[str],
        extensions: tuple[str, ...] = ("yaml", "json"),
        include_subfolders: bool = False,
) -> list[Path]:
    """
    Lists files in a directory with specific extensions.

    Args:
        path (Union[str, PathLike]): The directory path.
        extensions (Tuple[str, ...]): Allowed file extensions (default: ('csv', 'txt')).
        include_subfolders (bool): Whether to include subfolders (default: False).

    Returns:
        List[Path]: A list of file paths matching the given extensions.
    """
    try:
        files: list[Path] = []
        subfolder_pattern = "**/" if include_subfolders else ""
        for ext in extensions:
            files.extend(Path(path).glob(f'{subfolder_pattern}*.{ext.strip(".")}'))
        return files
    except Exception as err:
        logger.exception(f"Error reading directory {path}: {err}")
        return []


def ensure_path_exists(path: Path) -> None:
    """
    Ensures that a given path exists, creating directories if necessary.

    Args:
        path (Path): Path to a file or directory.

    Raises:
        ValueError: If the path cannot be created.
    """
    try:
        if path.exists():
            return  # Path already exists, no action needed
        if path.suffix:  # If it's a file, create its parent directory
            path.parent.mkdir(parents=True, exist_ok=True)
        else:  # If it's a directory, create it
            path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise ValueError(f"Failed to create path {path}: {e}") from e


def yaml_to_dict(file: str | PathLike) -> dict[str, Any] | None:
    """
    Loads configuration from a YAML file.

    Args:
        file (Path): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Parsed configuration dictionary, or an empty dict if the file does not exist or is invalid.
    """
    try:
        with open(file, encoding="utf8") as cfg:
            return yaml.safe_load(cfg) or {}
    except yaml.YAMLError as err:
        logger.error(err)
    except FileNotFoundError as err:
        logger.error(f"No such file or directory{file} {err}")


def merge_dicts(dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
    """
    Recursively merges two dictionaries.

    If both dictionaries have the same key and the value is also a dictionary, it merges them recursively.
    Otherwise, `dict2`'s value overwrites `dict1`'s value.

    Args:
        dict1 (Dict[Any, Any]): The first dictionary.
        dict2 (Dict[Any, Any]): The second dictionary.

    Returns:
        Dict[Any, Any]: The merged dictionary.
    """
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        return dict2
    for k in dict2:
        if k in dict1:
            dict1[k] = merge_dicts(dict1[k], dict2[k])
        else:
            dict1[k] = dict2[k]
    return dict1
