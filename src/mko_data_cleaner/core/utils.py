import logging
from os import PathLike
from pathlib import Path
from re import match, sub
from typing import Any

import yaml
from unidecode import unidecode

from mko_data_cleaner.core.errors import WrongDataSettings

logger = logging.getLogger(__name__)

# pattern used to check validity of column and table name before adding them
ALLOWED_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$"


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

    print(
        f"\r[{bar}] {percent:3d}% ({current:,}/{total:,}) — {message} ",
        end="",
        flush=True,
    )
    # new line after finishing the progress
    if current == total:
        print()


def is_valid_name(name: str, pattern: str = ALLOWED_PATTERN) -> bool:
    """
     Check whether provided name or list of names are valid
    (to be precise corresponds to ALLOWED_PATTERN) to use as table or column names
    :param name: str, list of names to be checked in string format
    :param pattern: str, regex pattern for name validation. If omitted global ALLOWED_PATTERN is used
    :return: bool, True or False
    """
    if not isinstance(name, str):
        raise ValueError(
            f"Invalid name: {name}, should be a string, {type(name)} is given"
        )

    if not match(pattern, str(name)):
        logger.warning(
            f"The name: '{str(name)}' is not valid, "
            f"use the allowed pattern '{ALLOWED_PATTERN}'."
        )
        return False
    return True


def validate_names(*names: str) -> None:
    """Validate table and column names."""
    invalid = [name for name in names if not is_valid_name(name)]
    if invalid:
        raise WrongDataSettings(f"Invalid names: {', '.join(invalid)}")


def make_valid(name: str) -> str:
    # 1. translate
    name = unidecode(name)
    # 2. clean
    name = sub(r"[^a-zA-Z0-9_]", "", name)
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
            name = f"col_{i}"
        if name in seen:
            idx = 1
            while name + "_" + str(idx) in seen:
                idx += 1
            name = name + "_" + str(idx)
        valid_names.append(name)
        seen.add(name)
    return valid_names


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
