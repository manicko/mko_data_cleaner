from os import PathLike
from pathlib import Path
from re import match, sub
import logging
import time
from collections.abc import Iterator
from datetime import date, datetime
from os import PathLike
from pathlib import Path
from typing import Any, Literal

import yaml

from pandas import DataFrame



logger = logging.getLogger(__name__)


# pattern used to check validity of column and table name before adding them
ALLOWED_PATTERN = f"^[a-zA-Z_][a-zA-Z0-9_]*$"
RESTRICTED_PATTERN = r'[#@$%&~*+=<>^`|(){}?!;:,.-\/"]'

DATA_TO_SQL_PARAMS = {
    'if_exists': 'append',
    'index': False,
    'index_label': None,
    'chunksize': 2000
}


def get_names_index(names: list[str], index: list[int]) -> dict[int:str]:
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
    if not isinstance(name, str) or not match(pattern, str(name)):
        print(f"The name: {str(name)} is not valid, "
              f"use lowercase english letters and digits")
        return False
    return True


def make_valid(name: str, pattern: str = RESTRICTED_PATTERN) -> str:
    return sub(pattern, '_', name)


def clean_names(*names: str) -> list[str]:
    """
     Check whether provided name or list of names are valid
    (to be precise corresponds to ALLOWED_PATTERN) to use as table or column names
    :param names: str, list of names to be checked in string format
    :param pattern: str, regex pattern for name validation. If omitted global ALLOWED_PATTERN is used
    :return: list[str], list of valid names
    """
    valid_names = []
    for i, name in enumerate(names):
        if not is_valid_name(name):
            name = make_valid(name)
        if is_valid_name(name) and name not in valid_names:
            valid_names.append(name)
        else:
            valid_names.append(f'col_{i}')
    return valid_names


def generate_column_names(col_num: int, prefix: str = 'col_') -> list:
    """
    Generates list of names in a form of {prefix} + {index}.
    i.e. col_0, col_1 etc.
    :param col_num: str, number of columns
    :param prefix: str, prefix to use before index
    :return: list, list of column names
    """
    col_names = [prefix + str(i) for i in range(col_num)]
    return col_names


def get_dir_content(path: str | PathLike, ext: str = 'yaml', subfolders=True):
    try:
        subfolders = '**/' if subfolders else ''
        files = Path(path).glob(f'{subfolders}*.{ext}')
    except Exception as err:
        raise err
    else:
        return files


def get_path(*path: str, mkdir: bool = False):
    path = Path(*path)
    if mkdir is True:
        try:
            path.mkdir(parents=True, exist_ok=True)
        except FileNotFoundError as err:
            raise FileNotFoundError(f"File or folder with path: '{path}' is not found", err)

    if path.exists() is False:
        try:
            root_dir = Path().absolute()
            from_root = Path.joinpath(root_dir, path)
            if from_root.exists() is False:
                raise NameError(f"File or folder with path: '{path}' is not found")
        except NameError:
            raise NameError(f"File or folder with path: '{path}' is not found")
        else:
            return from_root
    else:
        return path




def str_to_date(date_string: str):
    """
    converts string to date format
    :param date_string: str in format  %Y-%m-%d
    :return: date
    """
    try:
        date_string = datetime.strptime(date_string, "%Y-%m-%d").date()
    except (ValueError, TypeError) as err:
        logger.error(f"wrong format of date_string {date_string}: {err}")
        raise err
    else:
        return date_string






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





def dir_content_to_dict(files, suffix: str = "yaml"):
    return {file.name.removesuffix(suffix): file for file in files}


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


def resolve_path(path: str | Path, base_dir: Path | None = None) -> Path:
    """
    Resolves an absolute path, creating it if necessary.

    If a relative path is given, it is resolved against `base_dir`.

    Args:
        path (Union[str, Path]): The path to resolve (can be absolute or relative).
        base_dir (Union[Path, None], optional): The base directory for resolving relative paths.
            Defaults to the parent directory of this script.

    Returns:
        Path: The resolved absolute path.

    Raises:
        ValueError: If the path cannot be found or created.
    """
    path = Path(path).expanduser()  # Expands `~` (home directory)

    # If the path is absolute and exists, return it immediately
    if path.is_absolute():
        if path.exists():
            return path
        ensure_path_exists(path)  # If not found, attempt to create it
        return path

    # Ensure base_dir is a valid Path
    base_dir = base_dir or Path(__file__).resolve().parent.parent
    resolved_path = (base_dir / path).resolve()

    if not resolved_path.exists():
        ensure_path_exists(resolved_path)  # Create the path if it does not exist

    return resolved_path


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
