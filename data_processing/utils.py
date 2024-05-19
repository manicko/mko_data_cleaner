from os import PathLike
from pathlib import Path
from re import match, sub

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
