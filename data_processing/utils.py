
from pathlib import Path
from re import match

# pattern used to check validity of column and table name before adding them
NAME_PATTERN = f"^[a-zA-Z_][a-zA-Z0-9_]*$"


def is_valid_name(*names: str, pattern: str = None) -> bool:
    """
     Check whether provided name or list of names are valid
    (to be precise corresponds to NAME_PATTERN) to use as table or column names
    :param names: str, list of names to be checked in string format
    :param pattern: str, regex pattern for name validation. If omitted global NAME_PATTERN is used
    :return: bool, True or False
    """
    if pattern is None:
        pattern = NAME_PATTERN
    for name in names:
        if name is None or not match(pattern, str(name)):
            print(f"The name: {str(name)} is not valid, "
                  f"use lowercase english letters and digits")
            return False
    return True





def get_dir_content(*folder, ext: str = 'csv'):
    path = Path(*folder)
    ext = ''.join(filter(str.isalpha, ext))
    if path.exists() is False:
        raise NameError(f"File or folder with path: '{path}' is not found")
    else:
        return Path(path).glob(f'*.{ext}')


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
