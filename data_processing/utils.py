import yaml
from pathlib import Path


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


def get_dir_content(*folder, ext: str = 'csv'):
    path = Path(*folder)
    ext = ''.join(filter(str.isalpha, ext))
    if path.exists() is False:
        raise NameError(f"File or folder with path: '{path}' is not found")
    else:
        return Path(path).glob(f'*.{ext}')


def yaml_to_dict(file: str):
    with open(file, "r", encoding="utf8") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
        else:
            return data
