from pathlib import Path
import yaml
import traceback
import logging


class ReportConfig:
    def __init__(self, report_settings_file):
        self.report_settings = self.yaml_to_dict(report_settings_file)
        self.dict_setting = self.report_settings['DICT_FILE_SETTINGS']
        self.data_settings = self.report_settings['DATA_FILES_SETTINGS']
        self.db_settings = self.report_settings['DATABASE_SETTINGS']
        self.export_settings = self.report_settings['EXPORT_SETTINGS']
        self.reader_settings = self.report_settings['READ_SETTINGS']

        self.import_path = None
        self.export_path = None
        self.db_file = None
        self.dict_path = None

        # DB connection
        table_name = self.db_settings['table_name']

        # set dictionary params
        search_cols = self.data_settings['search_cols']
        actions = self.dict_setting['actions']
        clean_cols_ids = self.dict_setting['clean_cols_ids']
        clean_cols = list(clean_cols_ids.keys())

        # get data folder and files

    def set_working_paths(self):
        work_dir = Path(self.report_settings['PATH']).resolve()
        if not work_dir.is_dir():
            print(f"Data folder with path: '{work_dir}' is not found")
            logging.error(f"Data folder with path: '{work_dir}' is not found")
            exit()

        self.import_path = Path(work_dir, self.data_settings['folder'])
        self.export_path = Path(work_dir, self.export_settings['folder'])
        self.export_path.mkdir(parents=True, exist_ok=True)
        self.dict_path = Path(work_dir, self.dict_setting['folder'], self.dict_setting['file_name'])
        db_path = Path(work_dir, self.db_settings['folder'])
        db_path.mkdir(parents=True, exist_ok=True)
        self.db_file = Path(db_path, self.db_settings['file_name'] + '.db')

    @staticmethod
    def yaml_to_dict(file: str):
        try:
            with open(file, "r", encoding="utf8") as stream:
                data = yaml.safe_load(stream)
        except (FileNotFoundError, yaml.YAMLError) as exc:
            print(exc)
            raise exc
        else:
            return data


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
