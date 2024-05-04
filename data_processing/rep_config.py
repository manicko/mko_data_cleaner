import os
from pathlib import Path
import yaml
import logging


class ReportConfig:
    def __init__(self, report_settings_file):
        self.report_settings = self.yaml_to_dict(report_settings_file)

        self.dict_settings = self.report_settings['DICT_FILE_SETTINGS']
        self.data_settings = self.report_settings['DATA_FILES_SETTINGS']
        self.export_settings = self.report_settings['EXPORT_SETTINGS']['to_csv']
        self.reader_settings = self.report_settings['READ_SETTINGS']['from_csv']
        self.db_settings = self.report_settings['DATABASE_SETTINGS']

        self.import_path: str | os.PathLike | None = None
        self.export_path: str | os.PathLike | None = None
        self.db_file: str | os.PathLike | None = None
        self.dict_path: str | os.PathLike | None = None
        self.set_working_paths()

    def set_working_paths(self):
        work_dir = Path(self.report_settings['PATH']).resolve()
        if not work_dir.is_dir():
            print(f"Data folder with path: '{work_dir}' is not found")
            logging.error(f"Data folder with path: '{work_dir}' is not found")
            exit()
        # path with data files
        self.import_path = Path(work_dir, self.data_settings['folder'])

        # path to extract clean data
        self.export_path = Path(work_dir, self.report_settings['EXPORT_SETTINGS']['folder'])
        self.export_path.mkdir(parents=True, exist_ok=True)

        # path to cleaning dictionary
        self.dict_path = Path(work_dir, self.dict_settings['folder'], self.dict_settings['file_name'])

        # path to database file
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
