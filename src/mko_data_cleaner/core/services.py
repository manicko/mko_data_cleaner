import logging.config
from pathlib import Path
from datetime import datetime
from functools import cached_property

import mko_data_cleaner.core.utils as utils
from mko_data_cleaner.core.models import DataSettings, LoggingSettings
from mko_data_cleaner.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_data_cleaner.core.csv_woker import CSVWorker
from mko_data_cleaner.core.db_woker import DBWorker

class AppService:
    def __init__(self, app_paths: AppPaths, resolver: PathResolver):
        self.app_paths = app_paths
        self.resolver = resolver
        self.prepare_log_paths()
        self._base_path = None

    @cached_property
    def log_config(self) -> LoggingSettings:
        return LoggingSettings(**utils.yaml_to_dict(self.app_paths.log_config))

    @cached_property
    def app_config(self) -> DataSettings:
        return DataSettings(**utils.yaml_to_dict(self.app_paths.app_config))

    @property
    def base_path(self) -> Path:
        return self._base_path

    @base_path.setter
    def base_path(self, base_path: Path):
        try:
            self._base_path = self.resolver.ensure_dir(
                self.resolver.resolve(base_path)
            )
        except FileNotFoundError as e:
            logging.error(f'File not found: {base_path}, {e}')
            raise e


    @property
    def import_path(self) -> Path:
        return Path(self.base_path , self.app_config.data_paths.import_folder)

    @property
    def export_path(self) -> Path:
        return Path(self.base_path , self.app_config.data_paths.export_folder)

    @property
    def dict_path(self) -> Path:
        return Path(self.base_path , self.app_config.data_paths.dict_file)

    @property
    def db_path(self) -> Path:
        return Path(self.base_path , self.app_config.data_paths.db_file)


    def prepare_log_paths(self):
        # logger files
        for handler in self.log_config.handlers.values():
            if isinstance(handler, dict) and "filename" in handler:
                file_path = self.resolver.resolve(handler["filename"])
                self.resolver.ensure_file_parent(file_path)
                handler["filename"] = file_path

    def run_report(self, data_path: str | Path):
        start_time = datetime.now().replace(microsecond=0)
        print(
            f'\n{'-' * 10}  Обработка стартовала: {start_time} {'-' * 10}\n',
            flush=True,
        )
        self.base_path = data_path
        self.resolver.ensure_file_parent(self.db_path)
        self.resolver.ensure_dir(self.export_path)

        csv_worker = CSVWorker(
            data_path=self.import_path,
            data_settings=self.app_config.data_file_settings.model_dump(),
            reader_settings=self.app_config.read_settings.from_csv.model_dump(),
            dict_path=self.dict_path,
            dict_settings=self.app_config.dict_file_settings.model_dump(),
            export_path=self.export_path,
            export_settings=self.app_config.export_settings.to_csv.model_dump()
        )
        #
        sample_data_headers = utils.clean_names(*csv_worker.csv_headers)

        # get dictionary params
        search_cols_index = utils.get_names_index(sample_data_headers, self.app_config.data_file_settings.search_cols)
        search_cols = list(search_cols_index.values())
        actions =  self.app_config.dict_file_settings.actions
        clean_cols_ids = self.app_config.dict_file_settings.clean_cols_ids
        clean_cols = list(clean_cols_ids.keys())

        # DB settings
        table_name = self.app_config.database_settings.table_name
        db_worker = DBWorker(self.db_path, table_name)

        # creating database, datatable, search table
        db_worker.create_search_table(
            data_table=table_name,
            search_columns=search_cols,
            clean_columns=clean_cols,
            col_names=sample_data_headers,
        )

        # loading data to database
        rows_count = 0
        for chunk in csv_worker.get_csv_chunks(sample_data_headers):
            rows_count += db_worker.data_chunk_to_sql(chunk, table_name)

        print(f"{rows_count:,} rows were loaded to '{table_name}'")

        if not self.dict_path.is_file():
            csv_worker.get_merged_dictionary()

        # generate parameters to search and update columns
        params = csv_worker.get_clean_params(
            actions=actions.model_dump(),
            clean_cols_ids=clean_cols_ids,
            search_column_index=search_cols_index
        )

        # looping through search\update params and fill in data
        for col_name, param in params:
            db_worker.search_update_query(table_name, col_name, *param)

        # functionality to check if some rows are still empty after cleaning
        # get_n = select_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)

        csv_worker.export_sql_to_csv(
            db_con=db_worker.db_con,
            data_table=table_name
        )

        end_time = datetime.now().replace(microsecond=0)
        print(
            f'\n{'-' * 10}  Обработка завершена: {end_time}. '
            f'Общее время: {end_time - start_time} {'-' * 10}\n',
            flush=True,
        )


app_service = AppService(app_paths=APP_PATHS, resolver=PathResolver(APP_PATHS.user_dir))

logging.config.dictConfig(app_service.log_config.model_dump())

if __name__ == "__main__":
    app_service.run_report(r'data\snacks')
