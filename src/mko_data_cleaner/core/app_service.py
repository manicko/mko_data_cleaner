from pathlib import Path
from datetime import datetime
from functools import cached_property
from mko_data_cleaner.core.errors import ConfigError, DataValidationError
import mko_data_cleaner.core.utils as utils
from mko_data_cleaner.core.models import DataSettings, LoggingSettings, ActionType
from mko_data_cleaner.core.paths import APP_PATHS, AppPaths, PathResolver
from mko_data_cleaner.core.csv_service import CSVWorker
from mko_data_cleaner.core.db_service import DBWorker
from mko_data_cleaner.core.dict_service import MappingDict

import logging
import logging.config

logger = logging.getLogger("app_service")


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
        config_data = utils.yaml_to_dict(self.app_paths.app_config)
        try:
            return DataSettings(**config_data)
        except DataValidationError as e:
            raise ConfigError(f"Invalid config: {e}") from e

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
        return Path(self.base_path, self.app_config.data_paths.import_folder)

    @property
    def export_path(self) -> Path:
        return Path(self.base_path, self.app_config.data_paths.export_folder)

    @property
    def dict_path(self) -> Path:
        return Path(self.base_path, self.app_config.data_paths.dict_file)

    @property
    def db_path(self) -> Path:
        return Path(self.base_path, self.app_config.data_paths.db_file)

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

        # get dictionary params
        md = csv_worker.get_dictionary()
        mapping_dict = MappingDict(
            data=md,
            action_col_indexes=self.app_config.dict_file_settings.col_indexes
        )

        # DB settings
        db_worker = DBWorker(
            db_file=self.db_path,
            tbl_name=self.app_config.database_settings.table_name,
            index_column=self.app_config.data_file_settings.index_column,
            date_column=self.app_config.data_file_settings.date_column
        )

        # source columns and columns from dictionary
        db_worker.set_data_tbl_columns(
            *csv_worker.source_headers,
            extra_cols=mapping_dict.extra_col_names
        )

        mapping_dict.build_mapping(
            *db_worker.data_tbl_columns,
            extra_col_names=db_worker.extra_columns
        )

        # # create search table using indexes from dictionary
        db_worker.search_columns = mapping_dict.get_search_columns()
        db_worker.create_table_with_index()

        # loading data to database
        rows_count = 0
        col_count = len(csv_worker.source_headers)

        for chunk in csv_worker.get_data_chunks(db_worker.data_tbl_columns[:col_count]):
            rows_count += csv_worker.data_chunk_to_sql(
                chunk,
                db_worker.data_tbl_name,
                db_worker.db_con
            )

        logger.info(f"{rows_count:,} rows were loaded to data table")

        db_worker.update_distinct_table()

        # print(mapping_dict.get_data_mapping_by_action(ActionType.DELETE))
        # print(mapping_dict.get_data_mapping_by_action(ActionType.ADD))
        # print(mapping_dict.get_data_mapping_by_action(ActionType.REPLACE))

        # apply replace rules
        replace_df = mapping_dict.get_data_mapping_by_action(ActionType.REPLACE)


        csv_worker.data_chunk_to_sql(
                mapping_dict.data,
                'mapping_table',
                db_worker.db_con
            )

        db_worker.apply_mapping('mapping_table')

        # # # looping through search\update params and fill in data
        # for action,match_type, term, col in mapping_dict.get_action_params():
        #     print(params)

        # for col_name, param in params:
        #     db_worker.search_update_query(table_name, col_name, *param)
        #
        # # functionality to check if some rows are still empty after cleaning
        # # get_n = select_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)
        #
        # csv_worker.export_sql_to_csv(
        #     db_con=db_worker.db_con,
        #     data_table=table_name
        # )

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
