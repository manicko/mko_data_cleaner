import logging
import logging.config
from datetime import datetime
from functools import cached_property
from pathlib import Path
from time import perf_counter

import mko_data_cleaner.core.utils as utils
from mko_data_cleaner.core.csv_service import CSVWorker
from mko_data_cleaner.core.db_service import DBWorker
from mko_data_cleaner.core.dict_service import MappingDict
from mko_data_cleaner.core.errors import (
    ConfigError,
    DataValidationError,
    SettingsNotInitializedError,
)
from mko_data_cleaner.core.models import (
    DataSettings,
    LoggingSettings,
    MappingColumns,
    ReportResult,
)
from mko_data_cleaner.core.paths import (
    APP_DIR,
    APP_NAME,
    APP_PATHS,
    AppPaths,
    PathResolver,
)
from mko_data_cleaner.core.utils import progress_bar

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
            self._base_path = self.resolver.ensure_dir(self.resolver.resolve(base_path))
        except FileNotFoundError as e:
            logging.error(f"File not found: {base_path}, {e}")
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

    @staticmethod
    def _import_data(db_worker, csv_worker) -> int:
        rows_count = 0
        col_count = len(csv_worker.source_headers)

        for chunk in csv_worker.get_data_chunks(db_worker.data_tbl_columns[:col_count]):
            rows_count += chunk.write_database(
                table_name=db_worker.data_tbl_name,
                connection=db_worker.db_adb_con,
                engine="adbc",
                if_table_exists="append",
            )
            logger.debug(f"write_database → {rows_count:,} rows")
        db_worker.db_adb_con.commit()

        logger.info(f"{rows_count:,} rows were loaded to data table")

        db_worker.update_index_from_data()
        return rows_count

    def run_report(self, data_path: str | Path) -> ReportResult:
        start_time = datetime.now().replace(microsecond=0)
        t_start = perf_counter()
        print(
            f"\n{'-' * 10}  Обработка стартовала: {start_time} {'-' * 10}\n",
            flush=True,
        )
        self.base_path = data_path
        self.resolver.ensure_file_parent(self.db_path)
        self.resolver.ensure_dir(self.export_path)

        rows_loaded = 0
        rows_exported = 0
        null_data_files: list[Path] = []
        timings: dict[str, float] = {}

        csv_worker = CSVWorker(
            data_path=self.import_path,
            data_settings=self.app_config.data_file_settings.model_dump(),
            reader_settings=self.app_config.read_settings.from_csv.model_dump(),
            dict_path=self.dict_path,
            dict_settings=self.app_config.dict_file_settings.model_dump(),
            export_path=self.export_path,
            export_settings=self.app_config.export_settings.to_csv.model_dump(),
        )

        # reading mapping params
        df = csv_worker.get_dictionary()
        mapping_dict = MappingDict(
            data=df, action_col_indexes=self.app_config.dict_file_settings.col_indexes
        )

        # DB settings
        date_column = self.app_config.data_file_settings.date_column
        date_column = csv_worker.check_date_column(date_column)

        with DBWorker(
            db_file=self.db_path,
            tbl_name=self.app_config.database_settings.table_name,
            index_column=self.app_config.data_file_settings.index_column,
            date_column=date_column,
        ) as db_worker:

            # setting and clearing up column names before import
            db_worker.set_data_tbl_columns(
                *csv_worker.source_headers, extra_cols=mapping_dict.extra_col_names
            )

            # adjusting mapping in accordance with cleaned column names
            # prepare search patterns
            mapping_dict.build_mapping(
                *db_worker.data_tbl_columns, extra_col_names=db_worker.extra_columns
            )

            # creating index and search tables
            db_worker.search_columns = mapping_dict.search_columns
            db_worker.create_table_with_index()
            db_worker.create_rules_matches()

            # creating fts search table with triggers if there are
            # fts patterns in mapping
            if not mapping_dict.fts_data.is_empty():
                db_worker.link_search_table()

            # loading data to database
            t_import = perf_counter()
            rows_loaded = self._import_data(db_worker, csv_worker)
            timings["import"] = perf_counter() - t_import

            # importing fts rules to db
            t_mapping = perf_counter()
            if not mapping_dict.fts_data.is_empty():
                mapping_dict.fts_data.select(
                    [
                        MappingColumns.mapping_index,
                        MappingColumns.pattern,
                    ]
                ).write_database(
                    table_name="fts_temp_tbl",
                    connection=db_worker.db_adb_con,
                    engine="adbc",
                    if_table_exists="replace",
                )
                db_worker.insert_matches_from_fts("fts_temp_tbl")
                db_worker.db_adb_con.commit()

            if not mapping_dict.like_data.is_empty():
                # importing full dictionary to db and
                # creating mapping table with indexes
                mapping_dict.like_data.select(
                    [
                        MappingColumns.mapping_index,
                        MappingColumns.column_name,
                        MappingColumns.pattern,
                    ]
                ).write_database(
                    table_name="temp_tbl",
                    connection=db_worker.db_adb_con,
                    engine="adbc",
                    if_table_exists="replace",
                )
                db_worker.db_adb_con.commit()
                db_worker.insert_matches(mapping_table="temp_tbl")

            rules_count_total = mapping_dict.data.height
            rules_count = 0

            for action, data in mapping_dict.generate_rules_blocks():
                rules_count += data.height
                progress_bar(
                    message="Applying rules",
                    current=rules_count,
                    total=rules_count_total,
                )

                data.write_database(
                    table_name="mapping_table",
                    connection=db_worker.db_adb_con,
                    engine="adbc",
                    if_table_exists="replace",
                )
                db_worker.db_adb_con.commit()
                rules_columns = data.columns.copy()

                db_worker.apply_mapping(
                    mapping_table="mapping_table",
                    action_type=action,
                    extra_cols=rules_columns,
                    separator=self.app_config.dict_file_settings.add_separator,
                )

            timings["mapping"] = perf_counter() - t_mapping

            # checking non mapped data
            db_worker.build_non_mapped()

            null_data_file_paths: list[Path] = []
            csv_worker.export_sql_to_csv(
                db_con=db_worker.db_con,
                file_prefix="null_data",
                export_path=f"{self.base_path}",
                data_table=db_worker.non_mapped_table,
                output_files=null_data_file_paths,
            )
            null_data_files = sorted(null_data_file_paths)

            # synchronizing and exporting

            t_export = perf_counter()
            print(
                "Synchronizing the index table with the data table is in progress. "
                "Please be patient, this may take some time.",
                flush=True,
            )
            db_worker.sync_with_data_table()

            rows_exported = csv_worker.export_sql_to_csv(
                db_con=db_worker.db_con,
                data_table=db_worker.data_tbl_name,
                export_path=self.export_path,
            )
            timings["export"] = perf_counter() - t_export

        end_time = datetime.now().replace(microsecond=0)
        print(
            f"\n{'-' * 10}  Обработка завершена: {end_time}. "
            f"Общее время: {end_time - start_time} {'-' * 10}\n",
            flush=True,
        )

        return ReportResult(
            report_dir=self.base_path,
            import_dir=self.import_path,
            export_dir=self.export_path,
            db_path=self.db_path,
            null_data_files=null_data_files,
            started_at=start_time,
            finished_at=end_time,
            duration=perf_counter() - t_start,
            timings=timings,
            rows_loaded=rows_loaded,
            rows_exported=rows_exported,
        )


def build_app_paths(settings_dir: Path | None = None) -> AppPaths:
    """Build :class:`AppPaths`, optionally overriding the user settings directory.

    When ``settings_dir`` is ``None`` the platformdirs-backed :data:`APP_PATHS`
    singleton is returned so that production behavior is unchanged. Otherwise a
    new :class:`AppPaths` is constructed whose ``user_dir`` is ``settings_dir``,
    which makes ``user_settings_dir`` resolve to ``settings_dir / "settings"``,
    exactly mirroring :data:`APP_PATHS` for the default case.
    """
    if settings_dir is None:
        return APP_PATHS
    return AppPaths(app_dir=APP_DIR, app_name=APP_NAME, user_dir=Path(settings_dir))


def _require_settings(app_paths: AppPaths) -> None:
    """Validate that ``app_config.yaml`` and ``log_config.yaml`` exist.

    Raises :class:`SettingsNotInitializedError` when either required settings
    file is missing from the user settings directory.
    """
    missing: list[str] = []
    if not app_paths.app_config.exists():
        missing.append("app_config.yaml")
    if not app_paths.log_config.exists():
        missing.append("log_config.yaml")
    if missing:
        raise SettingsNotInitializedError(
            user_settings_dir=app_paths.user_settings_dir,
            missing_files=missing,
        )


def get_app_service(settings_dir: Path | None = None) -> AppService:
    """Lazily construct :class:`AppService`, validating settings first.

    This replaces the former module-level singleton so that
    ``import mko_data_cleaner.app`` performs no filesystem side effects and
    configures no logging. The :func:`logging.config.dictConfig` call that used
    to run at import time is moved here, after the service has been constructed
    and the settings gate has passed.
    """
    app_paths = build_app_paths(settings_dir)
    _require_settings(app_paths)
    service = AppService(app_paths=app_paths, resolver=PathResolver(app_paths.user_dir))
    logging.config.dictConfig(service.log_config.model_dump())
    return service


if __name__ == "__main__":
    get_app_service().run_report(r"data\mvideo")
