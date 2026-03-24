import logging
import os
import sqlite3
import traceback
from collections.abc import Generator
from pathlib import Path
from time import strftime

import polars as pl

from mko_data_cleaner.core.errors import WrongDataSettings
from mko_data_cleaner.core.utils import list_files_in_directory, progress_bar

logger = logging.getLogger(__name__)


class CSVWorker:
    """
    Worker for processing CSV files using Polars.

    Features:
        - streaming CSV reading
        - gzip support
        - SQLite export
        - dictionary merging
    """

    CHUNK_SIZE = 10000
    DATE_REGEX = r"^\d{4}[-/.]\d{2}[-/.]\d{2}$"
    DATE_SAMPLE_SIZE = 500

    def __init__(
        self,
        data_path: str | os.PathLike,
        data_settings: dict,
        reader_settings: dict,
        dict_path: str | os.PathLike,
        dict_settings: dict,
        export_path: str | os.PathLike,
        export_settings: dict,
    ):

        self.data_settings = data_settings
        self.reader_settings = reader_settings
        self.dict_path = Path(dict_path)
        self.dict_settings = dict_settings
        self.export_path = Path(export_path)
        self.export_settings = export_settings
        self.data_path = Path(data_path)

        self.data_files: list[Path] = []
        self.sample_data_file: Path | None = None
        self.source_headers: list[str] = []

        self._set_files_params()

    # ---------------------------------------------------------
    # CSV
    # ---------------------------------------------------------
    def _set_files_params(self):
        ext = self.data_settings.get("extension", "csv")
        self.data_files = list_files_in_directory(
            self.data_path,
            extensions=(ext,),
        )

        if self.data_files and len(self.data_files) > 0:
            self.sample_data_file = self.data_files[0]
            self.source_headers = self.get_csv_headers(self.sample_data_file)
        else:
            raise WrongDataSettings(
                f"Please check settings. "
                f"No files with extension: '{ext}' "
                f"found in: '{self.data_path}'. "
            )

    def get_csv_headers(self, file: str | Path) -> list[str]:
        """Read CSV headers using Polars."""

        try:
            df = pl.read_csv(file, n_rows=0, **self.reader_settings)
            return df.columns

        except Exception as err:
            logger.error(traceback.format_exc())
            raise err

    @staticmethod
    def _is_date_column(
        series: pl.Series,
        date_regex: str,
        threshold: float = 0.8,
    ) -> bool:
        """
        Check if a Series likely contains dates.

        Parameters
        ----------
        series : pl.Series
            Column to test
        date_regex : str
            Regex pattern for date detection
        threshold : float
            Minimum ratio of matching values

        Returns
        -------
        bool
        """

        sample_size = 500
        if series.dtype != pl.String:
            return False

        s = series.head(sample_size).drop_nulls()

        if s.is_empty():
            return False

        match_ratio = s.str.contains(date_regex).mean()

        return match_ratio >= threshold

    def _detect_date_column(self, df: pl.DataFrame, column: str = None) -> str | None:
        """
        Check if column contains dates and if not or not provided,
        trying to check automatically.

        Supports formats:
            DD/MM/YYYY
            DD.MM.YYYY
            DD-MM-YYYY

        Returns
        -------
        str
             column_name if detected else None
        """

        if column:
            try:
                series = df[column]
                if self._is_date_column(series, self.DATE_REGEX):
                    return column
            except pl.exceptions.ColumnNotFoundError:
                logger.error(f"Date column {column} not found.")

        for _, col in enumerate(df.columns):
            if self._is_date_column(df[col], self.DATE_REGEX):
                return col
        return None

    def check_date_column(self, column: str = None) -> tuple[int, str] | None:
        df = pl.read_csv(
            self.sample_data_file, n_rows=self.DATE_SAMPLE_SIZE, **self.reader_settings
        )
        return self._detect_date_column(df, column)

    def _read_csv_in_chunks(
        self, csv_file, headers
    ) -> Generator[pl.DataFrame, None, None]:
        """
        Stream CSV chunks using Polars.

        Returns
        -------
        Generator[pl.DataFrame]
        """
        try:
            df = pl.read_csv_batched(
                csv_file,
                batch_size=self.CHUNK_SIZE,
                new_columns=headers,
                **self.reader_settings,
            )
            while True:
                batches = df.next_batches(1)
                if not batches:
                    break
                yield batches[0]

        except Exception as err:
            logger.error(traceback.format_exc())
            raise err

    def get_data_chunks(
        self, col_names: list[str]
    ) -> Generator[pl.DataFrame, None, None]:
        """Yield CSV chunks from all files."""
        total = len(self.data_files)
        logger.info("Reading of %d files from folder %s", total, self.data_path)

        for i, file in enumerate(self.data_files, start=1):
            logger.debug("Reading file: %s", file)
            progress_bar(message="Reading data", current=i, total=total)

            yield from self._read_csv_in_chunks(file, col_names)

    # ---------------------------------------------------------
    # DICTIONARY
    # ---------------------------------------------------------

    def get_dictionary(self) -> pl.DataFrame:
        if not self.dict_path.is_file():
            self.get_merged_dictionary()
        return pl.read_csv(self.dict_path, **self.reader_settings)

    def get_merged_dictionary(self):
        try:
            dfs = []
            ext = self.dict_settings.get("extension", ".csv")
            for file in list_files_in_directory(
                self.dict_path.parent, extensions=(ext,)
            ):
                df = pl.read_csv(file, **self.reader_settings)
                dfs.append(df)
            merged = pl.concat(dfs)
            merged = merged.unique()
            merged = merged.sort("search_column_idx")
            merged.write_csv(
                self.dict_path,
                separator=";",
                include_header=True,
            )

        except Exception as err:
            logger.error(traceback.format_exc())
            raise err

        else:
            logger.info(f"Merged dictionary created: {self.dict_path}")

    # ---------------------------------------------------------
    # FILE NAMING
    # ---------------------------------------------------------

    def get_file_name(self, name_prefix, name_suffix):
        ext = self.get_files_suffix(self.export_settings["compression"])
        time_str = strftime("%Y%m%d-%H%M%S")
        return f"{name_prefix}_{time_str}_{name_suffix}{ext}"

    @staticmethod
    def get_files_suffix(compression: str | dict | None) -> str:
        base = ".csv"
        if not compression or compression == "infer":
            return base
        if isinstance(compression, dict):
            method = compression.get("method", "").lower()
        else:
            method = str(compression).lower()
        match method:
            case "gzip" | "gz":
                return base + ".gz"
            case "bz2":
                return base + ".bz2"
            case "xz":
                return base + ".xz"
            case "zip":
                return base + ".zip"
            case "zstd":
                return base + ".zst"
            case _:
                return base

    # ---------------------------------------------------------
    # EXPORT SQLITE -> CSV
    # ---------------------------------------------------------

    def export_sql_to_csv(
        self,
        db_con: sqlite3.Connection,
        data_table: str,
        file_prefix: str | None = None,
        export_path: Path | str | None = None,
    ):
        """
        Export SQLite table to CSV files using Polars.

        The table is exported in chunks to avoid loading the entire
        dataset into memory. Each chunk is written into a separate
        CSV file (optionally compressed).

        Parameters
        ----------
        db_con : sqlite3.Connection
            Active SQLite connection.
        data_table : str
            Source table name.
        file_prefix : str, optional
            Prefix for exported file names.
        export_path : Path | str, optional
            Directory for exported files.

        Returns
        -------
        None
        """

        logger.debug(f"Starting export from {data_table}")

        try:
            file_prefix = file_prefix or data_table
            export_path = Path(export_path or self.export_path)
            export_path.mkdir(parents=True, exist_ok=True)

            file_name = self.get_file_name(file_prefix, "{file_index}")

            params = self.export_settings.copy()

            max_rows = params.pop("chunk_size", 10000)

            file_index = 1
            row_counter = 0

            # -----------------------------
            # cursor for data
            # -----------------------------
            data_cursor = db_con.cursor()
            data_cursor.execute(f"SELECT * FROM {data_table}")

            columns = [col[0] for col in data_cursor.description]

            schema_overrides = {col: pl.Utf8 for col in columns}

            # -----------------------------
            # cursor for row count
            # -----------------------------
            count_cursor = db_con.cursor()
            count_cursor.execute(f"SELECT COUNT(*) FROM {data_table}")
            total_rows = count_cursor.fetchone()[0]

            if total_rows == 0:
                logger.debug(f"Table {data_table} is empty.")
                return

            rows = data_cursor.fetchmany(max_rows)

            while rows:
                df = pl.DataFrame(
                    rows,
                    schema=columns,
                    orient="row",
                    schema_overrides=schema_overrides,
                )

                row_counter += df.height

                progress_bar(
                    message=f"Exporting {data_table}",
                    current=row_counter,
                    total=total_rows,
                )

                file_path = export_path / file_name.format(file_index=file_index)

                df.write_csv(
                    file=file_path,
                    **params,
                )

                logger.debug(f"Exported chunk {file_index} ({df.height:,} rows)")

                file_index += 1
                rows = data_cursor.fetchmany(max_rows)

            logger.debug(
                f"Successfully exported {row_counter:,} rows from table '{data_table}'"
            )

        except Exception as err:
            logger.error(traceback.format_exc())
            raise err
