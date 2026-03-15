import os
import logging
import traceback
from pathlib import Path
from typing import Generator, Optional, Literal
from time import strftime

import polars as pl
import sqlite3

from .utils import get_dir_content

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

        self.data_files = get_dir_content(
            data_path,
            ext=self.data_settings.get("extension", "csv"),
        )
        self.sample_data_file = next(self.data_files)
        self.source_headers = self.get_csv_headers(self.sample_data_file)

    # ---------------------------------------------------------
    # CSV
    # ---------------------------------------------------------

    def get_csv_headers(self, file: str | Path) -> list[str]:
        """Read CSV headers using Polars."""

        try:
            df = pl.read_csv(file, n_rows=0, **self.reader_settings)
            return df.columns

        except Exception as err:
            logger.error(traceback.format_exc())
            raise err

    def _read_csv_in_chunks(self, csv_file, headers) -> Generator[pl.DataFrame, None, None]:
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

    def get_data_chunks(self, col_names: list[str]) -> Generator[pl.DataFrame, None, None]:
        """Yield CSV chunks from all files."""
        file = self.sample_data_file
        while file:
            logger.info(f"Reading file: {file}")
            yield from self._read_csv_in_chunks(file, col_names)
            file = next(self.data_files, False)


    @staticmethod
    def data_chunk_to_sql(
            chunk: pl.DataFrame,
            table_name: str,
            db_con: sqlite3.Connection,
            if_exists: Literal["append", "replace", "fail"] = "append",
            use_fallback: bool = False,
    ) -> int:
        """
        Write Polars DataFrame chunk into SQLite table.

        Prefers Polars write_database; falls back to executemany.

        Parameters
        ----------
        chunk : pl.DataFrame
            Data chunk.
        table_name : str
            Target table.
        db_con : sqlite3.Connection
            Open SQLite connection.
        if_exists : {"append","replace","fail"}
            Table behaviour.
        use_fallback : bool
            Force executemany.

        Returns
        -------
        int
            Inserted rows.
        """

        inserted_rows = 0

        if not use_fallback:
            try:

                uri = f"sqlite:///{db_con.execute('PRAGMA database_list').fetchone()[2]}"

                inserted_rows = chunk.write_database(
                    table_name=table_name,
                    connection=uri,
                    if_table_exists=if_exists,
                    engine="sqlalchemy",
                )

                logger.debug(f"write_database → {inserted_rows:,} rows")

                return inserted_rows

            except Exception as err:
                logger.warning(
                    f"write_database failed ({err}). Switching to executemany."
                )
                use_fallback = True

        if use_fallback:
            try:
                cursor = db_con.cursor()
                cols = chunk.columns
                placeholders = ",".join(["?"] * len(cols))
                sql = f"""
                INSERT INTO {table_name}
                ({",".join(cols)})
                VALUES ({placeholders})
                """
                cursor.executemany(
                    sql,
                    chunk.iter_rows()
                )
                inserted_rows = chunk.height
                logger.debug(f"executemany → {inserted_rows:,} rows")

            except Exception as err:
                db_con.rollback()
                logger.error(f"executemany failed: {err}")
                raise

        return inserted_rows

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
            for file in get_dir_content(
                self.dict_path.parent,
                self.dict_settings.get("extension", ".csv"),
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
    def get_files_suffix(compression: Optional[str | dict]) -> str:
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
        file_prefix: Optional[str] = None,
    ):
        """
        Export SQLite table to gzip CSV using Polars.
        """

        logger.info(f"Starting export from {data_table}")

        try:
            file_prefix = file_prefix or data_table
            file_name = self.get_file_name(file_prefix, "{file_index}")
            params = self.export_settings.copy()
            max_rows = params.pop("chunksize", 10000)
            file_index = 1
            row_counter = 0
            cursor = db_con.cursor()
            cursor.execute(f"SELECT * FROM {data_table}")
            columns = [col[0] for col in cursor.description]
            schema_overrides = {col: pl.Utf8 for col in columns}  # force to str
            rows = cursor.fetchmany(max_rows)

            while rows:
                df = pl.DataFrame(rows, schema=columns, orient="row", schema_overrides = schema_overrides)
                row_counter += df.height

                file = Path(
                    self.export_path,
                    file_name.format(file_index=file_index),
                )

                df.write_csv(
                    file,
                    compression=params.get("compression", "gzip"),
                )

                logger.debug(f"Exported {row_counter:,} rows")
                file_index += 1
                rows = cursor.fetchmany(max_rows)
            logger.info(f"{row_counter:,} rows exported")

        except Exception as err:
            logger.error(traceback.format_exc())
            raise err

