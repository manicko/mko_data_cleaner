from typing import (Any, List, Optional, Union)
import pandas as pd
from pathlib import Path
import time
import logging
import traceback


class CSVWorker:
    def __init__(self):
        pass

    def export_sql_to_csv(self,
                          db_con,
                          data_table: str,
                          file_path: str = None,
                          file_prefix: Optional[str] = None,
                          **to_csv_params: Optional[Any]) -> bool:
        """
        Exports SQLights data_table using pandas. Output filename will have timestamp.
        :param db_con: SQLAlchemy connectable, str, or sqlite3 connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library. If a DBAPI2 object, only sqlite3 is supported. The user is responsible
        for engine disposal and connection closure for the SQLAlchemy connectable; str
        connections are closed automatically. See
        `here <https://docs.sqlalchemy.org/en/13/core/connections.html>`_.
        :param data_table: str, name of the table to be exported
        :param to_csv_params: Optional[Any], use to update global CSV_EXPORT_PARAMS
        :param file_path: path to the CSV files storage
        :param file_prefix: Name of the output file to be used
        :param to_csv_params: dict, settings for Pandas' CSV export
        :return: bool, True or False depending on the operation success
        """

        if Path.is_dir(Path(file_path)) is False:
            print(f"Directory '{file_path}' is not a proper file path directory for CSV export")
            return False
        if file_prefix is None:
            file_prefix = data_table
        time_str = time.strftime("%Y%m%d-%H%M%S")

        # define base settings for pandas to_CSV

        try:
            params = to_csv_params.copy()
            max_file_rows = params.pop('chunksize', 10000)
            row_counter = 0
            file_index = 0
            print(f'Data writing in progress:')
            ext = ''
            if 'compression' in to_csv_params and 'method' in to_csv_params['compression']:
                ext = '.' + to_csv_params['compression']['method']
                ext = ext.replace('.gzip', '.gz')
            # if used with zip chunksize MUST be the same as max_file_rows
            # due to the bug in Pandas module
            output_file_name = Path(file_path, f'{file_prefix}_{time_str}_{str(file_index)}.csv{ext}')
            for data_chunk in pd.read_sql(f'SELECT * FROM {data_table}', db_con, chunksize=5000):
                row_counter += len(data_chunk.index)
                if row_counter > max_file_rows * file_index:
                    file_index += 1
                    params['header'] = True  # turn on headers for a new file
                    output_file_name = Path(file_path, f'{file_prefix}_{time_str}_{str(file_index)}.csv{ext}')
                data_chunk.to_csv(path_or_buf=output_file_name, **params)
                params['header'] = False  # turn of headers if continue
                print(f'{row_counter:,} rows', end='\r')

            print(f"{row_counter:,} data rows were successfully exported to: {output_file_name}")
            return True
        except pd.errors.DataError:
            logging.error(traceback.format_exc())

    def count_csv_columns(**reader_settings) -> int:
        """Counts columns with data in CSV file
        :param reader_settings: uses pandas csv_reader settings
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        :return: integer count of columns with data
        """
        reader_settings['nrows'] = 0  # override to read header row only
        try:
            csv_column_names = pd.read_csv(**reader_settings).columns.tolist()
            return len(csv_column_names)
        except pd.errors.DataError:
            logging.error(traceback.format_exc())

    def read_csv_chunks(**reader_settings) -> iter:
        """
        Generator to read data from CSV file in chunks
        :param reader_settings: dict, use pandas reader params
        :return: iterator
        """
        try:
            with pd.read_csv(**reader_settings) as csv_data_reader:
                for data_chunk in csv_data_reader:
                    yield data_chunk
        except pd.errors.DataError:
            logging.error(traceback.format_exc())

    def get_csv_columns(csv_reader_settings: dict[str, Any], sample_csv_file: Union[str, Path]) -> list[str]:
        """

        :param sample_csv_file: pathstring to CSV file
        :param csv_reader_settings: use to override global CSV_READ_PARAMS
            for details refer to pandas CSV reader settings
            https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

        :return:
        """
        if Path.is_file(Path(sample_csv_file)) is False:
            raise NameError(f"File: '{sample_csv_file}' not found")

        # copy and update settings for pandas CSV reader
        csv_read_params = csv_reader_settings.copy()
        csv_read_params['filepath_or_buffer'] = sample_csv_file

        # get columns count from CSV to reserve same number of columns in SQLight table
        num_cols = count_csv_columns(**csv_read_params)
        # generate column names to use for loader
        col_names = generate_column_names(num_cols)
        return col_names
