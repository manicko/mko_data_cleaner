from typing import (Any, List, Optional, Union)
from pathlib import Path
import logging
import traceback
import time
import pandas as pd

from .utils import (
    get_dir_content,
    read_csv_chunks

)


class CSVWorker:
    def __init__(self, data_path, data_settings, reader_settings, export_path, export_settings):
        self.data_settings = data_settings
        self.export_path = export_path
        self.reader_settings = reader_settings
        self.export_settings = export_settings

        self.data_files = get_dir_content(data_path, ext=self.data_settings.get('ext', 'csv'))
        self.sample_data_file = next(self.data_files)
        self.csv_headers = self.get_csv_headers(self.sample_data_file)

    def get_csv_headers(self, file) -> list:
        """Counts columns with data in CSV file
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        :return: integer count of columns with data
        """
        # get current settings
        try:
            cur_rows = None
            if 'nrows' in self.reader_settings:
                cur_rows = self.reader_settings.pop('nrows')
            csv_column_names = pd.read_csv(filepath_or_buffer=file, nrows=0, **self.reader_settings).columns.tolist()
        except pd.errors.DataError as err:
            logging.error(traceback.format_exc())
            raise err
        else:
            if cur_rows is not None:
                self.reader_settings['nrows'] = cur_rows
            return csv_column_names

    def get_chunk_from_csv(self, csv_file, headers) -> iter:
        """
        Generator to read data from CSV file in chunks
        :return: iterator
        """
        try:
            with pd.read_csv(
                    filepath_or_buffer=csv_file,
                    names=headers,
                    **self.reader_settings
            ) as csv_data_reader:
                for data_chunk in csv_data_reader:
                    yield data_chunk
        except pd.errors.DataError as err:
            logging.error(traceback.format_exc())
            raise err

    def get_csv_chunks(self, col_names: list[str]):
        """
        :param col_names:
        :return: Pandas data chunk
        """
        file = self.sample_data_file
        d = self.reader_settings.copy()
        print(f'Reading file: {file}')
        d['names'] = col_names
        d['filepath_or_buffer'] = file
        return read_csv_chunks(**d)
        # # x = self.get_chunk_from_csv(csv_file=file, headers=col_names)
        # print(x)
        # for d in x:
        #     yield d
        # # while file:
        #
        #     file = (next(self.data_files), False)

    def get_file_name(self, name_prefix, name_suffix):
        ext = ''
        time_str = time.strftime("%Y%m%d-%H%M%S")
        if 'compression' in self.export_settings and 'method' in self.export_settings['compression']:
            ext = '.' + self.export_settings['compression']['method']
            ext = ext.replace('.gzip', '.gz')
        output_file = f'{name_prefix}_{time_str}_{name_suffix}.csv{ext}'
        return output_file

    def export_sql_to_csv(self,
                          db_con,
                          data_table: str,
                          file_prefix: Optional[str] = None,
                          ):
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

        # define base settings for pandas to_CSV

        try:
            file_prefix = file_prefix if file_prefix else data_table
            file_name = self.get_file_name(file_prefix, '{file_index}')

            params = self.export_settings.copy()
            # if used with zip sql_chunk_size MUST be the same as max_file_rows
            # due to the bug in Pandas module
            max_file_rows = params.pop('chunksize', 10000)
            sql_chunk_size = 5000
            row_counter = 0
            file_index = 1
            file = Path(self.export_path, file_name.format(file_index=str(file_index)))
            print(f'Data writing in progress:')
            for data_chunk in pd.read_sql(f'SELECT * FROM {data_table}', db_con, chunksize=sql_chunk_size):
                row_counter += len(data_chunk.index)
                if row_counter > max_file_rows * file_index:
                    file_index += 1
                    params['header'] = True  # turn on headers for a new file
                    file = Path(self.export_path, file_name.format(file_index=str(file_index)))
                data_chunk.to_csv(path_or_buf=file, **params)
                params['header'] = False  # turn of headers if continue
                print(f'{row_counter:,} rows', end='\r')

            print(f"{row_counter:,} data rows were successfully exported to: {self.export_path}")
        except pd.errors.DataError as err:
            logging.error(traceback.format_exc())
            raise err
