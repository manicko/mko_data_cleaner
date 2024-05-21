import os
from typing import (Optional)
from pathlib import Path
import logging
import traceback
from time import time, strftime
import pandas as pd

from .utils import (
    get_dir_content
)


class CSVWorker:
    def __init__(self, data_path: str | os.PathLike, data_settings: dict, reader_settings: dict,
                 dict_path: str | os.PathLike, dict_settings: dict,
                 export_path: str | os.PathLike, export_settings: dict):
        self.data_settings = data_settings
        self.export_path = Path(export_path)
        self.reader_settings = reader_settings
        self.export_settings = export_settings
        self.dict_path = Path(dict_path)
        self.dict_settings = dict_settings

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
                    chunksize=10000,
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
        while file:
            print(f'Reading file: {file}')
            yield from self.get_chunk_from_csv(csv_file=file, headers=col_names)
            file = next(self.data_files, False)

    def get_file_name(self, name_prefix, name_suffix):
        ext = ''
        time_str = strftime("%Y%m%d-%H%M%S")
        if 'compression' in self.export_settings and 'method' in self.export_settings['compression']:
            ext = '.' + self.export_settings['compression']['method']
            ext = ext.replace('.gzip', '.gz')
        output_file = f'{name_prefix}_{time_str}_{name_suffix}.csv{ext}'
        return output_file

    def get_merged_dictionary(self):
        try:
            dict_list = []
            for file in get_dir_content(self.dict_path.parent, self.dict_settings['ext']):
                dict_data = pd.read_csv(filepath_or_buffer=file, **self.reader_settings)
                # dict_data['file'] = file.stem
                dict_list.append(dict_data)
                # print(f"'{file}' was loaded")
            df = pd.concat(dict_list, ignore_index=True)
            df.drop_duplicates(inplace=True, subset=df.columns.difference(['file']))
            df.sort_values(by=['search_column_idx'], ascending=True, inplace=True)
            df.to_csv(path_or_buf=self.dict_path, decimal=',', encoding='UTF-8', sep=';', index=False)
        except Exception as err:
            logging.error(traceback.format_exc())
            raise err
        else:
            print(f"Merged dictionary file: '{self.dict_path}' was successfully created")

    def get_clean_params(self,
                         clean_cols_ids: dict,
                         search_column_index: dict[int:str],
                         actions: dict | None = None
                         ):
        # read cleaning settings from the dictionary
        try:
            clean_params_df = pd.read_csv(
                filepath_or_buffer=self.dict_path,
                # usecols=list(actions.values()) + list(clean_cols_ids.values()),
                # names=list(actions.keys()) + list(clean_cols_ids.keys()),
                **self.reader_settings
            )
            # separate update settings and loop through
            upd_params_df = clean_params_df.loc[clean_params_df['action'] == 'upd']
            # looping through dictionary by column
            # if there are values to be set in that column (not empty rows)
            # we pass all nonempty rows to cleaner
            print(f'Data cleaning in progress: [', end='')
            start_time = time()
            for col_name in clean_cols_ids.keys():
                rs = upd_params_df.loc[upd_params_df[col_name].notnull(), [col_name] + ['search_column_idx'] + ['term']]
                rs['search_column_idx'] = rs['search_column_idx'].apply(pd.to_numeric)
                rs = rs.values.tolist()
                result = []
                for col, search_id, term in rs:
                    term = '"' + search_column_index[int(search_id)] + '":' + term
                    result.append([col, term])
                yield col_name, result
                print('==', end='')
        except Exception as err:
            logging.error(traceback.format_exc())
            raise err
        else:
            print(']')
            print(f"Data cleaning finished. Elapsed time: {time() - start_time:.2f} seconds")

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
        :param file_prefix: Name of the output file to be used
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
            print(f'Data writing in progress: [', end='')
            for data_chunk in pd.read_sql(f'SELECT * FROM {data_table}', db_con, chunksize=sql_chunk_size):
                row_counter += len(data_chunk.index)
                if row_counter > max_file_rows * file_index:
                    file_index += 1
                    params['header'] = True  # turn on headers for a new file
                    file = Path(self.export_path, file_name.format(file_index=str(file_index)))
                    print('=', end='')
                data_chunk.to_csv(path_or_buf=file, **params)
                params['header'] = False  # turn of headers if continue
                # print(f'{row_counter:,} rows', end='\r')

            print(f']\n{row_counter:,} data rows were successfully exported to: {self.export_path}')
        except pd.errors.DataError as err:
            logging.error(traceback.format_exc())
            raise err
