from os import PathLike
from pathlib import Path
from re import match
import pandas as pd
import logging
import traceback
# pattern used to check validity of column and table name before adding them
NAME_PATTERN = f"^[a-zA-Z_][a-zA-Z0-9_]*$"

DATA_TO_SQL_PARAMS = {
    'if_exists': 'append',
    'index': False,
    'index_label': None,
    'chunksize': 2000
}

def is_valid_name(*names: str, pattern: str = None) -> bool:
    """
     Check whether provided name or list of names are valid
    (to be precise corresponds to NAME_PATTERN) to use as table or column names
    :param names: str, list of names to be checked in string format
    :param pattern: str, regex pattern for name validation. If omitted global NAME_PATTERN is used
    :return: bool, True or False
    """
    if pattern is None:
        pattern = NAME_PATTERN
    for name in names:
        if not isinstance(name, str) or not match(pattern, str(name)):
            print(f"The name: {str(name)} is not valid, "
                  f"use lowercase english letters and digits")
            return False
    return True


def generate_column_names(col_num: int, prefix: str = 'col_') -> list:
    """
    Generates list of names in a form of {prefix} + {index}.
    i.e. col_0, col_1 etc.
    :param col_num: str, number of columns
    :param prefix: str, prefix to use before index
    :return: list, list of column names
    """
    col_names = [prefix + str(i) for i in range(col_num)]
    return col_names

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
def get_dir_content(path: str | PathLike, ext: str = 'csv'):
    try:
        files = Path(path).glob(f'*.{ext}')
    except Exception as err:
        raise err
    else:
        return files


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


def csv_to_search_table(db_con,
                        data_table: str,
                        col_names: list[str],
                        csv_file,
                        csv_reader_settings
                        ) -> int:
    """

    :param db_con:
    :param data_table:
    :param col_names:
    :param csv_file: pathstring to CSV file
    :param csv_reader_settings: use to override global CSV_READ_PARAMS
        for details refer to pandas CSV reader settings
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    :param sql_loader_settings:use to extend or override global CSV_READ_PARAMS
        for details refer to pandas to_sql settings
    :return: int, number of rows loaded to database
    """


    # copy and update settings for pandas CSV reader
    csv_read_params = csv_reader_settings.copy()
    csv_read_params['filepath_or_buffer'] = csv_file

    # create CSV data reader

    data_reader = read_csv_chunks(**csv_read_params, names=col_names)

    # load DATA to SQLight from CSV data reader
    rows_count = 0  # counter for data rows in CSV file
    for chunk in data_reader:  # loop through CSV file
        chunk.to_sql(**DATA_TO_SQL_PARAMS, name=data_table, con=db_con)  # load data
        rows_count += chunk.shape[0]  # count rows
    return rows_count