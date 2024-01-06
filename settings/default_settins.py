from pathlib import Path
import sqlite3
# defaults
# path to folder containing SQLight databases
ROOT_DIR = Path().absolute()
DB_PATH = '../data_base'
DB_NAME = 'db_example'
DB_FILE = Path.joinpath(ROOT_DIR, DB_PATH, DB_NAME + '.db')

# folder containing data for import export CSV
CSV_PATH = Path.joinpath(ROOT_DIR, r'../data/')

# file with raw data for cleaning
CSV_FILE_NAME = 'parfiumeriia_2023-02-01_2023-02-28_20240106_143305'
CSV_FILE = Path.joinpath(CSV_PATH, 'raw_data/', CSV_FILE_NAME + '.csv')

# file with the dictionary for data cleaning settings
DICT_NAME = 'parfiumeriia_2023-02-01_2023-03-22_20240106_143343'
DICT_FILE = Path(CSV_PATH, 'dict/', DICT_NAME + '.csv')

# folder to output CSV from database
CSV_PATH_OUT = Path.joinpath(CSV_PATH, 'clean_data/')

# # DB table keeping schema
MASTER_TABLE = 'sqlite_master'

# DB connection
DB_CONNECTION = sqlite3.connect(DB_FILE)

# # Data types used to add columns in SQLight data table
VALID_COLUMN_DTYPES = (
    'TEXT',
    'NUMERIC',
    'INTEGER',
    'REAL',
    'BLOB'
)

# pattern used to check validity of column and table name before adding them
NAME_PATTERN = f"^[a-zA-Z_][a-zA-Z0-9_]*$"

# # DTYPES settings for panda CSV reader
# # of limited use as at the moment supports setting on GLOBAL level only
CSV_COLUMN_DTYPES = {
    # 'index': 'INTEGER PRIMARY KEY'
    # 'ID':'object',
    # 'NUM':'int64',
    # 'Name':'object',
    # 'CategoryID': 'object',
    # 'CategoryName':'category',
    # 'BrandID':'object',
    # 'BrandName':'category'
}
#
# # general settings for pandas CSV reader
# # (https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
# # can be overriden on function call level
CSV_READ_PARAMS = {
    'sep': ';',
    'on_bad_lines': 'skip',
    'encoding': 'utf-8',
    'index_col': False,
    'dtype': CSV_COLUMN_DTYPES,
    'skiprows': None,
    'decimal': '.',
    'header': 0  # ignor column names in CSV file
}

CSV_EXPORT_PARAMS = {
    'sep': ';',
    'encoding': 'UTF-8',
    'mode': 'a',
    'header': True,
    'index': False
}

DATA_TO_SQL_PARAMS = {
    'if_exists': 'append',
    'index': False,
    'index_label': None,
    'chunksize': 2000
}
