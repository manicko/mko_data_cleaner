from data_processing.cleaner import *  # very bad practice must be rewritten to import functions
from functools import partial
from pathlib import Path

# defaults
# path to folder containing SQLight databases
ROOT_DIR = Path().absolute()
DB_PATH = 'data_base'
DB_NAME = 'db_example'
DB_FILE = Path.joinpath(ROOT_DIR, DB_PATH, DB_NAME + '.db')

# folder containing data for import export CSV
CSV_PATH = Path.joinpath(ROOT_DIR, r'data/')

# file with raw data for cleaning
CSV_FILE_NAME = 'example_data'
CSV_FILE = Path.joinpath(CSV_PATH, 'raw_data/', CSV_FILE_NAME + '.csv')

# file with the dictionary for data cleaning settings
DICT_NAME = 'example_dict'
DICT_FILE = Path(CSV_PATH, 'dict/', DICT_NAME + '.csv')

# folder to output CSV from database
CSV_PATH_OUT = Path.joinpath(CSV_PATH, 'clean_data/')

# # DB table keeping schema
# MASTER_TABLE = 'sqlite_master'

# DB connection
DB_CONNECTION = sqlite3.connect(DB_FILE)


# # Data types used to add columns in SQLight data table
# VALID_COLUMN_DTYPES = (
#     'TEXT',
#     'NUMERIC',
#     'INTEGER',
#     'REAL',
#     'BLOB'
# )
#
# # pattern used to check validity of column and table name before adding them
# NAME_PATTERN = f"^[a-zA-Z_][a-zA-Z0-9_]*$"
#
# # DTYPES settings for panda CSV reader
# # of limited use as at the moment supports setting on GLOBAL level only
# CSV_COLUMN_DTYPES = {
#     # 'index': 'INTEGER PRIMARY KEY'
#     # 'ID':'object',
#     # 'NUM':'int64',
#     # 'Name':'object',
#     # 'CategoryID': 'object',
#     # 'CategoryName':'category',
#     # 'BrandID':'object',
#     # 'BrandName':'category'
# }
#
# # general settings for pandas CSV reader
# # (https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
# # can be overriden on function call level
# CSV_READ_PARAMS = {
#     'sep': ';',
#     'on_bad_lines': 'skip',
#     'encoding': 'utf-8',
#     'index_col': False,
#     'dtype': CSV_COLUMN_DTYPES,
#     'skiprows': None,
#     'decimal': ','
# }
#
# CSV_EXPORT_PARAMS = {
#     'sep': ';',
#     'encoding': 'utf-8',
#     'index': False
# }
#
# DATA_TO_SQL_PARAMS = {
#     'if_exists': 'append',
#     'index': False,
#     'index_label': None
# }


def get_clean_params(**params):
    """
    reads CSV file with data cleaning settings
    :param params:
    :return:
    """
    # define base settings for pandas CSV reader
    csv_read_params: dict[str, Any] = {}
    if CSV_READ_PARAMS and isinstance(CSV_READ_PARAMS, dict):
        csv_read_params = CSV_READ_PARAMS.copy()
    if params:
        csv_read_params.update(params)
    return pd.read_csv(**csv_read_params)


if __name__ == '__main__':
    table_name = 'data_table'  # name for the table to load data
    search_cols = ['col_0',  # columns in the data table to be used for search
                   'col_1',
                   'col_2',
                   'col_3',
                   'col_4',
                   'col_5']

    actions = {  # columns' indexes in the file_search containing settings
        'action': 0,  # update or delete setting
        'term': 3  # search string used after match setting in the SQL query
    }
    clean_cols_ids = {  # indexes of columns containing output values
        'cat': 4,
        'adv': 5,
        'bra': 6,
        'prd': 7,
        'cln_0': 8,
        'cln_1': 9,
        'cln_2': 10,
        'cln_3': 11,
        'cln_4': 12,
        'cln_5': 13
    }
    clean_cols = list(clean_cols_ids.keys())

    read_params = {
        'filepath_or_buffer': DICT_FILE,
        'usecols': list(actions.values()) + list(clean_cols_ids.values()),
        'header': 0,
        'skiprows': 0,
        'names': list(actions.keys()) + list(clean_cols_ids.keys())
    }

    # creating database, datatable, search table and fill with data
    load_csv_to_sql(
        db_con=DB_CONNECTION,
        data_table=table_name,
        search_columns=search_cols,
        clean_columns=clean_cols,
        csv_file=CSV_FILE
    )

    # read cleaning settings from the file
    clean_params_df = get_clean_params(**read_params)

    # set default parameters for data cleaning before looping through search\update values
    cleaner = partial(
        search_update_query,
        db_con=DB_CONNECTION,
        data_table=table_name,
    )

    # separate update settings and loop through
    upd_params_df = clean_params_df.loc[clean_params_df['action'] == 'upd']
    for col_name in clean_cols_ids.keys():
        rs = upd_params_df.loc[upd_params_df[col_name].notnull(), [col_name] + ['term']]
        res = rs.values.tolist()
        cleaner(column=col_name, params=rs.values.tolist())

    # get_n = select_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)

    finalize(db_con=DB_CONNECTION, data_table=table_name, output_folder=CSV_PATH_OUT)
    DB_CONNECTION.close()
