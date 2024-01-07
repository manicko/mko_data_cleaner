
# # DB table keeping schema
MASTER_TABLE = 'sqlite_master'


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
