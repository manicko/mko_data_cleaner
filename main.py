import pandas as pd
from pathlib import Path
from functools import partial
from time import time
import sqlite3

from data_processing.cleaner import (
    create_search_table,
    search_update_query,
    finalize,
    get_csv_columns,
    csv_to_search_table
)
from data_processing.utils import (
    yaml_to_dict,
    get_path,
    get_dir_content
)

DATA_TO_SQL_PARAMS = {
    'if_exists': 'append',
    'index': False,
    'index_label': None,
    'chunksize': 2000
}

REPORT_SETTINGS = 'settings/report_settings.yaml'

if __name__ == '__main__':

    # read report settings
    r_settings = yaml_to_dict(REPORT_SETTINGS)
    dict_setting = r_settings['DICT_FILE_SETTINGS']
    data_settings = r_settings['DATA_FILES_SETTINGS']
    db_settings = r_settings['DATABASE_SETTINGS']
    export_settings = r_settings['EXPORT_SETTINGS']
    reader_settings = r_settings['READ_SETTINGS']

    # DB connection
    table_name = db_settings['table_name']
    db_path = get_path(r_settings['PATH'], db_settings['folder'], mkdir=True)
    db_file = Path(db_path, db_settings['file_name'] + '.db')
    db_connection = sqlite3.connect(db_file)

    # set dictionary params
    search_cols = data_settings['search_cols']
    actions = dict_setting['actions']
    clean_cols_ids = dict_setting['clean_cols_ids']
    clean_cols = list(clean_cols_ids.keys())
    dict_path = get_path(r_settings['PATH'], dict_setting['folder'], dict_setting['file_name'])

    # get data folder and files
    data_path = get_path(r_settings['PATH'], data_settings['folder'])
    export_path = get_path(r_settings['PATH'], export_settings['folder'], mkdir=True)
    data_files = get_dir_content(data_path, ext=data_settings.get('ext', 'csv'))

    sample_file = next(data_files)
    # get column names based on the sample file
    sample_columns = get_csv_columns(
        csv_reader_settings=reader_settings['from_csv'],
        sample_csv_file=sample_file
    )
    # creating database, datatable, search table
    create_search_table(
        db_con=db_connection,
        data_table=table_name,
        search_columns=search_cols,
        clean_columns=clean_cols,
        col_names=sample_columns,

    )
    # loading data to the search table and getting back rows count
    total_rows = 0
    file = sample_file
    while file:
        rows_count = csv_to_search_table(
            db_con=db_connection,
            data_table=table_name,
            csv_file=file,
            col_names=sample_columns,
            csv_reader_settings=reader_settings['from_csv'],
            sql_loader_settings=DATA_TO_SQL_PARAMS
        )
        print(f"{rows_count:,} rows from file '{file}' were loaded to '{table_name}'")
        total_rows += rows_count
        file = next(data_files, False)
    print(f"{total_rows:,} rows were loaded to '{table_name}'")

    # set default parameters for data cleaning before looping through search\update values
    cleaner = partial(
        search_update_query,
        db_con=db_connection,
        data_table=table_name,
    )

    # read cleaning settings from the dictionary
    clean_params_df = pd.read_csv(
        filepath_or_buffer=dict_path,
        usecols=list(actions.values()) + list(clean_cols_ids.values()),
        names=list(actions.keys()) + list(clean_cols_ids.keys()),
        **reader_settings['from_csv']
    )

    # separate update settings and loop through
    upd_params_df = clean_params_df.loc[clean_params_df['action'] == 'upd']

    # looping through dictionary by column
    # if there are values to be set in that column (not empty rows)
    # we pass all nonempty rows to cleaner
    print(f'Data cleaning in progress: [', end='')
    start_time = time()
    for col_name in clean_cols_ids.keys():
        rs = upd_params_df.loc[upd_params_df[col_name].notnull(), [col_name] + ['term']]
        print('==', end='')
        cleaner(column=col_name, params=rs.values.tolist())
    print(']')
    print(f"Data cleaning finished. Elapsed time: {time() - start_time:.2f} seconds")
    # functionality to check if some rows are still empty after cleaning
    # get_n = select_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)

    finalize(
        db_con=db_connection,
        data_table=table_name,
        output_folder=export_path,
        **export_settings['to_csv']
    )
    db_connection.close()
