import pandas as pd
from functools import partial
from time import time
from default_settins import (
    DICT_FILE,
    DB_CONNECTION,
    CSV_FILE,
    CSV_READ_PARAMS,
    DATA_TO_SQL_PARAMS,
    CSV_PATH_OUT,
)
from data_processing.cleaner import (
    load_csv_to_sql,
    search_update_query,
    finalize,
    merge_params_defaults)

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

    read_dict_params = {
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
        csv_file=CSV_FILE,
        csv_reader_settings=CSV_READ_PARAMS,
        sql_loader_settings=DATA_TO_SQL_PARAMS
    )

    # set default parameters for data cleaning before looping through search\update values
    cleaner = partial(
        search_update_query,
        db_con=DB_CONNECTION,
        data_table=table_name,
    )

    # read cleaning settings from the file
    merge_params_defaults(read_dict_params, CSV_READ_PARAMS)
    clean_params_df = pd.read_csv(**read_dict_params)

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

    finalize(db_con=DB_CONNECTION, data_table=table_name, output_folder=CSV_PATH_OUT)
    DB_CONNECTION.close()
