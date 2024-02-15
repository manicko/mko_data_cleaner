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
    reader_settings = r_settings['READ_SETTINGS']

    # set dictionary params

    actions = dict_setting['actions']
    clean_cols_ids = dict_setting['clean_cols_ids']
    clean_cols = list(clean_cols_ids.keys())
    dict_path = get_path(r_settings['PATH'], dict_setting['folder'])

    # get dictionary files from folde
    dict_files = get_dir_content(dict_path)
    sample_dict = next(dict_files)
    file = sample_dict
    dict_list = []
    while file:
        dict_data = pd.read_csv(
            filepath_or_buffer=file,
            **reader_settings['from_csv']
        )
        # dict_data['file'] = file.stem
        dict_list.append(dict_data)
        print(f"'{file}' was loaded'")
        file = next(dict_files, False)

    df = pd.concat(dict_list, ignore_index=True)
    out_dict_path = Path(dict_path, 'merged_dictionary.csv')
    df.drop_duplicates(inplace=True, subset=df.columns.difference(['file']))
    df.sort_values(by=['search_column_idx'], ascending=True, inplace=True)
    df.to_csv(path_or_buf=out_dict_path, encoding='UTF-8', sep=';', index=False)
