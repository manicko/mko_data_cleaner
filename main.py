import os

import pandas as pd
from pathlib import Path
from functools import partial
from time import time
import sqlite3

from data_processing.rep_config import ReportConfig
from data_processing.db_woker import DBWorker
from data_processing.csv_woker import CSVWorker

from data_processing.utils import (
    generate_column_names,
    is_valid_name,
    csv_to_search_table
)


def main(report_settings_file: str | os.PathLike):
    report_config = ReportConfig(report_settings_file)
    # print(report_config.import_path)
    # print(report_config.reader_settings)
    csv_worker = CSVWorker(
        data_path=report_config.import_path,
        data_settings=report_config.data_settings,
        reader_settings=report_config.reader_settings,
        export_path=report_config.export_path,
        export_settings=report_config.report_settings
    )

    sample_data_headers = csv_worker.csv_headers
    if not is_valid_name(sample_data_headers):
        sample_data_headers = generate_column_names(len(sample_data_headers))

    # DB settings
    table_name = report_config.db_settings['table_name']

    # get dictionary params
    search_cols = report_config.data_settings['search_cols']
    actions = report_config.dict_setting['actions']
    clean_cols_ids = report_config.dict_setting['clean_cols_ids']
    clean_cols = list(clean_cols_ids.keys())

    db_worker = DBWorker(report_config.db_file)

    # creating database, datatable, search table
    db_worker.create_search_table(
        data_table=table_name,
        search_columns=search_cols,
        clean_columns=clean_cols,
        col_names=sample_data_headers,
    )
    rows_count = 0

    csv_to_search_table(
        db_con=db_worker.db_con,
        data_table=table_name,
        col_names=sample_data_headers,
        csv_file=csv_worker.sample_data_file,
        csv_reader_settings=report_config.reader_settings
    )

    # print(csv_worker.get_csv_chunks(sample_data_headers))
    # for chunk in csv_worker.get_csv_chunks(sample_data_headers):
    #     rows_count += db_worker.data_chunk_to_sql(chunk)

    print(f"{rows_count:,} rows were loaded to '{table_name}'")

    #
    # # set default parameters for data cleaning before looping through search\update values
    # cleaner = partial(
    #     search_update_query,
    #     db_con=db_connection,
    #     data_table=table_name,
    # )
    #
    # # read cleaning settings from the dictionary
    # clean_params_df = pd.read_csv(
    #     filepath_or_buffer=dict_path,
    #     usecols=list(actions.values()) + list(clean_cols_ids.values()),
    #     names=list(actions.keys()) + list(clean_cols_ids.keys()),
    #     **reader_settings['from_csv']
    # )
    #
    # # separate update settings and loop through
    # upd_params_df = clean_params_df.loc[clean_params_df['action'] == 'upd']
    #
    # # looping through dictionary by column
    # # if there are values to be set in that column (not empty rows)
    # # we pass all nonempty rows to cleaner
    # print(f'Data cleaning in progress: [', end='')
    # start_time = time()
    # for col_name in clean_cols_ids.keys():
    #     rs = upd_params_df.loc[upd_params_df[col_name].notnull(), [col_name] + ['term']]
    #     print('==', end='')
    #     cleaner(column=col_name, params=rs.values.tolist())
    # print(']')
    # print(f"Data cleaning finished. Elapsed time: {time() - start_time:.2f} seconds")
    # # functionality to check if some rows are still empty after cleaning
    # # get_n = select_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)
    #
    # finalize(
    #     db_con=db_connection,
    #     data_table=table_name,
    #     output_folder=export_path,
    #     **export_settings['to_csv']
    # )


if __name__ == '__main__':
    REPORT_SETTINGS = 'settings/report_settings.yaml'
    main(REPORT_SETTINGS)
