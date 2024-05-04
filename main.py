import os
from data_processing.rep_config import ReportConfig
from data_processing.db_woker import DBWorker
from data_processing.csv_woker import CSVWorker

from data_processing.utils import (
    get_names_index,
    clean_names
)


def main(report_settings_file: str | os.PathLike):
    report_config = ReportConfig(report_settings_file)
    csv_worker = CSVWorker(
        data_path=report_config.import_path,
        data_settings=report_config.data_settings,
        reader_settings=report_config.reader_settings,
        dict_path=report_config.dict_path,
        dict_settings=report_config.dict_setting,
        export_path=report_config.export_path,
        export_settings=report_config.export_settings
    )

    sample_data_headers = clean_names(*csv_worker.csv_headers)

    # get dictionary params
    search_cols_index = get_names_index(sample_data_headers, report_config.data_settings['search_cols'])
    search_cols = list(search_cols_index.values())
    actions = report_config.dict_setting['actions']
    clean_cols_ids = report_config.dict_setting['clean_cols_ids']
    clean_cols = list(clean_cols_ids.keys())

    # DB settings
    db_worker = DBWorker(report_config.db_file)
    table_name = report_config.db_settings['table_name']

    # creating database, datatable, search table
    db_worker.create_search_table(
        data_table=table_name,
        search_columns=search_cols,
        clean_columns=clean_cols,
        col_names=sample_data_headers,
    )

    # loading data to database
    rows_count = 0
    for chunk in csv_worker.get_csv_chunks(sample_data_headers):
        rows_count += db_worker.data_chunk_to_sql(chunk, table_name)

    print(f"{rows_count:,} rows were loaded to '{table_name}'")

    if not os.path.isfile(report_config.dict_path):
        csv_worker.get_merged_dictionary()

    # generate parameters to search and update columns
    params = csv_worker.get_clean_params(
        actions=actions,
        clean_cols_ids=clean_cols_ids,
        search_column_index=search_cols_index
    )

    # looping through search\update params and fill in data
    for col_name, param in params:
        db_worker.search_update_query(table_name, col_name, *param)

    # functionality to check if some rows are still empty after cleaning
    # get_n = select_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)

    csv_worker.export_sql_to_csv(
        db_con=db_worker.db_con,
        data_table=table_name
    )


if __name__ == '__main__':

    REPORT_SETTINGS = 'settings/report_settings.yaml'
    main(REPORT_SETTINGS)
