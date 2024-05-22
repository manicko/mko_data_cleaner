import os
from data_processing.rep_config import ReportConfig
from data_processing.db_woker import DBWorker
from data_processing.csv_woker import CSVWorker
from datetime import datetime
from data_processing.utils import (
    get_names_index,
    clean_names
)


def main(report_settings_file: str | os.PathLike):
    start_time = datetime.now().replace(microsecond=0)
    print(f'\nОбработка стартовала {str(start_time)}.')

    report_config = ReportConfig(report_settings_file)
    csv_worker = CSVWorker(
        data_path=report_config.import_path,
        data_settings=report_config.data_settings,
        reader_settings=report_config.reader_settings,
        dict_path=report_config.dict_path,
        dict_settings=report_config.dict_settings,
        export_path=report_config.export_path,
        export_settings=report_config.export_settings
    )

    sample_data_headers = clean_names(*csv_worker.csv_headers)

    # get dictionary params
    search_cols_index = get_names_index(sample_data_headers, report_config.data_settings['search_cols'])
    search_cols = list(search_cols_index.values())
    actions = report_config.dict_settings['actions']
    clean_cols_ids = report_config.dict_settings['clean_cols_ids']
    clean_cols = list(clean_cols_ids.keys())
    index_column = report_config.data_settings.get('index_column', None)
    date_column = report_config.data_settings.get('date_column', None)

    # DB settings
    table_name = report_config.db_settings['table_name']
    db_worker = DBWorker(
        db_file=report_config.db_file,
        data_tbl_name=table_name,
        column_names=sample_data_headers,
        search_columns=search_cols,
        clean_columns=clean_cols,
        index_column=index_column,
        date_column=date_column
    )

    # creating database, datatable, search table
    db_worker.create_search_table()

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

    db_worker.clean_update_data(params)

    # functionality to check if some rows are still empty after cleaning
    # get_n = db_worker.elect_nulls(DB_CONNECTION, table_name, search_cols, clean_cols)

    csv_worker.export_sql_to_csv(
        db_con=db_worker.db_con,
        data_table=table_name
    )

    end_time = datetime.now().replace(microsecond=0)
    print(f'\nПодготовка отчетов завершена в {str(end_time)}. \nПодготовка заняла {str(end_time - start_time)}.')


if __name__ == '__main__':
    REPORT_SETTINGS = 'settings/report_settings.yaml'
    main(REPORT_SETTINGS)
