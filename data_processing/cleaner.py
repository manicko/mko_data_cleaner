import traceback
import logging
import sqlite3
from sqlite3 import Connection
from re import match
from typing import (Any, List, Optional, Union)
import pandas as pd
from pathlib import Path
import time

from settings.default_settins import (
    NAME_PATTERN,
    VALID_COLUMN_DTYPES,
    CSV_EXPORT_PARAMS,
)


def create_data_table(db_con: Connection, tbl_name: str, *tbl_columns: str) -> bool:
    """ Creates datatable in the database using 'tbl_name' and 'tbl_columns'
    :param db_con: SQLight3 connection object
    :param tbl_name: str, name of the table to be set
    :param tbl_columns: str, list of column names
    :return: bool, True or False depending on the operation success
    """
    is_valid = all((
        is_valid_name(tbl_name) is True,
        is_valid_name(*tbl_columns) is True,
        tbl_exist(db_con, tbl_name) is False
    ))
    if is_valid:
        try:
            query = f"CREATE TABLE {tbl_name} ({', '.join(tbl_columns)});"
            db_con.cursor().execute(query)
            print(f'Table \'{tbl_name}\' created successfully')
            db_con.cursor().close()
            db_con.commit()
            return True
        except sqlite3.Error:
            logging.error(traceback.format_exc())
    print(f'Table \'{tbl_name}\' could not be created')
    return False


def drop_tables(db_con: Connection, *tbl_names: str) -> dict[str:bool]:
    """
    Drops tables provided as list of table names from current database using connection to the database
    :param db_con: SQLight3 connection object, connection to the database
    :param tbl_names: str, name or multiple names of a tables to be dropped
    :return: dict[str:bool], dictionary with tbl_names as keys and drop status True or False as value
    """
    tbl_dropped = {}
    for name in tbl_names:
        try:
            query = f"DROP TABLE IF EXISTS {name}"
            db_con.cursor().execute(query)
            print(f"Table '{name}' was successfully dropped")
            db_con.commit()
            db_con.cursor().close()
            tbl_dropped[name] = True
        except sqlite3.Error:
            logging.error(traceback.format_exc())
            tbl_dropped[name] = False
    return tbl_dropped


def drop_triggers(db_con: Connection, *tr_names: str, tbl_name: Optional[str] = None) -> bool:
    """
    Delete list of Triggers using theis names from database.
    if names are not provided use tbl_name and the following pattern to generate Trigger names:
    {tbl_name}_insert;{tbl_name}_delete;{tbl_name}_update,
    :param db_con: SQLight Connection object
    :param tr_names: str, Trigger names to delete
    :param tbl_name: str, used if Trigger names are not provided
    :return: bool, True or False depending on the operation success
    """
    tr_dropped = []
    if not tr_names:  # trying to get Trigger names from table name
        if tbl_name is None:  # cancel operation if table name is not set
            return False
        tr_names = f'{tbl_name}_insert;{tbl_name}_delete;{tbl_name}_update'.split(';')
    for name in tr_names:
        try:
            query = f"DROP TRIGGER IF EXISTS {name}"
            db_con.cursor().execute(query)
            print(f"Trigger '{name}' was successfully dropped")
            db_con.commit()
            db_con.cursor().close()
            tr_dropped.append(True)
        except sqlite3.Error:
            logging.error(traceback.format_exc())
            tr_dropped.append(False)
            print(f"Not able to drop Trigger: '{name}'")
    return all(tr_dropped)


def tbl_exist(db_con: Connection, name_to_check: str) -> bool:
    """
    Check whether table with the name 'name_to_check' already in database.
    To avoid creation of a table with a same name as existing.
    :param db_con: SQLight3 connection object, use database connection to check statement
    :param name_to_check: str, the name to be checked in database
    :return: bool, False or True
    """
    # query returns 1 if table exists and 0 if not
    query = f"SELECT EXISTS (SELECT 1 FROM sqlite_master " \
            f"WHERE type = 'table' AND name = '{name_to_check}')"
    # execute(query).fetchone() returns tuple i.e. (1,) or (0,)
    exist = bool(db_con.cursor().execute(query).fetchone()[0])
    db_con.cursor().close()
    return exist


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
        if name is None or not match(pattern, str(name)):
            print(f"The name: {str(name)} is not valid, "
                  f"use lowercase english letters and digits")
            return False
    return True


def add_columns(db_con: Connection, tbl_name: str, **col_params: dict[str:str]) -> dict[str:bool]:
    """
    Adds columns to the datatable from the given list of column names and their types.
    :param db_con: SQLight3 connection object
    :param tbl_name: str, name of a table to add columns
    :param col_params: dict[str], Dictionary containing column name as 'key' and column 'type' as 'value'
    :return: dict[str:bool], dictionary with column names as keys and add status True or False as value
    """
    added_cols = {}
    for c_name, c_type in col_params.items():
        if is_valid_name(c_name) and c_type.upper() in VALID_COLUMN_DTYPES:
            try:
                query = f"ALTER TABLE {tbl_name} ADD {c_name} {c_type};"
                db_con.cursor().execute(query)
                print(f"Column '{c_name}' was successfully created in '{tbl_name}'")
                added_cols[c_name] = True
            except sqlite3.Error:
                logging.error(traceback.format_exc())
                added_cols[c_name] = False
    return added_cols


def link_search_table(db_con: Connection, tbl_name: str, *search_columns: str, suffix: Optional[str] = '_fts') -> bool:
    """
    Creates Virtual SQLight3 FTS 5 table using provided datatable as a content table.
    And setting triggers on update, delete and insert actions to keep it synchronised to the datatable.
    For mor details please check: https://www.sqlite.org/fts5.html#external_content_tables
    :param db_con: SQLight3 Connection object
    :param tbl_name: str, name of existing data table
    :param search_columns: str, names of columns in datatable
    to be used as a content for a Virtual table FTS (text-search)
    :param suffix: str, define the name of a search table as
    {data_table_name}{suffix} it is recommended to keep default
    :return: bool, True if operation succeeded
    """
    search_tbl = tbl_name + suffix
    columns = ','.join(search_columns)
    new_columns = ','.join(f'new.{c}' for c in search_columns)
    old_columns = ','.join(f'old.{c}' for c in search_columns)

    is_valid = all((
        is_valid_name(search_tbl) is True,
        is_valid_name(*search_columns) is True,
        tbl_exist(db_con, search_tbl) is False
    ))

    if is_valid:
        try:
            # ensure that there are no table with the same name
            query = f"CREATE VIRTUAL TABLE IF NOT EXISTS {search_tbl} " \
                    f"USING fts5({columns}, content={tbl_name})"
            db_con.cursor().execute(query)
            db_con.cursor().close()
            print(f"Search table '{search_tbl}' was successfully created")

            #  Triggers to keep the Search table up to date.
            db_con.cursor().executescript(
                '''
                   CREATE TRIGGER IF NOT EXISTS {table}_insert AFTER INSERT ON {table}
                   BEGIN
                       INSERT INTO {search_tbl} (rowid, {column_list}) 
                       VALUES (new.rowid, {new_columns});
                   END;
                   CREATE TRIGGER IF NOT EXISTS {table}_delete AFTER DELETE ON {table}
                   BEGIN
                       INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list}) 
                       VALUES ('delete', old.rowid, {old_columns});
                   END;
                   CREATE TRIGGER IF NOT EXISTS {table}_update AFTER UPDATE ON {table}
                   BEGIN
                       INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list}) 
                       VALUES ('delete', old.rowid, {old_columns});
                       INSERT INTO {search_tbl} (rowid, {column_list}) VALUES (new.rowid, {new_columns});
                   END;
               '''.format(
                    search_tbl=search_tbl,
                    table=tbl_name,
                    suffix=suffix,
                    column_list=columns,
                    new_columns=new_columns,
                    old_columns=old_columns
                )
            )
            db_con.cursor().close()
            db_con.commit()
            print(f"Search triggers were successfully created for '{search_tbl}' ")
            return True
        except sqlite3.Error:
            logging.error(traceback.format_exc())
    print(f"Search table '{search_tbl}' could not be linked")
    return False


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


def count_csv_columns(**reader_settings) -> int:
    """Counts columns with data in CSV file
    :param reader_settings: uses pandas csv_reader settings
    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    :return: integer count of columns with data
    """
    reader_settings['nrows'] = 0  # override to read header row only
    try:
        csv_column_names = pd.read_csv(**reader_settings).columns.tolist()
        return len(csv_column_names)
    except pd.errors.DataError:
        logging.error(traceback.format_exc())


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


def merge_params_defaults(params: dict[str, Any], default_params: dict[str, Any]):
    for key, val in default_params.items():
        params.setdefault(key, val)


def create_search_table(db_con: Connection,
                        data_table: str,
                        search_columns: list[str],
                        clean_columns: list[str],
                        col_names: list[str]
                        ) -> int:
    """
    creates datatable in the database
    :param db_con: SQLight3 connection object, connection to the datatable
    :param data_table: name of the existing table
    :param clean_columns:
    :param search_columns:
    :param col_names:
        https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    :return: count of data rows loaded
    """

    if tbl_exist(db_con, data_table) is True:
        raise NameError(f"There is already a table: '{data_table}' in database")

    creation = all((
        create_data_table(  # create empty datatable
            db_con,
            data_table,
            *col_names,
            *clean_columns
        ),
        link_search_table(  # create empty database for FTS search
            db_con,
            data_table,
            *search_columns
        )
    ))
    if creation is False:
        print(f'Failed to create tables')
        return False
    print(f'Datatable: {data_table} successfully created')
    return True


def get_csv_columns(csv_reader_settings: dict[str, Any], sample_csv_file: Union[str, Path]) -> list[str]:
    """

    :param sample_csv_file: pathstring to CSV file
    :param csv_reader_settings: use to override global CSV_READ_PARAMS
        for details refer to pandas CSV reader settings
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

    :return:
    """
    if Path.is_file(Path(sample_csv_file)) is False:
        raise NameError(f"File: '{sample_csv_file}' not found")

    # copy and update settings for pandas CSV reader
    csv_read_params = csv_reader_settings.copy()
    csv_read_params['filepath_or_buffer'] = sample_csv_file

    # get columns count from CSV to reserve same number of columns in SQLight table
    num_cols = count_csv_columns(**csv_read_params)
    # generate column names to use for loader
    col_names = generate_column_names(num_cols)
    return col_names


def csv_to_search_table(db_con: Connection,
                        data_table: str,
                        col_names: list[str],
                        csv_file: Union[str, Path],
                        csv_reader_settings: dict[str, Any],
                        sql_loader_settings: dict[str, Any]
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
    if Path.is_file(Path(csv_file)) is False:
        raise NameError(f"File: '{csv_file}' not found")

    # copy and update settings for pandas CSV reader
    csv_read_params = csv_reader_settings.copy()
    csv_read_params['filepath_or_buffer'] = csv_file
    num_cols = count_csv_columns(**csv_read_params)
    if num_cols != len(col_names):
        print(f"Number of columns in the file:{num_cols} does not correspond to sample file: {len(col_names)}")
    # create CSV data reader
    csv_read_params.setdefault('chunksize', 2000)
    data_reader = read_csv_chunks(**csv_read_params, names=col_names)

    # load DATA to SQLight from CSV data reader
    rows_count = 0  # counter for data rows in CSV file
    for chunk in data_reader:  # loop through CSV file
        chunk.to_sql(**sql_loader_settings, name=data_table, con=db_con)  # load data
        rows_count += chunk.shape[0]  # count rows
    return rows_count


def finalize(db_con: Connection,
             data_table: str,
             search_suffix: str = '_fts',
             export: Optional[bool] = True,
             output_folder: Optional[Union[str, Path]] = None) -> bool:
    """
    Export results to the file and drops tables
    :param db_con: SQLight3 connection object
    :param data_table: str, name of the table to be dropped
    :param search_suffix: str, suffix used for the search table
    :param export: bool, True if export is needed
    :param output_folder: str, Path to the output directory
    :return: bool, True if succeeded
    """
    if export is True:
        exported = export_sql_to_csv(db_con=db_con, data_table=data_table, file_path=output_folder)
        if exported is False:
            return False
    finalised = all((
        drop_triggers(db_con, tbl_name=data_table),
        all(drop_tables(db_con, data_table, data_table + search_suffix).values())
    ))
    return finalised


def get_table_sample(db_con: Connection, tbl_name: str, limit: int = 2):
    """

    :param db_con: SQLight3 connection object
    :param tbl_name:
    :param limit:
    :return:
    """
    try:
        query = f'SELECT * FROM {tbl_name} LIMIT {limit}'
        for tbl_row in db_con.cursor().execute(query):
            print(f"These are sample rows for {tbl_name}")
            print(tbl_row)
        db_con.cursor().close()
    except sqlite3.Error:
        logging.error(traceback.format_exc())


def get_table_columns(db_con: Connection, tbl_name: str) -> list:
    """
    Get names of table columns
    :param db_con: SQLight3 connection object
    :param tbl_name: str, table name
    :return: list of table column names
    """
    try:
        query = f'select * from {tbl_name}'
        cursor = db_con.execute(query)
        names = list(map(lambda x: x[0], cursor.description))
        cursor.close()
        return names
    except sqlite3.Error:
        logging.error(traceback.format_exc())


def select_nulls(db_con: Connection,
                 data_table: str,
                 search_columns: list,
                 clean_columns: list,
                 ) -> List[sqlite3.Row]:
    """
    Get rows with NULL values for defined columns
    :param db_con: SQLight3 connection object
    :param data_table:
    :param search_columns:
    :param clean_columns:
    :return: list, list of results
    """
    try:
        query = '''SELECT DISTINCT {search_columns} FROM {table} 
                    WHERE {clean_columns} IS NULL                    
                '''.format(table=data_table,
                           search_columns=", ".join(search_columns),
                           clean_columns=" | ".join(clean_columns))
        items = db_con.cursor().execute(query).fetchall()
        db_con.cursor().close()
        return items
    except sqlite3.Error:
        logging.error(traceback.format_exc())


#
#
# def search_query(db_con: Connection,
#                  data_table: str,
#                  term: str,
#                  search_suffix: str = '_fts'
#                  ) -> List[sqlite3.Row]:
#     """
#
#     :param db_con: SQLight3 connection object
#     :param data_table:
#     :param term:
#     :param search_suffix:
#     :return:
#     """
#     try:
#         query = '''SELECT ROWID FROM {table}{search_suffix}
#                     WHERE {table}{search_suffix} MATCH ? ORDER BY rank
#                 '''
#         items = db_con.cursor().execute(query.format(table=data_table, \
#         search_suffix=search_suffix), [term]).fetchall()
#         db_con.cursor().close()
#         return items
#     except sqlite3.Error:
#         logging.error(traceback.format_exc())

# def update_query(db_con: Connection,
#                  data_table: str,
#                  column: str,
#                  value: str,
#                  ids: list
#                  ) -> bool:
#     """
#
#     :param db_con: SQLight3 connection object
#     :param data_table:
#     :param column:
#     :param value:
#     :param ids:
#     :return:
#     """
#     try:
#         query = '''
#                 UPDATE {table} SET {column} = '{val}'
#                 WHERE ROWID IN ({ids})
#                 '''.format(
#             table=data_table,
#             column=column,
#             val=value,
#             ids=', '.join(map(str, ids))
#         )
#         db_con.cursor().execute(query).fetchall()
#         db_con.commit()
#         db_con.cursor().close()
#         return True
#     except sqlite3.Error:
#         logging.error(traceback.format_exc())


#
# def search_update_query(db_con: Connection,
#                         data_table: str,
#                         column: str,
#                         value: str,
#                         term: str,
#                         search_suffix: Optional[str] = '_fts'
#                         ) -> bool:
#     """
#     Updates the specified datatable Column rows with the specified Value
#     basing on the search results using Term as a search condition
#     :param db_con: SQLight3 connection object
#     :param data_table: str, name of the data table
#     :param column: str, name of a column to set as Value
#     :param value: str, Value to be placed in the updated column
#     :param term: str, Search term ising SQLight FTS5 syntax after MATCH
#     https://www.sqlite.org/fts5.html#full_text_query_syntax
#     :param search_suffix: str, suffix used while creating the search table,
#     default is recommended
#     :return: bool, True if the operation was successful
#     """
#     try:
#         query = '''
#                 UPDATE {table} SET {column} = '{val}'
#                 WHERE ROWID IN (
#                     SELECT ROWID FROM {table}{search_suffix}
#                     WHERE {table}{search_suffix} MATCH ? ORDER BY rank )
#                 '''.format(
#             table=data_table,
#             search_suffix=search_suffix,
#             column=column,
#             val=value
#         )
#         db_con.cursor().execute(query, [term])
#         db_con.commit()
#         db_con.cursor().close()
#         return True
#     except sqlite3.Error:
#         logging.error(traceback.format_exc())


def search_delete_query(db_con: Connection,
                        data_table: str,
                        term: str,
                        search_suffix: str = '_fts'
                        ) -> bool:
    """
        Delete datatable rows basing on the search results using Term as a search condition
        :param db_con: SQLight3 connection object
        :param data_table: str, name of the data table
        :param term: str, Search term ising SQLight FTS5 syntax after MATCH
        https://www.sqlite.org/fts5.html#full_text_query_syntax
        :param search_suffix: str, suffix used while creating the search table,
        default is recommended
        :return: bool, True if the operation was successful
        """
    try:
        query = '''              
                DELETE FROM {table} 
                WHERE ROWID IN (
                    SELECT ROWID FROM {table}{search_suffix} 
                    WHERE {table}{search_suffix} MATCH ? ORDER BY rank )                                   
                '''.format(
            table=data_table,
            search_suffix=search_suffix,
        )
        db_con.cursor().execute(query, [term])
        db_con.commit()
        db_con.cursor().close()
        return True
    except sqlite3.Error:
        logging.error(traceback.format_exc())


def search_update_query(db_con: Connection,
                        data_table: str,
                        column: str,
                        params: list[list],
                        search_suffix: Optional[str] = '_fts'
                        ) -> bool:
    """
    Updates the specified datatable Column rows with the specified Value
    basing on the search results using Term as a search condition
    :param db_con: SQLight3 connection object
    :param data_table: str, name of the data table
    :param column: str, name of a column to set as Value
    :param params: list of lists with pairs: search_term and search_values follows
            search_term: str, Value to be placed in the updated column
            search_value: str, Search term ising SQLight FTS5 syntax after MATCH
    https://www.sqlite.org/fts5.html#full_text_query_syntax
    :param search_suffix: str, suffix used while creating the search table,
    default is recommended
    :return: bool, True if the operation was successful
    """
    try:
        query = '''              
                UPDATE {table} SET {column} = ?
                WHERE ROWID IN (
                    SELECT ROWID FROM {table}{search_suffix} 
                    WHERE {table}{search_suffix} MATCH ? ORDER BY rank )                                   
                '''.format(
            table=data_table,
            search_suffix=search_suffix,
            column=column
        )
        db_con.cursor().executemany(query, params)
        db_con.commit()
        db_con.cursor().close()
        return True
    except sqlite3.Error:
        logging.error(traceback.format_exc())


def export_sql_to_csv(db_con: Connection,
                      data_table: str,
                      file_path: str = None,
                      file_prefix: Optional[str] = None,
                      **to_csv_params: Optional[Any]) -> bool:
    """
    Exports SQLights data_table using pandas. Output filename will have timestamp.
    :param db_con: SQLight3 connection object
    :param data_table: str, name of the table to be exported
    :param to_csv_params: Optional[Any], use to update global CSV_EXPORT_PARAMS
    :param file_path: path to the CSV files storage
    :param file_prefix: Name of the output file to be used
    :return: bool, True or False depending on the operation success
    """

    if Path.is_dir(Path(file_path)) is False:
        print(f"Directory '{file_path}' is not a proper file path directory for CSV export")
        return False
    if file_prefix is None:
        file_prefix = data_table
    time_str = time.strftime("%Y%m%d-%H%M%S")

    # define base settings for pandas to_CSV
    merge_params_defaults(to_csv_params, CSV_EXPORT_PARAMS)

    try:
        params = to_csv_params.copy()
        # compression = {'method': 'zip'}
        row_counter = 0
        max_file_rows = 1000000
        file_index = 0
        print(f'Data writing in progress:')

        output_file_name = Path(file_path, f'{file_prefix}_{time_str}.csv')
        for data_chunk in pd.read_sql(f'SELECT * FROM {data_table}', db_con, chunksize=5000):
            row_counter += len(data_chunk.index)
            if row_counter > max_file_rows * (1 + file_index):
                file_index += 1
                params['header'] = True
                output_file_name = Path(file_path, f'{file_prefix}_{time_str}_{str(file_index)}.csv')
            data_chunk.to_csv(path_or_buf=output_file_name, **params)
            params['header'] = False
            print(f'Rows count:{row_counter:,}', end='\r')
        print(f'Rows count:{row_counter:,}')
        print(f"Data was successfully exported to: {output_file_name}")
        return True
    except pd.errors.DataError:
        logging.error(traceback.format_exc())
