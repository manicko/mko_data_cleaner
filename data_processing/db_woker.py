import traceback
import logging
import sqlite3

from typing import (Any, List, Optional, Union)

from pathlib import Path

from .utils import is_valid_name

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

DATA_TO_SQL_PARAMS = {
    'if_exists': 'append',
    'index': False,
    'index_label': None,
    'chunksize': 2000
}

class DBWorker:
    def __init__(self, db_file, tbl_name: str = 'data_table'):
        self.db_con = sqlite3.connect(db_file)
        self.db_table = tbl_name

    def create_table(self, tbl_name, *tbl_columns: str) -> bool:
        """ Creates datatable in the database using 'tbl_name' and 'tbl_columns'
        :param tbl_name: name of a table to create
        :param tbl_columns: str, list of column names
        :return: bool, True or False depending on the operation success
        """

        if is_valid_name(tbl_name, *tbl_columns):
            query = f"CREATE TABLE IF NOT EXISTS {tbl_name} ({', '.join(tbl_columns)});"
            self.perform_query(query)
            print(f'Table \'{tbl_name}\' created successfully')
        else:
            print(f'Table \'{tbl_name}\' could not be created. Check names {tbl_name, *tbl_columns}')
        return False

    def perform_query(self, query: str, *term: tuple[str]):
        try:
            q = self.db_con.cursor().execute(query, term)
            self.db_con.cursor().close()
            self.db_con.commit()
        except sqlite3.Error as err:
            logging.error(traceback.format_exc())
            raise err
        else:
            return q

    def drop_table(self, table_name) -> bool:
        """
        Drop table from current database
        :param table_name: str, name of a table to be dropped
        :return: bool, drop status True or False as value
        """
        query = f"DROP TABLE IF EXISTS {table_name}"
        if self.perform_query(query):
            print(f"Table '{table_name}' was successfully dropped")
            return True
        return False

    def drop_tables(self, *tbl_names) -> dict[str:bool]:
        """
        Drops tables provided as list of table names from current database using connection to the database
        :param tbl_names: str, name or multiple names of a tables to be dropped
        :return: dict[str:bool], dictionary with tbl_names as keys and drop status True or False as value
        """
        tbl_dropped = {}
        for table_name in tbl_names:
            tbl_dropped[table_name] = self.drop_table(table_name)
        return tbl_dropped

    def drop_trigger(self, tr_name: str):
        """
        Delete Trigger using name.
        :param tr_name: str, Trigger name to delete
        :return: bool, True or False depending on the operation success
        """
        query = f"DROP TRIGGER IF EXISTS {tr_name}"
        self.perform_query(query)
        print(f"Trigger '{tr_name}' was successfully dropped")

    def drop_triggers(self, *tr_names: str | None, tbl_name: Optional[str] = None):
        """
        Delete list of Triggers using theis names from database.
        if names are not provided use tbl_name and the following pattern to generate Trigger names:
        {tbl_name}_insert;{tbl_name}_delete;{tbl_name}_update,
        :param tr_names: str, Trigger names to delete
        :param tbl_name: str, used if Trigger names are not provided
        :return: bool, True or False depending on the operation success
        """
        if not tr_names:  # trying to get Trigger names from table name
            if tbl_name is None:  # cancel operation if table name is not set
                raise NameError("Require table name to drop triggers")
            tr_names = f'{tbl_name}_insert;{tbl_name}_delete;{tbl_name}_update'.split(';')
        map(self.drop_trigger, tr_names)

    def tbl_exist(self, name_to_check: str) -> bool:
        """
        Check whether table with the name 'name_to_check' already in database.
        To avoid creation of a table with a same name as existing.
        :param name_to_check: str, the name to be checked in database
        :return: bool, False or True
        """
        # query returns 1 if table exists and 0 if not
        query = f"SELECT EXISTS (SELECT 1 FROM sqlite_master " \
                f"WHERE type = 'table' AND name = '{name_to_check}')"
        # fetchone() returns tuple i.e. (1,) or (0,)
        exist = bool(self.perform_query(query).fetchone()[0])
        return exist

    def add_column(self, tbl_name, col_name, col_type):
        """
        Adds columns to the datatable from the given list of column names and their types.
        :param col_name:str, Name of column to be added
        :param col_type:str, Type of column to be added
        :param tbl_name: str, name of a table to add columns
        :return: :bool, status True or False as value
        """
        if is_valid_name(col_name) and col_type.upper() in VALID_COLUMN_DTYPES:
            query = f"ALTER TABLE {tbl_name} ADD {col_name} {col_type};"
            self.perform_query(query)
            print(f"Column '{col_name}' was successfully created in '{tbl_name}'")

    def add_columns(self, tbl_name: str, **col_params: dict[str:str]):
        """
        Adds columns to the datatable from the given list of column names and their types.
        :param tbl_name: str, name of a table to add columns
        :param col_params: dict[str], Dictionary containing column name as 'key' and column 'type' as 'value'
        :return: dict[str:bool], dictionary with column names as keys and add status True or False as value
        """
        for c_name, c_type in col_params.items():
            self.add_column(tbl_name, c_name, c_type)

    def link_search_table(self, tbl_name: str, *search_columns: str | list[str],
                          suffix: Optional[str] = '_fts'):
        """
        Creates Virtual SQLight3 FTS 5 table using provided datatable as a content table.
        And setting triggers on update, delete and insert actions to keep it synchronised to the datatable.
        For mor details please check: https://www.sqlite.org/fts5.html#external_content_tables
        :param tbl_name: str, name of existing data table
        :param search_columns: str, names of columns in datatable
        to be used as a content for a Virtual table FTS (text-search)
        :param suffix: str, define the name of a search table as
        {data_table_name}{suffix} it is recommended to keep default
        :return: bool, True if operation succeeded
        """
        search_tbl = tbl_name + suffix
        if is_valid_name(search_tbl, *search_columns):
            # ensure that there are no table with the same name
            query = f"CREATE VIRTUAL TABLE IF NOT EXISTS {search_tbl} " \
                    f"USING fts5({','.join(search_columns)}, content={tbl_name})"
            self.perform_query(query)
            print(f"Search table '{search_tbl}' was successfully created")
            self.create_triggers(tbl_name, search_tbl, suffix, search_columns)

    def create_triggers(self, tbl_name, search_tbl, suffix, search_columns):
        columns = ','.join(search_columns)
        new_columns = ','.join(f'new.{c}' for c in search_columns)
        old_columns = ','.join(f'old.{c}' for c in search_columns)

        #  Triggers to keep the Search table up to date.
        query = {
            'insert': '''   
                            CREATE TRIGGER IF NOT EXISTS {table}_insert AFTER INSERT ON {table}
                            BEGIN
                                INSERT INTO {search_tbl} (rowid, {column_list}) 
                                VALUES (new.rowid, {new_columns});
                            END;
                            ''',
            'delete': '''
                            CREATE TRIGGER IF NOT EXISTS {table}_delete AFTER DELETE ON {table}
                            BEGIN
                               INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list}) 
                               VALUES ('delete', old.rowid, {old_columns});
                            END;
                            ''',
            'update': '''
                            CREATE TRIGGER IF NOT EXISTS {table}_update AFTER UPDATE ON {table}
                            BEGIN
                               INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list}) 
                               VALUES ('delete', old.rowid, {old_columns});
                               INSERT INTO {search_tbl} (rowid, {column_list}) VALUES (new.rowid, {new_columns});
                            END;        
                        '''
        }

        for q_trigger in query.values():
            q = q_trigger.format(search_tbl=search_tbl, table=tbl_name, suffix=suffix,
                                 column_list=columns, new_columns=new_columns, old_columns=old_columns
                                 )
            self.perform_query(q)

        print(f"Search triggers were successfully created for '{search_tbl}' ")

    def create_search_table(self,
                            data_table: str,
                            search_columns: list[str],
                            clean_columns: list[str],
                            col_names: list[str]
                            ):
        """
        creates datatable in the database
        :param data_table: name of the existing table
        :param clean_columns:
        :param search_columns:
        :param col_names:
            https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
        :return: count of data rows loaded
        """

        # create empty datatable
        self.create_table(data_table, *col_names, *clean_columns)
        # create empty database for FTS search
        self.link_search_table(data_table, search_columns)
        print(f'Datatable: {data_table} successfully created')

    def data_chunk_to_sql(self,
                          chunk,
                          data_table,
                          sql_loader_settings: dict[str, Any] = {}
                          ) -> int:
        """
        :param data_table:
        :param chunk: chunk of data as Panda's object
        :param sql_loader_settings:use to extend or override global CSV_READ_PARAMS
            for details refer to pandas to_sql settings
        :return: int, number of rows loaded to database
        """
        if not sql_loader_settings:
            sql_loader_settings = DATA_TO_SQL_PARAMS
        chunk.to_sql(**sql_loader_settings, name=data_table, con=self.db_con)  # load data
        return chunk.shape[0]

    def finalize(self,
                 data_table: str,
                 search_suffix: str = '_fts',
                 export: Optional[bool] = True,
                 output_folder: Optional[Union[str, Path]] = None,
                 **export_params: [dict],
                 ) -> bool:
        """
        Export results to the file and drops tables
        :param data_table: str, name of the table to be dropped
        :param search_suffix: str, suffix used for the search table
        :param export: bool, True if export is needed
        :param output_folder: str, Path to the output directory
        :param export_params: dict, settings for csv export for Pandas
        :return: bool, True if succeeded
        """
        if export is True:
            exported = self.export_sql_to_csv(
                data_table=data_table,
                file_path=output_folder,
                **export_params
            )
            if exported is False:
                return False
        finalised = all((
            self.drop_triggers(tbl_name=data_table),
            all(self.drop_tables(data_table, data_table + search_suffix).values())
        ))
        return finalised

    def get_table_sample(self, tbl_name: str, limit: int = 2):
        """
        :param tbl_name:
        :param limit:
        :return:
        """
        query = f'SELECT * FROM {tbl_name} LIMIT {limit}'
        for tbl_row in self.perform_query(query):
            print(f"These are sample rows for {tbl_name}")
            print(tbl_row)

    def get_table_columns(self, tbl_name: str) -> list:
        """
        Get names of table columns
        :param tbl_name: str, table name
        :return: list of table column names
        """
        query = f'select * from {tbl_name}'
        cursor = self.db_con.execute(query)
        names = list(map(lambda x: x[0], cursor.description))
        cursor.close()
        return names

    def select_nulls(self,
                     data_table: str,
                     search_columns: list,
                     clean_columns: list,
                     ) -> List[sqlite3.Row]:
        """
        Get rows with NULL values for defined columns
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
            items = self.db_con.cursor().execute(query).fetchall()
            self.db_con.cursor().close()
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

    def search_delete_query(self,
                            data_table: str,
                            *term: str,
                            search_suffix: str = '_fts'
                            ):
        """
            Delete datatable rows basing on the search results using Term as a search condition
            :param data_table: str, name of the data table
            :param term: str, Search term ising SQLight FTS5 syntax after MATCH
            https://www.sqlite.org/fts5.html#full_text_query_syntax
            :param search_suffix: str, suffix used while creating the search table,
            default is recommended
            :return: bool, True if the operation was successful
            """

        query = '''              
                    DELETE FROM {table} 
                    WHERE ROWID IN (
                        SELECT ROWID FROM {table}{search_suffix} 
                        WHERE {table}{search_suffix} MATCH ? ORDER BY rank )                                   
                    '''.format(
            table=data_table,
            search_suffix=search_suffix,
        )
        self.perform_query(query, *term)

    def search_update_query(self,
                            data_table: str,
                            column: str,
                            *params: list[list],
                            search_suffix: Optional[str] = '_fts'
                            ):
        """
        Updates the specified datatable Column rows with the specified Value
        basing on the search results using Term as a search condition
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
            self.db_con.cursor().executemany(query, params)
            self.db_con.commit()
            self.db_con.cursor().close()
        except sqlite3.Error:
            logging.error(traceback.format_exc())

    def __del__(self):
        self.db_con.close()
