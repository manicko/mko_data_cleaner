import traceback
import logging
import sqlite3
from functools import cached_property
from typing import (Any, List, Optional)

from .errors import WrongDataSettings
from .utils import clean_names, validate_names, make_valid
from pathlib import Path

logger = logging.getLogger(__name__)

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
    REQUIRED_FIELDS = (
        "db_file",
        "db_con",
        "data_tbl_name",
        "data_tbl_columns",
        "search_columns",
        "extra_columns",
    )

    def __init__(self, db_file: Path, tbl_name: str = 'data_table', index_column: str | None = None,
                 date_column: str | None = None):
        self.db_file = db_file
        self.db_con = sqlite3.connect(self.db_file)
        self.data_tbl_name = make_valid(tbl_name)
        self.index_column = make_valid(index_column) if index_column else None
        self.date_column = make_valid(date_column) if date_column else None
        self._index_tbl_name = None
        self._data_tbl_columns = None
        self._search_columns = None
        self._extra_columns = None
        self._init_base()

    def _init_base(self):
        self.db_con.cursor().execute("PRAGMA journal_mode = WAL")
        self.db_con.cursor().execute("PRAGMA synchronous = OFF")
        self.db_con.cursor().execute("PRAGMA temp_store = MEMORY")

    def _validate_required(self) -> None:
        for name in self.REQUIRED_FIELDS:
            if not getattr(self, name):
                raise WrongDataSettings(f"{name} not set")

    @property
    def data_tbl_columns(self):
        return self._data_tbl_columns

    def set_data_tbl_columns(self, *main_cols, extra_cols: list | None = None):
        extra_cols = extra_cols or []
        self._data_tbl_columns = clean_names(*main_cols, *extra_cols)
        self._extra_columns = self.data_tbl_columns[-len(extra_cols):]

    @property
    def search_columns(self):
        return self._search_columns

    @search_columns.setter
    def search_columns(self, search_columns: list[int]):
        tbl_cols_set = set(self.data_tbl_columns)
        self._search_columns = [name for name in search_columns if name in tbl_cols_set]

    @property
    def extra_columns(self):
        return self._extra_columns

    @cached_property
    def column_index(self):
        if self.data_tbl_columns:
            return {i: name for i, name in enumerate(self.data_tbl_columns)}
        return {}

    def get_col_names(self, index) -> str | None:
        return self.column_index.get(index, None)

    def create_table(self, tbl_name: str, *tbl_columns: str) -> None:
        """ Creates datatable in the database using 'tbl_name' and 'tbl_columns'
        :param tbl_name: name of a table to create
        :param tbl_columns: str, list of column names
        :return:
        """
        tbl_name, *tbl_columns = clean_names(tbl_name, *tbl_columns)
        query = f"CREATE TABLE IF NOT EXISTS {tbl_name} ({', '.join(tbl_columns)});"
        self.perform_query(query)
        print(f'Table \'{tbl_name}\' created successfully')

    def update_distinct_table(self):
        if self.index_column:
            insert_columns = select_columns = f"{', '.join(self.search_columns)}, {self.index_column}"
            if self.date_column:
                insert_columns += f', {self.date_column}'
                select_columns += f', MAX({self.date_column})'
            query = (f"INSERT INTO {self._index_tbl_name} ({insert_columns}) "
                     f"SELECT {select_columns}"
                     f"FROM {self.data_tbl_name} "
                     f"GROUP BY {self.index_column};")
            # print(query)
            self.perform_query(query)
            logger.info(f'Table \'{self._index_tbl_name}\' updated successfully')

    def update_values(self, target_tbl_name, from_tbl_name, index_col, *tbl_columns: str):
        query_col = [f'{name}={from_tbl_name}.{name}' for name in tbl_columns if name != index_col]
        query = (f"UPDATE {target_tbl_name} "
                 f"SET  {', '.join(query_col)}  "
                 f"FROM {from_tbl_name} "
                 f"WHERE {target_tbl_name}.{index_col} = {from_tbl_name}.{index_col};")
        self.perform_query(query)
        logger.info(f'Table \'{target_tbl_name}\' updated successfully')

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
        try:
            if self.perform_query(query):
                logger.info(f"Table '{table_name}' was successfully dropped")
                return True
            return False
        except sqlite3.Error:
            logging.error(traceback.format_exc())
            return False

    def drop_tables(self, *tbl_names) -> dict[str, bool]:
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
        logger.info(f"Trigger '{tr_name}' was successfully dropped")

    def drop_triggers(self, *tr_names: str | None, tbl_name: str | None = None):
        """
        Delete list of Triggers using theis names from database.
        if names are not provided use tbl_name and the following pattern to generate Trigger names:
        {tbl_name}_insert;{tbl_name}_delete;{tbl_name}_update,
        :param tr_names: str, Trigger names to delete
        :param tbl_name: str, used if Trigger names are not provided
        :return: None
        """
        if not tr_names:  # trying to get Trigger names from table name
            if not tbl_name:  # cancel operation if table name is not set
                logger.error(f"Not possible to drop triggers because tbl_name is empty")
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

    def add_column(self, tbl_name: str, col_name: str, col_type: str):
        """
        Adds columns to the datatable from the given list of column names and their types.
        :param col_name:str, Name of column to be added
        :param col_type:str, Type of column to be added
        :param tbl_name: str, name of a table to add columns
        :return: :bool, status True or False as value
        """
        col_name = clean_names(col_name)
        query = f"ALTER TABLE {tbl_name} ADD {col_name} {col_type.upper()};"

        self.perform_query(query)
        logger.info(f"Column '{col_name}' was successfully created in '{tbl_name}'")

    def add_columns(self, tbl_name: str, **col_params: str):
        """
        Adds columns to the datatable from the given list of column names and their types.
        :param tbl_name: str, name of a table to add columns
        :param col_params: dict[str,str], Dictionary containing column name as 'key' and column 'type' as 'value'
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
        validate_names(search_tbl, *search_columns)
        # ensure that there are no table with the same name
        query = f"CREATE VIRTUAL TABLE IF NOT EXISTS {search_tbl} " \
                f"USING fts5({','.join(search_columns)}, content={tbl_name})"
        self.perform_query(query)
        logger.info(f"Search table '{search_tbl}' was successfully created")
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
                      CREATE TRIGGER IF NOT EXISTS {table}_delete AFTER
                      DELETE
                      ON {table}
                      BEGIN
                      INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list})
                      VALUES ('delete', old.rowid, {old_columns});
                      END;
                      ''',
            'update': '''
                      CREATE TRIGGER IF NOT EXISTS {table}_update AFTER
                      UPDATE ON {table}
                      BEGIN
                      INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list})
                      VALUES ('delete', old.rowid, {old_columns});
                      INSERT INTO {search_tbl} (rowid, {column_list})
                      VALUES (new.rowid, {new_columns});
                      END;
                      '''
        }

        for q_trigger in query.values():
            q = q_trigger.format(search_tbl=search_tbl, table=tbl_name, suffix=suffix,
                                 column_list=columns, new_columns=new_columns, old_columns=old_columns
                                 )
            self.perform_query(q)

        logger.info(f"Search triggers were successfully created for '{search_tbl}' ")

    def create_search_table(self):
        """
        creates datatable in the database
            https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
        :return: count of data rows loaded
        """
        # check we have all required for building the table
        self._validate_required()

        # create empty datatable
        self.create_table(self.data_tbl_name, *self.data_tbl_columns)

        self._create_index_table()
        # link base to FTS search table
        self.link_search_table(
            self._index_tbl_name or self.data_tbl_name,
            *self.search_columns)
        logger.info(f'Datatable: {self.data_tbl_name} successfully created')

    def _create_index_table(self):
        if self.index_column:
            self._index_tbl_name = self.data_tbl_name + '_distinct'
            index_tbl_column_names = list((
                *self.search_columns,
                *self.extra_columns,
                self.index_column,
                self.date_column
            ))
            # create index base
            self.create_table(self._index_tbl_name, *index_tbl_column_names)

    def create_table_with_index(self):
        self._validate_required()
        # create empty datatable
        self.create_table(self.data_tbl_name, *self.data_tbl_columns)
        self._create_index_table()

    def clean_update_data(self, params):
        # update clean columns in the data table
        if self.index_column:
            logger.info(f"Updating clean columns in the data table")
            self.update_values(self.data_tbl_name, self._index_tbl_name,
                               self.index_column, *self.extra_columns)

    def data_chunk_to_sql(self,
                          chunk,
                          data_table,
                          sql_loader_settings: dict[str, Any] = None
                          ) -> int:
        """
        :param data_table:
        :param chunk: chunk of data as Panda's object
        :param sql_loader_settings:use to extend or override global CSV_READ_PARAMS
            for details refer to pandas to_sql settings
        :return: int, number of rows loaded to database
        """
        if sql_loader_settings is None:
            sql_loader_settings = DATA_TO_SQL_PARAMS
        chunk.to_sql(**sql_loader_settings, name=data_table, con=self.db_con)  # load data
        return chunk.shape[0]

    def get_table_sample(self, tbl_name: str, limit: int = 2):
        """
        :param tbl_name:
        :param limit:
        :return:
        """
        query = f'SELECT * FROM {tbl_name} LIMIT {limit}'
        for tbl_row in self.perform_query(query):
            print(f"These are sample rows for {tbl_name}", flush=True)
            print(tbl_row, flush=True)

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
                     ) -> List[sqlite3.Row] | None:
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

    @staticmethod
    def _get_action_registry():
        return {
            "DELETE": "d",
            "REPLACE": "r",
            "ADD": "a",
        }

    def apply_mapping(
            self,
            mapping_table,
            column_name_col="column_name",
            pattern_col="pattern",
            separator=", ",
    ):
        """
        Apply mapping rules in order:
        DELETE → REPLACE → ADD
        """

        actions = self._get_action_registry()

        target_table = self._get_target_table()

        self._ensure_rules_index(mapping_table, column_name_col)

        self._build_rule_matches(
            target_table,
            mapping_table,
            column_name_col,
            pattern_col,
        )

        self._apply_delete(target_table, actions["DELETE"])

        self._build_rules_joined(
            mapping_table,
            actions["REPLACE"],
            actions["ADD"],
        )

        self._apply_replace(target_table, actions["REPLACE"])

        self._ensure_tags_table()

        self._apply_add(target_table, actions["ADD"], separator)

    def _apply_delete(self, target_table, action_code):
        sql = f"""
        DELETE FROM {target_table}
        WHERE rowid IN (
            SELECT DISTINCT data_rowid
            FROM temp_rule_matches
            WHERE action = '{action_code}'
        )
        """

        self.perform_query(sql)

    def _apply_replace(self, target_table, action_code):

        agg_cols = ",\n".join(
            f"""
            MAX(CASE
                WHEN r.{col} IS NOT NULL
                THEN r.{col}
            END) AS {col}
            """
            for col in self.extra_columns
        )

        update_clause = ",\n".join(
            f"{col} = COALESCE(rule_values.{col}, {target_table}.{col})"
            for col in self.extra_columns
        )

        sql = f"""
        WITH rule_values AS (
            SELECT
                data_rowid,
                {agg_cols}
            FROM temp_rules_joined r
            WHERE r.action = '{action_code}'
            GROUP BY data_rowid
        )

        UPDATE {target_table}
        SET
            {update_clause}
        FROM rule_values
        WHERE {target_table}.rowid = rule_values.data_rowid
        """

        self.perform_query(sql)

    def _apply_add(self, target_table, action_code, separator):

        tag_unions = []

        for col in self.extra_columns:
            tag_unions.append(
                f"""
                SELECT
                    rj.data_rowid AS rowid,
                    '{col}' AS column_name,
                    rj.{col} AS value
                FROM temp_rules_joined rj
                WHERE rj.action = '{action_code}'
                AND rj.{col} IS NOT NULL
                AND rj.{col} != ''
                """
            )

        tag_expand_query = "\nUNION ALL\n".join(tag_unions)

        sql = f"""
        INSERT OR IGNORE INTO data_table_tags(rowid, column_name, value)
        {tag_expand_query}
        """

        self.perform_query(sql)

        update_clauses = []

        for col in self.extra_columns:
            update_clauses.append(
                f"""
                {col} = (
                    SELECT GROUP_CONCAT(value, '{separator}')
                    FROM (
                        SELECT DISTINCT value
                        FROM data_table_tags
                        WHERE rowid = {target_table}.rowid
                        AND column_name = '{col}'
                        ORDER BY value
                    )
                )
                """
            )

        update_clause = ",\n".join(update_clauses)

        sql = f"""
        UPDATE {target_table}
        SET {update_clause}
        WHERE rowid IN (
            SELECT DISTINCT rowid FROM data_table_tags
        )
        """

        self.perform_query(sql)

    def _get_target_table(self):
        if self._index_tbl_name:
            return self._index_tbl_name
        return self.data_tbl_name

    def _ensure_rules_index(self, mapping_table, column_name_col):
        sql = f"""
        CREATE INDEX IF NOT EXISTS idx_rules_column
        ON {mapping_table}({column_name_col});
        """
        self.perform_query(sql)

    def _build_rule_matches(
            self,
            target_table,
            mapping_table,
            column_name_col,
            pattern_col,
    ):

        union_queries = []
        for col in self.search_columns:
            union_queries.append(
                f"""
                SELECT
                    data.rowid AS data_rowid,
                    rules.rowid AS rule_rowid,
                    rules.action AS action
                FROM {target_table} AS data
                JOIN {mapping_table} AS rules
                ON rules.{column_name_col} = '{col}'
                AND rules.{pattern_col} IS NOT NULL
                AND UPPER(data.{col}) LIKE rules.{pattern_col}
                """
            )

        rule_match_query = "\nUNION ALL\n".join(union_queries)

        self.perform_query("DROP TABLE IF EXISTS temp_rule_matches")

        sql = f"""
        CREATE TEMP TABLE temp_rule_matches AS
        {rule_match_query}
        """
        self.perform_query(sql)

        self.perform_query(
            """
            CREATE INDEX temp_rule_matches_idx
                ON temp_rule_matches (data_rowid)
            """
        )

    def _build_rules_joined(self, mapping_table, replace_code, add_code):

        self.perform_query("DROP TABLE IF EXISTS temp_rules_joined")

        sql = f"""
        CREATE TEMP TABLE temp_rules_joined AS
        SELECT
            rm.data_rowid,
            r.*
        FROM temp_rule_matches rm
        JOIN {mapping_table} r
        ON r.rowid = rm.rule_rowid
        WHERE rm.action IN ('{replace_code}', '{add_code}')
        """

        self.perform_query(sql)

    def _ensure_tags_table(self):

        self.perform_query(
            """
            CREATE TABLE IF NOT EXISTS data_table_tags
            (
                rowid
                INTEGER,
                column_name
                TEXT,
                value
                TEXT
            )
            """
        )

        self.perform_query(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_tag
                ON data_table_tags(rowid, column_name, value)
            """
        )

        self.perform_query(
            """
            CREATE INDEX IF NOT EXISTS idx_tags_row_col
                ON data_table_tags(rowid, column_name)
            """
        )

    def delete_base_file(self):
        try:
            self.db_file.unlink()
        except Exception as err:
            logging.error(traceback.format_exc())
            logger.error(f'not able to delete database file {self.db_file}, {err}')
        else:
            logger.info(f'Data base was purified successfully')

    def __del__(self):
        # self.drop_triggers(tbl_name=data_table),
        # self.drop_tables(data_table, data_table + search_suffix)
        self.db_con.close()
        # self.delete_base_file()
