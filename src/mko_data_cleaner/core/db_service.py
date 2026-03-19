import logging
import sqlite3
import traceback
from functools import cached_property
from pathlib import Path
from typing import Any
import polars as pl

from .errors import WrongDataSettings
from .models import ActionType, MappingColumns
from .utils import clean_names, make_valid, validate_names
import adbc_driver_sqlite.dbapi as adb

logger = logging.getLogger(__name__)

# # DB table keeping schema
MASTER_TABLE = "sqlite_master"

# # Data types used to add columns in SQLight data table
VALID_COLUMN_DTYPES = ("TEXT", "NUMERIC", "INTEGER", "REAL", "BLOB")

DATA_TO_SQL_PARAMS = {
    "if_exists": "append",
    "index": False,
    "index_label": None,
    "chunksize": 2000,
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
    AVAILABLE_ACTIONS = {
        "DELETE": "d",
        "REPLACE": "r",
        "ADD": "a",
    }

    def __init__(
        self,
        db_file: Path,
        tbl_name: str = "data_table",
        index_column: str | None = None,
        date_column: str | None = None,
        use_temp_tables:bool = True,
    ):
        self.db_file = db_file
        self.db_con = sqlite3.connect(self.db_file)
        self.db_adb_con = adb.connect(str(self.db_file.as_posix()))
        self.data_tbl_name = make_valid(tbl_name)
        self.index_column = make_valid(index_column) if index_column else None
        self.date_column = make_valid(date_column) if date_column else None
        self.use_temp_tables = use_temp_tables
        self._index_tbl_name = None
        self._data_tbl_columns = None
        self._search_columns = None
        self._extra_columns = None
        self.non_mapped_table = "non_mapped"
        self._full_matches_table: str = "full_matches_table"
        self._joined_matches_table: str = "joined_matches_table"
        self._init_base()

    @property
    def target_table(self):
        if self._index_tbl_name:
            return self._index_tbl_name
        return self.data_tbl_name

    def _init_base(self):
        cur = self.db_con.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA temp_store=MEMORY")
        cur.execute("PRAGMA cache_size=-100000")

        cur.close()

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
        self._extra_columns = self.data_tbl_columns[-len(extra_cols) :]

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

    def create_table(
        self, tbl_name: str, *tbl_columns: str, temporary: bool = False
    ) -> None:
        """Creates datatable in the database using 'tbl_name' and 'tbl_columns'
        :param tbl_name: name of a table to create
        :param tbl_columns: str, list of column names
        :param temporary: bool - if true, will create temporary table
        :return:
        """
        temp = "TEMP" if temporary else ""
        tbl_name, *tbl_columns = clean_names(tbl_name, *tbl_columns)

        query = (
            f"CREATE {temp} TABLE IF NOT EXISTS {tbl_name} ({', '.join(tbl_columns)});"
        )

        self.perform_query(query)
        logger.debug(f"Table '{tbl_name}' created successfully")

    def update_index_from_data(self):
        if self.index_column:
            insert_columns = select_columns = (
                f"{', '.join(self.search_columns)}, {self.index_column}"
            )
            if self.date_column:
                insert_columns += f", {self.date_column}"
                select_columns += f", MAX({self.date_column})"
            query = (
                f"INSERT INTO {self._index_tbl_name} ({insert_columns}) "
                f"SELECT {select_columns} "
                f"FROM {self.data_tbl_name} "
                f"GROUP BY {self.index_column};"
            )
            # print(query)
            self.perform_query(query)
            logger.debug(f"Table '{self._index_tbl_name}' updated successfully")

    def perform_query(self, query: str, params: tuple | None = None):
        try:
            q = self.db_con.cursor().execute(query, params or ())
            self.db_con.cursor().close()
            self.db_con.commit()
        except sqlite3.Error as err:
            logging.error(
                f"SQL Error {err}, Query = ' {query} ', Terms =' {params} ' {traceback.format_exc()}"
            )
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
            self.db_con.cursor().close()
            if self.perform_query(query):
                logger.debug(f"Table '{table_name}' was successfully dropped")
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
        logger.debug(f"Trigger '{tr_name}' was successfully dropped")

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
                logger.error("Not possible to drop triggers because tbl_name is empty")
                raise NameError("Require table name to drop triggers")
            tr_names = f"{tbl_name}_insert;{tbl_name}_delete;{tbl_name}_update".split(
                ";"
            )
        map(self.drop_trigger, tr_names)

    # def tbl_exist(self, name_to_check: str) -> bool:
    #     """
    #     Check whether table with the name 'name_to_check' already in database.
    #     To avoid creation of a table with a same name as existing.
    #     :param name_to_check: str, the name to be checked in database
    #     :return: bool, False or True
    #     """
    #     # query returns 1 if table exists and 0 if not
    #     query = (
    #         f"SELECT EXISTS (SELECT 1 FROM sqlite_master "
    #         f"WHERE type = 'table' AND name = '{name_to_check}')"
    #     )
    #     # fetchone() returns tuple i.e. (1,) or (0,)
    #     exist = bool(self.perform_query(query).fetchone()[0])
    #     return exist

    def tbl_exists(self, name: str) -> bool:
        sql = """
              SELECT name FROM sqlite_master  WHERE name = ?
              UNION
              SELECT name FROM sqlite_temp_master WHERE name = ?
              """
        rows = self.db_con.execute(sql, (name, name)).fetchall()
        return len(rows) > 0

    def add_column(self, tbl_name: str, col_name: str, col_type: str):
        """
        Adds columns to the datatable from the given list of column names and their types.
        :param col_name:str, Name of column to be added
        :param col_type:str, Type of column to be added
        :param tbl_name: str, name of a table to add columns
        :return: :bool, status True or False as value
        """
        col_name = clean_names(col_name)[0]
        sql = f'ALTER TABLE "{tbl_name}" ADD COLUMN "{col_name}" {col_type.upper()};'
        self.perform_query(sql)
        logger.debug(f"Column '{col_name}' was successfully created in '{tbl_name}'")

    def add_columns(self, tbl_name: str, **col_params: str):
        """
        Adds columns to the datatable from the given list of column names and their types.
        :param tbl_name: str, name of a table to add columns
        :param col_params: dict[str,str], Dictionary containing column name as 'key' and column 'type' as 'value'
        :return: dict[str:bool], dictionary with column names as keys and add status True or False as value
        """
        for c_name, c_type in col_params.items():
            self.add_column(tbl_name, c_name, c_type)

    def link_search_table(
        self,
        tbl_name: str,
        *search_columns: str | list[str],
        suffix: str | None = "_fts",
    ):
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
        query = (
            f"CREATE VIRTUAL TABLE IF NOT EXISTS {search_tbl} "
            f"USING fts5({','.join(search_columns)}, content={tbl_name})"
        )
        self.perform_query(query)
        logger.debug(f"Search table '{search_tbl}' was successfully created")
        self.create_triggers(tbl_name, search_tbl, suffix, search_columns)

    def create_triggers(self, tbl_name, search_tbl, suffix, search_columns):
        columns = ",".join(search_columns)
        new_columns = ",".join(f"new.{c}" for c in search_columns)
        old_columns = ",".join(f"old.{c}" for c in search_columns)

        #  Triggers to keep the Search table up to date.

        query = {
            "insert": """
                      CREATE TRIGGER IF NOT EXISTS {table}_insert AFTER INSERT ON {table}
                      BEGIN
                      INSERT INTO {search_tbl} (rowid, {column_list})
                      VALUES (new.rowid, {new_columns});
                      END;
                      """,
            "delete": """
                      CREATE TRIGGER IF NOT EXISTS {table}_delete AFTER
                      DELETE
                      ON {table}
                      BEGIN
                      INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list})
                      VALUES ('delete', old.rowid, {old_columns});
                      END;
                      """,
            "update": """
                      CREATE TRIGGER IF NOT EXISTS {table}_update AFTER
                      UPDATE ON {table}
                      BEGIN
                      INSERT INTO {search_tbl} ({search_tbl}, rowid, {column_list})
                      VALUES ('delete', old.rowid, {old_columns});
                      INSERT INTO {search_tbl} (rowid, {column_list})
                      VALUES (new.rowid, {new_columns});
                      END;
                      """,
        }

        for q_trigger in query.values():
            q = q_trigger.format(
                search_tbl=search_tbl,
                table=tbl_name,
                suffix=suffix,
                column_list=columns,
                new_columns=new_columns,
                old_columns=old_columns,
            )
            self.perform_query(q)

        logger.debug(f"Search triggers were successfully created for '{search_tbl}' ")

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
            self._index_tbl_name or self.data_tbl_name, *self.search_columns
        )
        logger.debug(f"Datatable: {self.data_tbl_name} successfully created")

    def _create_index_table(self):
        if self.index_column:
            self._index_tbl_name = self.data_tbl_name + "_distinct"
            index_tbl_column_names = list(
                (*self.search_columns, *self.extra_columns, self.index_column)
            )
            if self.date_column:
                index_tbl_column_names.append(self.date_column)
            # create index base
            self.create_table(
                self._index_tbl_name, *index_tbl_column_names, temporary=self.use_temp_tables
            )

    def _create_index(self, table_name: str, *columns: str, unique_index: bool = False):
        unique = "UNIQUE" if unique_index else ""
        sql = f"""
            CREATE {unique} INDEX IF NOT EXISTS {table_name}_{'_'.join(columns)}_index
            ON {table_name}({', '.join(columns)});
            """
        self.perform_query(sql)

    def create_table_with_index(self):
        self._validate_required()
        # create empty datatable
        self.create_table(self.data_tbl_name, *self.data_tbl_columns)
        self._create_index_table()
        self._create_index(self.data_tbl_name, self.index_column)
        self._create_index(self._index_tbl_name, self.index_column)

    def data_chunk_to_sql(
        self, chunk, data_table, sql_loader_settings: dict[str, Any] = None
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
        chunk.to_sql(
            **sql_loader_settings, name=data_table, con=self.db_con
        )  # load data
        return chunk.shape[0]

    def get_table_sample(self, tbl_name: str, limit: int = 2):
        """
        :param tbl_name:
        :param limit:
        :return:
        """
        query = f"SELECT * FROM {tbl_name} LIMIT {limit}"
        for tbl_row in self.perform_query(query):
            print(f"These are sample rows for {tbl_name}", flush=True)
            print(tbl_row, flush=True)

    def get_table_columns(self, tbl_name: str) -> list:
        """
        Get names of table columns
        :param tbl_name: str, table name
        :return: list of table column names
        """
        sql = """
              SELECT name
              FROM pragma_table_info(?)
              ORDER BY cid; 
              """
        cursor = self.db_con.execute(sql, (tbl_name,))
        return [row[0] for row in cursor.fetchall()]

    def build_non_mapped(self) -> list[sqlite3.Row] | None:
        """
        Get rows with NULL values for defined columns
        """
        select_cols = ", ".join(self.search_columns + self.extra_columns)
        query = f"""
            CREATE TEMP TABLE {self.non_mapped_table} AS 
                SELECT DISTINCT {select_cols} 
                FROM {self.target_table} 
                WHERE   {' | '.join(self.extra_columns)} IS NULL                
            """
        self.perform_query(query)

    def apply_mapping(
        self, mapping_table: str, action_type: str, extra_cols: list, separator=", "
    ):
        """
        Apply mapping rules in order:
        DELETE → REPLACE → ADD
        """

        self._build_joined_matches(mapping_table)

        match action_type:
            case ActionType.DELETE:
                self._apply_delete()
            case ActionType.REPLACE:
                self._apply_replace(extra_cols)
            case ActionType.ADD:
                self._ensure_tags_table()
                self._apply_add(separator)
            case _:
                pass

    def sync_with_data_table(self):
        if self._index_tbl_name:
            self._sync_tables(
                self.data_tbl_name,
                self._index_tbl_name,
                self.index_column,
                *self.extra_columns,
            )

    def _sync_tables(self, target_tbl, source_tbl, index_col, *cols):

        cols_update = ", ".join(f"{c}=s.{c}" for c in cols if c != index_col)

        update_sql = f"""
        UPDATE {target_tbl} AS t
        SET {cols_update}
        FROM {source_tbl} AS s
        WHERE t.{index_col} = s.{index_col};
        """

        self.perform_query(update_sql)

        insert_cols = ", ".join(cols)

        insert_sql = f"""
        INSERT INTO {target_tbl} ({insert_cols})
        SELECT {insert_cols}
        FROM {source_tbl} AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM {target_tbl} AS t
            WHERE t.{index_col}=s.{index_col}
        );
        """

        self.perform_query(insert_sql)

        delete_sql = f"""
        DELETE FROM {target_tbl} AS t
        WHERE NOT EXISTS (
            SELECT 1
            FROM {source_tbl} AS s
            WHERE s.{index_col}=t.{index_col}
        );
        """

        self.perform_query(delete_sql)
        logger.debug(f"Sync {target_tbl} with {source_tbl} on {index_col}")

    def _build_joined_matches(self, mapping_table):

        self.perform_query(f"DROP TABLE IF EXISTS {self._joined_matches_table}")

        tmp = "TEMP" if self.use_temp_tables else ""
        sql = f"""
        CREATE {tmp} TABLE {self._joined_matches_table} AS 
        SELECT
            rm.data_rowid,
            r.*
        FROM {self._full_matches_table} rm
        JOIN {mapping_table} r
            ON r.{MappingColumns.mapping_index} = rm.{MappingColumns.mapping_index}        
        """

        self.perform_query(sql)

    def _apply_delete(self) -> None:

        sql = f"""
            DELETE FROM {self.target_table}
            WHERE rowid IN (
                SELECT DISTINCT data_rowid
                FROM {self._joined_matches_table} 
                )                            
            """

        self.perform_query(sql)

        logger.debug(f"Apply delete rules to {self.target_table}")
        self.perform_query(sql)

    def _apply_replace(self, column_names: list):

        extra_cols = set(self.extra_columns) & set(column_names)
        update_clause = ",\n".join(f"{col} = jm.{col}" for col in extra_cols)
        #
        # sql =f"""
        #         UPDATE data_table_distinct
        #         SET
        #             {update_clause}
        #         FROM {self._joined_matches_table} jm
        #         WHERE {self.target_table}.rowid = jm.data_rowid
        #               AND jm.rowid = (
        #                 SELECT rowid
        #                 FROM {self._joined_matches_table} jm2
        #                 WHERE jm2.data_rowid = jm.data_rowid
        #                 ORDER BY jm2.{MappingColumns.mapping_index} DESC
        #                 LIMIT 1
        #             );
        #
        #         """
        sql = f"""
                    UPDATE {self.target_table}
                    SET {update_clause}                    
                    FROM {self._joined_matches_table} jm
                    JOIN (
                        SELECT
                            data_rowid,
                            MAX ({MappingColumns.mapping_index}) AS max_idx
                        FROM {self._joined_matches_table}
                        GROUP BY data_rowid
                    ) mx
                    ON mx.data_rowid = jm.data_rowid
                    AND mx.max_idx = jm.{MappingColumns.mapping_index}
                    WHERE data_table_distinct.rowid = jm.data_rowid;
            """

        # select_cols = ",\n".join(f"{col}" for col in extra_cols)
        # update_clause = ",\n".join(f"{col} = rr.{col}" for col in extra_cols)
        #
        # sql = f"""
        #         WITH ranked_rules AS (
        #             SELECT
        #                 data_rowid,
        #                 {select_cols},
        #                 ROW_NUMBER() OVER(
        #                     PARTITION BY data_rowid
        #                     ORDER BY {MappingColumns.mapping_index} DESC
        #                 ) AS rn
        #             FROM {self._joined_matches_table}
        #         )
        #         UPDATE {self.target_table}
        #         SET
        #             {update_clause}
        #         FROM ranked_rules rr
        #         WHERE rr.rn = 1
        #         AND {self.target_table}.rowid = rr.data_rowid
        #         """

        # select_cols = ",\n".join(f"{col}" for col in extra_cols)

        # update_clause = ",\n".join(
        #     f"{col} = rule_values.{col}"
        #     for col in extra_cols
        # )
        # sql = f"""
        # WITH ranked_rules AS (
        #     SELECT
        #         r.data_rowid,
        #         {select_cols},
        #         ROW_NUMBER() OVER(
        #             PARTITION BY r.data_rowid
        #             ORDER BY r.{MappingColumns.mapping_index} DESC
        #         ) AS rn
        #     FROM {self._joined_matches_table} r
        # ),
        #
        # rule_values AS (
        #     SELECT
        #         data_rowid,
        #         {select_cols}
        #     FROM ranked_rules
        #     WHERE rn = 1
        # )
        #
        # UPDATE {target_table}
        # SET
        #     {update_clause}
        # FROM rule_values
        # WHERE {target_table}.rowid = rule_values.data_rowid
        # """

        logger.debug(f"Apply replace rules to {self.target_table}")
        self.perform_query(sql)

    def _apply_add(self, separator=", "):

        logger.debug(f"Apply add rules to {self.target_table}")

        tag_unions = []

        for col in self.extra_columns:
            tag_unions.append(f"""
                SELECT
                    rm.data_rowid AS rowid,
                    '{col}' AS column_name,
                    rm.{col} AS value
                FROM {self._joined_matches_table} rm
                WHERE rm.{col} IS NOT NULL
                AND rm.{col} != ''
                """)

        tag_expand_query = "\nUNION ALL\n".join(tag_unions)

        insert_sql = f"""
        INSERT OR IGNORE INTO data_table_tags(rowid, column_name, value)
        {tag_expand_query}
        """

        self.perform_query(insert_sql)

        # получаем реально используемые колонки
        cols_query = """
                     SELECT DISTINCT column_name
                     FROM data_table_tags \
                     """
        used_columns = self.perform_query(cols_query).fetchall()
        used_columns = [row[0] for row in used_columns]

        # обновляем только эти колонки
        for col in used_columns:
            update_sql = f"""
            UPDATE {self.target_table}
            SET {col} = (
                SELECT GROUP_CONCAT(value, '{separator}')
                FROM (
                    SELECT DISTINCT value
                    FROM data_table_tags
                    WHERE rowid = {self.target_table}.rowid
                    AND column_name = '{col}'
                    ORDER BY value
                )
            )
            WHERE rowid IN (
                SELECT rowid
                FROM data_table_tags
                WHERE column_name = '{col}'
            )
            """

            self.perform_query(update_sql)

    def create_rules_matches(self, mapping_table: str):

        column_name_col = MappingColumns.column_name
        pattern_col = MappingColumns.pattern
        index_col = MappingColumns.mapping_index
        matches_table = self._full_matches_table

        union_queries = []

        for col in self.search_columns:
            union_queries.append(f"""
                SELECT DISTINCT 
                    data.rowid AS data_rowid,
                    rules.{index_col}                                       
                FROM {self.target_table} AS data
                JOIN {mapping_table} AS rules
                ON rules.{column_name_col} = '{col}'
                AND rules.{pattern_col} IS NOT NULL
                AND UPPER(data.{col}) LIKE rules.{pattern_col}
                """)

        rule_match_query = "\nUNION ALL\n".join(union_queries)

        self.drop_table(matches_table)

        sql = f"""
         CREATE TEMP TABLE {matches_table} AS
         {rule_match_query}
         """
        self.perform_query(sql)

        self._create_index(matches_table, "data_rowid")
        self._create_index(matches_table, index_col)
        self.drop_table(mapping_table)

    def _ensure_tags_table(self):
        self.create_table(
            "data_table_tags", "rowid", "column_name", "value", temporary=self.use_temp_tables
        )

        self._create_index(
            "data_table_tags", "rowid", "column_name", "value", unique_index=True
        )
        self._create_index("data_table_tags", "rowid", "column_name")




    def _create_table_from_df(self, df: pl.DataFrame, table: str):
        def pl_to_sqlite(dtype):
            if dtype == pl.Int64:
                return "INTEGER"
            if dtype == pl.Float64:
                return "REAL"
            return "TEXT"

        cols = [
            f"{name} {pl_to_sqlite(dtype)}"
            for name, dtype in zip(df.columns, df.dtypes)
        ]

        schema = ", ".join(cols)

        self.db_con.execute(f"CREATE TABLE {table} ({schema})")

    def polars_to_sqlite(self,
            df: pl.DataFrame,
            table: str,
            columns: list[str],
            if_exists: str = "append",  # append | replace
    ):
        """
        Write Polars DataFrame to SQLite.

        Args:
            df: Polars DataFrame
            table: table name
            columns: column names
            if_exists:
                - append: insert into existing table
                - replace: drop + recreate table
                - truncate: delete data, keep schema
        """

        if if_exists == "replace":
            self.drop_table(table)
            self._create_table_from_df(df, table)


        placeholders = ",".join(["?"] * len(df.columns))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        self.perform_query(query)
        self.db_con.cursor().executemany(query, df.iter_rows())
        self.db_con.commit()


    def _delete_base_files(self):
        for f in self.db_file.parent.glob(self.db_file.name + "*"):
            try:
                f.unlink(missing_ok=True)
            except PermissionError:
                logger.warning(f"Cannot delete {f}")

    def close(self):
        logger.info("Cleaning up temporary files")
        # self.drop_triggers(tbl_name=self.data_tbl_name),
        # self.drop_tables(self.data_tbl_name, self.target_table)
        try:
            self.db_con.execute("PRAGMA wal_checkpoint(FULL)")
        except Exception:
            pass

        self.db_con.close()
        self.db_adb_con.close()

        self._delete_base_files()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # pass
        self.close()
