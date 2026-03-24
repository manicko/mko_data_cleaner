import logging
import sqlite3
import traceback
from functools import cached_property
from pathlib import Path
from typing import Any

import adbc_driver_sqlite.dbapi as adb

from .errors import WrongDataSettings
from .models import ActionType, MappingColumns
from .utils import clean_names, make_valid, validate_names

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
        use_temp_tables: bool = True,
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
        self.use_fts = False
        self._fts_table_name: str | None = None
        self.non_mapped_table = "non_mapped"
        self._full_matches_table: str = "full_matches_table"
        self._joined_matches_table: str = "joined_matches_table"
        self._init_base()

    # ---------------------------------------------------------
    # properties
    # ---------------------------------------------------------

    @property
    def target_table(self):
        if self._index_tbl_name:
            return self._index_tbl_name
        return self.data_tbl_name

    @property
    def data_tbl_columns(self):
        return self._data_tbl_columns

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

    def set_data_tbl_columns(self, *main_cols, extra_cols: list | None = None):
        extra_cols = extra_cols or []
        self._data_tbl_columns = clean_names(*main_cols, *extra_cols)
        self._extra_columns = self.data_tbl_columns[-len(extra_cols) :]

    # ---------------------------------------------------------
    # init logic
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # Base SQL functions
    # ---------------------------------------------------------

    def create_table(
        self,
        tbl_name: str,
        *tbl_columns: str,
        temporary: bool = False,
        **typed_columns: str,
    ) -> None:
        """
        Creates table in the database.

        Args:
            tbl_name: table name
            *tbl_columns: column names (default type TEXT)
            temporary: create TEMP table if True
            **typed_columns: column_name=TYPE
        """

        # -------------------------
        # validate and clean names
        # -------------------------
        all_columns = list(tbl_columns) + list(typed_columns.keys())
        tbl_name, *all_columns = clean_names(tbl_name, *all_columns)

        # восстановим после clean_names
        plain_cols = all_columns[: len(tbl_columns)]
        typed_cols_keys = all_columns[len(tbl_columns) :]

        # -------------------------
        # validate types
        # -------------------------
        typed_cols = {}
        for key, val in zip(typed_cols_keys, typed_columns.values(), strict=True):
            dtype = val.upper()
            if dtype not in VALID_COLUMN_DTYPES:
                raise WrongDataSettings(
                    f"Invalid dtype '{val}' for column '{key}'. "
                    f"Allowed: {VALID_COLUMN_DTYPES}"
                )
            typed_cols[key] = dtype

        # -------------------------
        # build columns SQL
        # -------------------------
        columns_sql = []

        # обычные колонки → TEXT по умолчанию
        for col in plain_cols:
            columns_sql.append(f"{col} TEXT")

        # колонки с типами
        for col, dtype in typed_cols.items():
            columns_sql.append(f"{col} {dtype}")

        with_cols = ", ".join(columns_sql) if columns_sql else ""
        temp = "TEMP" if temporary else ""

        query = f"CREATE {temp} TABLE IF NOT EXISTS {tbl_name} ({with_cols});"

        self.perform_query(query)
        logger.debug(f"Table '{tbl_name}' created successfully")

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

    def tbl_exists(self, name: str) -> bool:
        sql = """
              SELECT name
              FROM sqlite_master
              WHERE name = ?
              UNION
              SELECT name
              FROM sqlite_temp_master
              WHERE name = ?
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

    # ---------------------------------------------------------
    # Index table
    # ---------------------------------------------------------

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
                self._index_tbl_name,
                *index_tbl_column_names,
                temporary=self.use_temp_tables,
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

    # ---------------------------------------------------------
    # FTS search
    # ---------------------------------------------------------
    def link_search_table(self, suffix: str | None = "_fts"):
        """
        Creates Virtual SQLight3 FTS 5 table using provided datatable as a content table.
        And setting triggers on update, delete and insert actions to keep it synchronised to the datatable.
        For mor details please check: https://www.sqlite.org/fts5.html#external_content_tables
        to be used as a content for a Virtual table FTS (text-search)
        :param suffix: str, define the name of a search table as
        {data_table_name}{suffix} it is recommended to keep default
        :return:
        """

        tbl_name = self._index_tbl_name or self.data_tbl_name
        self._fts_table_name = search_tbl = tbl_name + suffix

        search_columns = self.search_columns.copy()

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

    # ---------------------------------------------------------
    # Data import
    # ---------------------------------------------------------

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

    # ---------------------------------------------------------
    # Mapping - tables
    # ---------------------------------------------------------
    def create_rules_matches(self):
        mapping_index = MappingColumns.mapping_index
        data_rowid = MappingColumns.data_rowid
        matches_table_columns = {
            MappingColumns.data_rowid: "INTEGER",
            MappingColumns.mapping_index: "INTEGER",
        }
        self.drop_table(self._full_matches_table)
        self.create_table(
            self._full_matches_table,
            temporary=self.use_temp_tables,
            **matches_table_columns,
        )
        self._create_index(self._full_matches_table, data_rowid)
        self._create_index(self._full_matches_table, mapping_index)

    def insert_matches_from_fts(self, mapping_table: str):
        index_col = MappingColumns.mapping_index
        data_rowid_col = MappingColumns.data_rowid
        pattern_col = MappingColumns.pattern
        sql = f"""
                INSERT INTO {self._full_matches_table} ({data_rowid_col}, {index_col})
                SELECT {self._fts_table_name}.rowid, mt.{index_col}
                FROM {self._fts_table_name}
                JOIN {mapping_table} AS mt
                ON {self._fts_table_name} MATCH mt.{pattern_col}
                """

        self.perform_query(sql)

        # self.drop_table(mapping_table)

    def insert_matches(self, mapping_table: str):

        column_name_col = MappingColumns.column_name
        index_col = MappingColumns.mapping_index
        data_rowid_col = MappingColumns.data_rowid
        pattern_col = MappingColumns.pattern
        matches_table = self._full_matches_table

        union_queries = []

        for col in self.search_columns:
            union_queries.append(f"""            
                SELECT DISTINCT 
                    data.rowid AS {data_rowid_col},
                    rules.{index_col}                                       
                FROM {self.target_table} AS data
                JOIN {mapping_table} AS rules
                ON rules.{column_name_col} = '{col}'
                AND rules.{pattern_col} IS NOT NULL
                AND data.{col} LIKE rules.{pattern_col} COLLATE NOCASE
                """)

        rule_match_query = "\nUNION ALL\n".join(union_queries)

        sql = f""" 
        INSERT INTO {matches_table} ({data_rowid_col}, {index_col})
         {rule_match_query}
         """
        self.perform_query(sql)
        self.drop_table(mapping_table)

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

    # ---------------------------------------------------------
    # Mapping processing
    # ---------------------------------------------------------

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
        extra_cols_set = set(self.extra_columns)
        extra_cols = [col for col in column_names if col in extra_cols_set]

        select_cols = ",\n".join(f"{col}" for col in extra_cols)
        update_clause = ",\n".join(f"{col} = rr.{col}" for col in extra_cols)

        sql = f"""
                WITH ranked_rules AS (
                    SELECT
                        data_rowid,
                        {select_cols},
                        ROW_NUMBER() OVER(
                            PARTITION BY data_rowid
                            ORDER BY {MappingColumns.mapping_index} DESC, rowid DESC
                        ) AS rn
                    FROM {self._joined_matches_table}
                )
                UPDATE {self.target_table}
                SET
                    {update_clause}
                FROM ranked_rules rr
                WHERE rr.rn = 1
                AND {self.target_table}.rowid = rr.data_rowid
                """

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

    def _ensure_tags_table(self):
        self.create_table(
            "data_table_tags",
            "rowid",
            "column_name",
            "value",
            temporary=self.use_temp_tables,
        )

        self._create_index(
            "data_table_tags", "rowid", "column_name", "value", unique_index=True
        )
        self._create_index("data_table_tags", "rowid", "column_name")

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

    # ---------------------------------------------------------
    # Finalization
    # ---------------------------------------------------------

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
        except Exception as e:
            logger.warning(f"Cannot clean up temporary files: {e}")

        self.db_con.close()
        self.db_adb_con.close()

        self._delete_base_files()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # pass
        self.close()
