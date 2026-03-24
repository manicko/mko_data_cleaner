import logging
from collections.abc import Generator

import polars as pl

from mko_data_cleaner.core.models import (
    ActionType,
    DictColumnsIndexes,
    MappingColumns,
    MatchType,
)

logger = logging.getLogger(__name__)


class MappingDict:
    def __init__(self, data: pl.DataFrame, action_col_indexes: DictColumnsIndexes):
        self.data = data

        self._table_columns: list[str] = []
        self.data_col_index: dict[str, list[str]] = {}
        self.search_columns: set[str] = set()

        self._data_actions: pl.DataFrame = pl.DataFrame()
        self._data_mapping: pl.DataFrame = pl.DataFrame()

        self.fts_data: pl.DataFrame = pl.DataFrame()
        self.like_data: pl.DataFrame = pl.DataFrame()

        self.action_col_indexes = self._set_col_indexed(action_col_indexes)
        self._initialize_mapping()

    @property
    def extra_col_names(self) -> list[str]:
        if not self._data_mapping.is_empty():
            return list(self._data_mapping.columns)
        return []

    @extra_col_names.setter
    def extra_col_names(self, column_names: list[str]):
        if len(column_names) != self._data_mapping.width:
            raise ValueError(
                f"Length mismatch: Expected {self._data_mapping.width} , "
                f"got {len(column_names)}."
            )
        self._data_mapping.columns = column_names

    @staticmethod
    def _set_col_indexed(action_col_indexes: DictColumnsIndexes) -> dict[str, int]:
        return dict(sorted(action_col_indexes.model_dump().items(), key=lambda x: x[1]))

    @staticmethod
    def _drop_empty_columns(_df: pl.DataFrame) -> pl.DataFrame:
        return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]

    def get_data_mapping_by_action(self, action_type: ActionType | str) -> pl.DataFrame:
        try:
            return self.data.filter(pl.col(MappingColumns.action) == action_type)
        except Exception as err:
            logger.error(err)
            raise err

    @staticmethod
    def group_by_cols(df: pl.DataFrame) -> Generator[pl.DataFrame]:
        current_cols = None
        current_block: list[dict] = []
        for row in df.iter_rows(named=True):
            non_empty = {c: v for c, v in row.items() if v is not None}
            if current_cols == set(non_empty.keys()):
                current_block.append(non_empty)
                continue
            if current_block:
                yield pl.from_dicts(current_block)
            current_block = [non_empty]
            current_cols = set(non_empty.keys())
        if current_block:
            yield pl.from_dicts(current_block)

    def generate_rules_blocks(self) -> Generator[tuple[str, pl.DataFrame]]:
        keep_cols = [
            MappingColumns.mapping_index,
            *self.extra_col_names,
        ]
        actions = [ActionType.DELETE, ActionType.REPLACE, ActionType.ADD]
        for action in actions:
            df_action = self.data.filter(
                pl.col(MappingColumns.action) == action
            ).select(keep_cols)
            match action:
                case ActionType.DELETE | ActionType.ADD:
                    yield action, df_action
                case ActionType.REPLACE:
                    for df in self.group_by_cols(df_action):
                        yield action, df

    def _initialize_mapping(self):
        # First we use action column indexes to update their names and
        # extract extra columns going after the max action column
        action_names = list(self.action_col_indexes.keys())
        action_idx = list(self.action_col_indexes.values())

        # all cols after max action index are used as data mapping columns - extra cols
        data_start_idx = max(action_idx) + 1
        extra_col_idx = list(range(data_start_idx, self.data.width))

        # we keep only the action and extra columns
        self._data_actions = self.data[:, action_idx]
        self._data_mapping = self.data[:, extra_col_idx]

        # keep only non-empty columns for mapping
        self._data_mapping = self._drop_empty_columns(self._data_mapping)
        self.extra_col_names = (
            self._data_mapping.columns
        )  # dirty naming will be updated after db load

        # replace user defined names with internal standard
        self._data_actions.columns = action_names

    def build_mapping(self, *tbl_columns: str, extra_col_names: list[str]):
        self._table_columns = list(tbl_columns)
        # update extra_col_names with names from db
        self.extra_col_names = extra_col_names
        self.data = pl.concat(
            [self._data_actions, self._data_mapping], how="horizontal"
        )

        # adding index to dataframe
        self.data = self.data.with_row_index(MappingColumns.mapping_index, offset=1)

        # build dictionary with data column index : name
        self.data_col_index = self._get_data_col_index()

        # split dataframes on 2 blocks with fts and without
        fts_mask = pl.col(MappingColumns.match) == MatchType.FTS
        self.fts_data = self.data.filter(fts_mask)
        self.like_data = self.data.filter(~fts_mask)

        if not self.like_data.is_empty():
            self.like_data = self._build_query(self.like_data)

        if not self.fts_data.is_empty():
            self.fts_data = self._build_fts_query(self.fts_data)

    def _get_data_col_index(
        self,
        search_col: str = MappingColumns.search,
        column_name_col: str = MappingColumns.column_name,
    ):
        """build dictionary with data column index : name"""
        if self._table_columns:
            return {
                search_col: list(range(len(self._table_columns))),
                column_name_col: self._table_columns,
            }
        return None

    def _build_query(
        self,
        df: pl.DataFrame,
        search_col: str = MappingColumns.search,
        term_col: str = MappingColumns.term,
        match_type_col: str = MappingColumns.match,
        pattern_col: str = MappingColumns.pattern,
        column_name_col: str = MappingColumns.column_name,
    ) -> pl.DataFrame:
        """
        Build mapping table for SQL LIKE matching.

        Output dataframe contains:
            column_name – name of column in data_table to search
            pattern     – LIKE pattern

        Parameters
        ----------
        tbl_names : list[str]
            list of searchable column names

        term_col : str
            column containing search term

        search_col : str
            column containing index of search column

        match_type_col : str
            search type:
                f – full match
                p – partial
                s – starts with
                e – ends with

        pattern_col : str
            name of resulting pattern column
        """

        # ensure search column contains numbers - indexes of columns to search
        data = df.with_columns(pl.col(search_col).cast(pl.Int64), strict=False)

        data = data.join(pl.DataFrame(self.data_col_index), on=search_col, how="left")

        pattern_expr = self._build_search_like_pattern(match_type_col, term_col)

        data = data.with_columns(pattern_expr.str.to_uppercase().alias(pattern_col))
        self.search_columns.update(
            data[column_name_col].drop_nulls().unique().to_list()
        )
        return data

    @staticmethod
    def _build_search_like_pattern(match_type_col: str, term_col: str) -> pl.Expr:
        """
        Convert match type to SQL LIKE pattern.

        f → term
        p → %term%
        s → term%
        e → %term
        """

        return (
            pl.when(pl.col(match_type_col) == MatchType.FULL_MATCH)
            .then(pl.col(term_col))
            .when(pl.col(match_type_col) == MatchType.PARTIAL_MATCH)
            .then(pl.concat_str([pl.lit("%"), pl.col(term_col), pl.lit("%")]))
            .when(pl.col(match_type_col) == MatchType.STARTS_WITH)
            .then(pl.concat_str([pl.col(term_col), pl.lit("%")]))
            .when(pl.col(match_type_col) == MatchType.ENDS_WITH)
            .then(pl.concat_str([pl.lit("%"), pl.col(term_col)]))
            .otherwise(None)
        )

    def _build_fts_query(
        self,
        df: pl.DataFrame,
        search_col: str = MappingColumns.search,
        term_col: str = MappingColumns.term,
        pattern_col: str = MappingColumns.pattern,
        separator="|",
    ) -> pl.DataFrame:
        """
        Build mapping table for SQL fts matching.

        """

        # ensure search column contains numbers - indexes of columns to search
        pattern_col_values = []

        for row in df.iter_rows(named=True):
            try:
                indexes = list(
                    map(lambda x: (int(x.strip())), row[search_col].split(separator))
                )
                filters = row[term_col].split(separator)
                match_query = self.build_match_query(indexes, filters)
            except (ValueError, AttributeError) as e:
                logger.warning(f"Invalid fts search term {row}, skipping. Error {e}")
                match_query = ""

            pattern_col_values.append(match_query)

        df_result = df.with_columns(pl.Series(pattern_col, pattern_col_values))
        # print(df_result[MappingColumns.pattern].to_list())
        return df_result

    def build_match_query(self, indexes: list[int], filters: list[str]) -> str:
        parts = []

        columns_list: list[str] = self.data_col_index[MappingColumns.column_name]
        for col_idx, value in zip(indexes, filters, strict=True):
            col_name = columns_list[col_idx]
            self.search_columns.add(col_name)
            # base protection
            safe_value = value.replace('"', "")

            # adding brackets to make search more controllable
            if "(" not in safe_value:
                safe_value = f'"({safe_value})"'

            parts.append(f"{col_name}:{safe_value}")
        return " ".join(parts)
