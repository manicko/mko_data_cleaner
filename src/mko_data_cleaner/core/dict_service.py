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
        self.extra_col_names: list = []
        self.action_col_indexes = self._set_col_indexed(action_col_indexes)
        self._prepare_mapping()
        self._table_columns: list[str] | None = None

    @staticmethod
    def _set_col_indexed(action_col_indexes: DictColumnsIndexes) -> dict[str, int]:
        return dict(sorted(action_col_indexes.model_dump().items(), key=lambda x: x[1]))

    def get_search_columns(self):
        return self.data[MappingColumns.column_name].unique().to_list()

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
            MappingColumns.column_name,
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

    def _prepare_mapping(self):
        # First we extract the action and nonempty data mapping columns:
        action_names = list(self.action_col_indexes.keys())
        action_idx = list(self.action_col_indexes.values())
        data_start_idx = max(action_idx) + 1

        # ----- all cols after max action index are used as data mapping columns
        col_candidates = action_idx + list(range(data_start_idx, self.data.width))
        df = self.data[:, col_candidates]

        # ------ keep only non-empty columns
        df = self._drop_empty_columns(df)
        self.extra_col_names = df.columns[len(action_idx) :]

        df.columns = action_names + self.extra_col_names

        self.data = df

    def build_mapping(self, *tbl_columns: str, extra_col_names: list[str]):
        self._table_columns = list(tbl_columns)
        # update extra_col_names with names from db
        self.extra_col_names = extra_col_names.copy()
        self.data.columns = (
            self.data.columns[: -len(self.extra_col_names)] + self.extra_col_names
        )
        # add fts5_search_columns
        self.data = self._build_query(self._table_columns)
        self.data = self.data.with_row_index(MappingColumns.mapping_index, offset=1)

    def _build_query(
        self,
        tbl_names: list[str],
        term_col: str = MappingColumns.term,
        search_col: str = MappingColumns.search,
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

        # map column index → column name
        name_map_df = pl.DataFrame(
            {search_col: list(range(len(tbl_names))), column_name_col: tbl_names}
        )

        # ensure search column contains numbers - indexes of columns to search
        self.data.with_columns(pl.col(search_col).cast(pl.Int64))

        df = self.data.join(name_map_df, on=search_col, how="left")

        pattern_expr = self._build_search_like_pattern(match_type_col, term_col)

        df = df.with_columns(pattern_expr.str.to_uppercase().alias(pattern_col))

        return df

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
