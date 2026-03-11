import polars as pl

from mko_data_cleaner.core.models import DictColumnsIndexes, ActionType

import logging

logger = logging.getLogger(__name__)


class MappingDict:
    def __init__(self, data: pl.DataFrame, action_col_indexes: DictColumnsIndexes):
        self.data = data
        self.extra_col_names: list = []
        self._headers = list(self.data.columns)
        self.action_col_indexes = self._set_col_indexed(action_col_indexes)
        self._prepare_mapping()
        self._table_columns: list[str] | None = None

    @property
    def headers(self):
        return self._headers

    @staticmethod
    def _set_col_indexed(action_col_indexes: DictColumnsIndexes) -> dict[str, int]:
        return dict(sorted(action_col_indexes.model_dump().items(), key=lambda x: x[1]))

    @headers.setter
    def headers(self, *extra_cols_names: str) -> None:
        if len(extra_cols_names) == len(self.extra_col_names):
            self._headers = list(self.action_col_indexes.keys())
            self._headers += list(extra_cols_names)
            try:
                self.data.columns = self.headers.copy()
            except Exception as err:
                logger.error(err)
                raise err
    def get_search_columns(self):
        return self.data["column_name"].unique().to_list()

    @staticmethod
    def _drop_empty_columns(_df: pl.DataFrame) -> pl.DataFrame:
        return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]

    def get_data_mapping_by_action(self, action_type: ActionType | str) -> pl.DataFrame:
        try:
            return self.data.filter(pl.col('action') == action_type)
        except Exception as err:
            logger.error(err)
            raise err

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
        self.extra_col_names = df.columns[len(action_idx):]

        df.columns = action_names + self.extra_col_names

        self.data = df

    def build_mapping(self, *tbl_columns: str, extra_col_names: list[str]):
        self._table_columns = list(tbl_columns)
        # update extra_col_names with names from db
        self.extra_col_names = extra_col_names.copy()
        # add fts5_search_columns
        self.data = self._build_query(self._table_columns)

    def _build_query(
            self,
            tbl_names: list[str],
            term_col: str = "term",
            search_col: str = "search",
            match_type_col: str = "match",
            pattern_col: str = "pattern",
            column_name_col: str = "column_name",
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
        name_map_df = pl.DataFrame({
            search_col: list(range(len(tbl_names))),
            column_name_col: tbl_names
        })

        df = self.data.join(
            name_map_df,
            on=search_col,
            how="left"
        )

        pattern_expr = self._build_search_like_pattern(
            match_type_col,
            term_col
        )

        df = df.with_columns(
            pattern_expr.str.to_uppercase().alias(pattern_col)
        )

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
                pl.when(pl.col(match_type_col) == "f")
                .then(pl.col(term_col))

                .when(pl.col(match_type_col) == "p")
                .then(pl.concat_str([pl.lit("%"), pl.col(term_col), pl.lit("%")]))

                .when(pl.col(match_type_col) == "s")
                .then(pl.concat_str([pl.col(term_col), pl.lit("%")]))

                .when(pl.col(match_type_col) == "e")
                .then(pl.concat_str([pl.lit("%"), pl.col(term_col)]))

                .otherwise(None)
            )