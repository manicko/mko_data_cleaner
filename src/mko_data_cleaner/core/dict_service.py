import pandas as pd
from mko_data_cleaner.core.models import DictColumnsIndexes
from functools import cached_property
import logging
from time import time
import traceback

logger = logging.getLogger(__name__)


class MappingDict:
    def __init__(self, data: pd.DataFrame, col_indexes: DictColumnsIndexes, source_headers: list[str]):
        self.data = data
        self.col_indexes = col_indexes.model_copy()
        self.source_headers = source_headers.copy()

    @cached_property
    def extra_columns_list(self) -> list[str]:
        """Returns list of column names presented after term column by index."""

        start_idx = self.col_indexes.term
        candidate_columns = self.data.columns[start_idx + 1:]

        non_empty_columns = [
            col for col in candidate_columns
            if self.data[col].notna().any()
        ]
        return non_empty_columns

    @cached_property
    def source_search_col_indexes(self) -> list[int]:
        column_index = self.col_indexes.search
        series = self.data.iloc[:, column_index].dropna().astype(str).str.strip()
        try:
            int_series = series.astype(int)
        except Exception as e:
            raise ValueError(
                f"Column '{column_index}' contains non-integer values"
            ) from e
        return list(set(map(int, int_series.unique())))

    @cached_property
    def search_columns_list(self) -> list[str]:
        return [self.source_headers[i] for i in self.source_search_col_indexes]

    def get_action_params(
            self,
            clean_cols_ids: dict[str, int],
            search_column_index: dict[int, str]
    ):
        """
        Generates structured cleaning rules.

        Returns:
            Generator[dict]: {
                "action": str,
                "match_expr": str,
                "search_column": str,
                "columns": list[tuple[str, str]]
            }
        """

        try:
            print("Data cleaning rules parsing: [", end="")
            start_time = time()
            match_expr = ''
            for _, row in self.data.iterrows():

                action = str(row.iloc[self.col_indexes.action])
                match_type = str(row.iloc[self.col_indexes.match])
                search_idx = int(row.iloc[self.col_indexes.search])
                term = str(row.iloc[self.col_indexes.term])

                if action not in {"a", "r", "d"}:
                    continue

                if match_type not in {"f", "p", "s", "e"}:
                    continue

                search_column = search_column_index[search_idx]

                # ---- FTS MATCH expression generation ----
                match match_type:
                    case "f":  # full match
                        match_expr = f'"{term}"'
                    case "p":  # partial
                        match_expr = term
                    case "s":  # starts with
                        match_expr = f'"{term}*"'
                    case "e":  # ends with
                        match_expr = f'"*{term}"'
                    case _:
                        continue

                # ---- Collect all non-null target columns ----
                target_columns = []

                for col_name in clean_cols_ids.keys():
                    value = row.get(col_name)
                    if pd.notna(value):
                        target_columns.append((col_name, str(value)))

                if not target_columns or len(match_expr) == 0:
                    continue

                yield {
                    "action": action,
                    "match_expr": match_expr,
                    "search_column": search_column,
                    "columns": target_columns
                }

                print("==", end="")

        except Exception:
            logging.error(traceback.format_exc())
            raise

        else:
            print("]")
            print(
                f"Rules parsing finished. "
                f"Elapsed time: {time() - start_time:.2f} seconds"
            )
