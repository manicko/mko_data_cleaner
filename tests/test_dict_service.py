import polars as pl

from mko_data_cleaner.core.dict_service import MappingDict
from mko_data_cleaner.core.models import MatchType


def test_drop_empty_columns():
    df = pl.DataFrame({"a": [1, 2], "b": [None, None]})

    result = MappingDict._drop_empty_columns(df)

    assert "a" in result.columns
    assert "b" not in result.columns


def test_group_by_cols():
    df = pl.DataFrame(
        [
            {"a": 1, "b": None},
            {"a": 2, "b": None},
            {"a": 3, "b": 4},
        ]
    )

    groups = list(MappingDict.group_by_cols(df))

    assert len(groups) == 2
    assert groups[0].height == 2


def test_like_pattern_full():
    expr = MappingDict._build_search_like_pattern("match", "term")

    df = pl.DataFrame({"match": [MatchType.FULL_MATCH], "term": ["APPLE"]})

    result = df.with_columns(expr.alias("pattern"))

    assert result["pattern"][0] == "APPLE"


def test_like_pattern_partial():
    expr = MappingDict._build_search_like_pattern("match", "term")

    df = pl.DataFrame({"match": [MatchType.PARTIAL_MATCH], "term": ["APP"]})

    result = df.with_columns(expr.alias("pattern"))

    assert result["pattern"][0] == "%APP%"
