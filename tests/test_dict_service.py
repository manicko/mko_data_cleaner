import polars as pl

from mko_data_cleaner.core.dict_service import MappingDict
from mko_data_cleaner.core.models import MappingColumns, MatchType


def test_mapping_build(sample_dictionary, dict_indexes):
    """
    Проверяет сборку MappingDict + build_mapping с реальными колонками и индексами из конфига.
    """
    mapping = MappingDict(data=sample_dictionary, action_col_indexes=dict_indexes)

    # Реальные колонки из типичной Mediascope выгрузки + extra колонки
    main_columns = ["adId", "researchDate", "programName", "channelName", "brand"]
    extra_columns = ["brand_normalized"]

    mapping.build_mapping(*main_columns, extra_col_names=extra_columns)



    for df in (mapping.like_data, mapping.fts_data):
        if not df.is_empty():
            _cols = set(df.columns)
            assert MappingColumns.mapping_index in _cols
            assert MappingColumns.pattern in _cols
            assert MappingColumns.column_name in _cols
            assert MappingColumns.action in _cols
            assert MappingColumns.match in _cols
            assert MappingColumns.term in _cols

    # Проверка генерации паттернов
    if not mapping.like_data.is_empty():
        df = mapping.like_data
        patterns = df[MappingColumns.pattern].to_list()

        assert "COCA-COLA" in patterns  # FULL_MATCH → без %
        assert any(p and p.startswith("%") and p.endswith("%") for p in patterns)  # PARTIAL
        assert any(p and p.endswith("%") for p in patterns)  # STARTS_WITH
        assert df[MappingColumns.column_name].is_in(main_columns).all()




def test_pattern_generation(sample_dictionary, dict_indexes):
    """
    Проверяет корректную генерацию LIKE-паттернов после build_mapping
    с использованием реальных колонок и индексов из конфига пользователя.
    """
    mapping = MappingDict(data=sample_dictionary, action_col_indexes=dict_indexes)

    main_cols = ["adId", "researchDate", "programName", "channelName", "brand"]
    extra_cols = ["brand_normalized"]

    mapping.build_mapping(*main_cols, extra_col_names=extra_cols)

    df = mapping.like_data
    if not df.is_empty():
        patterns = df[MappingColumns.pattern].to_list()
        column_names = df[MappingColumns.column_name].to_list()

        # Основные ожидаемые паттерны
        assert "COCA-COLA" in patterns, "FULL_MATCH должен давать термин без %"
        assert "%КАНАЛ%" in patterns, "PARTIAL_MATCH должен оборачиваться в %...%"
        assert "ПЕРВЫЙ%" in patterns, "STARTS_WITH должен давать термин%"

        # Проверка, что column_name правильно сопоставлен с search индексом
        assert "channelName" in column_names
        assert "brand" in column_names


        # Паттерны должны быть в UPPERCASE (как делает код)
        for p in patterns:
            if p is not None:
                assert p == p.upper(), f"Паттерн должен быть в верхнем регистре: {p}"

        # Проверка DELETE правила — сейчас паттерн генерируется (term не пустой)
        # Если в будущем захотите None для DELETE — нужно изменить данные или логику
        delete_patterns = df.filter(pl.col(MappingColumns.action) == "d")[
            MappingColumns.pattern
        ].to_list()
        assert len(delete_patterns) == 1
        assert delete_patterns[0] is not None  # сейчас поведение такое


def test_build_search_like_pattern_real_cases(sample_dictionary, dict_indexes):
    """Проверяет генерацию LIKE-паттернов на реальных данных."""
    mapping = MappingDict(data=sample_dictionary, action_col_indexes=dict_indexes)

    mapping.build_mapping(
        "adId",
        "researchDate",
        "programName",
        "channelName",
        "brand",
        extra_col_names=["brand_normalized"],
    )

    df = mapping.like_data
    if not df.is_empty():

        # Полный матч
        full = df.filter(pl.col(MappingColumns.match) == MatchType.FULL_MATCH)
        assert full[MappingColumns.pattern].to_list() == ["COCA-COLA", "УДАЛИТЬ", "СПОНСОР"]

        # Partial / Starts
        partial_starts = df.filter(
            pl.col(MappingColumns.match).is_in(
                [MatchType.PARTIAL_MATCH, MatchType.STARTS_WITH]
            )
        )
        assert any("%" in p for p in partial_starts[MappingColumns.pattern].to_list() if p)


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
