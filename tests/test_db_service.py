def test_create_table(db_worker):
    db_worker.create_table("test_table", "col1", "col2")

    assert db_worker.tbl_exists("test_table")


def test_add_column(db_worker):
    db_worker.create_table("test_table", "col1")
    db_worker.add_column("test_table", "col2", "TEXT")
    columns = db_worker.get_table_columns("test_table")
    assert "col2" in columns


def test_drop_table(db_worker):
    db_worker.create_table("test_table", "col1")
    assert db_worker.tbl_exists("test_table")
    db_worker.drop_table("test_table")
    assert not db_worker.tbl_exists("test_table")


def test_create_table_with_index_realistic(db_worker):
    """
    Реалистичный тест создания таблиц с использованием колонок из реального конфига.
    Проверяет весь путь: set_data_tbl_columns → search_columns → create_table_with_index.
    """
    main_columns = [
        "adId",
        "researchDate",
        "programName",
        "channelName",
        "brand",
        "advertiser",
    ]
    extra_columns = ["brand_normalized", "channel_group", "sponsor"]

    db_worker.set_data_tbl_columns(*main_columns, extra_cols=extra_columns)

    # Ключевой момент: search_columns должны быть подмножеством основных колонок
    db_worker.search_columns = ["brand", "channelName", "programName"]

    db_worker.create_table_with_index()

    # Основные утверждения
    assert db_worker.tbl_exists(db_worker.data_tbl_name)
    assert db_worker.tbl_exists(db_worker._index_tbl_name)

    columns = db_worker.get_table_columns(db_worker.data_tbl_name)
    assert "adId" in columns
    assert "brand_normalized" in columns
    assert len(columns) == len(main_columns) + len(extra_columns)

    # Проверка, что индексная таблица содержит нужные колонки
    index_columns = db_worker.get_table_columns(db_worker._index_tbl_name)
    assert "adId" in index_columns
    assert "brand" in index_columns


def test_perform_query(db_worker):
    """Покрывает основной метод выполнения SQL (используется везде)."""
    db_worker.create_table("new_test_table", "col1")
    insert_q = "INSERT INTO new_test_table (col1) VALUES (?)"
    db_worker.perform_query(insert_q, ("test_value",))

    select_q = "SELECT col1 FROM new_test_table"
    cursor = db_worker.perform_query(select_q)
    result = cursor.fetchone()[0]

    assert result == "test_value"
    assert db_worker.tbl_exists("new_test_table")


def test_tbl_exist(db_worker):
    """Явный тест проверки существования таблицы."""
    db_worker.create_table("exist_table", "col1")
    assert db_worker.tbl_exists("exist_table")
    assert not db_worker.tbl_exists("non_existent_table")


def test_set_data_tbl_columns(db_worker):
    """Покрывает установку колонок и extra_columns с автоматической валидацией и очисткой имён."""
    # 1. Нормальный случай
    db_worker.set_data_tbl_columns(
        "id", "name", "value", extra_cols=["tag", "category"]
    )
    assert db_worker.data_tbl_columns == ["id", "name", "value", "tag", "category"]
    assert db_worker.extra_columns == ["tag", "category"]

    # 2. "Грязные" имена → проверяем реальное поведение clean_names + make_valid
    db_worker.set_data_tbl_columns(
        "wrong column!",  # → wrongcolumn
        "Неправильно с пробелами",  # → Nepravilnosprobelami
        "12*&&*invalid",  # → 12invalid
        "already_valid",
        extra_cols=[
            "1 starts with digit",  # → 1startswithdigit
            " leading space",  # → leadingspace
            "tag",  # → tag
            "duplicate_tag",  # → duplicate_tag (дубликат → duplicate_tag_1 не создаётся, т.к. "tag" уже есть)
        ],
    )

    # Реальный результат после clean_names
    expected_columns = [
        "wrongcolumn",
        "Nepravilnosprobelami",
        "12invalid",
        "already_valid",
        "1startswithdigit",
        "leadingspace",
        "tag",
        "duplicate_tag",
    ]

    expected_extra = ["1startswithdigit", "leadingspace", "tag", "duplicate_tag"]

    assert db_worker.data_tbl_columns == expected_columns
    assert db_worker.extra_columns == expected_extra


def test_search_columns_setter(db_worker):
    """Покрывает setter search_columns (фильтрация по существующим колонкам)."""
    db_worker.set_data_tbl_columns("col1", "col2", "col3")
    db_worker.search_columns = ["col1", "col3", "invalid_col"]
    assert db_worker.search_columns == ["col1", "col3"]


def test_column_index_and_get_col_names(db_worker):
    """Покрывает cached_property column_index и get_col_names."""
    db_worker.set_data_tbl_columns("id", "name", "value")
    assert db_worker.column_index == {0: "id", 1: "name", 2: "value"}
    assert db_worker.get_col_names(1) == "name"
    assert db_worker.get_col_names(99) is None


def test_add_columns(db_worker):
    """Покрывает массовое добавление колонок разных типов."""
    db_worker.create_table("test_table", "col1")
    db_worker.add_columns("test_table", col2="TEXT", col3="INTEGER", col4="REAL")
    columns = db_worker.get_table_columns("test_table")
    assert {"col2", "col3", "col4"}.issubset(columns)


def test_drop_tables(db_worker):
    """Покрывает массовое удаление таблиц + возврат словаря статусов."""
    db_worker.create_table("t1", "col1")
    db_worker.create_table("t2", "col1")
    result = db_worker.drop_tables("t1", "t2", "non_existent")
    assert result["t1"] is True
    assert result["t2"] is True
    assert result["non_existent"] is True
    assert not db_worker.tbl_exists("t1")
    assert not db_worker.tbl_exists("t2")


def test_get_table_columns(db_worker):
    """Покрывает получение списка колонок таблицы."""
    db_worker.create_table("test_table", "id", "name", "value")
    cols = db_worker.get_table_columns("test_table")
    assert cols == ["id", "name", "value"]


def test_create_search_table(db_worker):
    """
    Проверка create_search_table():
    - создаётся основная таблица
    - создаётся индексная таблица
    - создаётся FTS5 таблица
    """

    db_worker.set_data_tbl_columns("id", "col1", "col2", extra_cols=["extra_tag"])
    db_worker.index_column = "id"
    db_worker.search_columns = ["col1", "col2"]
    db_worker.create_table_with_index()
    db_worker.link_search_table()

    # 1️ основная таблица
    assert db_worker.tbl_exists(db_worker.data_tbl_name)

    # 2️ индексная таблица
    index_table = f"{db_worker.data_tbl_name}_distinct"

    tables = db_worker.perform_query(
        "SELECT name FROM sqlite_temp_master WHERE type='table'"
    ).fetchall()

    temp_tables = {t[0] for t in tables}

    assert index_table in temp_tables

    # 3️ FTS таблица
    fts_table = f"{index_table}_fts"

    fts_tables = db_worker.perform_query(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()

    all_tables = {t[0] for t in fts_tables}

    assert fts_table in all_tables
