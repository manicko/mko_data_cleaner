import importlib.util
import os
import subprocess
import sys
import textwrap
from pathlib import Path

from mko_data_cleaner.core.db_service import DBWorker


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


def test_set_data_tbl_columns_empty_extra_cols(db_worker):
    """Empty extra_cols must set extra_columns to [], not all data columns.

    Regression test for the ``[-0:]`` Python gotcha: when ``extra_cols=[]``
    the slice ``[-len([]) :]`` evaluates to ``[-0 :]`` which is ``[0 :]``
    returning the entire column list. The fix guards the slice so an empty
    extra_cols yields an empty extra_columns.
    """
    db_worker.set_data_tbl_columns("id", "name", "value", extra_cols=[])
    assert db_worker.data_tbl_columns == ["id", "name", "value"]
    assert db_worker.extra_columns == []


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


def test_fts5_triggers_sync_after_mutation(db_worker):
    """FTS5 triggers must keep the virtual table in sync after INSERT/UPDATE/DELETE.

    Extends test_create_search_table (TST-005): instead of only checking
    FTS table existence, this test mutates the source (index) table and
    verifies via MATCH queries that the FTS index reflects inserts,
    updates, and deletes.
    """
    db_worker.set_data_tbl_columns("id", "col1", "col2", extra_cols=["extra_tag"])
    db_worker.index_column = "id"
    db_worker.search_columns = ["col1", "col2"]
    db_worker.create_table_with_index()
    db_worker.link_search_table()

    target = db_worker._index_tbl_name
    fts_table = db_worker._fts_table_name
    assert fts_table is not None

    insert_q = f"INSERT INTO {target} (id, col1, col2, extra_tag) VALUES (?, ?, ?, ?)"

    # 1. INSERT — FTS should reflect it
    db_worker.perform_query(insert_q, ("r1", "hello", "foo", None))
    result = db_worker.perform_query(
        f"SELECT rowid FROM {fts_table} WHERE {fts_table} MATCH 'hello'"
    ).fetchall()
    assert len(result) == 1

    # 2. UPDATE searchable column — old value removed, new value indexed
    db_worker.perform_query(
        f"UPDATE {target} SET col1 = ? WHERE id = ?",
        ("goodbye", "r1"),
    )
    result = db_worker.perform_query(
        f"SELECT rowid FROM {fts_table} WHERE {fts_table} MATCH 'hello'"
    ).fetchall()
    assert len(result) == 0
    result = db_worker.perform_query(
        f"SELECT rowid FROM {fts_table} WHERE {fts_table} MATCH 'goodbye'"
    ).fetchall()
    assert len(result) == 1

    # 3. DELETE — FTS entry removed
    db_worker.perform_query(f"DELETE FROM {target} WHERE id = ?", ("r1",))
    result = db_worker.perform_query(
        f"SELECT rowid FROM {fts_table} WHERE {fts_table} MATCH 'goodbye'"
    ).fetchall()
    assert len(result) == 0


# --------------------------------------------------------------------------- #
# PyArrow installation guard (robustness for the ADBC write path)
# --------------------------------------------------------------------------- #
def test_pyarrow_guard_noop_when_absent(tmp_path: Path, monkeypatch):
    """Guard is a no-op when find_spec('pyarrow') returns None — DBWorker succeeds.

    Simulates an environment where pyarrow is fully absent. The ADBC SQLite
    write path operates without pyarrow (verified on polars >= 1.38.1), so
    DBWorker must construct normally.
    """
    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        if name == "pyarrow":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    db_file = tmp_path / "noop_test.db"
    worker = DBWorker(db_file=db_file, tbl_name="data_table", index_column="id")
    assert worker.db_adb_con is not None
    worker.close()


def test_pyarrow_broken_guard_raises_actionable_error(tmp_path: Path):
    """Guard raises PyArrowInstallationError when pyarrow spec exists but import fails.

    Simulates a broken/partial pyarrow installation by placing a fake
    ``pyarrow.py`` on PYTHONPATH that raises ImportError on import. Follows
    the subprocess-isolation pattern from tests/test_app.py.
    """
    fake_site = tmp_path / "fake_pyarrow_site"
    fake_site.mkdir()
    (fake_site / "pyarrow.py").write_text(
        "raise ImportError('broken native module')\n",
        encoding="utf-8",
    )

    db_file = tmp_path / "guard_test.db"

    script = textwrap.dedent(
        """\
        import sys
        from mko_data_cleaner.core.db_service import DBWorker
        from mko_data_cleaner.core.errors import PyArrowInstallationError

        try:
            DBWorker(db_file=sys.argv[1], tbl_name="data_table", index_column="id")
            sys.stderr.write("FAIL: guard did not fire\\n")
            sys.exit(1)
        except PyArrowInstallationError as exc:
            sys.stdout.write(
                "GUARD_OK: PyArrowInstallationError: " + str(exc) + "\\n"
            )
            sys.exit(0)
        except Exception as exc:
            sys.stderr.write(
                "FAIL: wrong exception "
                + type(exc).__name__
                + ": "
                + str(exc)
                + "\\n"
            )
            sys.exit(1)
        """
    )

    env = os.environ.copy()
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(fake_site) + (os.pathsep + existing_pp if existing_pp else "")
    )

    result = subprocess.run(
        [sys.executable, "-c", script, str(db_file)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, (
        f"subprocess failed (rc={result.returncode}):\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    assert "GUARD_OK" in result.stdout
    assert "PyArrowInstallationError" in result.stdout
    assert "--force-reinstall" in result.stdout


def test_apply_delete_executes_delete_once(db_worker, mocker):
    """``_apply_delete`` must run the DELETE statement exactly once.

    Regression test for the duplicated ``perform_query`` call that previously
    executed the identical DELETE a second time — dead work because the matched
    rows were already removed by the first call.
    """
    # --- arrange: target table with at least two rows ---
    db_worker.create_table(db_worker.data_tbl_name, "adId", "brand", "sponsor")

    target = db_worker.target_table

    insert_q = f"INSERT INTO {target} (adId, brand, sponsor) VALUES (?, ?, ?)"
    for ad_id in ("ad1", "ad2", "ad3"):
        db_worker.perform_query(insert_q, (ad_id, "BrandX", None))

    rowids = [
        r[0]
        for r in db_worker.perform_query(f"SELECT rowid FROM {target}").fetchall()
    ]
    assert len(rowids) == 3

    # TEMP joined_matches_table referencing only the first inserted row.
    db_worker.perform_query(
        f"CREATE TEMP TABLE {db_worker._joined_matches_table} (data_rowid INTEGER)"
    )
    db_worker.perform_query(
        f"INSERT INTO {db_worker._joined_matches_table} (data_rowid) VALUES (?)",
        (rowids[0],),
    )

    # --- act: spy BEFORE the call-under-test so only its query is recorded ---
    spy = mocker.spy(db_worker, "perform_query")

    db_worker._apply_delete()

    # --- assert: single execution ---
    assert spy.call_count == 1

    executed_sql = spy.call_args_list[0].args[0]
    assert "DELETE FROM" in executed_sql
    assert target in executed_sql

    # --- assert: matched row gone, unmatched rows remain ---
    remaining = [
        r[0]
        for r in db_worker.db_con.execute(f"SELECT rowid FROM {target}").fetchall()
    ]
    assert rowids[0] not in remaining
    assert len(remaining) == 2


def test_apply_replace_picks_highest_mapping_index(db_worker, mocker):
    """``_apply_replace`` must apply the highest mapping_index rule when
    multiple REPLACE rules match the same data row (TST-003).

    Modeled on ``test_apply_delete_executes_delete_once``: sets up a target table
    with rows and a TEMP joined_matches_table referencing them, spies on
    ``perform_query``, calls the private method, and asserts on the SQL and
    observable outcome.
    """
    # --- arrange: target table with at least three rows ---
    db_worker.set_data_tbl_columns("adId", "brand", extra_cols=["sponsor"])
    db_worker.create_table(db_worker.data_tbl_name, "adId", "brand", "sponsor")
    db_worker.search_columns = ["brand"]

    target = db_worker.target_table

    insert_q = f"INSERT INTO {target} (adId, brand, sponsor) VALUES (?, ?, ?)"
    for ad_id in ("ad1", "ad2", "ad3"):
        db_worker.perform_query(insert_q, (ad_id, "BrandX", None))

    rowids = [
        r[0]
        for r in db_worker.perform_query(f"SELECT rowid FROM {target}").fetchall()
    ]
    assert len(rowids) == 3

    # TEMP joined_matches_table with 2 entries for the same rowid
    # but different mapping_index values (1 and 2). Highest must win.
    db_worker.perform_query(
        f"CREATE TEMP TABLE {db_worker._joined_matches_table} "
        f"(data_rowid INTEGER, mapping_index INTEGER, sponsor TEXT)"
    )
    db_worker.perform_query(
        f"INSERT INTO {db_worker._joined_matches_table} "
        f"(data_rowid, mapping_index, sponsor) VALUES (?, ?, ?)",
        (rowids[0], 2, "FromRule2"),
    )
    db_worker.perform_query(
        f"INSERT INTO {db_worker._joined_matches_table} "
        f"(data_rowid, mapping_index, sponsor) VALUES (?, ?, ?)",
        (rowids[0], 1, "FromRule1"),
    )

    # --- act: spy BEFORE the call-under-test so only its query is recorded ---
    spy = mocker.spy(db_worker, "perform_query")

    db_worker._apply_replace(column_names=["sponsor"])

    # --- assert: single UPDATE execution ---
    assert spy.call_count == 1

    executed_sql = spy.call_args_list[0].args[0]
    assert "UPDATE" in executed_sql
    assert target in executed_sql

    # --- assert: highest mapping_index (2) wins ---
    result = db_worker.db_con.execute(
        f"SELECT sponsor FROM {target} WHERE rowid = ?",
        (rowids[0],),
    ).fetchone()
    assert result[0] == "FromRule2"

    # --- assert: unmatched rows are untouched ---
    for rid in rowids[1:]:
        result = db_worker.db_con.execute(
            f"SELECT sponsor FROM {target} WHERE rowid = ?",
            (rid,),
        ).fetchone()
        assert result[0] is None


# --------------------------------------------------------------------------- #
# build_non_mapped — NULL-detection via SQLite bitwise '|'  (TST-016, TST-006)
# --------------------------------------------------------------------------- #
def test_build_non_mapped_with_extra_cols(db_worker, extra_cols):
    """build_non_mapped selects rows where the extra column is NULL.

    Uses the ``extra_cols`` fixture (['tag']) to wire the previously-unused
    fixture into a real test path (TST-016).
    """
    # 'tag' is passed only as an extra column — set_data_tbl_columns must
    # not receive it twice or clean_names would rename the duplicate to tag_1.
    db_worker.set_data_tbl_columns("id", "name", extra_cols=extra_cols)
    db_worker.index_column = "id"
    db_worker.search_columns = ["name"]
    db_worker.create_table_with_index()

    # After create_table_with_index the index table (data_table_distinct)
    # is the active target_table, so we insert rows directly into it.
    target = db_worker.target_table
    insert_q = f"INSERT INTO {target} (id, name, tag) VALUES (?, ?, ?)"
    db_worker.perform_query(insert_q, ("id1", "Name1", None))
    db_worker.perform_query(insert_q, ("id2", "Name2", "tag_value"))

    db_worker.build_non_mapped()

    rows = db_worker.perform_query(
        f"SELECT name FROM {db_worker.non_mapped_table}"
    ).fetchall()
    names = [r[0] for r in rows]
    assert "Name1" in names  # tag is NULL → selected
    assert "Name2" not in names  # tag has value → not selected


def test_build_non_mapped_multi_extra_cols(db_worker):
    """build_non_mapped with 2+ extra columns selects rows where ANY is NULL.

    SQLite's ``|`` (bitwise OR) propagates NULL: ``tag | grp IS NULL`` is
    True when either column is NULL.  This verifies the multi-extra-column
    branch (TST-006 coverage gap).
    """
    db_worker.set_data_tbl_columns("id", "name", extra_cols=["tag", "grp"])
    db_worker.index_column = "id"
    db_worker.search_columns = ["name"]
    db_worker.create_table_with_index()

    target = db_worker.target_table
    insert_q = f"INSERT INTO {target} (id, name, tag, grp) VALUES (?, ?, ?, ?)"
    db_worker.perform_query(insert_q, ("id1", "Name1", None, "group_a"))
    db_worker.perform_query(insert_q, ("id2", "Name2", "tag_value", None))
    db_worker.perform_query(insert_q, ("id3", "Name3", "tag_value", "group_b"))

    db_worker.build_non_mapped()

    rows = db_worker.perform_query(
        f"SELECT name FROM {db_worker.non_mapped_table}"
    ).fetchall()
    names = [r[0] for r in rows]
    assert "Name1" in names  # tag NULL → selected
    assert "Name2" in names  # grp NULL → selected
    assert "Name3" not in names  # both set → not selected
