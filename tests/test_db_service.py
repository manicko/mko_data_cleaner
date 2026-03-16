def test_create_table(db_worker):
    db_worker.create_table("test_table", "col1", "col2")

    assert db_worker.tbl_exist("test_table")


def test_add_column(db_worker):
    db_worker.create_table("test_table", "col1")
    db_worker.add_column("test_table", "col2", "TEXT")
    columns = db_worker.get_table_columns("test_table")
    assert "col2" in columns


def test_drop_table(db_worker):
    db_worker.create_table("test_table", "col1")
    assert db_worker.tbl_exist("test_table")
    db_worker.drop_table("test_table")
    assert not db_worker.tbl_exist("test_table")
