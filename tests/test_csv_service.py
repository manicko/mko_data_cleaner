import sqlite3

import polars as pl

from mko_data_cleaner.core.csv_service import CSVWorker


def test_get_files_suffix():
    assert CSVWorker.get_files_suffix("gzip") == ".csv.gz"
    assert CSVWorker.get_files_suffix("bz2") == ".csv.bz2"
    assert CSVWorker.get_files_suffix(None) == ".csv"


def test_is_date_column():
    series = pl.Series(["2024-01-01", "2024-02-01", "2024-03-01"])

    result = CSVWorker._is_date_column(series, r"^\d{4}[-/.]\d{2}[-/.]\d{2}$")

    assert result is True


def test_get_file_name(tmp_path):

    worker = CSVWorker.__new__(CSVWorker)

    worker.export_settings = {"compression": "gzip"}

    name = worker.get_file_name("prefix", "1")

    assert name.startswith("prefix_")
    assert name.endswith(".csv.gz")


def test_export_sql_to_csv_returns_row_count(tmp_path):
    """export_sql_to_csv returns the row count as int."""
    con = sqlite3.connect(tmp_path / "test.db")
    con.execute("CREATE TABLE data_table (id INTEGER, name TEXT, value TEXT)")
    con.executemany(
        "INSERT INTO data_table VALUES (?, ?, ?)",
        [(1, "Alice", "tag1"), (2, "Bob", "tag2"), (3, "Charlie", None)],
    )
    con.commit()

    worker = CSVWorker.__new__(CSVWorker)
    worker.export_settings = {"compression": "gzip", "chunk_size": 10000}

    count = worker.export_sql_to_csv(
        db_con=con,
        data_table="data_table",
        export_path=tmp_path / "export",
    )

    assert isinstance(count, int)
    assert count == 3
    con.close()


def test_export_sql_to_csv_returns_zero_for_empty_table(tmp_path):
    """export_sql_to_csv returns 0 when the table has no rows."""
    con = sqlite3.connect(tmp_path / "test.db")
    con.execute("CREATE TABLE empty_table (id INTEGER)")
    con.commit()

    worker = CSVWorker.__new__(CSVWorker)
    worker.export_settings = {"compression": "gzip"}

    count = worker.export_sql_to_csv(
        db_con=con,
        data_table="empty_table",
        export_path=tmp_path / "export",
    )

    assert isinstance(count, int)
    assert count == 0
    con.close()
