import sqlite3

import polars as pl

from mko_data_cleaner.core.csv_service import CSVWorker


def test_get_files_suffix():
    assert CSVWorker.get_files_suffix("gzip") == ".csv.gz"
    assert CSVWorker.get_files_suffix("bz2") == ".csv.bz2"
    assert CSVWorker.get_files_suffix(None) == ".csv"


def test_is_date_column():
    series = pl.Series(["2024-01-01", "2024-02-01", "2024-03-01"])

    result = CSVWorker._is_date_column(series, CSVWorker.DATE_REGEX)

    assert result is True


def test_is_date_column_false_for_non_string():
    """Non-String dtype series returns False (no date parsing of integers)."""
    series = pl.Series([20240101, 20240201, 20240301], dtype=pl.Int64)

    result = CSVWorker._is_date_column(series, CSVWorker.DATE_REGEX)

    assert result is False


def test_is_date_column_false_for_all_null():
    """All-null series returns False (no dates to validate)."""
    series = pl.Series([None, None, None], dtype=pl.String)

    result = CSVWorker._is_date_column(series, CSVWorker.DATE_REGEX)

    assert result is False


def test_is_date_column_false_for_low_match_ratio():
    """Series where date match ratio is below 0.8 threshold returns False."""
    series = pl.Series(["2024-01-01", "not-a-date", "also-not-a-date"], dtype=pl.String)

    result = CSVWorker._is_date_column(series, CSVWorker.DATE_REGEX)

    assert result is False  # only 1/3 ≈ 33% < 80% threshold


def test_check_date_column_auto_detection(tmp_path):
    """check_date_column scans all columns when no column is specified
    and returns the first date-like column name.
    """
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(
        "id,date_str,other\n"
        "1,2024-01-01,hello\n"
        "2,2024-02-01,world\n"
        "3,2024-03-01,foo\n"
        "4,2024-04-01,bar\n",
        encoding="utf-8",
    )

    worker = CSVWorker.__new__(CSVWorker)
    worker.sample_data_file = csv_path
    worker.reader_settings = {"separator": ",", "has_header": True}

    # No column argument → auto-detection fallback scans all columns
    result = worker.check_date_column()
    assert result == "date_str"


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


def test_export_sql_to_csv_populates_output_files(tmp_path):
    """export_sql_to_csv appends every written file path to output_files."""
    con = sqlite3.connect(tmp_path / "test.db")
    con.execute("CREATE TABLE data_table (id INTEGER, name TEXT)")
    con.executemany(
        "INSERT INTO data_table VALUES (?, ?)",
        [(1, "Alice"), (2, "Bob")],
    )
    con.commit()

    worker = CSVWorker.__new__(CSVWorker)
    worker.export_settings = {"compression": "gzip", "chunk_size": 1}

    output_paths: list = []
    count = worker.export_sql_to_csv(
        db_con=con,
        data_table="data_table",
        export_path=tmp_path / "export",
        output_files=output_paths,
    )

    assert count == 2
    assert len(output_paths) == 2
    assert all(p.exists() for p in output_paths)
    con.close()


def test_export_sql_to_csv_roundtrip_content(tmp_path):
    """export_sql_to_csv writes CSV files whose content matches the source table.

    Verifies round-trip integrity (TST-004): with chunk_size=1, each row lands
    in its own file; reading all files back yields the original 3 rows with
    correct column headers and cell values.
    """
    con = sqlite3.connect(tmp_path / "test.db")
    con.execute("CREATE TABLE data_table (id INTEGER, name TEXT, value TEXT)")
    con.executemany(
        "INSERT INTO data_table VALUES (?, ?, ?)",
        [(1, "Alice", "tag1"), (2, "Bob", "tag2"), (3, "Charlie", None)],
    )
    con.commit()

    export_path = tmp_path / "export"
    worker = CSVWorker.__new__(CSVWorker)
    worker.export_settings = {
        "compression": "gzip",
        "chunk_size": 1,
        "separator": ";",
    }

    count = worker.export_sql_to_csv(
        db_con=con,
        data_table="data_table",
        export_path=tmp_path / "export",
    )

    assert count == 3

    # chunk_size=1 → one file per row → 3 files
    export_files = sorted(export_path.glob("*.csv.gz"))
    assert len(export_files) == 3

    # Read all chunks back and verify aggregate content
    dfs = [pl.read_csv(f, separator=";") for f in export_files]
    merged = pl.concat(dfs)
    assert merged.height == 3
    assert merged.columns == ["id", "name", "value"]

    # Names are preserved (order-agnostic across chunks)
    assert sorted(merged["name"].to_list()) == ["Alice", "Bob", "Charlie"]

    # Cell values are preserved
    value_map = dict(zip(merged["name"].to_list(), merged["value"].to_list(), strict=True))
    assert value_map["Alice"] == "tag1"
    assert value_map["Bob"] == "tag2"
    # None is written as an empty CSV field, which polars parses back as null
    assert value_map["Charlie"] is None

    con.close()


def test_get_merged_dictionary_merges_dedups_sorts(tmp_path):
    """get_merged_dictionary merges fragments, deduplicates, and sorts by
    search_column_idx (TST-010).

    Uses production-format column headers (``search_column_idx``) per OD-6
    Option C — isolated unit test without modifying the e2e fixture.
    """
    dict_dir = tmp_path / "dict"
    dict_dir.mkdir()

    # Fragment 1 — rows with search_column_idx 4 and 1
    (dict_dir / "dict_1.csv").write_text(
        "action;match;search_column_idx;value;term;tag\n"
        "d;f;4;NULL;Brand2;\n"
        "a;f;1;NULL;Apple;fruit_tag\n",
        encoding="utf-8",
    )
    # Fragment 2 — row 1 is a duplicate of fragment 1's first row; row 3 has idx 2
    (dict_dir / "dict_2.csv").write_text(
        "action;match;search_column_idx;value;term;tag\n"
        "d;f;4;NULL;Brand2;\n"
        "r;f;2;NULL;Banana;fruit_tag\n",
        encoding="utf-8",
    )

    merged_path = dict_dir / "merged_dictionary.csv"

    worker = CSVWorker.__new__(CSVWorker)
    worker.dict_path = merged_path
    worker.dict_settings = {"extension": "csv"}
    worker.reader_settings = {"separator": ";", "has_header": True}

    worker.get_merged_dictionary()

    assert merged_path.exists()

    result = pl.read_csv(merged_path, separator=";")
    # 3 unique rows (4 total − 1 duplicate)
    assert len(result) == 3
    # Sorted by search_column_idx: 1, 2, 4
    search_values = result["search_column_idx"].to_list()
    assert search_values == [1, 2, 4]


def test_get_merged_dictionary_sorts_multi_digit(tmp_path):
    """get_merged_dictionary sorts search_column_idx numerically, not lexically.

    Single-digit values (1, 2, 11, 22) verify that sort order is correct
    regardless of infer_schema_length (Int64 inference).
    """
    dict_dir = tmp_path / "dict"
    dict_dir.mkdir()

    (dict_dir / "dict_1.csv").write_text(
        "action;match;search_column_idx;value;term;tag\n"
        "a;f;11;NULL;Eleven;\n"
        "a;f;2;NULL;Two;\n",
        encoding="utf-8",
    )
    (dict_dir / "dict_2.csv").write_text(
        "action;match;search_column_idx;value;term;tag\n"
        "a;f;1;NULL;One;\n"
        "a;f;22;NULL;TwentyTwo;\n",
        encoding="utf-8",
    )

    merged_path = dict_dir / "merged_dictionary.csv"

    worker = CSVWorker.__new__(CSVWorker)
    worker.dict_path = merged_path
    worker.dict_settings = {"extension": "csv"}
    worker.reader_settings = {"separator": ";", "has_header": True}

    worker.get_merged_dictionary()

    result = pl.read_csv(merged_path, separator=";")
    search_values = result["search_column_idx"].to_list()
    assert search_values == [1, 2, 11, 22]
