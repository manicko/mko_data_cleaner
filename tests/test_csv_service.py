import gzip
import sqlite3

import polars as pl
import pytest

from mko_data_cleaner.core.csv_service import CSVWorker
from mko_data_cleaner.core.errors import DictionaryStructureError, WrongDataSettings


def test_get_files_suffix():
    # Existing assertions
    assert CSVWorker.get_files_suffix("gzip") == ".csv.gz"
    assert CSVWorker.get_files_suffix("bz2") == ".csv.bz2"
    assert CSVWorker.get_files_suffix(None) == ".csv"
    # Pre-match branches
    assert CSVWorker.get_files_suffix("infer") == ".csv"  # line 245
    assert (
        CSVWorker.get_files_suffix({"method": "gzip"}) == ".csv.gz"
    )  # line 247 (dict-form)
    # Match cases
    assert CSVWorker.get_files_suffix("xz") == ".csv.xz"  # line 256
    assert CSVWorker.get_files_suffix("zip") == ".csv.zip"  # line 258
    assert CSVWorker.get_files_suffix("zstd") == ".csv.zst"  # line 260
    assert CSVWorker.get_files_suffix("gz") == ".csv.gz"  # alias for gzip
    # Default fallback
    assert CSVWorker.get_files_suffix("unknown") == ".csv"  # line 262
    assert CSVWorker.get_files_suffix("csv") == ".csv"  # default
    assert CSVWorker.get_files_suffix("json") == ".csv"  # default
    assert CSVWorker.get_files_suffix("parquet") == ".csv"  # default


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
    value_map = dict(
        zip(merged["name"].to_list(), merged["value"].to_list(), strict=True)
    )
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


def test_export_sql_to_csv_gzip(tmp_path):
    """export_sql_to_csv with gzip compression produces valid .csv.gz files."""
    con = sqlite3.connect(tmp_path / "test.db")
    con.execute("CREATE TABLE data_table (id INTEGER, name TEXT, value TEXT)")
    con.executemany(
        "INSERT INTO data_table VALUES (?, ?, ?)",
        [(1, "Alice", "tag1"), (2, "Bob", "tag2"), (3, "Charlie", None)],
    )
    con.commit()

    worker = CSVWorker.__new__(CSVWorker)
    worker.export_settings = {
        "compression": "gzip",
        "chunk_size": 1,
        "separator": ";",
    }

    output_paths: list = []
    count = worker.export_sql_to_csv(
        db_con=con,
        data_table="data_table",
        export_path=tmp_path / "export",
        output_files=output_paths,
    )

    assert count == 3
    assert len(output_paths) == 3

    # Verify gzip-specific format: .csv.gz extension AND valid gzip binary
    for f in output_paths:
        assert str(f).endswith(".csv.gz")
        with gzip.open(f, "rt", encoding="utf-8") as gz:
            content = gz.read()
            assert len(content) > 0

    # Verify content round-trip (null handling preserved)
    dfs = [pl.read_csv(f, separator=";") for f in output_paths]
    merged = pl.concat(dfs)
    assert merged.height == 3
    assert merged.columns == ["id", "name", "value"]
    value_map = dict(
        zip(merged["name"].to_list(), merged["value"].to_list(), strict=True)
    )
    assert value_map["Alice"] == "tag1"
    assert value_map["Bob"] == "tag2"
    assert value_map["Charlie"] is None
    con.close()


def test_import_csv_no_delimiter_raises(tmp_path):
    """CSVWorker raises WrongDataSettings when no files with the specified extension are found."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # Create a file with wrong extension — triggers "No files found" path
    (data_dir / "not_csv.txt").write_text("id,name\n1,Alice\n")

    data_settings = {"extension": "gz"}  # looking for .gz files
    with pytest.raises(WrongDataSettings, match="No files with extension"):
        CSVWorker(
            data_path=data_dir,
            data_settings=data_settings,
            reader_settings={},
            dict_path=tmp_path / "dict.csv",
            dict_settings={},
            export_path=tmp_path / "export",
            export_settings={},
        )


def test_get_dictionary_separator_mismatch_raises(tmp_path):
    """get_dictionary raises DictionaryStructureError when the dictionary CSV
    uses a separator that doesn't match the config (Polars reads it as 1 column).
    """
    dict_path = tmp_path / "dict" / "merged_dictionary.csv"
    dict_path.parent.mkdir(parents=True)
    # Comma-separated dictionary while config expects ';'
    dict_path.write_text(
        "action,match,search_column_idx,value,term,tag\n"
        "d,f,4,NULL,Brand2,\n",
        encoding="utf-8",
    )

    worker = CSVWorker.__new__(CSVWorker)
    worker.dict_path = dict_path
    worker.reader_settings = {"separator": ";", "has_header": True}
    worker.dict_settings = {
        "col_indexes": {"action": 0, "match": 1, "search": 2, "term": 4}
    }

    with pytest.raises(DictionaryStructureError) as exc_info:
        worker.get_dictionary()

    msg = str(exc_info.value)
    assert "merged_dictionary.csv" in msg
    assert "separator" in msg
    assert "reading dictionary" in msg


def test_set_files_params_sorts_data_files(tmp_path, monkeypatch):
    """``data_files`` is sorted by path regardless of discovery order.

    De-duplication keeps the first occurrence in input order, so the file order
    must be deterministic and not depend on the filesystem-dependent glob order.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for name in ("zeta.csv", "alpha.csv", "middle.csv"):
        (data_dir / name).write_text("AdId;YEAR\n1;2024\n", encoding="utf-8")

    unsorted_files = [
        data_dir / "zeta.csv",
        data_dir / "alpha.csv",
        data_dir / "middle.csv",
    ]
    monkeypatch.setattr(
        "mko_data_cleaner.core.csv_service.list_files_in_directory",
        lambda *args, **kwargs: unsorted_files,
    )

    worker = CSVWorker.__new__(CSVWorker)
    worker.data_settings = {"extension": "csv"}
    worker.reader_settings = {"separator": ";", "has_header": True}
    worker.data_path = data_dir

    worker._set_files_params()

    assert worker.data_files == sorted(unsorted_files)


def _make_dedup_worker(data_files: list, reader_settings: dict) -> CSVWorker:
    """Build a CSVWorker instance without running ``__init__`` for unit tests."""
    worker = CSVWorker.__new__(CSVWorker)
    worker.data_files = sorted(data_files)
    worker.reader_settings = reader_settings
    return worker


def test_load_deduplicated_removes_cross_file_duplicate(tmp_path):
    """Two files sharing one row collapse to one; the first occurrence survives."""
    file_a = tmp_path / "a.csv"
    file_a.write_text(
        "AdId;YEAR\n1;2024\n2;2024\n",
        encoding="utf-8",
    )
    file_b = tmp_path / "b.csv"
    file_b.write_text(
        "AdId;YEAR\n2;2024\n3;2025\n",
        encoding="utf-8",
    )

    worker = _make_dedup_worker(
        [file_a, file_b], {"separator": ";", "has_header": True}
    )

    df, rows_read, rows_removed = worker.load_deduplicated(["AdId", "YEAR"])

    assert rows_read == 4
    assert rows_removed == 1
    assert df.height == 3
    # First occurrence (from file_a) survives, order preserved.
    assert df["AdId"].to_list() == [1, 2, 3]


def test_load_deduplicated_removes_duplicate_beyond_chunk_size(tmp_path, monkeypatch):
    """A duplicate separated by more than CHUNK_SIZE rows is still removed.

    ``load_deduplicated`` builds one global plan, so chunk size is irrelevant to
    the result; the test pins that behaviour at the primitive level (D3).
    """
    monkeypatch.setattr(CSVWorker, "CHUNK_SIZE", 2)

    rows = [f"{i};v{i}" for i in range(1, 9)]  # 8 unique rows
    rows.append("3;v3")  # duplicate of row index 2, far beyond chunk size
    file_a = tmp_path / "a.csv"
    file_a.write_text("AdId;VAL\n" + "\n".join(rows) + "\n", encoding="utf-8")

    worker = _make_dedup_worker([file_a], {"separator": ";", "has_header": True})

    df, rows_read, rows_removed = worker.load_deduplicated(["AdId", "VAL"])

    assert rows_read == 9
    assert rows_removed == 1
    assert df.height == 8
    assert df["AdId"].to_list() == list(range(1, 9))


def test_load_deduplicated_null_differs_from_empty_string(tmp_path):
    """``null`` and ``""`` are distinct values and do not collapse (D11b)."""
    file_a = tmp_path / "a.csv"
    file_a.write_text(
        'AdId;VAL\n1;\n2;""\n',
        encoding="utf-8",
    )

    worker = _make_dedup_worker([file_a], {"separator": ";", "has_header": True})

    df, rows_read, rows_removed = worker.load_deduplicated(["AdId", "VAL"])

    assert rows_read == 2
    assert rows_removed == 0
    assert df.height == 2
    assert df["VAL"].null_count() == 1
    assert df["VAL"].to_list() == [None, ""]


def test_load_deduplicated_is_case_sensitive(tmp_path):
    """Rows differing only in letter case are not duplicates (D11c)."""
    file_a = tmp_path / "a.csv"
    file_a.write_text(
        "AdId;VAL\n1;Apple\n2;apple\n",
        encoding="utf-8",
    )

    worker = _make_dedup_worker([file_a], {"separator": ";", "has_header": True})

    df, rows_read, rows_removed = worker.load_deduplicated(["AdId", "VAL"])

    assert rows_read == 2
    assert rows_removed == 0
    assert df.height == 2


def test_load_deduplicated_collapses_identical_null_rows(tmp_path):
    """Rows identical across all columns including a null value collapse (D11a)."""
    file_a = tmp_path / "a.csv"
    file_a.write_text(
        "AdId;VAL\n1;\n1;\n",
        encoding="utf-8",
    )

    worker = _make_dedup_worker([file_a], {"separator": ";", "has_header": True})

    df, rows_read, rows_removed = worker.load_deduplicated(["AdId", "VAL"])

    assert rows_read == 2
    assert rows_removed == 1
    assert df.height == 1


def test_load_deduplicated_header_only_returns_empty_frame(tmp_path):
    """Header-only / zero-row input yields an empty frame and (0, 0)."""
    file_a = tmp_path / "a.csv"
    file_a.write_text("AdId;YEAR\n", encoding="utf-8")

    worker = _make_dedup_worker([file_a], {"separator": ";", "has_header": True})

    df, rows_read, rows_removed = worker.load_deduplicated(["AdId", "YEAR"])

    assert df.is_empty()
    assert rows_read == 0
    assert rows_removed == 0
