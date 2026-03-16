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
