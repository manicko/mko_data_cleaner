import gzip
from pathlib import Path

import polars as pl
import pytest

from mko_data_cleaner.core.db_service import DBWorker
from mko_data_cleaner.core.models import DictColumnsIndexes


@pytest.fixture
def realistic_config():
    return {
        "data_paths": {
            "import_folder": "raw_data",
            "export_folder": "clean_data",
            "dict_file": "dict/merged_dictionary.csv",
            "db_file": "data_base/db_example.db",
        },
        "data_file_settings": {
            "extension": "gz",
            "index_column": "adId",
            "date_column": "researchDate",
        },
        "dict_file_settings": {
            "extension": "gz",
            "add_separator": ", ",
            "col_indexes": {"action": 0, "match": 1, "search": 2, "term": 4},
        },
        "database_settings": {"table_name": "data_table"},
    }


@pytest.fixture
def db_worker(tmp_path: Path, realistic_config):

    db_file = tmp_path / "test.db"

    with DBWorker(
        db_file=db_file,
        tbl_name=realistic_config["database_settings"]["table_name"],
        index_column=realistic_config["data_file_settings"]["index_column"],
        date_column=realistic_config["data_file_settings"]["date_column"],
    ) as worker:
        yield worker


@pytest.fixture
def dict_indexes(realistic_config):
    return DictColumnsIndexes(**realistic_config["dict_file_settings"]["col_indexes"])


@pytest.fixture
def sample_dictionary():
    """Realistic dictionary fixture for tests (matches user config col_indexes)."""
    data = [
        ["a", "f", 3, None, "Coca-Cola", "brand"],
        ["r", "p", 1, None, "канал", "channelName"],
        ["r", "s", 1, None, "Первый", "channelName"],
        ["d", "f", 5, None, "Удалить", None],
        ["a", "f", 4, None, "Спонсор", "sponsor_tag"],
        ["a", "fts", "0|1", None, "Apple|Company", "fruit_tag"],
    ]

    return pl.DataFrame(
        data,
        schema={
            "action": pl.String,
            "match": pl.String,
            "search": pl.String,  # search column index
            "extra_col": pl.String,
            "term": pl.String,
            "tag": pl.String,
        },
        orient="row",
    )


@pytest.fixture
def extra_cols():
    """List of extra column names to pass to ``set_data_tbl_columns``.

    Used by ``test_build_non_mapped_with_extra_cols`` to wire this fixture
    into a real test path (TST-016).
    """
    return ["tag"]


@pytest.fixture
def sample_mapping_df():
    """Reserved for future integration tests of ``group_by_cols``.

    No current consumer (TST-016).
    """
    return pl.DataFrame(
        {
            "action": ["a", "r", "d"],
            "match": ["f", "p", "s"],
            "search": [0, 1, 0],
            "term": ["Apple", "Sam", "Test"],
            "tag": ["fruit", "brand", "delete"],
        }
    )


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Path to a temporary sqlite database file.

    Reserved for future integration tests. No current consumer (TST-016).
    """
    return tmp_path / "test_database.db"


@pytest.fixture
def db_worker_memory(tmp_path):
    """DBWorker backed by an on-disk sqlite file (``tmp_path``).

    Reserved for future integration tests that need an isolated worker.
    No current consumer (TST-016).
    """

    db_file = tmp_path / "memory.db"

    with DBWorker(db_file=db_file, tbl_name="data_table", index_column="id") as worker:
        yield worker


@pytest.fixture
def synthetic_report(tmp_path: Path) -> Path:
    """Create a tiny synthetic report directory for end-to-end tests.

    Mirrors the e2e scenario in scripts/smoke_install.py (lines 112-160)
    and tests/test_app.py::test_process_data_end_to_end. When either the
    data or the assertions change, update ALL copies:
      1. scripts/smoke_install.py (maintenance comment)
      2. tests/test_app.py::test_process_data_end_to_end
      3. tests/test_app.py::test_null_data_files_excludes_stale
    Shared assertions: rows_loaded==3, rows_exported==2,
    export_dir/import_dir/report_dir resolved, export dir exists with
    .gz files, null_data_files is list, duration>0, timings keys.
    """
    report_dir = tmp_path / "report"
    raw_data_dir = report_dir / "raw_data"
    dict_dir = report_dir / "dict"
    raw_data_dir.mkdir(parents=True)
    dict_dir.mkdir(parents=True)

    csv_lines = [
        "AdId;researchDate;YEAR;MONTH;BRAND",
        "id1;2024-01-01;2024;01;Brand1",
        "id2;2024-01-15;2024;01;Brand2",
        "id3;2024-02-01;2024;02;Brand1",
    ]
    with gzip.open(raw_data_dir / "sample.csv.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(csv_lines) + "\n")

    dict_lines = [
        "action;match;search;value;term;tag",
        "d;f;4;NULL;Brand2;",
        "a;f;4;NULL;Brand1;added_by_brand",
    ]
    with open(dict_dir / "merged_dictionary.csv", "w", encoding="utf-8") as f:
        f.write("\n".join(dict_lines) + "\n")

    return report_dir
