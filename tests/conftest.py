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
    """Реалистичный словарь для тестов (соответствует col_indexes из конфига пользователя)."""
    data = [
        ["a", "f", 3, None, "Coca-Cola", "brand"],
        ["r", "p", 1, None, "канал", "channelName"],
        ["r", "s", 1, None, "Первый", "channelName"],
        ["d", "f", 5, None, "Удалить", None],
        ["a", "f", 4, None, "Спонсор", "sponsor_tag"],
    ]

    return pl.DataFrame(
        data,
        schema={
            "action": pl.String,
            "match": pl.String,
            "search": pl.Int64,  # индекс колонки поиска
            "extra_col": pl.String,
            "term": pl.String,
            "tag": pl.String,
        },
        orient="row",
    )


@pytest.fixture
def extra_cols():
    return ["tag"]


@pytest.fixture
def sample_mapping_df():
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
    """Temporary sqlite database file"""
    return tmp_path / "test_database.db"


@pytest.fixture
def db_worker_memory(tmp_path):

    db_file = tmp_path / "memory.db"

    with DBWorker(db_file=db_file, tbl_name="data_table", index_column="id") as worker:
        yield worker
