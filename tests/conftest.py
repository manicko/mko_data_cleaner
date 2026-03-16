from pathlib import Path

import polars as pl
import pytest

from mko_data_cleaner.core.db_service import DBWorker
from mko_data_cleaner.core.models import DictColumnsIndexes


@pytest.fixture
def dict_indexes():
    return DictColumnsIndexes(action=0, match=1, search=2, term=3)


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
def db_worker(tmp_db_path: Path):
    """
    DBWorker fixture with automatic cleanup.

    Uses context manager so __exit__ is guaranteed.
    """

    with DBWorker(
        db_file=tmp_db_path, tbl_name="data_table", index_column="id"
    ) as worker:
        yield worker


@pytest.fixture
def db_worker_memory(tmp_path):

    db_file = tmp_path / "memory.db"

    with DBWorker(db_file=db_file, tbl_name="data_table", index_column="id") as worker:
        yield worker
