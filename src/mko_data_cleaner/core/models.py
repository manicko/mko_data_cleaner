from __future__ import annotations

import codecs
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    NonNegativeInt,
    StringConstraints,
)


def validate_encoding(v: str) -> str:
    codecs.lookup(v)
    return v


EncodingStr = Annotated[
    str,
    BeforeValidator(validate_encoding),
]

NameConstrained = Annotated[
    str,
    StringConstraints(
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
        min_length=1,
        max_length=63,
    ),
]


# ---------------Database
class TableModel(BaseModel):
    table_name: NameConstrained
    column_name: NameConstrained


class Database(BaseModel):
    model_config = ConfigDict(extra="allow")
    table_name: NameConstrained = Field(default="data_table")


# ---------------Logging
class LoggingSettings(BaseModel):
    version: NonNegativeInt = 1
    disable_existing_loggers: bool = False
    formatters: dict[str, Any]
    handlers: dict[str, Any]
    loggers: dict[str, Any]
    root: dict[str, Any]


class WorkingPaths(BaseModel):
    model_config = ConfigDict(extra="forbid")

    import_folder: Path = Path("raw_data")
    export_folder: Path = Path("clean_data")

    dict_file: str = "dict/merged_dictionary.csv"
    db_file: str = "data_base/db_example.db"


# --------------- Data files
class DataFileExtension(StrEnum):
    default = "csv"
    csv = "csv"
    gz = "gz"


class DataFile(BaseModel):
    model_config = ConfigDict(extra="allow")
    extension: DataFileExtension
    index_column: str = None
    date_column: str = None  # 'researchDate' researchMonth


class PolarsWriteCSV(BaseModel):
    model_config = ConfigDict(extra="allow")
    separator: str = ";"
    decimal_comma: bool = True
    include_header: bool = True
    chunk_size: NonNegativeInt = 10000
    compression: Literal["gzip", "bz2", "zip", "xz", "zstd"] | None = "gzip"


class PolarsReadCSV(BaseModel):
    model_config = ConfigDict(extra="allow")
    separator: str = ";"
    encoding: EncodingStr = "utf-8"
    skip_rows: NonNegativeInt | None = 0
    decimal_comma: bool = True
    has_header: bool = True
    ignore_errors: bool = False
    rechunk: bool = False
    infer_schema_length: bool = False


# ---------------Dictionary
class ActionType(StrEnum):
    ADD = "a"
    REPLACE = "r"
    DELETE = "d"


class MatchType(StrEnum):
    FULL_MATCH = "f"
    PARTIAL_MATCH = "p"
    ENDS_WITH = "e"
    STARTS_WITH = "s"
    FTS = "fts"


class MappingColumns(StrEnum):
    # renamed from source
    action = "action"
    match = "match"
    search = "search"
    term = "term"

    # generated
    mapping_index = "mapping_index"
    column_name = "column_name"  # name based on search column with index
    pattern = "pattern"
    data_rowid = "data_rowid"


class DictColumnsIndexes(BaseModel):
    model_config = ConfigDict(extra="allow")
    action: NonNegativeInt = Field(default=0)  # replace, add or delete setting
    match: NonNegativeInt = Field(default=1)
    search: NonNegativeInt = Field(default=2)
    term: NonNegativeInt = Field(default=4)


class DataDict(BaseModel):
    model_config = ConfigDict(extra="allow")
    extension: DataFileExtension
    col_indexes: DictColumnsIndexes = DictColumnsIndexes()
    add_separator: str = ", "
    fts_separator: str = "|"


class ReadCSV(BaseModel):
    from_csv: PolarsReadCSV


class WriteCSV(BaseModel):
    to_csv: PolarsWriteCSV


class DataSettings(BaseModel):
    data_paths: WorkingPaths
    data_file_settings: DataFile
    dict_file_settings: DataDict
    database_settings: Database
    read_settings: ReadCSV
    export_settings: WriteCSV
