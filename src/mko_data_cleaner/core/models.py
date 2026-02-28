from pathlib import Path
from typing import Any, Literal, Annotated
import codecs
from enum import StrEnum
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    StringConstraints,
    BeforeValidator
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
        pattern=r"^[A-Za-z][A-Za-z0-9_]*$",
        min_length=1,
        max_length=63,
    ),
]


class TableModel(BaseModel):
    table_name: NameConstrained
    column_name: NameConstrained


# Logging settings
class LoggingSettings(BaseModel):
    version: NonNegativeInt = 1
    disable_existing_loggers: bool = False
    formatters: dict[str, Any]
    handlers: dict[str, Any]
    loggers: dict[str, Any]
    root: dict[str, Any]


class DataFileExtension(StrEnum):
    default = "csv"
    csv = "csv"
    gz = "gz"


class WorkingPaths(BaseModel):
    model_config = ConfigDict(extra="forbid")

    import_folder: Path = Path('raw_data')
    export_folder: Path = Path('clean_data')

    dict_file: str = 'dict/merged_dictionary.csv'
    db_file: str = 'data_base/db_example.db'




class DataFile(BaseModel):
    model_config = ConfigDict(extra="allow")
    extension: DataFileExtension
    search_cols: list[NonNegativeInt]


class DictColumns(BaseModel):
    model_config = ConfigDict(extra="allow")
    action: NonNegativeInt = Field(default=0),  # replace, add or delete setting
    term: NonNegativeInt = Field(default=3)  # search string used after match setting in the SQL query


class DataDict(BaseModel):
    extension: DataFileExtension
    actions: DictColumns = DictColumns()
    clean_cols_ids: dict[str, NonNegativeInt]


class Database(BaseModel):
    model_config = ConfigDict(extra="allow")
    table_name: NameConstrained = Field(default='data_table')


class PandasReadCSV(BaseModel):
    model_config = ConfigDict(extra="allow")
    sep: str = ';'
    on_bad_lines: Literal["error", "warn", "skip"] = "skip"
    encoding: EncodingStr = 'utf-8'
    index_col: NonNegativeInt | Literal[False] | None = None
    skiprows: NonNegativeInt | None = False
    decimal: Literal[',', '.'] = ','
    header: NonNegativeInt | Literal["infer"] | None = 0


class PandasToCSV(BaseModel):
    model_config = ConfigDict(extra="allow")
    sep: Literal[',', ';'] = ','
    chunksize: NonNegativeInt | None = 10000
    encoding: EncodingStr = 'utf-8-sig'
    decimal: Literal[',', '.'] = ','
    header: bool = True
    index: bool = False
    mode: Literal["w", "x", "a"] | None = "a"
    compression: Literal["infer", "gzip", "bz2", "zip", "xz", "zstd"] | None = "gzip"


class ReadCSV(BaseModel):
    from_csv: PandasReadCSV

class WriteCSV(BaseModel):
    to_csv: PandasToCSV

class DataSettings(BaseModel):
    data_paths: WorkingPaths
    data_file_settings: DataFile
    dict_file_settings: DataDict
    database_settings: Database
    read_settings: ReadCSV
    export_settings: WriteCSV