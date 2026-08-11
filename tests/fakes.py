from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


class FakeDatabaseAdapter:
    """In-memory fake for DatabasePort."""

    def __init__(self, responses: Optional[Dict[str, Any]] = None) -> None:
        self.responses = responses or {}
        self.executed: List[Dict[str, Any]] = []
        self.files_executed: List[str] = []
        self.connected = True

    def test(self) -> bool:
        return self.connected

    def execute(
        self,
        query: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        commit: bool = False,
        return_exception: bool = False,
        query_id: Optional[str] = None,
    ) -> Union[List[Any], Tuple[str, str, Any]]:
        self.executed.append({"query": query, "schema": schema_name, "params": params})
        key = (query.strip(), schema_name)
        if key in self.responses:
            return self.responses[key]
        return []

    def execute_in_shell(self, query: str, database: Optional[str] = None, query_id: Optional[str] = None) -> None:
        self.executed.append({"query": query, "database": database, "shell": True})

    def execute_from_file(self, file_path: str, database: Optional[str] = None) -> None:
        self.files_executed.append(file_path)

    def execute_stream_to_file(
        self,
        query: str,
        output_file: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        fetch_size: int = 1000,
        query_id: Optional[str] = None,
    ) -> None:
        pass


class FakeSchemaAdapter:
    """In-memory fake for SchemaPort."""

    def __init__(
        self,
        databases: Optional[List[str]] = None,
        tables: Optional[Dict[str, List[str]]] = None,
        views: Optional[Dict[str, List[str]]] = None,
        create_statements: Optional[Dict[str, str]] = None,
    ) -> None:
        self.databases = set(databases or [])
        self.tables: Dict[str, List[str]] = tables or {}
        self.views: Dict[str, List[str]] = views or {}
        self.create_statements = create_statements or {}
        self.columns: Dict[str, List[str]] = {}
        self.keys: Dict[str, Dict[str, List[str]]] = {}

    def database_exists(self, schema_name: str) -> bool:
        return schema_name in self.databases

    def create_database(self, schema_name: str, drop_existing: bool = False) -> None:
        if drop_existing:
            self.databases.discard(schema_name)
        self.databases.add(schema_name)

    def drop_database(self, schema_name: str) -> None:
        self.databases.discard(schema_name)

    def table_exists(self, schema_name: str, table_name: str, exclude_views: bool = False) -> bool:
        return table_name in self.tables.get(schema_name, [])

    def fetch_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        return {
            "table_schema": schema_name,
            "table_name": table_name,
            "engine": "InnoDB",
            "table_collation": "utf8mb4_unicode_ci",
            "row_format": "Dynamic",
            "table_rows": 0,
            "data_length": 0,
            "index_length": 0,
            "total_bytes": 0,
            "column_count": 0,
            "nullable_columns": 0,
            "columns_with_default": 0,
            "index_count": 0,
            "unique_index_count": 0,
            "avg_row_length": 0,
        }

    def column_exists(self, schema_name: str, table_name: str, column_name: str) -> bool:
        return column_name in self.columns.get(f"{schema_name}.{table_name}", [])

    def key_exists(self, schema_name: str, table_name: str, key_name: str) -> bool:
        return key_name in self.keys.get(f"{schema_name}.{table_name}", {})

    def get_tables(self, schema_name: str, include_views: bool = False) -> List[str]:
        return self.tables.get(schema_name, [])

    def get_views(self, schema_name: str) -> List[str]:
        return self.views.get(schema_name, [])

    def get_create_table(self, schema_name: str, table_name: str) -> str:
        return self.create_statements.get(f"{schema_name}.{table_name}", "")

    def get_create_view(self, schema_name: str, view_name: str) -> str:
        return self.create_statements.get(f"{schema_name}.{view_name}", "")

    def is_view(self, schema_name: str, name: str) -> bool:
        return name in self.views.get(schema_name, [])

    def get_column_names(self, schema_name: str, table_name: str) -> List[str]:
        return self.columns.get(f"{schema_name}.{table_name}", [])

    def get_column_datatypes(self, schema_name: str, table_name: str) -> List[str]:
        return ["INT"] * len(self.get_column_names(schema_name, table_name))

    def has_primary_key(self, schema_name: str, table_name: str) -> bool:
        return "PRIMARY" in self.keys.get(f"{schema_name}.{table_name}", {})

    def get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        return self.keys.get(f"{schema_name}.{table_name}", {}).get("PRIMARY", [])

    def get_keys(self, schema_name: str, table_name: str) -> Dict[str, List[str]]:
        return self.keys.get(f"{schema_name}.{table_name}", {})

    def create_table_like(self, *args, **kwargs) -> None:
        pass

    def drop_table(self, schema_name: str, table_name: str) -> None:
        if table_name in self.tables.get(schema_name, []):
            self.tables[schema_name].remove(table_name)

    def rename_table(self, *args, **kwargs) -> None:
        pass

    def drop_keys(self, *args, **kwargs) -> None:
        pass


class FakeDumpAdapter:
    """In-memory fake for DumpPort."""

    def __init__(self) -> None:
        self.dumps: List[Dict[str, Any]] = []

    def dump_table_data(self, schema_name: str, table_name: str, output_file: str, where: str = "TRUE", compress: bool = False) -> None:
        self.dumps.append({"schema": schema_name, "table": table_name, "file": output_file, "where": where, "chunk": False, "compress": compress})

    def dump_table_chunk(self, schema_name: str, table_name: str, output_file: str, where: str = "TRUE", chunk_column: str = "row_id", chunk_start: int = 0, chunk_end: int = 0, compress: bool = False) -> None:
        self.dumps.append({"schema": schema_name, "table": table_name, "file": output_file, "where": where, "chunk": True, "chunk_start": chunk_start, "chunk_end": chunk_end, "compress": compress})


class FakeEnvironmentAdapter:
    """Composite fake adapter implementing the per-environment interface."""

    def __init__(
        self,
        database: Optional[FakeDatabaseAdapter] = None,
        schema: Optional[FakeSchemaAdapter] = None,
        dump: Optional[FakeDumpAdapter] = None,
        filesystem: Optional[FakeFilesystemAdapter] = None,
    ) -> None:
        self.db = database or FakeDatabaseAdapter()
        self.schema = schema or FakeSchemaAdapter()
        self.dump = dump or FakeDumpAdapter()
        self.fs = filesystem or FakeFilesystemAdapter()

    def test(self) -> bool:
        return self.db.test()

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)

    def execute_in_shell(self, *args, **kwargs):
        return self.db.execute_in_shell(*args, **kwargs)

    def execute_from_file(self, *args, **kwargs):
        return self.db.execute_from_file(*args, **kwargs)

    def execute_file_with_sed(self, file_path: str, database: Optional[str] = None, sed_pattern: str = "") -> None:
        self.db.files_executed.append(file_path)

    def execute_stream_to_file(self, *args, **kwargs):
        return self.db.execute_stream_to_file(*args, **kwargs)

    def database_exists(self, *args, **kwargs):
        return self.schema.database_exists(*args, **kwargs)

    def create_database(self, *args, **kwargs):
        return self.schema.create_database(*args, **kwargs)

    def drop_database(self, *args, **kwargs):
        return self.schema.drop_database(*args, **kwargs)

    def table_exists(self, *args, **kwargs):
        return self.schema.table_exists(*args, **kwargs)

    def fetch_table_metadata(self, *args, **kwargs):
        return self.schema.fetch_table_metadata(*args, **kwargs)

    def get_exact_count(self, schema_name: str, table_name: str) -> int:
        rows = self.execute(f"SELECT COUNT(*) FROM `{schema_name}`.`{table_name}`", schema_name=schema_name)
        return int(rows[0][0]) if rows else 0

    def column_exists(self, *args, **kwargs):
        return self.schema.column_exists(*args, **kwargs)

    def key_exists(self, *args, **kwargs):
        return self.schema.key_exists(*args, **kwargs)

    def get_tables(self, *args, **kwargs):
        return self.schema.get_tables(*args, **kwargs)

    def get_views(self, *args, **kwargs):
        return self.schema.get_views(*args, **kwargs)

    def get_create_table(self, *args, **kwargs):
        return self.schema.get_create_table(*args, **kwargs)

    def get_create_view(self, *args, **kwargs):
        return self.schema.get_create_view(*args, **kwargs)

    def is_view(self, *args, **kwargs):
        return self.schema.is_view(*args, **kwargs)

    def get_column_names(self, *args, **kwargs):
        return self.schema.get_column_names(*args, **kwargs)

    def get_column_datatypes(self, *args, **kwargs):
        return self.schema.get_column_datatypes(*args, **kwargs)

    def has_primary_key(self, *args, **kwargs):
        return self.schema.has_primary_key(*args, **kwargs)

    def get_primary_keys(self, *args, **kwargs):
        return self.schema.get_primary_keys(*args, **kwargs)

    def get_keys(self, *args, **kwargs):
        return self.schema.get_keys(*args, **kwargs)

    def create_table_like(self, *args, **kwargs):
        return self.schema.create_table_like(*args, **kwargs)

    def drop_table(self, *args, **kwargs):
        return self.schema.drop_table(*args, **kwargs)

    def rename_table(self, *args, **kwargs):
        return self.schema.rename_table(*args, **kwargs)

    def drop_keys(self, *args, **kwargs):
        return self.schema.drop_keys(*args, **kwargs)

    def dump_table_data(self, *args, **kwargs):
        return self.dump.dump_table_data(*args, **kwargs)

    def dump_table_chunk(self, *args, **kwargs):
        return self.dump.dump_table_chunk(*args, **kwargs)

    @property
    def filesystem(self):
        return self.fs


class FakeAdapterRegistry:
    """In-memory registry of fake environment adapters."""

    def __init__(self, adapters: Dict[str, FakeEnvironmentAdapter], config: Optional[Any] = None) -> None:
        self._adapters = adapters
        self.config = config

    def get(self, env_name: Optional[str] = None) -> FakeEnvironmentAdapter:
        if env_name is None:
            env_name = next(iter(self._adapters))
        return self._adapters[env_name]

    def names(self):
        return list(self._adapters.keys())


class FakeFilesystemAdapter:
    """In-memory fake for FilesystemPort."""

    def __init__(self) -> None:
        self.files: Dict[Path, str] = {}
        self.dirs: set = set()

    def write_text(self, path: Path, content: str) -> None:
        self.files[path] = content
        self.dirs.add(path.parent)

    def read_text(self, path: Path) -> str:
        return self.files[path]

    def ensure_dir(self, path: Path) -> None:
        self.dirs.add(path)

    def exists(self, path: Path) -> bool:
        return path in self.files

    def list_sql_files(self, folder: Path, compress: bool = False) -> List[Path]:
        if compress:
            return sorted(p for p in self.files if p.suffix == ".gz")
        plain = sorted(p for p in self.files if p.suffix == ".sql")
        gz = sorted(p for p in self.files if p.suffix == ".gz")
        return plain + gz
