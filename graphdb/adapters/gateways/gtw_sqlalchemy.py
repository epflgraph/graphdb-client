from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError

from graphdb.adapters.gateways.utils import build_ssl_connect_args
from graphdb.domain.exceptions import QueryExecutionError
from graphdb.domain.models.mdl_connection import ConnectionParams


def create_sqlalchemy_engine(params: ConnectionParams) -> Engine:
    ssl_connect_args = build_ssl_connect_args(params.ssl)
    engine_kwargs = {"pool_pre_ping": True}
    if ssl_connect_args:
        engine_kwargs["connect_args"] = {"ssl": ssl_connect_args}

    if params.sqlalchemy_url:
        engine_url = str(params.sqlalchemy_url)
    else:
        dialect = str(params.sqlalchemy_dialect or "mysql")
        driver = str(params.sqlalchemy_driver or "pymysql")
        engine_url = (
            f"{dialect}+{driver}://"
            f"{params.username}:{params.password}@{params.host_address}:{params.port}/"
        )

    engine = create_engine(engine_url, **engine_kwargs)

    @event.listens_for(engine, "connect")
    def set_sql_mode(dbapi_conn, _):
        with dbapi_conn.cursor() as cur:
            cur.execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")

    return engine


class SQLAlchemyQueryExecutorGateway:
    """Adapter for executing queries through a SQLAlchemy engine."""

    def __init__(self, engine: Engine, env_name: str = "default") -> None:
        self.engine = engine
        self.env_name = env_name

    def test(self) -> bool:
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                return True
        except Exception:
            return False

    def execute(
        self,
        query: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        commit: bool = False,
        return_exception: bool = False,
        query_id: Optional[str] = None,
    ) -> Union[List[Any], Tuple[str, str, Any]]:
        connection_ctx = self.engine.begin() if commit else self.engine.connect()
        try:
            with connection_ctx as connection:
                if schema_name:
                    connection.execute(text(f"USE `{schema_name}`"))
                result = connection.execute(text(query), parameters=params or {})
                if result.returns_rows:
                    rows = result.fetchall()
                else:
                    rows = []
        except (DataError, IntegrityError, SQLAlchemyError) as e:
            if return_exception:
                dbapi_code = getattr(e.orig, "args", [None])[0] if hasattr(e, "orig") else None
                return type(e).__name__, str(e), dbapi_code
            raise QueryExecutionError(
                f"Error executing query{f' [{query_id}]' if query_id else ''}: {e}"
            ) from e
        return rows

    def execute_stream_to_file(
        self,
        query: str,
        output_file: str,
        schema_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        fetch_size: int = 1000,
        query_id: Optional[str] = None,
    ) -> None:
        if not output_file:
            raise ValueError("output_file must be provided")

        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

        connection = self.engine.connect()
        try:
            if schema_name:
                connection.execute(text(f"USE `{schema_name}`"))

            exec_conn = connection.execution_options(stream_results=True)
            result = exec_conn.execute(text(query), parameters=params or {})

            if not result.returns_rows:
                raise QueryExecutionError(
                    "execute_query_stream_to_file only supports SELECT queries"
                )

            with open(output_file, "w", encoding="utf-8") as f:
                while True:
                    chunk = result.fetchmany(fetch_size)
                    if not chunk:
                        break
                    for row in chunk:
                        f.write(json.dumps(dict(row._mapping), ensure_ascii=False, default=str) + "\n")

        except MemoryError:
            try:
                connection.invalidate()
            except Exception:
                pass
            raise

        except (DataError, IntegrityError, SQLAlchemyError) as e:
            raise QueryExecutionError(
                f"Error executing query{f' [{query_id}]' if query_id else ''}: {e}"
            ) from e

        finally:
            try:
                connection.close()
            except Exception:
                pass
