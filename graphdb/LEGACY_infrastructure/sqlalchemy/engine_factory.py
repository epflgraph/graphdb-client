from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine

from graphdb.domain.connection import ConnectionParams
from graphdb.infrastructure.shared.ssl_options import build_ssl_connect_args


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
