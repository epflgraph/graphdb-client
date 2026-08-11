from __future__ import annotations

from typing import Any

from graphdb.adapters.gateways.gtw_sqlalchemy import create_sqlalchemy_engine
from graphdb.domain.models.mdl_connection import ConnectionParams


class EngineInitiateAdapter:
    """Adapter for SQLAlchemy engine initialization."""


    def __init__(self, graphdb: Any) -> None:

        self._graphdb = graphdb


    def _create_engine(self, params):

        # Backward-compatible wrapper around the new engine factory.

        if isinstance(params, dict):

            params = ConnectionParams(

                host_address=params["host_address"],

                port=params["port"],

                username=params["username"],

                password=params["password"],

                ssl=params.get("ssl"),

                client_bin=params.get("client_bin"),

                dump_bin=params.get("dump_bin"),

                engine_flavor=params.get("engine_flavor"),

                sqlalchemy_url=params.get("sqlalchemy_url"),

                sqlalchemy_dialect=params.get("sqlalchemy_dialect"),

                sqlalchemy_driver=params.get("sqlalchemy_driver"),

            )

        return create_sqlalchemy_engine(params)


    def initiate_engine(self, server_name):

        if server_name not in self._graphdb.params:

            available = ", ".join(sorted(self._graphdb.params.keys()))

            raise ValueError(

                f"could not find configuration for mysql server '{server_name}'. "

                f"Available environments: [{available}]"

            )

        return self._graphdb.params[server_name], self._graphdb.engine[server_name]

