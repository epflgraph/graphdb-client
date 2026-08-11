from __future__ import annotations

from typing import Any

from sqlalchemy import text


class EngineTestAdapter:
    """Adapter for testing MySQL connectivity."""


    def __init__(self, graphdb: Any) -> None:

        self._graphdb = graphdb


    def test(self, engine_name=None):

        """

        Test the MySQL connection by executing a simple query.

        """

        if engine_name is None:

            engine_name = self._graphdb.default_engine_name

        try:

            connection = self._graphdb.engine[engine_name].connect()

            result = connection.execute(text("SELECT 1")).fetchone()

            connection.close()

            if result is None:

                return False

            return result[0] == 1

        except Exception as e:

            print(f"Error connecting to MySQL {engine_name}: {e}")

            return False

