"""
    Postgres Connection Utils
"""

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import sql

__all__ = \
    (
        'PGMix',
        'sql'
    )


class PGMix:
    def connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            dbname=self._pg_key["db_name"],
            user=self._pg_key["user"],
            password=self._pg_key["pwd"],
            host=self._pg_key["host"],
            port=self._pg_key.get('port')
        )

    def __init__(self, key):
        self._pg_key = key
        self._pg_conn = self.connect()
        self._extras = psycopg2.extras
        self._extensions = psycopg2.extensions

    def cursor(self, named=True) -> psycopg2.extensions.cursor:
        if self._pg_conn.closed:
            self._pg_conn = self.connect()
        return self._pg_conn.cursor(
            cursor_factory=psycopg2.extras.NamedTupleCursor if named else None)

    def commit(self):
        self._pg_conn.commit()

    # def _connection(self) -> psycopg2.extensions.connection:
    #     return self._pg_conn
