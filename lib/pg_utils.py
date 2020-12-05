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
    def __init__(self, key):
        self._pg_conn = psycopg2.connect(dbname=key["db_name"], user=key["user"],
                                         password=key["pwd"], host=key["host"], port=key.get('port'))
        self._extras = psycopg2.extras
        self._extensions = psycopg2.extensions

    def _cursor(self, named=False) -> psycopg2.extensions.cursor:
        return self._pg_conn.cursor(
            cursor_factory=psycopg2.extras.NamedTupleCursor if named else None)

    def _commit(self):
        self._pg_conn.commit()

    def _connection(self) -> psycopg2.extensions.connection:
        return self._pg_conn
