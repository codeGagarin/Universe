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
    PG_KEY = None
    """ 
        Field using for non __init__ db key initialization 
        
        class SomeClass(PGMin):
            PG_KEY = KeyChain.BLA_BLA # Second priority, optional   
    
        def __init__(self):
            PGMix.__init__(self, KeyChain.BLA_BLA)  # First priority, optional
    
    """

    def __connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            dbname=self.PG_KEY["db_name"],
            user=self.PG_KEY["user"],
            password=self.PG_KEY["pwd"],
            host=self.PG_KEY["host"],
            port=self.PG_KEY.get('port')
        )

    def __init_ext(self):
        if not hasattr(self, '_extras'):
            self._extras = psycopg2.extras
            self._extensions = psycopg2.extensions
            self._conn = self.__connect()

    def __init__(self, pg_key=None):
        assert pg_key or self.PG_KEY, 'PG_KEY is not defined. Pls, use __init__ or PG_KEY usage'
        if pg_key:
            self.PG_KEY = pg_key
        self.__init_ext()

    def cursor(self, named=True) -> psycopg2.extensions.cursor:
        self.__init_ext()
        if self._conn.closed:
            self._conn = self.__connect()
        return self._conn.cursor(
            cursor_factory=psycopg2.extras.NamedTupleCursor if named else None)

    def commit(self):
        self._conn.commit()

