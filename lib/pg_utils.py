import io
from typing import Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import sql
from pydantic import BaseModel

__all__ = \
    (
        'PGMix',
        'sql',
        'DataBaseKey'
    )


class DataBaseKey(BaseModel):
    db_name: str
    user: str
    pwd: str
    host: str
    port: Optional[int] = None


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
        key = self.PG_KEY if isinstance(self.PG_KEY, dict) else self.PG_KEY.dict()
        return psycopg2.connect(
            dbname=key["db_name"],
            user=key["user"],
            password=key["pwd"],
            host=key["host"],
            port=key.get('port')
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

    def to_csv(self, query):
        """ Return query result as csv-stream """
        csv_query = sql.SQL("COPY ({}) TO STDOUT WITH CSV HEADER").format(query)
        data = io.StringIO()
        with self.cursor() as cursor:
            cursor.copy_expert(csv_query, data)
            data.seek(0)
        return data


from unittest import TestCase


class TestPGMix(TestCase):
    def test_init(self):
        from keys import KeyChain
        PGMix(KeyChain.PG_KEY)
        PGMix(DataBaseKey(**KeyChain.PG_KEY))

