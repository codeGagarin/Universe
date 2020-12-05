"""
    Utils and Activity for Apdex values calculated
"""
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List

from lib.pg_utils import PGMix, sql as pgs
from lib.schedutils import Activity
from keys import KeyChain


class _Period:
    def __init__(self, stamp: datetime):
        self.begin = datetime(
            stamp.year, stamp.month, stamp.day, stamp.hour)
        self.end = self.begin + timedelta(hours=1)

    def __repr__(self):
        return self.begin.strftime('%d%H')


@dataclass
class _ApdexRecord:
    n: int
    ns: int
    nt: int


class _Log(List[_Period]):
    def __init__(self, base1s: str):
        super().__init__(self)
        self.base1s = base1s

    def __repr__(self):
        return f'{self.base1s}:' + '-'.join(map(repr, self))


class ApdexUtils(PGMix):
    APDEX_TABLE = 'ApdexLines'

    def __init__(self, db_key, base1s, table_name=None):
        super().__init__(db_key)
        self.base1s = base1s
        self.table_name = table_name or self.APDEX_TABLE
        self.log = _Log(base1s)

    def _get_next_period(self) -> _Period:  # return next empty period for APDEX calc or None

        select_query = pgs.SQL(
            'SELECT start FROM {} WHERE base1s={} AND apdex IS Null ORDER BY start ASC').format(
            pgs.Identifier(self.table_name),
            pgs.Literal(self.base1s),
        )

        cursor = self._cursor(named=True)
        cursor.execute(select_query)
        rows = cursor.fetchone()

        return _Period(rows.start) if rows else None

    def _get_ops_for(self, period: _Period) -> list:

        select_query = pgs.SQL('SELECT DISTINCT ops_uid AS apdex_operation_id FROM {} WHERE '
                               '    start>={} AND start<{} AND base1s={}').format(
            pgs.Identifier(self.table_name),
            pgs.Literal(period.begin), pgs.Literal(period.end),
            pgs.Literal(self.base1s),
        )

        cursor = self._cursor(named=True)
        cursor.execute(select_query)
        rows = cursor.fetchall()

        return [row.apdex_operation_id for row in rows]

    def _get_n_ns_nt_for(self, operation, period: _Period) -> _ApdexRecord:  # return dict
        sq_case = pgs.SQL('COUNT(CASE WHEN status={} THEN 1 END)')
        sq_ns_case = sq_case.format(pgs.Literal('NS'))
        sq_nt_case = sq_case.format(pgs.Literal('NT'))

        n_query = pgs.SQL('SELECT COUNT(*) AS n, {} AS ns, {} AS nt FROM {} '
                          'WHERE start>={} AND start<{} AND ops_uid={} AND base1s={}').format(
            sq_ns_case, sq_nt_case,
            pgs.Identifier(self.table_name),
            pgs.Literal(period.begin), pgs.Literal(period.end),
            pgs.Literal(operation),
            pgs.Literal(self.base1s),
        )
        cursor = self._cursor(named=True)
        cursor.execute(n_query)
        rec = cursor.fetchone()

        return _ApdexRecord(n=rec.n, ns=rec.ns, nt=rec.nt)

    def _set_for(self, operation, period: _Period, apdex_value):  # set APDEX value for ops/hour
        update_query = pgs.SQL(
            'UPDATE {} SET apdex={} WHERE start>={} AND start<{} AND base1s={} AND ops_uid={}'
        ).format(
            pgs.Identifier(self.table_name),
            pgs.Literal(apdex_value),
            pgs.Literal(period.begin), pgs.Literal(period.end),
            pgs.Literal(self.base1s),
            pgs.Literal(operation),
        )
        cursor = self._cursor()
        cursor.execute(update_query)
        self._commit()

    def calculate(self, max_hours):
        for _ in range(max_hours):
            period = self._get_next_period()

            if not period:
                break

            ops_list = self._get_ops_for(period)
            for ops in ops_list:

                v = self._get_n_ns_nt_for(ops, period)
                apdex_value = (v.ns + v.nt / 2) / v.n

                self._set_for(ops, period, apdex_value)

            self.log.append(period)


class ApdexCalc(Activity):
    DEFAULT_MAX_HOURS = 24

    def _fields(self) -> str:
        return 'base1s'

    def run(self):
        calc = ApdexUtils(KeyChain.PG_PERF_KEY, self['base1s'])
        calc.calculate(self.DEFAULT_MAX_HOURS)
        print(calc.log)


from unittest import TestCase
from lib.schedutils import NullStarter


class _ApdexCalcTest(TestCase):
    def setUp(self) -> None:
        self.a = ApdexCalc(NullStarter())

    def test_run(self):
        self.a['base1s'] = 'tjtest'
        self.a.DEFAULT_MAX_HOURS = 3
        self.a.run()


