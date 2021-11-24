"""
    Utils and Activity for Apdex values calculated
"""
from datetime import datetime, timedelta
from typing import List

from lib.pg_utils import PGMix, sql as pgs
from lib.schedutils import Activity
from keys import KeyChain

import cProfile

class _Period:
    def __init__(self, stamp: datetime):
        self.begin = datetime(
            stamp.year, stamp.month, stamp.day, stamp.hour)
        self.end = self.begin + timedelta(hours=1)

    def __repr__(self):
        return self.begin.strftime('%d%H')


class _Log(List[_Period]):
    def __init__(self, base1s: str):
        super().__init__(self)
        self.base1s = base1s

    def __repr__(self):
        return f'{self.base1s}:' + '-'.join(map(repr, self))


class ApdexUtils(PGMix):
    def __init__(self, db_key, base1s):
        super().__init__(db_key)
        self.base1s = base1s
        self.APDEX_TABLE = 'ApdexLines'
        self.APDEX_FIELD = 'apdex'
        self.log = _Log(base1s)

    ''' Return next empty APDEX period for calc or None '''
    def _get_next_period(self) -> _Period:

        select_query = pgs.SQL(
            'SELECT start FROM {} WHERE base1s={} AND {} IS Null ORDER BY start ASC').format(
            pgs.Identifier(self.APDEX_TABLE),
            pgs.Literal(self.base1s),
            pgs.Identifier(self.APDEX_FIELD),
        )

        cursor = self.cursor(named=True)
        cursor.execute(select_query)
        rows = cursor.fetchone()

        return _Period(rows.start) if rows else None

    def _ops_apdex_for(self, period: _Period):  # return dict
        sq_case = pgs.SQL('COUNT(CASE WHEN status={} THEN 1 END)')
        sq_ns_case = sq_case.format(pgs.Literal('NS'))
        sq_nt_case = sq_case.format(pgs.Literal('NT'))

        n_query = pgs.SQL('SELECT ops_uid as id, ({} + ({}::real/2))::real / COUNT(*)::real as apdex FROM {} '
                          'WHERE start>={} AND start<{} AND base1s={} group by ops_uid').format(
            sq_ns_case, sq_nt_case,
            pgs.Identifier(self.APDEX_TABLE),
            pgs.Literal(period.begin), pgs.Literal(period.end),
            pgs.Literal(self.base1s)
        )
        cursor = self.cursor(named=True)
        cursor.execute(n_query)
        return cursor.fetchall()

    def _set_apdex_for(self, operation, period: _Period, apdex_value):  # set APDEX value for ops/hour
        update_query = pgs.SQL(
            'UPDATE {} SET {}={} WHERE start>={} AND start<{} AND base1s={} AND ops_uid={}'
        ).format(
            pgs.Identifier(self.APDEX_TABLE),
            pgs.Identifier(self.APDEX_FIELD),
            pgs.Literal(apdex_value),
            pgs.Literal(period.begin), pgs.Literal(period.end),
            pgs.Literal(self.base1s),
            pgs.Literal(operation),
        )
        cursor = self.cursor()
        cursor.execute(update_query)
        self.commit()

    def calculate(self, max_hours):
        for _ in range(max_hours):
            period = self._get_next_period()

            if not period:
                break

            ops_map = self._ops_apdex_for(period)
            for ops in ops_map or ():
                self._set_apdex_for(ops.id, period, ops.apdex)

            self.log.append(period)


class ApdexCalc(Activity):
    DEFAULT_MAX_HOURS = 100

    def _fields(self) -> str:
        return 'base1s'

    def run(self):
        prf = cProfile.Profile()
        prf.enable()
        calc = ApdexUtils(KeyChain.PG_PERF_KEY, self['base1s'])
        calc.calculate(self.DEFAULT_MAX_HOURS)
        print(calc.log)
        prf.disable()
        prf.print_stats(sort=2)


from unittest import TestCase
from lib.schedutils import NullStarter


class _ApdexUtilsTest(TestCase):
    def setUp(self) -> None:
        self.util = ApdexUtils(KeyChain.PG_PERF_KEY, 'tjtest')

    def test_calculate(self):
        u = self.util
        hours = 1
        u.calculate(hours)
        print(u.log)


class _ApdexCalcTest(TestCase):
    def setUp(self) -> None:
        self.a = ApdexCalc(NullStarter())

    def test_run(self):
        self.a['base1s'] = 'tjtest'
        self.a.DEFAULT_MAX_HOURS = 3
        self.a.run()
