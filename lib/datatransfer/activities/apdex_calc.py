"""
    Utils and Activity for Apdex values calculated
"""
from datetime import datetime, timedelta
from typing import List
import cProfile

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
    def get_next_period(self) -> _Period:

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

    # todo: old version should be removed after new version compliance check
    def ops_apdex_for(self, period: _Period):  # return dict
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

    def ops_apdex_for2(self, period: _Period):
        apdex_calc_query = pgs.SQL(
            ''' WITH
                    empty_uid AS (
                        SELECT ops_uid AS id FROM {0} WHERE
                                start >= {1}::TIMESTAMP AND start < {2}::TIMESTAMP
                            AND
                                base1s = {3}
                            AND
                                apdex IS Null
                        GROUP BY ops_uid
                    )
                SELECT ops_uid AS id,
                       (COUNT(CASE WHEN status='NS' THEN 1 END) + (COUNT(CASE WHEN status='NT' THEN 1 END)::REAL/2))::REAL
                           / COUNT(*)::REAL AS apdex FROM {0}
                    WHERE
                            start >= {1}::TIMESTAMP AND start < {2}::TIMESTAMP
                        AND
                            base1s = {3}
                        AND
                            ops_uid IN (SELECT id FROM empty_uid)
                        GROUP BY ops_uid '''
        ).format(
            pgs.Identifier(self.APDEX_TABLE),
            pgs.Literal(period.begin), pgs.Literal(period.end),
            pgs.Literal(self.base1s),
        )
        cursor = self.cursor(named=True)
        cursor.execute(apdex_calc_query)
        return cursor.fetchall()

    def calculate(self, max_hours):
        batch_cursor = self.cursor()

        batch_update_query = pgs.SQL(
            'UPDATE {} SET {}=%s WHERE start>=%s AND start<%s AND base1s={} AND ops_uid=%s'
        ).format(
            pgs.Identifier(self.APDEX_TABLE),
            pgs.Identifier(self.APDEX_FIELD),
            pgs.Literal(self.base1s),
        )
        batch_params_list = []

        for _ in range(max_hours):
            period = self.get_next_period()

            if not period:
                break

            ops_map = self.ops_apdex_for2(period)
            for ops in ops_map or ():
                batch_params_list.append((ops.apdex, period.begin, period.end, ops.id))

            self.log.append(period)

            self._extras.execute_batch(batch_cursor, batch_update_query, batch_params_list)
            self.commit()


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
        self.utils = ApdexUtils(KeyChain.PG_PERF_KEY, 'tjtest')

    def test_calc_results(self):
        period = self.utils.get_next_period()
        self.assertIsNotNone(period, 'Period is empty, test is impossible!')
        result_foo = self.utils.ops_apdex_for(period)
        result_bar = self.utils.ops_apdex_for2(period)
        self.assertEqual(len(result_foo), len(result_bar), 'Results are not equals!')
        self.assertNotEqual(len(result_foo), 0, 'Results len is 0, test is impossible!')
        self.assertEqual(set(result_foo), set(result_bar))


class _ApdexCalcTest(TestCase):
    def setUp(self) -> None:
        self.a = ApdexCalc(NullStarter())

    def test_run(self):
        self.a['base1s'] = 'tjtest'
        self.a.DEFAULT_MAX_HOURS = 3
        self.a.run()

