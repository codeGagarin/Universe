from datetime import datetime, timedelta

from lib.schedutils import Activity, NullStarter
from lib.pg_utils import PGMix
from .utils import RollupRule, AggregateRule
from . import utils
from keys import KeyChain


class CounterLinesRoll(Activity):
    rule = RollupRule(
        source='CounterLines',
        key_field='counter',
        rollup_value_field='flt_value',
        rollup_fields='host type context base1s'.split(),
        stamp_field='stamp',
        data_filter={'no_filter': 'defined bellow'},
        aggregate_rules=[
            AggregateRule('sum', 'flt_value_sum'),
            AggregateRule('count', 'flt_value_count'),
            AggregateRule('avg', 'flt_value_avg'),
            AggregateRule('min', 'flt_value_min'),
            AggregateRule('max', 'flt_value_max'),
        ]
    )
    roll_data_map = {
        'CounterLinesRoll1Min': timedelta(minutes=1),
        'CounterLinesRoll15Min': timedelta(minutes=15),
    }

    def _fields(self) -> str:
        return 'from to base1s'

    @staticmethod
    def full_period(_from: datetime, _to: datetime):
        # hour indent available only
        return \
            datetime(
                year=_from.year,
                month=_from.month,
                day=_from.day,
                hour=_from.hour
            ), \
            datetime(
                year=_to.year,
                month=_to.month,
                day=_to.day,
                hour=_to.hour+1
            )

    def run(self):
        rollup_interval_count = 0
        self.rule.data_filter = {'base1s': self['base1s']}

        _from, _to = self.full_period(self['from'], self['to'])
        with utils.get_connection(KeyChain.PG_PERF_KEY) as conn:
            for table_name, delta in self.roll_data_map.items():
                interval_list = utils.plan_interval_list_for_period(_from, _to, delta)
                for interval in interval_list:
                    utils.clear_interval(conn, interval, self.rule, table_name)
                    utils.roll_up_interval_into(conn, interval, table_name, self.rule)
                    rollup_interval_count += 1

        print(f'Rollup: {rollup_interval_count} interval(s)')


import unittest
from unittest import TestCase


class _CounterLinesRollTest(TestCase):
    def setUp(self) -> None:
        self.counters_roll = CounterLinesRoll(NullStarter)

    def test_run(self):
        a = self.counters_roll
        a['from'] = datetime(2020, 9, 29, 12, 5)
        a['to'] = a['from'] + timedelta(hours=1)
        a['base1s'] = 'tjtest'
        a.run()

    def test_full_period(self):
        _from = datetime(2020, 9, 29, 12, 5)
        _to = _from + timedelta(hours=1)
        __from, __to = CounterLinesRoll.full_period(_from, _to)
        self.assertEqual(__from, datetime(2020, 9, 29, 12), '_from fail')
        self.assertEqual(__to, datetime(2020, 9, 29, 14), '_to fail')


if __name__ == '__main__':
    unittest.main()
