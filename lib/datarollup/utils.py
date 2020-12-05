import psycopg2
from psycopg2 import extras, sql
from datetime import datetime, timedelta
from dataclasses import dataclass, field

__all__ = (
    # classes
    'RollupRule',
    'RollupInterval',
    'AggregateRule',

    # helpers
    'plan_interval_list_for_period',
    'roll_up_interval_into',
    'read_key_enums',
    'get_connection',
)


@dataclass
class RollupInterval:
    left_bound: datetime
    right_bound: datetime

    def middle_date(self):
        return self.left_bound + (self.right_bound - self.left_bound)/2

    def __add__(self, delta: timedelta):
        return RollupInterval(
            self.left_bound + delta,
            self.right_bound + delta)


@dataclass()
class AggregateRule:
    function: str
    destination_field_name: str

    def get_select_sql(self, source_value_field):
        return sql.SQL('{}({})').format(
            sql.SQL(self.function),
            sql.Identifier(source_value_field)
        )

    def get_insert_sql(self):
        return sql.SQL('{}').format(
            sql.Identifier(self.destination_field_name)
        )


@dataclass()
class RollupRule:
    source: str
    key_field: str
    rollup_value_field: str
    rollup_fields: list
    stamp_field: str
    data_filter: dict = field(default_factory=dict)
    aggregate_rules: list = field(default_factory=list)

    def gen_filter_query(self):
        return sql.Composed([sql.SQL(' and {} = {}').format(
            sql.Identifier(key), sql.Literal(self.data_filter[key]),
        ) for key in self.data_filter.keys()])


def get_connection(db_key):
    return psycopg2.connect(dbname=db_key["db_name"], user=db_key["user"],
                            password=db_key["pwd"], host=db_key["host"], port=db_key.get('port'))


def clear_interval(conn, interval: RollupInterval, rule: RollupRule, table_name: str):
    q_delete_counter_data = sql.SQL(
        'delete from {0} '
        ' where {1} >= {2} and {1} < {3} {4}'
    ).format(
        sql.Identifier(table_name),
        sql.Identifier(rule.stamp_field),
        sql.Literal(interval.left_bound),
        sql.Literal(interval.right_bound),
        rule.gen_filter_query()
    )
    cursor = conn.cursor()
    cursor.execute(q_delete_counter_data)
    conn.commit()


def read_key_enums(conn, interval: RollupInterval, rule: RollupRule):
    q_counter_enum = sql.SQL(
        'select {0} from {1} '
        ' where {2} >= {3} and {2} < {4} {5} group by {0}'
    ).format(
        sql.Identifier(rule.key_field),
        sql.Identifier(rule.source),
        sql.Identifier(rule.stamp_field),
        sql.Literal(interval.left_bound),
        sql.Literal(interval.right_bound),
        rule.gen_filter_query()
    )
    cursor = conn.cursor()
    cursor.execute(q_counter_enum)
    return [counter[0] for counter in cursor.fetchall()]


def roll_up_interval_into(conn, interval: RollupInterval, destination_table: str, rule: RollupRule):

    # external aggregation map
    key_list = read_key_enums(conn, interval, rule)

    select_fields = [rule.key_field] + rule.rollup_fields
    q_rollup_template = sql.SQL(
        'select {0}, {2} from {3} '
        ' where {4} = {5} and '
        ' {6} >= {7} and {6} < {8} {9} group by {2}'
    )

    sq_aggregate_select = sql.SQL(', ').join(
        [agg_rule.get_select_sql(rule.rollup_value_field) for agg_rule in rule.aggregate_rules])

    roll_up_data = []
    cursor = conn.cursor()
    for key_value in key_list:
        q_rollup = q_rollup_template.format(
            sq_aggregate_select,
            sql.SQL(''),  # reserved
            sql.SQL(', ').join([sql.Identifier(_field) for _field in select_fields]),
            sql.Identifier(rule.source),
            sql.Identifier(rule.key_field),
            sql.Literal(key_value),
            sql.Identifier(rule.stamp_field),
            sql.Literal(interval.left_bound),
            sql.Literal(interval.right_bound),
            rule.gen_filter_query()
        )

        cursor.execute(q_rollup)
        roll_up_data.append(cursor.fetchone())

    q_insert_batch = sql.SQL('insert into {} ({}, {}, {}) values ({}, {}, {})').format(
        sql.Identifier(destination_table),
        sql.Identifier(rule.stamp_field),
        sql.SQL(', ').join([agg_rule.get_insert_sql() for agg_rule in rule.aggregate_rules]),
        sql.SQL(', ').join([sql.Identifier(_field) for _field in select_fields]),
        sql.Literal(interval.middle_date()),
        sql.SQL(', ').join([sql.SQL('%s') for _ in enumerate(rule.aggregate_rules)]),
        sql.SQL(', ').join([sql.SQL('%s') for _ in enumerate(select_fields)])
    )

    psycopg2.extras.execute_batch(cursor, q_insert_batch, roll_up_data)
    conn.commit()


def plan_interval_list_for_period(from_date, to_date, interval: timedelta) -> list:
    assert from_date < to_date
    result = []
    cur_date = from_date
    while True:
        if cur_date + interval > to_date:
            interval = to_date - cur_date
        result.append(RollupInterval(
            cur_date,
            cur_date + interval,
        ))
        if cur_date + interval >= to_date:
            break
        cur_date += interval
    return result


from unittest import TestCase

from keys import KeyChain


class DataRollupTest(TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(KeyChain.PG_PERF_KEY)
        self.base1s = 'tjtest'
        self.rollup_rule = RollupRule(
            source='CounterLines',
            key_field='counter',
            rollup_value_field='flt_value',
            rollup_fields='host type context base1s'.split(),
            stamp_field='stamp',
            data_filter={'base1s': 'tjtest'},
            aggregate_rules=[
                AggregateRule('sum', 'flt_value_sum'),
                AggregateRule('count', 'flt_value_count'),
                AggregateRule('avg', 'flt_value_avg'),
                AggregateRule('min', 'flt_value_min'),
                AggregateRule('max', 'flt_value_max'),
            ]
        )

    def test_get_counter_enums(self):
        interval = RollupInterval(
            datetime(2020, 9, 29),
            datetime(2020, 9, 29) + timedelta(minutes=5)
        )

        res = read_key_enums(self.conn, interval, self.rollup_rule)
        print(*res, sep='\n')
        print(len(res))

    def test_roll_up_interval(self):
        _from = datetime(2020, 9, 29)
        _delta = timedelta(minutes=5)
        interval1 = RollupInterval(_from, _from + _delta)
        interval2 = interval1 + _delta

        table_name = 'CounterLinesRoll1Min'

        clear_interval(self.conn, interval1, self.rollup_rule, table_name)
        roll_up_interval_into(self.conn, interval1, table_name, self.rollup_rule)

        clear_interval(self.conn, interval2, self.rollup_rule, table_name)
        roll_up_interval_into(self.conn, interval2, table_name, self.rollup_rule)

    def test_get_interval_list_for_period(self):
        _from = datetime.now()
        _to = _from + timedelta(hours=1)
        interval = timedelta(minutes=5)
        interval_list = plan_interval_list_for_period(_from, _to, interval)
        print(interval_list)

    def test_rollup_rule(self):
        rule = self.rollup_rule
        s = 'bar foo'
        rule.source = s
        self.assertEqual(s, rule.source)

