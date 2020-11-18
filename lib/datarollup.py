import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta

from lib.tablesync import download_table

from keys import KeyChain

_counter_field_map = 'base1s counter host type context'.split()

_rollup_rules = download_table(KeyChain.PERF_ROLLUP_RULES['csv'])
for line in _rollup_rules['data']:
    line[0] = line[0].strip() if (line[0]) else None
    line[1] = line[1].strip() if (line[1]) else None

_table_rollup_interval = {
    'CounterLinesRoll1Min': timedelta(minutes=1),
    'CounterLinesRoll15Min': timedelta(minutes=15),
}


def get_connection(db_key):
    return psycopg2.connect(dbname=db_key["db_name"], user=db_key["user"],
                            password=db_key["pwd"], host=db_key["host"], port=db_key.get('port'))


def get_latest_date(conn, from_date, to_date, base1s, table_name):
    """ get actual latest counter date for period """
    q_select_latest_date = sql.SQL(
        'select max("stamp") from {} '
        ' where "stamp" between {} and {} and "base1s" = {}'
    ).format(
        sql.Identifier(table_name),
        sql.Literal(from_date),
        sql.Literal(to_date),
        sql.Literal(base1s)
    )

    cursor = conn.cursor()
    cursor.execute(q_select_latest_date)
    result = cursor.fetchone()[0]
    return result or from_date


# def update_counter_data(conn, from_date, to_date, base1s):
#     # get latest counter date for add new records
#     from_date = get_latest_date(conn, from_date, to_date, base1s)
#
#     q_copy_counter_data = sql.SQL(
#         'insert into "CounterLinesRoll" ({0}) select {0} from "CounterLines" '
#         ' where "stamp" between {1} and {2} and "base1s" = {3}'
#     ).format(
#         sql.SQL(', ').join([sql.Identifier(field) for field in _counter_field_map]),
#         sql.Literal(from_date),
#         sql.Literal(to_date),
#         sql.Literal(base1s)
#     )
#     cursor = conn.cursor()
#     cursor.execute(q_copy_counter_data)
#     conn.commit()


def clear_period(conn, from_date, to_date, base1s, table_name):
    q_delete_counter_data = sql.SQL(
        'delete from {} '
        ' where "stamp" between {} and {} and "base1s" = {}'
    ).format(
        sql.Identifier(table_name),
        sql.Literal(from_date),
        sql.Literal(to_date),
        sql.Literal(base1s)
    )
    cursor = conn.cursor()
    cursor.execute(q_delete_counter_data)
    conn.commit()


def get_counter_enums(conn, from_date, to_date, base1s):
    q_counter_enum = sql.SQL(
        'select counter from "CounterLines" '
        ' where "stamp" between {} and {} and "base1s" = {} group by counter'
    ).format(
        sql.Literal(from_date),
        sql.Literal(to_date),
        sql.Literal(base1s)
    )
    cursor = conn.cursor()
    cursor.execute(q_counter_enum)
    return [counter[0] for counter in cursor.fetchall()]


def get_agr_function(for_counter):
    default = 'avg'
    result = _rollup_rules.get(for_counter, 'fail')
    if result == 'fail':
        print('New counter find:')
        print(f'[{for_counter}]')
        result = default
    elif result is None:
        result = default
    return result


def roll_up_interval(conn, from_date, base1s, table_name, interval):
    to_date = from_date + interval
    counter_list = get_counter_enums(conn, from_date, to_date, base1s)

    q_rollup_template = sql.SQL(
        'select {0}(flt_value), {1} from "CounterLines" '
        ' where "stamp" between {2} and {3} and "base1s" = {4} group by {1}'
    )

    roll_up_data = []

    for counter in counter_list:
        func = get_agr_function(counter)
        q_rollup = q_rollup_template.format(
            sql.SQL(func),
            sql.SQL(', ').join([sql.Identifier(field) for field in _counter_field_map]),
            sql.Literal(from_date),
            sql.Literal(to_date),
            sql.Literal(base1s)
        )

        cursor = conn.cursor()
        cursor.execute(q_rollup)
        roll_up_data.append(cursor.fetchone())




from unittest import TestCase


class DataRollupTest(TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(KeyChain.PG_PERF_KEY)
        self.base1s = 'tjtest'

    def test_get_latest_date(self):
        _from = datetime(2020, 9, 28)
        _to = datetime(2020, 9, 28) + timedelta(days=3)
        res = get_latest_date(self.conn, _from, _to, self.base1s)
        print(res)


    def test_update_latest_date(self):
        _from = datetime(2020, 9, 28)
        _to = datetime(2020, 9, 28) + timedelta(days=3)

        clear_period(self.conn, _from, _to, 'tjtest')

    def test_get_counter_enums(self):
        _from = datetime(2020, 9, 28)
        _to = datetime(2020, 9, 28) + timedelta(days=3)
        res = get_counter_enums(self.conn, _from, _to, self.base1s)
        print(res)
        print(len(res))

    def test_roll_up_interval(self):
        _from = datetime(2020, 9, 29)
        roll_up_interval(self.conn, _from, self.base1s, 'CounterLinesRoll1Min', timedelta(minutes=5))
