import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from unittest import TestCase


from keys import KeyChain
from lib.schedutils import Activity, NullStarter
import lib.bots.perf_alarm as alarm


class Monitoring(Activity):
    def get_crontab(self):
        return '40 */1 * * *'

    def run(self):
        db_key = KeyChain.PG_PERF_KEY
        connection = psycopg2.connect(dbname=db_key["db_name"], user=db_key["user"],
                                      password=db_key["pwd"], host=db_key["host"], port=db_key.get('port'))
        cursor = connection.cursor()

        now = datetime.now()
        delta = timedelta(minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
        last_hour_begin = now - delta - timedelta(hours=1)
        last_hour_end = now - delta

        check_query = sql.SQL('select count(*) from {} where {}={} and {} >= {} and {} < {}').format(
            sql.Identifier('CounterLines'),
            sql.Identifier('base1s'), sql.Literal('vgunf'),
            sql.Identifier('stamp'), sql.Literal(last_hour_begin),
            sql.Identifier('stamp'), sql.Literal(last_hour_end)
        )

        cursor.execute(check_query)
        count = cursor.fetchone()[0]

        if count == 0:
            alarm.alarm("[VGUNF]:No counters have been loaded in the past hour!")


class MonitoringTest(TestCase):
    def test_activaty(self):
        m = Monitoring(NullStarter())
        m.run()
