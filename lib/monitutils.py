import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import subprocess

import requests
import json

from unittest import TestCase

from urllib3.util.retry import Retry
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter



from keys import KeyChain
from lib.schedutils import Activity, NullStarter
import lib.telebots.perf_alarm as alarm


class Monitoring(Activity):
    def get_crontab(self):
        return '40 */1 * * *'

    def check_income_counter_data(self, base1s):
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
            sql.Identifier('base1s'), sql.Literal(base1s),
            sql.Identifier('stamp'), sql.Literal(last_hour_begin),
            sql.Identifier('stamp'), sql.Literal(last_hour_end)
        )

        cursor.execute(check_query)
        count = cursor.fetchone()[0]

        if count == 0:
            alarm.alarm("[VGUNF]:No counters have been loaded in the past hour!")

    def check_komtet_503_error(self):
        url = 'https://orbita40.space/'
        s = requests.session()
        r = s.get(url, headers={'User-Agent': 'Monitoring Activity'})

        if r.status_code == 503:
            print('Komtet Orbita 503 error detected!')
            subprocess.run(['sh', 'cmd/uwr'])
        s.close()

    def run(self):
        self.check_income_counter_data('vgunf')
        self.check_komtet_503_error()


class MonitoringTest(TestCase):
    def setUp(self) -> None:
        self.activity = Monitoring(NullStarter())

    def test_activaty(self):
        self.activity.run()

    def test_check_komtet_503_error(self):
        self.activity.check_komtet_503_error()
