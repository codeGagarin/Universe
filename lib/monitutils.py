import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import subprocess

import requests

from keys import KeyChain
from lib.schedutils import Activity, NullStarter
import lib.telebots.perf_alarm as alarm
from lib.pg_utils import PGMix


class Monitoring(Activity, PGMix):
    def __init__(self, ldr):
        Activity.__init__(self, ldr)
        PGMix.__init__(self, KeyChain.PG_PERF_KEY)

    @classmethod
    def get_crontab(cls):
        return '40 */1 * * *'

    def check_income_counter_data(self, base1s):
        with self.cursor() as cursor:
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
        r = s.get(url, verify=False, headers={'User-Agent': 'Monitoring Activity'})

        print(f'Status code:{r.status_code}')
        if r.status_code in (503, 502, 501, 500):
            print('Komtet Orbita 503 error detected!')
            subprocess.run(['sh', 'cmd/uwr'])
            print('Restart')
        s.close()

    def run(self):
        self.check_income_counter_data('vgunf')
        self.check_komtet_503_error()
