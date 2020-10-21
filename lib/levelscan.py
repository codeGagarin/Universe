import re
import requests
from unittest import TestCase
from datetime import datetime

import psycopg2
from psycopg2 import sql

from lib.schedutils import Activity
from keys import KeyChain


class LevelScan(Activity):
    def run(self):
        scan_levels(KeyChain.PG_PERF_KEY)

    def get_crontab(self):
        return '21 * * * *'


def scan_levels(pg_key):
    url = 'http://www.meteo.nw.ru/weather/lo_levelsd.php'
    response = requests.get(url)
    response.encoding = 'windows-1251'
    html = response.text
    re_html = r"markers_mapm = \[([^\[]+)\]"
    array = re.findall(re_html, html, re.MULTILINE)[0]
    re_array = r"'bubble':'<b>(.+)</b><br>уровень: <i>(.+)</i> см<br /><br /><br />"
    points = re.findall(re_array, array, re.MULTILINE)

    connection = psycopg2.connect(dbname=pg_key["db_name"], user=pg_key["user"],
                                  password=pg_key["pwd"], host=pg_key["host"], port=pg_key.get('port'))

    cursor = connection.cursor()

    stamp = datetime.now()
    for point in points:
        query = sql.SQL('insert into {} ({}, {}, {}) values ({}, {}, {})').format(
            sql.Identifier('Levels'),
            sql.Identifier('stamp'),
            sql.Identifier('point'),
            sql.Identifier('level'),
            sql.Literal(stamp),
            sql.Literal(point[0]),
            sql.Literal(int(point[1])),
        )
        cursor.execute(query)
    connection.commit()
