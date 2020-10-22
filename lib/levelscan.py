import re
import requests
from datetime import datetime
from collections import namedtuple

import psycopg2
from psycopg2 import sql
from psycopg2 import extras

from lib.schedutils import Activity
from keys import KeyChain


class LevelScan(Activity):
    def run(self):
        scan_levels(KeyChain.PG_LEVEL_KEY)

    def get_crontab(self):
        return '20 * * * *'


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


class FZLevelScan(Activity):
    def get_crontab(self):
        return '*/30 * * * *'

    def run(self):
        scan_fz_level(KeyChain.PG_LEVEL_KEY)


_Record = namedtuple('_Record', ['url', 're', 'names'])
_fz_table = 'FZLevels'

root = [
    _Record(
        'http://spun.fkpkzs.ru/Level/C2',
        r"<td class=\"timestampvalue\"><span>(.+)<\/span><\/td>\s+<td.+?>"
        r"<span>(.+)<\/span><\/td>\s+<td.+?><span>(.+)<\/span><\/td",
        ['Горбатый Город', 'Горбатый Море']
    ),
    _Record(
        'http://spun.fkpkzs.ru/Level/C1',
        r"<td class=\"timestampvalue\"><span>(.+)<\/span><\/td>\s+<td.+?>"
        r"<span>(.+)<\/span><\/td>\s+<td.+?><span>(.+)<\/span><\/td",
        ['Тунель Город', 'Тунель Море']
    ),
    _Record(
        'http://spun.fkpkzs.ru',
        r'<td class="timestampvalue"><span>(.+)<\/span><\/td>\s+<td.+?><span>(.+)<\/span>',
        ['Горный институт']
    )
]


def _update_data(conn, params):
    key = 'point stamp'
    field = 'value'
    key_list = key.split()
    key_query = ', '.join(key_list)
    field_query = ', '.join(key.split() + [field])
    values_query = '%s, %s, %s'
    conflict_query = f'{field}=excluded.{field}'
    submit_query = \
        'insert into "' + _fz_table + '"(' + field_query + ') values (' + values_query + ')' \
                                                                                         ' on conflict (' + key_query + ') do update set ' + conflict_query
    cursor = conn.cursor()
    psycopg2.extras.execute_batch(cursor, submit_query, params)
    conn.commit()


def _uni_driver(conn, data, field_names: list):
    submit_params = []
    for rec in data:
        stamp = datetime.strptime(rec[0], "%d.%m.%Y %H:%M")
        for i in range(0, len(field_names)):
            submit_params.append([field_names[i], stamp, int(rec[i + 1])])
    _update_data(conn, submit_params)


def scan_fz_level(pg_key):
    conn = psycopg2.connect(dbname=pg_key["db_name"], user=pg_key["user"],
                            password=pg_key["pwd"], host=pg_key["host"], port=pg_key.get('port'))
    for record in root:
        response = requests.get(record.url)
        response.encoding = 'windows-1251'
        html = response.text
        data = re.findall(record.re, html, re.MULTILINE)
        _uni_driver(conn, data, record.names)


from unittest import TestCase
from lib.schedutils import NullStarter


class LevelScanActivityTest(TestCase):
    def test_scan_fz_level(self):
        scan_fz_level(KeyChain.PG_LEVEL_KEY)

    def test_fz_level_scan_activiy(self):
        a = FZLevelScan(NullStarter())
        a.run()
