import tempfile
import requests
import os
import csv
from unittest import TestCase
from datetime import datetime
from datetime import timedelta

import psycopg2
import psycopg2.extras

from keys import KeyChain
# from loader import Loader
from activities.activity import Activity

_index = {
    'tjexcdescr': {
        'path': 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSz33JT'
                '-RwNwpbUeXw9xhms6rzjmnmTtmyStdAgruxDmdmUyY9uAcLxE6tQTe-'
                '_ONHVtX_PQXrDxUMi/pub?gid=0&single=true&output=csv',
        'table': 'TJDescription',
        'mapping': {
            'key': None,
            'value': 'Exception',
            'description': 'Description'
        },
        'key': 'key value',
        'db_key': KeyChain.PG_PERF_KEY,
    }
}


class TableSyncActivity(Activity):
    def __init__(self, ldr, params=None):
        super().__init__(ldr, params)
        self.due_date = None

    def _fields(self):
        return 'index'

    def run(self):
        table_index = self['index']
        table_sync(table_index)

    def apply(self, due_date=None):
        if not due_date:
            due_date = datetime.now() + timedelta(seconds=30)
        super().apply(due_date)
        self.due_date = due_date + timedelta(seconds=60-due_date.second)  # next minute


def download_table(url):
    resp = requests.get(url, allow_redirects=True)
    resp.encoding = 'utf-8'
    fd, tmp_file_path = tempfile.mkstemp()
    os.write(fd, resp.content)
    os.close(fd)

    csv_file = open(tmp_file_path, 'r')
    with csv_file:
        csv_data = iter(csv.reader(csv_file))
        table = {
            'header': list(next(csv_data)),
            'data': []  # [[value for value in record] for record in csv_data]
        }
    os.remove(tmp_file_path)
    return table


def table_update(db_key, table_name: str, table_data: dict, mapping: dict, key: str):
    connection = psycopg2.connect(dbname=db_key["db_name"], user=db_key["user"],
                                  password=db_key["pwd"], host=db_key["host"], port=db_key.get('port'))
    cursor = connection.cursor()
    key_list = key.split()
    key_sub_query = ', '.join(key_list)
    field_sub_query = ', '.join(mapping.keys())
    values_sub_query = ', '.join('%s' for i in range(0, len(mapping)))
    if len(mapping)-len(key_list) == 1:
        conflict_sub_query = ', '.join([f'{field}=excluded.{field}' for field in mapping.keys() if field not in key_list])
    else:
        field_list = [str(field) for field in mapping.keys() if field not in key_list]
        left_part = f'{", ".join(field_list)}'
        right_part = f'{", ".join("exclded.{}".format(f) for f in field_list)}'
        conflict_sub_query = f'({left_part})=({right_part})'

    submit_query = \
        'insert into "' + table_name + '"(' + field_sub_query + ') values ('+values_sub_query+')' \
        ' on conflict (' + key_sub_query + ') do update set ' + conflict_sub_query

    hdr = table_data['header']

    def _trans_mapping(record: list) -> list:
        result = []
        for k, v in mapping.items():
            if v in hdr:
                result.append(record[hdr.index(v)])
            else:
                result.append('')
        return result

    params = [[*_trans_mapping(rec)] for rec in table_data['data']]

    psycopg2.extras.execute_batch(cursor, submit_query, params)
    connection.commit()


def table_sync(table_index):
    table_meta = _index[table_index]
    table_data = download_table(table_meta['path'])
    table_update(table_meta['db_key'], table_meta['table'], table_data, table_meta['mapping'], table_meta['key'])


class Tablesynctest(TestCase):
    def test_table_sync(self):
        table_sync('tjexcdescr')

    def _test_activity(self):
        ldr = None  # Loader(KeyChain)
        act = TableSyncActivity(ldr)
        act['index'] = 'tjexcdescr'
        act.apply()
        act.run()

    def test_download_table(self):
        download_table(KeyChain.PG_STARTER_KEY['cron_tabs'])