import cProfile
import pstats
import psycopg2
import psycopg2.sql as sql

import io
from unittest import TestCase

import lib.perfutils as pu
from keys_pr import KeyChain


def load_query_run(db_key, get_query_func, times=100):
    pr = cProfile.Profile()
    pr.enable()

    connection = psycopg2.connect(dbname=db_key["db_name"], user=db_key["user"],
                                  password=db_key["pwd"], host=db_key["host"], port=db_key.get('port'))
    cursor = connection.cursor()
    query = get_query_func()
    for i in range(0, times):
        cursor.execute(query)

    pr.disable()
    ps = pstats.Stats(pr).sort_stats('cumulative')
    ps.print_stats()


def get_calc_query():
    return sql.SQL('select count(*), type from "monitdata" group by type')


def get_upload_query():
    return sql.SQL('select * from "CounterLines" limit 100000')


class CalcTest(TestCase):
    def test_orbita_space(self):
        load_query_run(KeyChain.PG_YANDEX_PERF_KEY, get_calc_query, 1)

    def test_orbita_ssd(self):
        load_query_run(KeyChain.PG_YANDEX_SSD, get_calc_query, 1)

    def test_komtet(self):
        load_query_run(KeyChain.PG_PERF_KEY, get_calc_query, 1)

    def test_orbita_ssd_cpu(self):
        load_query_run(KeyChain.PG_YANDEX_SSD_CPU, get_calc_query, 1)


class UploadTest(TestCase):
    def test_orbita_space(self):
        load_query_run(KeyChain.PG_YANDEX_PERF_KEY, get_upload_query, 1)

    def test_orbita_ssd(self):
        load_query_run(KeyChain.PG_YANDEX_SSD, get_upload_query, 1)

    def test_komtet(self):
        load_query_run(KeyChain.PG_PERF_KEY, get_upload_query, 1)

    def test_orbita_ssd_cpu(self):
        load_query_run(KeyChain.PG_YANDEX_SSD_CPU, get_upload_query, 1)


