from keys import KeyChain
from unittest import TestCase
import re
import ftplib
import tempfile
from datetime import datetime

from clickhouse_driver.client import Client
import psycopg2
import psycopg2.extras
from psycopg2 import sql as pgs


dir_logs = '/logs'
dir_done = f'{dir_logs}/done'


class ABaseAdapter:
    def __init__(self, key, base1s_id):
        self.base1s_id = base1s_id
        self.key = key

    def submit_file(self, file):  # return file_id
        return 0

    def submit_line(self, line):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

def connect_server(key):
    ftp_con = ftplib.FTP(key['host'])
    ftp_con.login(key['user'], key['pwd'])
    return ftp_con


def get_files_for_sync(con):
    return [f[0] for f in con.mlsd(dir_logs) if f[0].find('.log') != -1]  # return only log files


def _parse_line(line: str, db_adapter: ABaseAdapter, file_meta):
    re_header = r'^(\d\d):(\d\d)\.(\d+)-(\d+),(\w+),(\d+),'
    header = re.findall(re_header, line)

    re_params = r'([\w:]+)=([^,\r]+)'
    params = re.findall(re_params, line)
    record = file_meta.copy()
    record['min'] = header[0][0]
    record['ss'] = header[0][1]
    record['ms'] = header[0][2]
    record['dur'] = header[0][3]
    record['event'] = header[0][4]
    record['lvl'] = header[0][5]
    record['source'] = line

    db_adapter.submit_line(record)


def _get_file_meta(log_name):
    re_params = r'_(\d+)_(\d\d)(\d\d)(\d\d)(\d\d)'
    prm = re.findall(re_params, log_name)[0]
    return {
        'rphost': int(prm[0]),
        'yy': int(prm[1]),
        'mm': int(prm[2]),
        'dd': int(prm[3]),
        'hh': int(prm[4])
    }


def parse_log_file(con, log_name, db_adapter: ABaseAdapter):
    # Download log file
    tmp_dir = tempfile.gettempdir()
    out_name = f"{tmp_dir}/{log_name}"
    log_file = open(out_name, 'wb')
    con.retrbinary("RETR " + f'{dir_logs}/{log_name}', log_file.write)
    log_file.close()

    # Parse local copy
    log_file = open(out_name, encoding='utf-16')

    eof = False
    re_hdr = r'^\d\d:\d\d\.\d+-'
    accumulate_line = ''
    file_meta = _get_file_meta(log_name)
    file_meta['Id'] = db_adapter.submit_file(log_name)

    while True:
        log_line = log_file.readline()

        if not log_line:
            _parse_line(accumulate_line, db_adapter, file_meta)
            break
        elif re.match(re_hdr, log_line):  # new line tag found
            if len(accumulate_line) == 0:  # no line accumulated
                accumulate_line += log_line
            else:
                _parse_line(accumulate_line, db_adapter, file_meta)
                accumulate_line = log_line
                continue
        else:
            accumulate_line += log_line

    log_file.close()


class PGAdapter(ABaseAdapter):
    def __init__(self, key, base1s_id):
        super().__init__(key, base1s_id)
        self._con = psycopg2.connect(dbname=key["db_name"], user=key["user"],
                                     password=key["pwd"], host=key["host"])

    def _check_tjfile_exist(self, file_name):
        result = -1  # in case file not exist
        check_query = pgs.SQL('SELECT {} FROM {} WHERE {}={} and {}={}').format(
            pgs.Identifier('id'),
            pgs.Identifier('TJFiles'),
            pgs.Identifier('base1s'),
            pgs.Literal(self.base1s_id),
            pgs.Identifier('name'),
            pgs.Literal(file_name),
        )

        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = check_query.as_string(self._con)
        cursor.execute(qstr)
        rows = cursor.fetchall()

        if len(rows) != 0:
            result = rows[0].id

        return result

    def _remove_tjfile_data(self, file_id):
        pass

    def _get_new_tjfile_id(self, file_name):
        insert_query = pgs.SQL('INSERT INTO {}({},{},{}) VALUES ({},{},{}) RETURNING {}').format(
            pgs.Identifier('TJFiles'),
            pgs.Identifier('name'),
            pgs.Identifier('base1s'),
            pgs.Identifier('last_update'),
            pgs.Literal(file_name),
            pgs.Literal(self.base1s_id),
            pgs.Literal(datetime.now()),
            pgs.Identifier('id'),
        )

        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = insert_query.as_string(self._con)
        cursor.execute(qstr)
        self._con.commit()
        return cursor.fetchone().id


    def submit_file(self, file_name):
        file_id = self._check_tjfile_exist(file_name)
        if file_id != -1:  # this file already submitted
            self._remove_tjfile_data(file_id)
        else:
            file_id = self._get_new_tjfile_id(file_name)
        return file_id

    def submit_line(self, line):
        pass

#  ---- Test Area ---- #
class TestAdapter(ABaseAdapter):
    def _prn(self, msg):
        print(f"BaseAdapter: {msg}")

    def __init__(self, key, base1s_id):
        super().__init__(key, base1s_id)
        self._prn(f'Connect with key: {key} and base1s_id:{base1s_id}')

    def submit_file(self, file):  # return file_id
        self._prn(f'Submit file {file}')
        return 1

    def submit_line(self, line):
        self._prn(f'{line}')


class FtptjparserTestCase(TestCase):
    def setUp(self):
        self.ftp_key = KeyChain.FTP_TJ_KEYS['tjtest']

    def test_connect_server(self):
        ftp_con = connect_server(self.ftp_key)
        ftp_con.quit()

    def test_get_files_for_sync(self):
        ftp_con = connect_server(self.ftp_key)
        log_files = get_files_for_sync(ftp_con)
        print('-Follow files found:')
        for f in log_files:
            print(f)
        print('-End')
        ftp_con.quit()

    def test_parse_log_file(self):
        ftp_con = connect_server(self.ftp_key)
        log_files = get_files_for_sync(ftp_con)
        parse_log_file(ftp_con, log_files[0], PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user']))
        ftp_con.close()
