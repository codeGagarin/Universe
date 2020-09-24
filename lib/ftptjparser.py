from unittest import TestCase
import re
import ftplib
import tempfile
from datetime import datetime
import traceback

import psycopg2
import psycopg2.extras
from psycopg2 import sql as pgs

from keys import KeyChain

dir_logs = '/logs'
dir_done = f'{dir_logs}/done'
dir_fail = f'{dir_logs}/fail'



class ABaseAdapter:
    def __init__(self, key, base1s_id):
        self.base1s_id = base1s_id
        self.key = key

    def submit_file(self, file):  # return file_id
        return 0

    def submit_line(self, line):
        pass

    def update_file_status(self, file_id, lines_count, duration, isOk, fail_descr=None):
        pass


def connect_server(key):
    ftp_con = ftplib.FTP(key['host'])
    ftp_con.login(key['user'], key['pwd'])
    return ftp_con


def get_files_for_sync(con, max_count=1000):
    result = []
    files = con.mlsd(dir_logs)
    count = 0
    for f in files:
        if f[0].find('.log') != -1:
            result.append(f[0])
            count += 1
            if count == max_count:
                break
    return result


def _parse_line(line: str, db_adapter: ABaseAdapter, file_meta):
    re_header = r'^(\d\d):(\d\d)\.(\d+)-(\d+),(\w+),(\d+),'
    header = re.findall(re_header, line)
    print(f'{line}\n')
    re_params = r'([\w:]+)=([^,\r]+)'
    params = {g[0]: g[1] for g in re.findall(re_params, line)}

    record = file_meta.copy()
    record['min'] = header[0][0]
    record['ss'] = header[0][1]
    record['ms'] = header[0][2]
    record['dur'] = header[0][3]
    record['event'] = header[0][4]
    record['lvl'] = header[0][5]

    record['osthread'] = params.get('OSThread')
    record['exception'] = params.get('Exception')
    record['descr'] = params.get('Descr')

    r = record
    record['stamp'] = datetime(2000+int(r['yy']), int(r['mm']), int(r['dd']),
                               int(r['hh']), int(r['min']), int(r['ss']), int(r['ms']))

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


def parse_log_file(ftp_con, log_name, db_adapter: ABaseAdapter):
    # Download log file
    begin = datetime.now()
    lines_count = 0
    is_ok = False
    fail_descr = None

    tmp_dir = tempfile.gettempdir()
    out_name = f"{tmp_dir}/{log_name}"
    log_file = open(out_name, 'wb')
    ftp_con.retrbinary("RETR " + f'{dir_logs}/{log_name}', log_file.write)
    log_file.close()

    # Parse local copy
    log_file = open(out_name, encoding='utf-16')

    re_hdr = r'^\d\d:\d\d\.\d+-'
    accumulate_line = ''
    file_meta = _get_file_meta(log_name)
    file_id = db_adapter.submit_file(log_name)
    file_meta['file_id'] = file_id

    try:
        while True:
            log_line = log_file.readline()

            if not log_line:
                if len(accumulate_line):
                    _parse_line(accumulate_line, db_adapter, file_meta)
                    lines_count += 1
                break
            elif re.match(re_hdr, log_line):  # new line tag found
                if len(accumulate_line) == 0:  # no line accumulated
                    accumulate_line += log_line
                else:
                    _parse_line(accumulate_line, db_adapter, file_meta)
                    lines_count += 1
                    accumulate_line = log_line
                    continue
            else:
                accumulate_line += log_line
    except Exception:
        fail_descr = traceback.format_exc()
        is_ok = False
    else:
        is_ok = True

    ftp_con.rename(f'{dir_logs}/{log_name}', f'{dir_done if is_ok else dir_fail}/{log_name}')
    log_file.close()
    duration = (datetime.now() - begin).seconds
    db_adapter.update_file_status(file_id, lines_count, duration, is_ok, fail_descr)
    return is_ok


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
        delete_query = pgs.SQL('DELETE FROM {} WHERE {}={}').format(
            pgs.Identifier('TJLines'),
            pgs.Identifier('file_id'),
            pgs.Literal(file_id)
        )

        params = {
            'lines_count': 0,
            'duration': 0,
            'status': 'update',
            'fail_descr': None,
            'last_update': datetime.now()
        }

        update_query = pgs.SQL('UPDATE {} SET ({}) = ({}) WHERE {}={}').format(
            pgs.Identifier('TJFiles'),
            pgs.SQL(', ').join(pgs.Identifier(key) for key in params.keys()),
            pgs.SQL(', ').join(pgs.Literal(value) for value in params.values()),
            pgs.Identifier('id'),
            pgs.Literal(file_id)
        )

        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(delete_query)
        cursor.execute(update_query)
        self._con.commit()


    def _get_new_tjfile_id(self, file_name):
        params = {
            'lines_count': 0,
            'duration': 0,
            'status': 'new',
            'fail_descr': None,
            'last_update': datetime.now(),
            'name': file_name,
            'base1s': self.base1s_id
        }

        insert_query = pgs.SQL('INSERT INTO {}({}) VALUES ({}) RETURNING {}').format(
            pgs.Identifier('TJFiles'),
            pgs.SQL(', ').join(pgs.Identifier(key) for key in params.keys()),
            pgs.SQL(', ').join(pgs.Literal(value) for value in params.values()),
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

    def update_file_status(self, file_id, lines_count, duration, isOk, fail_descr=None):
        params = {
            'lines_count': lines_count,
            'duration': duration,
            'status': 'done' if isOk else 'fail',
            'fail_descr':  fail_descr
        }
        update_query = pgs.SQL('UPDATE {} SET ({})=({}) WHERE {}={}').format(
            pgs.Identifier('TJFiles'),
            pgs.SQL(', ').join(pgs.Identifier(key) for key in params.keys()),
            pgs.SQL(', ').join(pgs.Literal(value) for value in params.values()),
            pgs.Identifier('id'),
            pgs.Literal(file_id),
        )

        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = update_query.as_string(self._con)
        cursor.execute(qstr)
        self._con.commit()
        pass

    def submit_line(self, line):
        insert_query = pgs.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            pgs.Identifier('TJLines'),
            pgs.SQL(', ').join(pgs.Identifier(field) for field in line.keys()),
            pgs.SQL(', ').join(pgs.Literal(value) for value in line.values())
        )
        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = insert_query.as_string(self._con)
        cursor.execute(qstr)
        self._con.commit()

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

    def test_submit_update_file(self):
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        file_name = f'test_{int(datetime.now().timestamp())}.log'
        file_id = adapter.submit_file(file_name)
        adapter.update_file_status(file_id, 10, 1, False, 'Epic fail')
        file_id = adapter.submit_file(file_name)
        adapter.update_file_status(file_id, 27, 3, True)


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
        log_files = get_files_for_sync(ftp_con, 150)
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        for file in log_files:
            parse_log_file(ftp_con, file, adapter)
        ftp_con.close()

