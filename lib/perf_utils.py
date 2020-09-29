from unittest import TestCase
import re
import ftplib
import tempfile
from datetime import datetime
import traceback
import xml.etree.ElementTree as ET

import psycopg2
import psycopg2.extras
from psycopg2 import sql as pgs

from keys import KeyChain

type_logs = 'logs'
dir_logs = f'/{type_logs}'
dir_logs_done = f'{dir_logs}/done'
dir_logs_fail = f'{dir_logs}/fail'

type_apdx = 'apdx'
dir_apdx = f'/{type_apdx}'
dir_apdx_done = f'{dir_apdx}/done'
dir_apdx_fail = f'{dir_apdx}/fail'

class ABaseAdapter:
    def __init__(self, key, base1s_id):
        self.base1s_id = base1s_id
        self.key = key

    def submit_file(self, file, file_type):  # return file_id
        return 0

    def submit_line(self, line, line_type):
        pass

    def update_file_status(self, file_id, lines_count, duration, isOk, fail_descr=None):
        pass


def _process_unify(ftp_key, adapter, files_getter, file_parser, file_type, max_files=500, move_done=True):
    ftp_con = connect_server(ftp_key)
    log_files = files_getter(ftp_con, max_files)
    files_ok = 0
    files_fail = 0
    for file in log_files:
        is_ok = file_parser(ftp_con, file, adapter, move_done)
        if is_ok:
            files_ok += 1
        else:
            files_fail += 1
    print(f"{ftp_key['user']}: [{file_type}] f.{files_fail}:k.{files_ok}")
    ftp_con.close()


def process_apdx(ftp_key, adapter, max_files=500, move_done=True):
    _process_unify(ftp_key, adapter, get_apdx_files_for_sync, parse_apdx_file, type_apdx, max_files=500, move_done=move_done)


def process_logs(ftp_key, adapter, max_files=500, move_done=True):
    _process_unify(ftp_key, adapter, get_tj_files_for_sync, parse_log_file, type_logs, max_files=500, move_done=move_done)

def connect_server(key):
    ftp_con = ftplib.FTP(key['host'])
    ftp_con.login(key['user'], key['pwd'])
    return ftp_con


def _get_file_list(ftp_con, dir, ext, max_count):
    result = []
    files = ftp_con.mlsd(dir)
    count = 0
    for f in files:
        if f[0].find(f'.{ext}') != -1:
            result.append(f[0])
            count += 1
            if count == max_count:
                break
    return result


def get_tj_files_for_sync(ftp_con, max_count=1000):
    return _get_file_list(ftp_con, dir_logs, 'log', max_count)


def get_apdx_files_for_sync(ftp_con, max_count=1000):
    return _get_file_list(ftp_con, dir_apdx, 'xml', max_count)


def _parse_tj_line(line: str, db_adapter: ABaseAdapter, file_meta):
    re_header = r'^(\d\d):(\d\d)\.(\d+)-(\d+),(\w+),(\d+),'
    header = re.findall(re_header, line)
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
    db_adapter.submit_line(record, type_logs)


def _get_tj_file_meta(log_name):
    re_params = r'_(\d+)_(\d\d)(\d\d)(\d\d)(\d\d)'
    prm = re.findall(re_params, log_name)[0]
    return {
        'rphost': int(prm[0]),
        'yy': int(prm[1]),
        'mm': int(prm[2]),
        'dd': int(prm[3]),
        'hh': int(prm[4])
    }


def parse_log_file(ftp_con, log_name, db_adapter: ABaseAdapter, move_done=True):
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
    file_meta = _get_tj_file_meta(log_name)
    file_id = db_adapter.submit_file(log_name, type_logs)
    file_meta['file_id'] = file_id

    try:
        while True:
            log_line = log_file.readline()

            if not log_line:
                if len(accumulate_line):
                    _parse_tj_line(accumulate_line, db_adapter, file_meta)
                    lines_count += 1
                break
            elif re.match(re_hdr, log_line):  # new line tag found
                if len(accumulate_line) == 0:  # no line accumulated
                    accumulate_line += log_line
                else:
                    _parse_tj_line(accumulate_line, db_adapter, file_meta)
                    lines_count += 1
                    accumulate_line = log_line
                    continue
            else:
                accumulate_line += log_line
    except Exception:
        if not move_done:
            raise Exception
        fail_descr = traceback.format_exc()
        is_ok = False
    else:
        is_ok = True

    if move_done:
        ftp_con.rename(f'{dir_logs}/{log_name}', f'{dir_logs_done if is_ok else dir_logs_fail}/{log_name}')
    log_file.close()
    duration = (datetime.now() - begin).seconds
    db_adapter.update_file_status(file_id, lines_count, duration, is_ok, fail_descr)
    return is_ok


def parse_apdx_file(ftp_con, apdx_name, db_adapter: ABaseAdapter, move_done=True):
    # Download log file
    begin = datetime.now()
    lines_count = 0
    is_ok = False
    fail_descr = None

    tmp_dir = tempfile.gettempdir()
    out_name = f"{tmp_dir}/{apdx_name}"
    apdx_file = open(out_name, 'wb')
    ftp_con.retrbinary("RETR " + f'{dir_apdx}/{apdx_name}', apdx_file.write)
    apdx_file.close()

    # Parse local copy
    # apdx_file = open(out_name, encoding='cp1251')
    apdx_file = open(out_name)

    file_id = db_adapter.submit_file(apdx_name, type_apdx)

    try:
        root = ET.parse(apdx_file).getroot()
        for ops in root:
            for measure in ops:
                ops_attribute = ops.attrib
                measure_attribute = measure.attrib
                line = {
                    'file_id': file_id,
                    'ops_uid': ops_attribute['uid'],
                    'ops_name': ops_attribute['name'],
                    'duration': float(measure_attribute['value']),
                    'user': measure_attribute['userName'],
                    'start': datetime.strptime(measure_attribute['tSaveUTC'], '%Y-%m-%dT%H:%M:%S'),
                    'session': int(measure_attribute['sessionNumber']),
                    'fail': not bool(measure_attribute['runningError']),
                }
                db_adapter.submit_line(line, type_apdx)

    except Exception:
        if not move_done:
            raise Exception
        fail_descr = traceback.format_exc()
        is_ok = False
    else:
        is_ok = True

    if move_done:
        ftp_con.rename(f'{dir_apdx}/{apdx_name}', f'{dir_apdx_done if is_ok else dir_apdx_fail}/{apdx_name}')

    apdx_file.close()
    duration = (datetime.now() - begin).seconds
    db_adapter.update_file_status(file_id, lines_count, duration, is_ok, fail_descr)
    return is_ok


class PGAdapter(ABaseAdapter):
    tables = {
        type_logs: 'TJLines',
        type_apdx: 'ApdexLines'
    }

    def __init__(self, key, base1s_id):
        super().__init__(key, base1s_id)
        self._con = psycopg2.connect(dbname=key["db_name"], user=key["user"],
                                     password=key["pwd"], host=key["host"])

    def _check_tjfile_exist(self, file_name, file_type):
        result = -1  # in case file not exist
        check_query = pgs.SQL('SELECT {} FROM {} WHERE {}={} and {}={} and {}={}').format(
            pgs.Identifier('id'),
            pgs.Identifier('TJFiles'),
            pgs.Identifier('base1s'),
            pgs.Literal(self.base1s_id),
            pgs.Identifier('name'),
            pgs.Literal(file_name),
            pgs.Identifier('type'),
            pgs.Literal(file_type),

        )

        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = check_query.as_string(self._con)
        cursor.execute(qstr)
        rows = cursor.fetchall()

        if len(rows) != 0:
            result = rows[0].id

        return result

    def _remove_file_data(self, file_id, file_type):
        delete_query = pgs.SQL('DELETE FROM {} WHERE {}={}').format(
            pgs.Identifier(self.tables[file_type]),
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


    def _get_new_tjfile_id(self, name, type):
        params = {
            'lines_count': 0,
            'duration': 0,
            'status': 'new',
            'fail_descr': None,
            'last_update': datetime.now(),
            'name': name,
            'base1s': self.base1s_id,
            'type': type,
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

    def submit_file(self, name, file_type):
        file_id = self._check_tjfile_exist(name, file_type)
        if file_id != -1:  # this file already submitted
            self._remove_file_data(file_id, file_type)
        else:
            file_id = self._get_new_tjfile_id(name, file_type)
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

    def _unify_line_submit(self, line, table_name):
        insert_query = pgs.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            pgs.Identifier(table_name),
            pgs.SQL(', ').join(pgs.Identifier(field) for field in line.keys()),
            pgs.SQL(', ').join(pgs.Literal(value) for value in line.values())
        )
        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = insert_query.as_string(self._con)
        cursor.execute(qstr)
        self._con.commit()

    def _logs_line_submit(self, line):
        self._unify_line_submit(line, self.tables[type_logs])

    def _apdx_line_submit(self, line):
        self._unify_line_submit(line, self.tables[type_apdx])

    def submit_line(self, line, line_type):
        line_submitters = {
            type_logs: self._logs_line_submit,
            type_apdx: self._apdx_line_submit
        }
        line_submitters[line_type](line)

#  ---- Test Area ---- #
class TestAdapter(ABaseAdapter):
    def _prn(self, msg):
        print(f"BaseAdapter: {msg}")

    def __init__(self, key, base1s_id):
        super().__init__(key, base1s_id)
        self._prn(f'Connect with key: {key} and base1s_id:{base1s_id}')

    def submit_file(self, name, file_type):  # return file_id
        self._prn(f'Submit file {name}:{file_type}')
        return 1

    def submit_line(self, line, line_type):
        self._prn(f'{line_type}:{line}')


class FtptjparserTestCase(TestCase):
    def setUp(self):
        self.ftp_key = KeyChain.FTP_TJ_KEYS['tjtest']

    def test_submit_update_file(self):
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        file_name = f'test_{int(datetime.now().timestamp())}.log'
        file_id = adapter.submit_file(file_name, type_logs)
        adapter.update_file_status(file_id, 10, 1, False, 'Epic fail')
        file_id = adapter.submit_file(file_name, type_logs)
        adapter.update_file_status(file_id, 27, 3, True)

    def test_connect_server(self):
        ftp_con = connect_server(self.ftp_key)
        ftp_con.quit()

    def test_get_files_for_sync(self):
        ftp_con = connect_server(self.ftp_key)
        log_files = get_tj_files_for_sync(ftp_con)
        print('-Follow files found:')
        for f in log_files:
            print(f)
        print('-End')
        ftp_con.quit()

    def test_parse_log_file(self):
        ftp_con = connect_server(self.ftp_key)
        log_files = get_tj_files_for_sync(ftp_con, 150)
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        for file in log_files:
            parse_log_file(ftp_con, file, adapter, False)
        ftp_con.close()

    def test_parse_apdx_file(self):
        ftp_con = connect_server(self.ftp_key)
        log_files = get_apdx_files_for_sync(ftp_con, 150)
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        for file in log_files:
            parse_apdx_file(ftp_con, file, adapter, False)
        ftp_con.close()
        ftp_con.close()

    def test_process_logs(self):
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        process_logs(self.ftp_key, adapter, move_done=False)

    def test_process_apdx(self):
        adapter = PGAdapter(KeyChain.PG_PERF_KEY, self.ftp_key['user'])
        process_apdx(self.ftp_key, adapter, move_done=False)