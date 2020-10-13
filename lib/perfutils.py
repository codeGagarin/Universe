from unittest import TestCase
import re
import ftplib
import tempfile
from datetime import datetime
from datetime import timedelta
from collections import namedtuple
import csv

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

type_cntr = 'cntr'
dir_cntr = f'/{type_cntr}'
dir_cntr_done = f'{type_cntr}/done'
dir_cntr_fail = f'{type_cntr}/fail'

file_tree = {t:
    {
        'root': f'/{t}',
        'done': f'{t}/done',
        'fail': f'{t}/fail'
    } for t in (type_logs, type_apdx, type_cntr)}


class Period:
    def __init__(self, stamp: datetime = None):
        if stamp is None:
            self.begin = None
            self.end = None
        else:
            self.begin = datetime(
                stamp.year, stamp.month, stamp.day, stamp.hour)
            self.end = self.begin + timedelta(hours=1) - timedelta(microseconds=1)

    def __eq__(self, other):
        return other is not None and self.begin == other.begin and self.end == other.end

    def is_empty(self):
        return True if self.begin is None else False


class ABaseAdapter:
    def __init__(self, key, base1s_id):
        self.base1s_id = base1s_id
        self.key = key
        self.log_data = {
            type_logs: {'done': 0, 'fail': 0},
            type_apdx: {'done': 0, 'fail': 0, 'periods': 0},
            type_cntr: {'done': 0, 'fail': 0},
        }
        self.GMT = key.get('GMT', 0)

    def get_log_str(self):
        result = f'{self.base1s_id} '
        logs = self.log_data.get(type_logs)
        result += f"[{type_logs}]:f.{logs['fail']}:d.{logs['done']} "
        apdx = self.log_data.get(type_apdx)
        result += f"[{type_apdx}]:f.{apdx['fail']}:d.{apdx['done']}:p.{apdx['periods']} "
        cntr = self.log_data.get(type_cntr)
        result += f"[{type_cntr}]:f.{cntr['fail']}:d.{cntr['done']} "
        return result

    def submit_file(self, file, file_type):  # return file_id
        pass

    def submit_line(self, line, line_type):
        pass

    def update_file_status(self, file_id, duration, isOk, fail_descr=None):
        pass

    def apdx_get_next_period(self) -> Period:  # return next empty period for APDEX calc or None
        pass

    def apdx_get_ops_for(self, p: Period) -> list:  # return ops list
        pass

    def apdx_get_n_ns_nt_for(self, operation, p: Period):  # return dict
        pass

    def apdx_set_for(self, operation, p: Period, apdex_value):  # set APDEX value for ops/hour
        pass


def _process_unify(ftp_key, adapter, files_getter, file_parser, file_type, max_files=500, move_done=True):
    ftp_con = connect_server(ftp_key)
    log_files = files_getter(ftp_con, max_files)
    files_done = 0
    files_fail = 0
    for file in log_files:
        is_ok = file_parser(ftp_con, file, adapter, move_done)
        if is_ok:
            files_done += 1
        else:
            files_fail += 1
    adapter.log_data[file_type]['done'] = files_done
    adapter.log_data[file_type]['fail'] = files_fail
    ftp_con.close()


def process_cntr(ftp_key, adapter, max_files=500, move_done=True):
    _process_unify(ftp_key, adapter, get_cntr_files_for_sync, parse_cntr_file, type_cntr, max_files, move_done)


def process_apdx(ftp_key, adapter, max_files=500, move_done=True):
    _process_unify(ftp_key, adapter, get_apdx_files_for_sync, parse_apdx_file, type_apdx, max_files, move_done)
    _calculate_apdex(adapter)


def _calculate_apdex(adapter: ABaseAdapter):
    max_hours = 24
    i = 0
    while True:
        period = adapter.apdx_get_next_period()
        if period.is_empty() or i >= max_hours:
            break
        i += 1
        ops_list = adapter.apdx_get_ops_for(period)
        for ops in ops_list:
            v = adapter.apdx_get_n_ns_nt_for(ops, period)
            adpex = (v.ns + v.nt / 2) / v.n
            adapter.apdx_set_for(ops, period, adpex)
    adapter.log_data[type_apdx]['periods'] = i


def process_logs(ftp_key, adapter, max_files=500, move_done=True):
    _process_unify(ftp_key, adapter, get_tj_files_for_sync, parse_logs_file, type_logs, max_files=500,
                   move_done=move_done)


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


def get_cntr_files_for_sync(ftp_con, max_count=1000):
    return _get_file_list(ftp_con, dir_cntr, 'csv', max_count)


def _parse_tj_line(line: str, db_adapter: ABaseAdapter, file_meta):
    re_header = r'^(\d\d):(\d\d)\.(\d+)-(\d+),(\w+),(\d+),'
    header = re.findall(re_header, line)
    re_params = r'([\w:]+)=([^,\r]+)'
    params = {g[0]: g[1] for g in re.findall(re_params, line)}

    yy = 2000 + int(file_meta['yy'])
    mm = int(file_meta['mm'])
    dd = int(file_meta['dd'])
    hh = int(file_meta['hh'])
    mnt = int(header[0][0])
    ss = int(header[0][1])
    ms = int(header[0][2])

    local_stamp = datetime(yy, mm, dd, hh, mnt, ss, ms)
    utc_stamp = local_stamp - timedelta(minutes=180) + timedelta(hours=db_adapter.GMT)  # GMT+3 compensation
    record = {
        'file_id': file_meta['file_id'],
        'rphost': file_meta['rphost'],
        'ms': ms,
        'dur': header[0][3],
        'event': header[0][4], 'lvl': header[0][5],
        'osthread': params.get('OSThread'),
        'exception': params.get('Exception'),
        'descr': params.get('Descr'),
        'stamp': utc_stamp,
        'source': line
    }
    db_adapter.submit_line(record, type_logs)


def _parser_logs(local_file, file_id, file_name, db_adapter):
    # Parse local copy
    log_file = open(local_file, encoding='utf-16')
    with log_file:
        def get_meta(log_name):
            re_params = r'_(\d+)_(\d\d)(\d\d)(\d\d)(\d\d)'
            prm = re.findall(re_params, log_name)[0]
            return {
                'rphost': int(prm[0]),
                'yy': int(prm[1]),
                'mm': int(prm[2]),
                'dd': int(prm[3]),
                'hh': int(prm[4])
            }

        re_hdr = r'^\d\d:\d\d\.\d+-'
        accumulate_line = ''
        file_meta = get_meta(file_name)
        file_meta['file_id'] = file_id

        while True:
            log_line = log_file.readline()

            if not log_line:
                if len(accumulate_line):
                    _parse_tj_line(accumulate_line, db_adapter, file_meta)
                break
            elif re.match(re_hdr, log_line):  # new line tag found
                if len(accumulate_line) == 0:  # no line accumulated
                    accumulate_line += log_line
                else:
                    _parse_tj_line(accumulate_line, db_adapter, file_meta)
                    accumulate_line = log_line
                    continue
            else:
                accumulate_line += log_line


def _parser_cntr(local_file, file_id, file_name, db_adapter):
    # Parse local copy
    cntr_file = open(local_file, encoding="utf-16")
    with cntr_file:
        def get_hdr_params(the_hdr):
            pure_hdr = the_hdr[1: len(the_hdr)]
            re_hrp = r'\\\\(.+)\\(.+)\\(.+)'
            raw = re.findall(re_hrp, '\n'.join(pure_hdr))
            result = {
                i: {
                    'id': pure_hdr[i],
                    'host': raw[i][0],
                    'context': raw[i][1],
                    'type': raw[i][2],
                } for i in range(0, len(raw))}
            return result

        cntr_data = iter(csv.reader(cntr_file))
        hdr = next(cntr_data)
        params = get_hdr_params(hdr)
        for cntr_line in cntr_data:
            stamp = datetime.strptime(cntr_line[0], '%m/%d/%Y %H:%M:%S.%f') \
                    - timedelta(hours=3) + timedelta(hours=db_adapter.GMT)  # GMT 0 correction
            cntr_values = cntr_line[1: len(cntr_line)]
            for i in range(0, len(cntr_values)):
                str_value = cntr_values[i].replace(' ', '')
                line = {
                    'file_id': file_id,
                    'stamp': stamp,
                    'counter': params[i]['id'],
                    'host': params[i]['host'],
                    'context': params[i]['context'],
                    'type': params[i]['type'],
                    'flt_value': float(str_value) if str_value else None,
                    'str_value': str_value,
                }
                db_adapter.submit_line(line, type_cntr)


def _parser_apdx(local_file, file_id, file_name, db_adapter):
    apdx_file = open(local_file)
    with apdx_file:
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
                    'start': datetime.strptime(measure_attribute['tSaveUTC'], '%Y-%m-%dT%H:%M:%S')
                             + timedelta(hours=db_adapter.GMT),
                    'session': int(measure_attribute['sessionNumber']),
                    'fail': not bool(measure_attribute['runningError']),
                    'target': float(ops_attribute['targetValue']),
                    'priority': ops_attribute['priority'],
                }
                line['status'] = 'NS' if line['target'] >= line['duration'] else 'NT'
                db_adapter.submit_line(line, type_apdx)


def _parse_unify_file(ftp_con, file_type, file_name, db_adapter, parser, move_done):
    # Download log file
    begin = datetime.now()
    is_ok = False
    fail_descr = None

    root_dir = file_tree[file_type]["root"]
    done_dir = file_tree[file_type]["done"]
    fail_dir = file_tree[file_type]["fail"]

    tmp_dir = tempfile.gettempdir()
    out_name = f"{tmp_dir}/{file_name}"
    parse_file = open(out_name, 'wb')
    ftp_con.retrbinary("RETR " + f'{root_dir}/{file_name}', parse_file.write)
    parse_file.close()

    file_id = db_adapter.submit_file(file_name, file_type)

    try:
        parser(out_name, file_id, file_name, db_adapter)
        is_ok = True
    except Exception:
        if not move_done:
            raise Exception
        fail_descr = traceback.format_exc()
        is_ok = False
    else:
        is_ok = True

    if move_done:
        ftp_con.rename(f'{root_dir}/{file_name}', f'{done_dir if is_ok else fail_dir}/{file_name}')

    duration = (datetime.now() - begin).seconds
    db_adapter.update_file_status(file_id, duration, is_ok, fail_descr)
    return is_ok


def parse_cntr_file(ftp_con, cntr_name, db_adapter: ABaseAdapter, move_done=True):
    _parse_unify_file(ftp_con, type_cntr, cntr_name, db_adapter, _parser_cntr, move_done)


def parse_logs_file(ftp_con, logs_name, db_adapter: ABaseAdapter, move_done=True):
    _parse_unify_file(ftp_con, type_logs, logs_name, db_adapter, _parser_logs, move_done)


def parse_apdx_file(ftp_con, apdx_name, db_adapter: ABaseAdapter, move_done=True):
    _parse_unify_file(ftp_con, type_apdx, apdx_name, db_adapter, _parser_apdx, move_done)


class PGAdapter(ABaseAdapter):
    tables = {
        type_logs: 'TJLines',
        type_apdx: 'ApdexLines',
        type_cntr: 'CounterLines'
    }

    def __init__(self, key, base1s_id):
        super().__init__(key, base1s_id)
        self._con = psycopg2.connect(dbname=key["db_name"], user=key["user"],
                                     password=key["pwd"], host=key["host"], port=key.get('port'))
        self._batch_params = []
        self._batch_type = ''
        self._batch_hdr = ()

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
        self._batch_params.clear()
        return file_id

    def update_file_status(self, file_id, duration, isOk, fail_descr=None):
        lines_count = len(self._batch_params)
        params = {
            'lines_count': lines_count,
            'duration': duration,
            'status': 'done' if isOk else 'fail',
            'fail_descr': fail_descr
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

        if len(self._batch_params):
            params_count = len(self._batch_params[0])
            hdr_params = [self.tables[self._batch_type]] + self._batch_hdr

            part_into = ' ('+', '.join(['{}' for i in range(0, params_count)])+')'
            part_values = ' ('+', '.join(['%s' for i in range(0, params_count)])+')'

            batch_query = pgs.SQL('insert into {} ' + part_into).format(
                *[pgs.Identifier(h) for h in hdr_params]
            ) + pgs.SQL(' values ' + part_values)
            psycopg2.extras.execute_batch(cursor, batch_query, self._batch_params)

        self._con.commit()
        self._batch_params.clear()

    def _unify_line_submit(self, line, table_name):
        insert_query = pgs.SQL('INSERT INTO {} ({}) VALUES ({})').format(
            pgs.Identifier(table_name),
            pgs.SQL(', ').join(pgs.Identifier(field) for field in line.keys()),
            pgs.SQL(', ').join(pgs.Literal(value) for value in line.values())
        )
        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = insert_query.as_string(self._con)
        cursor.execute(qstr)

    def submit_line(self, line, line_type):
        line['base1s'] = self.base1s_id
        #  self._unify_line_submit(line, self.tables[line_type])
        self._batch_params.append(list(line.values()))
        self._batch_type = line_type
        self._batch_hdr = list(line.keys())

    def apdx_get_next_period(self):  # return next empty period for APDEX calc or None
        select_query = pgs.SQL(
            'SELECT {} FROM {} WHERE {}={} AND {} is Null ORDER BY {} DESC LIMIT 1').format(
            pgs.Identifier('start'),
            pgs.Identifier('ApdexLines'),
            pgs.Identifier('base1s'),
            pgs.Literal(self.base1s_id),
            pgs.Identifier('apdex'),
            pgs.Identifier('start'),
        )
        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        qstr = select_query.as_string(self._con)
        cursor.execute(qstr)
        rows = cursor.fetchall()
        return Period(rows[0].start) if len(rows) != 0 else Period()

    def apdx_get_ops_for(self, period: Period):  # return ops list
        # select distinct ops_uid from "ApdexLines" where start between '2020-10-01' and '2020-10-02' order by ops_uid
        select_query = pgs.SQL('SELECT DISTINCT {} FROM {} WHERE {} BETWEEN {} AND {} AND {}={}').format(
            pgs.Identifier('ops_uid'),
            pgs.Identifier('ApdexLines'),
            pgs.Identifier('start'),
            pgs.Literal(period.begin),
            pgs.Literal(period.end),
            pgs.Identifier('base1s'),
            pgs.Literal(self.base1s_id),
        )

        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(select_query)
        rows = cursor.fetchall()
        return [row.ops_uid for row in rows]

    def apdx_get_n_ns_nt_for(self, operation, period: Period):  # return dict
        n_query = pgs.SQL('SELECT count(*) FROM {} WHERE {} BETWEEN {} AND {} AND {}={} AND {}={}').format(
            pgs.Identifier('ApdexLines'),
            pgs.Identifier('start'),
            pgs.Literal(period.begin),
            pgs.Literal(period.end),
            pgs.Identifier('ops_uid'),
            pgs.Literal(operation),
            pgs.Identifier('base1s'),
            pgs.Literal(self.base1s_id),
        )
        ns_query = n_query + pgs.SQL(' AND {}={}').format(
            pgs.Identifier('status'),
            pgs.Literal('NS'),
        )
        nt_query = n_query + pgs.SQL(' AND {}={}').format(
            pgs.Identifier('status'),
            pgs.Literal('NT'),
        )
        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(n_query)
        n = cursor.fetchone()[0]
        cursor.execute(ns_query)
        ns = cursor.fetchone()[0]
        cursor.execute(nt_query)
        nt = cursor.fetchone()[0]
        return namedtuple('AdpexParams', 'n, ns, nt')(n, ns, nt)

    def apdx_set_for(self, operation, period: Period, apdex_value):  # set APDEX value for ops/hour
        update_query = pgs.SQL('UPDATE {} SET {}={} WHERE {} BETWEEN {} AND {} AND {}={} AND {}={}').format(
            pgs.Identifier('ApdexLines'),
            pgs.Identifier('apdex'),
            pgs.Literal(apdex_value),
            pgs.Identifier('start'),
            pgs.Literal(period.begin),
            pgs.Literal(period.end),
            pgs.Identifier('base1s'),
            pgs.Literal(self.base1s_id),
            pgs.Identifier('ops_uid'),
            pgs.Literal(operation),
        )
        cursor = self._con.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(update_query)
        self._con.commit()


class FtptjparserTestCase(TestCase):
    def setUp(self):
        self.ftp_key = KeyChain.FTP_TJ_KEYS['tjtest']
        self.pg_key = KeyChain.PG_YANDEX_PERF_KEY

    def test_process_cntr(self):
        adapter = PGAdapter(self.pg_key, self.ftp_key['user'])
        process_cntr(self.ftp_key, adapter, move_done=False, max_files=1)
        print(adapter.get_log_str())

    def test_submit_update_file(self):
        adapter = PGAdapter(self.pg_key, self.ftp_key['user'])
        file_name = f'test_{int(datetime.now().timestamp())}.log'
        file_id = adapter.submit_file(file_name, type_logs)
        adapter.update_file_status(file_id, 1, False, 'Epic fail')
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
        adapter = PGAdapter(self.pg_key, self.ftp_key['user'])
        for file in log_files:
            parse_logs_file(ftp_con, file, adapter, False)
        ftp_con.close()

    def test_parse_apdx_file(self):
        ftp_con = connect_server(self.ftp_key)
        log_files = get_apdx_files_for_sync(ftp_con, 150)
        adapter = PGAdapter(self.pg_key, self.ftp_key['user'])
        for file in log_files:
            parse_apdx_file(ftp_con, file, adapter, False)
        ftp_con.close()

    def test_process_logs(self):
        adapter = PGAdapter(self.pg_key, self.ftp_key['user'])
        process_logs(self.ftp_key, adapter, move_done=False)
        print(adapter.get_log_str())

    def test_process_apdx(self):
        adapter = PGAdapter(self.pg_key, self.ftp_key['user'])
        process_apdx(self.ftp_key, adapter, move_done=False, max_files=1)
        print(adapter.get_log_str())
