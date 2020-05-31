from datetime import datetime, date, timedelta
from unittest import TestCase
import json
import hashlib
from collections import namedtuple
import cProfile, pstats, io
import inspect
from html import escape

import psycopg2
from psycopg2 import sql
from psycopg2 import extras

_eval_map = {5: 4, 4: 2, 3: 5, 2: 6, 1: 1}


def _hash(s: str):
    """ bigint hash for random string, presents params ID for params storage """
    return int(hashlib.shake_128(s.encode()).hexdigest(7), 18)


def _dt_min(d: date):
    return datetime.combine(d, datetime.min.time())


def _dt_max(d: date):
    return datetime.combine(d, datetime.max.time())


def _get_period(now: date, period_type: str, delta: int, with_time=False):
    """
    Возвращает словарь с параметрами периода
    :param now: текущая дата
    :param period_type: тип периода day, week, month, qtr
    :param delta: сколько периодов отнять или прибавить
    :return: параметры периода:
        'from' -- дата начала
        'to' -- дата окончания
        'id' -- идентификатор периода 1945 (45 неделя 19-го года)
        'type' -- тип периода (см: period_type)
        'with_time' -- 'from' and 'to' is datetime type
    """
    result = {'from': None, 'to': None, 'type': period_type, 'id': None}
    if period_type is 'month':
        y = now.year
        m = now.month
        sign = -1 if delta < 0 else 1
        delta_y = (delta * sign // 12) * sign
        delta_m = (delta * sign % 12) * sign
        y += delta_y
        m += delta_m
        if m > 12:
            y += 1
            m -= 12
        elif m < 1:
            y -= 1
            m += 12
        result['from'] = date(y, m, 1)
        y, m = (y, m + 1) if m != 12 else (y + 1, 1)
        next_month = date(y, m, 1)
        result['to'] = next_month - timedelta(days=1)
        result['id'] = result['from'].strftime('%-mm%y')
    elif period_type is 'week':
        result['from'] = now + timedelta(days=-now.isoweekday() + 1, weeks=delta)
        result['to'] = result['from'] + timedelta(days=6)
        result['id'] = result['from'].strftime('%-Ww%y')
    elif period_type is 'day':
        result['from'] = now + timedelta(days=delta)
        result['to'] = result['from']
        result['id'] = result['from'].strftime('%m%d')
    if with_time:
        result['from'] = _dt_min(result['from'])
        result['to'] = _dt_max(result['to'])
    return result


class Report:
    _report_map = None

    def set_up(self, url_params):  # safety plug, optional override in subclass
        pass

    def _D(self):
        """ Jinja call for get_data() method """
        return self._data

    def _N(self):
        return self._navigate

    def _P(self):
        """ Jinja call for _params attribute """
        return self._params

    def __init__(self, params=None, url_params=None):
        self._data = None
        self.web_server_name = ''  # using for absolute url generation for email report
        self._params = params if params else {}
        self._navigate = {}
        self._EXT = 0  # get_data execution time, jinja profiling
        self.set_up(url_params)

    def _prepare_data(self, db_conn):
        """
        only this method takes "DB connection" object,
        it should be overwritten in Report subclasses
        """
        pass

    def url_for_static(self, res_path):
        return '{}/static/{}'.format(
            self.web_server_name,
            res_path
        )

    def url_for(self, target, params=None):
        if not params:
            params = {}
        return '{}/{}/?{}'.format(
            self.web_server_name,
            target,
            '&'.join('{}={}'.format(escape(str(k)), escape(str(v))) for k, v in params.items())
        )

    @classmethod
    def get_connection(cls, acc_key=None):
        cn = psycopg2.connect(dbname=acc_key["db_name"], user=acc_key["user"],
                              password=acc_key["pwd"], host=acc_key["host"])
        cn.autocommit = False
        return {'cn': cn, 'ql': [], 'pp': [], 'need_stat': None}

    def request_data(self, conn):
        if conn['need_stat']:
            pr = cProfile.Profile()
            pr.enable()
            start = datetime.now()

        self._data = self._prepare_data(conn)

        # todo: add postpone query call here
        conn['cn'].commit()
        # conn['cn'].close()

        if conn['need_stat']:
            self._params["query_list"] = conn['ql']
            self._EXT = datetime.now() - start
            pr.disable()
            s = io.StringIO()
            ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
            ps.print_stats()
            self._params['pstats'] = s.getvalue()  # using into report profile_stat macro

    @classmethod
    def get_type(cls):
        return cls.__name__

    @classmethod
    def _get_map(cls):
        def _reg(c):
            cls._report_map[c.get_type()] = c

        if not cls._report_map:
            cls._report_map = {}
            _reg(DiagReport)
            _reg(HelpdeskReport)
            _reg(TaskReport)
            _reg(ExpensesReport)
        return cls._report_map

    @classmethod
    def factory(cls, conn, url_params):
        """ Produce report object from IDX with DB params resolved """
        idx = url_params['idx']
        params = cls._idx_to_params(conn, idx)
        if not params:
            return None
        conn['need_stat'] = url_params.get('stat', None)
        report_class = cls._get_map().get(params.get('type'))
        if not report_class:
            return None
        return report_class(params, url_params)

    @classmethod
    def default_map(cls, conn):
        result = {}
        for report_name, report_class in cls._get_map().items():
            for param_name, param in report_class.get_report_map().items():
                result[f"{report_name} -- {param_name}"] = report_class.get_idx(conn, param['params'])
        return result

    @classmethod
    def _json_to_dict(cls, value: str):
        def dict_converter(source):
            # scan json-source tree and replace datetime strings to datetime objects
            def date_converter(s):
                try:
                    return datetime.strptime(s, 'Datetime:%Y-%m-%d %H:%M:%S.%f')
                except:
                    pass
                try:
                    return datetime.strptime(s, 'Date:%Y-%m-%d').date()
                except:
                    pass
                return s

            def list_converter(lst):
                for i in range(len(lst)):
                    r = lst[i]
                    if isinstance(r, str):
                        lst[i] = date_converter(r)
                    elif isinstance(r, dict):
                        dict_converter(r)
                    elif isinstance(r, list):
                        list_converter(r)

            for k, v in source.items():
                if isinstance(v, list):
                    list_converter(v)
                elif isinstance(v, dict):
                    dict_converter(v)
                elif isinstance(v, str):
                    source[k] = date_converter(v)
            return source

        return json.loads(value, object_hook=dict_converter)

    @classmethod
    def _dict_to_json(cls, value: dict):
        def converter(o):
            if isinstance(o, datetime):
                return o.strftime('datetime:%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(o, date):
                return o.strftime('date:%Y-%m-%d')

        return json.dumps(value, default=converter)

    @classmethod
    def _params_to_idx(cls, conn, params):
        json_params = cls._dict_to_json(params)
        value_hash = _hash(json_params)

        # try update params if exist in database
        query = sql.SQL('UPDATE "Params" SET "last_touch" = {}, "touch_count" = "touch_count"+1'
                        ' WHERE "id"={} RETURNING "id"').format(
            sql.Literal(datetime.now()),
            sql.Literal(value_hash)
        )
        query_v95 = sql.SQL('INSERT INTO "Params" ("id", "params", "last_touch", "touch_count") '
                            ' VALUES({0}, {1}, {2}, {3})'
                            ' ON CONFLICT ("id") DO UPDATE SET "last_touch"={2}, "touch_count"="touch_count+1"').format(
            sql.Literal(value_hash),
            sql.Literal(json_params),
            sql.Literal(datetime.now()),
            sql.Literal(0)
        )
        result = cls._sql_exec(conn, query)

        if not len(result):
            query = sql.SQL('INSERT INTO {} ({}, {}, {}, {}) VALUES ({}, {}, {}, {})').format(
                sql.Identifier('Params'),
                sql.Identifier('id'),
                sql.Identifier('params'),
                sql.Identifier('last_touch'),
                sql.Identifier('touch_count'),
                sql.Literal(value_hash),
                sql.Literal(json_params),
                sql.Literal(datetime.now()),
                sql.Literal(0)
            )
            cls._sql_exec(conn, query, postpone=True)
        return {'idx': value_hash}

    @classmethod
    def users_to_email(cls, conn, idlist):
        result = []
        ids_for_request = []
        for idl in idlist:
            (ids_for_request if type(idl) is int else result).append(idl)
        if len(ids_for_request):
            query = sql.SQL('SELECT "Email" as eml FROM "Users" WHERE "Id" IN {}').format(
                sql.Literal(tuple(ids_for_request)))

            def fact(row, res):
                res.append(row.eml)

            cls._sql_exec(conn, query, result, fact, named_result=True)
        return result

    @classmethod
    def _idx_to_params(cls, conn, idx):
        query = sql.SQL('UPDATE "Params" SET "last_touch" = {}, "touch_count" = "touch_count"+1'
                        ' WHERE "id"={} RETURNING "params"').format(
            sql.Literal(datetime.now()),
            sql.Literal(idx)
        )
        result = cls._sql_exec(conn, query)
        if not len(result):
            return None
        return cls._json_to_dict(result[0][0])

    @classmethod
    def _get_def_params(cls):  # todo: deprecated need to delete, should using [get_report_map]
        """ Override it in subclasses for default report link generation """
        return {}

    @classmethod
    def get_report_map(cls):
        return {}

    @classmethod
    def get_idx(cls, conn, params=None):
        """ convert report param data to uninq IDX whit DB touch action """
        if not params:
            params = cls._get_def_params()
        params['type'] = cls.get_type()
        return cls._params_to_idx(conn, params)

    @classmethod
    def _sql_exec(cls, conn, query, result=None, result_factory=None, named_result=False, postpone=False):
        ql = conn['ql']
        cn = conn['cn']
        ns = conn['need_stat']

        if named_result:
            cursor = cn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        else:
            cursor = cn.cursor()

        sql_str = query.as_string(cn)
        if ns:
            ql.append((inspect.stack()[2][3], inspect.stack()[1][3], sql_str))

        cursor.execute(sql_str)
        if result is None:
            result = []
        try:
            rows = cursor.fetchall()
        except Exception:
            # here empty query result case
            return result

        if result_factory:
            for row in rows:
                result_factory(row, result)
        else:
            result.extend(rows)
        cursor.close()
        return result

    def _add_navigate_point(self, _db_conn, caption: str, params: dict):
        self._navigate[caption] = self.get_idx(_db_conn, params)

    def get_navigate(self):
        return self._navigate


class DiagReport(Report):
    @classmethod
    def get_report_map(cls):
        return {
            'Default':
                {
                    'params': {}
                }
        }

    def set_up(self, url_params):
        if url_params:
            upd_flag = url_params.get('upd')
            if upd_flag:
                self._params['upd'] = upd_flag

        if not self._params.get('from'):
            self._params['from'] = (datetime.today() - timedelta(days=1)).date()
        if not self._params.get('to'):
            self._params['to'] = self._params['from']

    def _prepare_data(self, conn):
        sp = self._params
        prev_point = _get_period(sp['from'], 'day', -1)
        next_point = _get_period(sp['from'], 'day', 1)
        self._add_navigate_point(conn, 'Prev day', {'from': prev_point['from'], 'to': prev_point['to']})
        self._add_navigate_point(conn, 'Next day', {'from': next_point['from'], 'to': next_point['to']})

        start = datetime.combine(sp['from'], datetime.min.time())
        fin = datetime.combine(sp['to'], datetime.max.time())

        # update fail status if request it
        upd_id = sp.get('upd_id')
        if upd_id:
            query = sql.SQL('UPDATE "Loader" SET "status"={} WHERE "id"={}').format(
                sql.Literal('todo'), sql.Literal(upd_id)
            )
            self._sql_exec(conn, query)

        query = sql.SQL('SELECT "id", "type", "status", "start", "finish", "duration", "params",'
                        ' "result", NULL as status_detail FROM {} WHERE {} ORDER BY {} DESC').format(
            sql.Identifier('Loader'),
            sql.SQL('{} BETWEEN {} AND {}').format(
                sql.Identifier('start'),
                sql.Literal(start),
                sql.Literal(fin)
            ),
            sql.Identifier('start')
        )

        def _status_detail(rec):
            upd_flag = sp.get('upd')
            res = None
            if upd_flag and rec.status in ('fail', 'working', 'finish'):
                detail_params = {
                    'from': sp['from'],
                    'to': sp['to'],
                    'upd_id': rec.id,
                }
                res = self.get_idx(conn, detail_params)
            return res

        def factory(rec, res):
            res.append(rec._replace(status_detail=_status_detail(rec)))

        result = []
        result = self._sql_exec(conn, query, result, factory, named_result=True)
        return {'body': result}

    def get_template(self):
        return 'diag.html'


class HelpdeskReport(Report):
    @classmethod
    def get_report_map(cls):
        return {
            "RRTWeekly": {
                'to': (9377, 9380, 9379),
                'cc': ('igor.belov@rrt.ru',),
                'subj': '[Weekly] Утилизация за неделю',
                'params': {
                    'services': (188,),
                    'executors': (9377, 9380, 9379),
                    'frame': 'weekly'
                }
            },
            "StationITWeekly": {
                'to': (7162, 9131, 8724, 9070),
                'cc': ('alexey.makarov@station-hotels.ru', 'igor.belov@station-hotels.ru'),
                'subj': '[Weekly] Недельный отчет Helpdesk',
                'params': {
                    'services': (139,),
                    'executors': (7162, 9131, 9070),
                    'frame': 'weekly'
                }
            },
            'Prosto12': {
                'to': (396, 5994, 405, 7154, 5995, 390, 43, 9423),
                'cc': ('v.ulianov@prosto12.ru', 'i.belov@prosto12.ru'),
                'subj': '[Weekly] Недельный отчет Helpdesk',
                'params': {
                    'services': (),
                    'executors': (396, 5994, 405, 7154, 5995, 390, 43, 9423),
                    'frame': 'weekly',
                },
            },
            'Intratool': {
                'to': (8827,  # MaG
                       ),
                'cc': ('v.ulianov@prosto12.ru', 'i.belov@prosto12.ru',),
                'subj': '[Weekly] Недельный отчет Helpdesk',
                'params': {
                    'services': (66, 118, 134, 136, 137, 169),
                    'executors': (
                        7379,  # DrA
                        5329,  # VaL
                        5599,  # LaA
                        7988,  # PlE
                        396,  # KrS
                        5731,  # PoI
                        8882,  # MaA
                        7154,  # StM
                        5372,  # GoV
                        5994,  # LaA
                        5912,  # FeM
                        8958,  # DeA
                        8949,  # KaI
                    ),
                    'frame': 'weekly',
                },
            },
        }

    def set_up(self, params_url):
        sp = self._params

        def _dflt(key, def_value):
            r = sp.get(key)
            if not r:
                r = sp[key] = def_value
            return r

        frame = _dflt('frame', 'daily')  # str: daily, weekly, monthly, qtrly
        delta = _dflt('frame_delta', 0)  # int: 0 -- current period
        report_date = _dflt('report_date', date.today())  # date: report on date, default -- today()

        m = {'daily': 'day', 'weekly': 'week', 'monthly': 'month', 'qtr': 'qtr'}
        if frame == 'daily':
            p = _get_period(report_date - timedelta(days=delta), 'week', 0, with_time=True)
        else:
            p = _get_period(report_date, m[frame], delta - 1, with_time=True)

        sp['from'] = p['from']
        sp['to'] = p['to']
        sp['period'] = p

    def get_template(self):
        return 'hdesk.html'

    def _prepare_data(self, cn):
        data = {}

        ss = sql.SQL
        si = sql.Identifier
        sl = sql.Literal
        se = self.__class__._sql_exec
        sp = self._params

        # navigate section begin
        def _nav_params(nav_frame: str):
            return {
                'frame': nav_frame,
                'executors': sp['executors'],
                'services': sp['services'],
            }

        self._add_navigate_point(cn, 'monthly', _nav_params('monthly'))
        self._add_navigate_point(cn, 'weekly', _nav_params('weekly'))
        self._add_navigate_point(cn, 'daily', _nav_params('daily'))

        # navigate section end

        def _unfold(srv_list):  # unfold services list (wrap parent to child services)
            result = {}
            if len(srv_list):
                q = ss('select s."Id" as id, s."Name" as name, s."ParentId" as parent from  "Services" s '
                       'where (s."Id" in {0} or s."ParentId" in {0}) '
                       'and s."IsArchive" = False order by s."Path"').format(
                    sl(tuple(srv_list))
                )

                def _fct(rec, res):
                    res[rec.id] = rec

                se(cn, q, result, _fct, named_result=True)
            return result

        srv = sp['services']  # service filter
        srv_ufd = _unfold(srv)  # services unfold dict { id : namedtuple(id, name, parent) }
        srv_ufl = tuple(srv_ufd.keys())  # services unfold list
        srv_where = ss('AND t."ServiceId" IN {}').format(sl(srv_ufl)) if len(srv_ufl) else ss('')

        exs = tuple(sp['executors'])  # tuple() for correct sql.Literal list converting
        frame = sp['frame']  # daily, weekly, monthly

        def _fact(row, res):
            res[row[0]] = row[1]

        def _get_body(index):
            return {i: [] for i in index}

        def _get_detail_utl(report: Report.__class__, params: dict):
            res = {}
            for idx in exs:
                params['executors'] = [idx]
                params['services'] = srv_ufl  # for service data isolation
                res[idx] = report.get_idx(cn, params)
            return res

        def _get_detail_srv(report: Report.__class__, params: dict):
            res = {}
            for idx in srv_ufl:
                params['services'] = [idx]
                res[idx] = report.get_idx(cn, params)
            return res

        def _do_query(query, fact=_fact):
            res = {}
            se(cn, query, res, fact)
            return res

        def _names(params):
            res = {}
            if len(params['index']):
                res = _do_query(ss('SELECT {}, {} FROM {} WHERE {} IN {}').format(
                    si('Id'), si('Name'), si(params['table']),
                    si('Id'), sl(params['index']))
                )
            return res

        def _own_tasks_d(params):
            return _get_detail_utl(TaskReport, {'frame': 'open'})

        def _own_tasks(params):
            return _do_query(ss('SELECT e."UserId" as id, count(t."Id") as cc FROM "Tasks" t, "Executors" e '
                                ' WHERE e."TaskId"=t."Id" AND t."Closed" is NULL AND e."UserId" IN {} '
                                ' {} GROUP BY id').format(
                sl(exs), srv_where
            ))

        def _dnt_d(params):
            return _get_detail_utl(TaskReport, {'frame': 'closed', 'from': params['from'], 'to': params['to']})

        def _dnt(params):
            q = ss('SELECT e.{} as id, count(t.{}) FROM {} t, {} e '
                   'WHERE e.{}=t.{} AND t.{} BETWEEN {} AND {} '
                   'AND e.{} IN {} {} GROUP BY id').format(
                si('UserId'), si('Id'), si('Tasks'), si('Executors'),
                si('TaskId'), si('Id'), si('Closed'), sl(params['from']), sl(params['to']),
                si('UserId'), sl(exs), srv_where
            )
            return _do_query(q)

        def _dnu_d(params):
            return _get_detail_utl(ExpensesReport, {'from': params['from'], 'to': params['to']})

        def _dnu(params):
            q = ss('SELECT e."UserId" as id, sum("Minutes") FROM "Expenses" e '
                   'LEFT JOIN "Tasks" t ON e."TaskId"=t."Id" '
                   'WHERE e."DateExp" BETWEEN {} AND {} '
                   'AND e."UserId" IN {} {} GROUP BY id').format(
                sl(params['from']), sl(params['to']),
                sl(exs), srv_where
            )
            return _do_query(q)

        def _dn_header(num: int, period_type, sel):
            return {
                'params': _get_period(self._params['from'], period_type, num, True),
                'selector': sel
            }

        def _eval_d(evaluate: int):
            detail = {
                'frame': 'closed',
                'from': self._params['from'],
                'to': self._params['to'],
                'evaluate': evaluate
            }
            return _get_detail_utl(TaskReport, detail)

        def _eval(evaluate: int):
            q = sql.SQL('select e."UserId", count(e."TaskId") '
                        ' from "Executors" e '
                        ' left join "Tasks" t on e."TaskId" = t."Id" '
                        ' where t."EvaluationId"={} and e."UserId" in {} and t."Closed" between {} and {} '
                        ' {} group by e."UserId"').format(
                sl(evaluate), sl(exs), sl(self._params['from']), sl(self._params['to']), srv_where
            )
            return _do_query(q)

        def _ev_header(evaluate: int, selector):
            return {
                'params': _eval_map[evaluate],
                'selector': selector,
            }

        def _sm_header(selector, params=None):  # simple header
            return {'params': params, 'selector': selector}

        def _get_util_map():
            return (
                ('*', {
                    'name': _sm_header(_names, {'table': 'Users', 'index': exs}),
                    'detail_own_tasks': _sm_header(_own_tasks_d),
                    'own_tasks': _sm_header(_own_tasks),
                }),
                ('daily weekly', {
                    'd1t': _dn_header(0, 'day', _dnt),
                    'd1t_d': _dn_header(0, 'day', _dnt_d),
                    'd1u': _dn_header(0, 'day', _dnu),
                    'd1u_d': _dn_header(0, 'day', _dnu_d),

                    'd2t': _dn_header(1, 'day', _dnt),
                    'd2t_d': _dn_header(1, 'day', _dnt_d),
                    'd2u': _dn_header(1, 'day', _dnu),
                    'd2u_d': _dn_header(1, 'day', _dnu_d),

                    'd3t': _dn_header(2, 'day', _dnt),
                    'd3t_d': _dn_header(2, 'day', _dnt_d),
                    'd3u': _dn_header(2, 'day', _dnu),
                    'd3u_d': _dn_header(2, 'day', _dnu_d),

                    'd4t': _dn_header(3, 'day', _dnt),
                    'd4t_d': _dn_header(3, 'day', _dnt_d),
                    'd4u': _dn_header(3, 'day', _dnu),
                    'd4u_d': _dn_header(3, 'day', _dnu_d),

                    'd5t': _dn_header(4, 'day', _dnt),
                    'd5t_d': _dn_header(4, 'day', _dnt_d),
                    'd5u': _dn_header(4, 'day', _dnu),
                    'd5u_d': _dn_header(4, 'day', _dnu_d),

                    'd6t': _dn_header(5, 'day', _dnt),
                    'd6t_d': _dn_header(5, 'day', _dnt_d),
                    'd6u': _dn_header(5, 'day', _dnu),
                    'd6u_d': _dn_header(5, 'day', _dnu_d),

                    'd7t': _dn_header(6, 'day', _dnt),
                    'd7t_d': _dn_header(6, 'day', _dnt_d),
                    'd7u': _dn_header(6, 'day', _dnu),
                    'd7u_d': _dn_header(6, 'day', _dnu_d),

                    'w1t': _dn_header(-0, 'week', _dnt),
                    'w1t_d': _dn_header(-0, 'week', _dnt_d),
                    'w1u': _dn_header(-0, 'week', _dnu),
                    'w1u_d': _dn_header(-0, 'week', _dnu_d),
                }),
                ('weekly', {

                    'w2t': _dn_header(-1, 'week', _dnt),
                    'w2t_d': _dn_header(-1, 'week', _dnt_d),
                    'w2u': _dn_header(-1, 'week', _dnu),
                    'w2u_d': _dn_header(-1, 'week', _dnu_d),

                    'w3t': _dn_header(-2, 'week', _dnt),
                    'w3t_d': _dn_header(-2, 'week', _dnt_d),
                    'w3u': _dn_header(-2, 'week', _dnu),
                    'w3u_d': _dn_header(-2, 'week', _dnu_d),

                    'w4t': _dn_header(-3, 'week', _dnt),
                    'w4t_d': _dn_header(-3, 'week', _dnt_d),
                    'w4u': _dn_header(-3, 'week', _dnu),
                    'w4u_d': _dn_header(-3, 'week', _dnu_d),

                }),
                ('monthly', {
                    'm1t': _dn_header(-0, 'month', _dnt),
                    'm1t_d': _dn_header(-0, 'month', _dnt_d),
                    'm1u': _dn_header(-0, 'month', _dnu),
                    'm1u_d': _dn_header(-0, 'month', _dnu_d),

                    'm2t': _dn_header(-1, 'month', _dnt),
                    'm2t_d': _dn_header(-1, 'month', _dnt_d),
                    'm2u': _dn_header(-1, 'month', _dnu),
                    'm2u_d': _dn_header(-1, 'month', _dnu_d),

                    'm3t': _dn_header(-2, 'month', _dnt),
                    'm3t_d': _dn_header(-2, 'month', _dnt_d),
                    'm3u': _dn_header(-2, 'month', _dnu),
                    'm3u_d': _dn_header(-2, 'month', _dnu_d),

                    'm4t': _dn_header(-3, 'month', _dnt),
                    'm4t_d': _dn_header(-3, 'month', _dnt_d),
                    'm4u': _dn_header(-3, 'month', _dnu),
                    'm4u_d': _dn_header(-3, 'month', _dnu_d),
                }),
                ('*', {
                    'e5': _ev_header(5, _eval),
                    'e5_d': _ev_header(5, _eval_d),
                    'e4': _ev_header(4, _eval),
                    'e4_d': _ev_header(4, _eval_d),
                    'e3': _ev_header(3, _eval),
                    'e3_d': _ev_header(3, _eval_d),
                    'e2': _ev_header(2, _eval),
                    'e2_d': _ev_header(2, _eval_d),
                    'e1': _ev_header(1, _eval),
                    'e1_d': _ev_header(1, _eval_d),
                })
            )

        def _get_head_body(h_map, index):
            head = {}
            for r in h_map:
                if frame in r[0].split() or '*' in r[0].split():
                    head.update(r[1])

            fields = [key for key in head.keys()]
            RU = namedtuple('RU', fields)  # record utilization type
            body = _get_body(index)
            for val in head.values():
                column = val['selector'](val['params'])
                for eid in index:
                    body[eid].append(column.get(eid, 0))

            # namedtuplizer
            named_body = []
            for rec in body.values():
                named_body.append(RU(*rec))

            return {'head': head, 'body': named_body}

        head_map = _get_util_map()
        data['utl'] = _get_head_body(head_map, exs)

        def _parent(params):
            res = {}
            if len(srv_ufl):
                res = _do_query(
                    ss('select s."Id", s."ParentId" from "Services" s where s."Id" in {}').format(sl(srv_ufl)))
            return res

        def _do_query_grp(q):
            result = _do_query(q)
            result_new = {}
            if len(srv_ufl):
                for k, v in result.items():
                    result_new[k] = v
                    parent = srv_ufd[k].parent
                    if parent:
                        result_new[parent] = result_new.get(parent, 0) + v
            else:
                result_new = result
            return result_new

        def _income(params):
            return _do_query_grp(ss('select t."ServiceId", count(t."Id") from "Tasks" t'
                                    ' where t."Created" between {} and {} {}'
                                    ' group by t."ServiceId" ').format(
                sl(sp['from']), sl(sp['to']), srv_where
            ))

        def _income_d(params):
            detail = {
                'frame': 'created',
                'from': sp['from'],
                'to': sp['to'],
            }
            return _get_detail_srv(TaskReport, detail)

        def _closed(params):
            return _do_query_grp(ss('select t."ServiceId", count(t."Id") from "Tasks" t'
                                    ' where t."Closed" between {} and {} {}'
                                    ' group by t."ServiceId" ').format(
                sl(sp['from']), sl(sp['to']), srv_where
            ))

        def _closed_d(params):
            detail = {
                'frame': 'closed',
                'from': sp['from'],
                'to': sp['to'],
            }
            return _get_detail_srv(TaskReport, detail)

        def _closed_exp(params):
            return _do_query_grp(ss('select t."ServiceId", sum(e."Minutes") from "Tasks" t'
                                    ' right join "Expenses" e on t."Id" = e."TaskId"'
                                    ' where t."Closed" between {} and {} {}'
                                    ' group by t."ServiceId" ').format(
                sl(sp['from']), sl(sp['to']), srv_where
            ))

        def _closed_exp_d(params):
            detail = {
                'frame': 'closed',
                'from': sp['from'],
                'to': sp['to'],
            }
            return _get_detail_srv(ExpensesReport, detail)

        def _open(params):
            return _do_query_grp(ss('select t."ServiceId", count(t."Id") from "Tasks" t'
                                    ' where t."Closed" is NULL {}'
                                    ' group by t."ServiceId" ').format(
                srv_where
            ))

        def _open_d(frame_name):
            detail = {
                'frame': frame_name,
            }
            return _get_detail_srv(TaskReport, detail)

        def _no_exec(params):
            return _do_query_grp(ss('select t."ServiceId", count(t."Id") from "Tasks" t'
                                    ' where t."Closed" is NULL {}'
                                    ' and t."Id" not in (select "TaskId" from "Executors")'
                                    ' group by t."ServiceId" ').format(
                srv_where
            ))

        def _no_deadline(params):
            return _do_query_grp(ss('select t."ServiceId", count(t."Id") from "Tasks" t'
                                    ' where t."Closed" is NULL and t."Deadline" is NULL'
                                    ' {}'
                                    ' group by t."ServiceId" ').format(
                srv_where
            ))

        def _get_srv_map():
            return (
                ('*', {
                    'name': _sm_header(_names, {'table': 'Services', 'index': srv_ufl}),
                    'parent': _sm_header(_parent),
                    'income': _sm_header(_income),
                    'income_d': _sm_header(_income_d),
                    'closed': _sm_header(_closed),
                    'closed_d': _sm_header(_closed_d),
                    'closed_exp': _sm_header(_closed_exp),
                    'closed_exp_d': _sm_header(_closed_exp_d),
                    'open': _sm_header(_open),
                    'open_d': _sm_header(_open_d, 'open'),
                    'no_exec': _sm_header(_no_exec),
                    'no_exec_d': _sm_header(_open_d, 'no_exec'),
                    'no_deadline': _sm_header(_no_deadline),
                    'no_deadline_d': _sm_header(_open_d, 'no_deadline'),
                }),
            )

        head_map = _get_srv_map()
        data['srv'] = _get_head_body(head_map, srv_ufl)

        return data


class TaskReport(Report):

    def set_up(self, params_url):
        pass

    def get_template(self):
        return "task.html"

    def _prepare_data(self, conn):
        if self._data:
            return self._data

        sp = self._params
        ss = sql.SQL
        sc = sql.Composed
        si = sql.Identifier
        sl = sql.Literal
        se = self.__class__._sql_exec

        def _filter():
            frame = sp.get('frame')
            executors = sp.get('executors')
            ffrom = sp.get('from')
            tto = sp.get('to')
            services = tuple(sp.get('services'))
            close_period = sp.get('close_period')
            income_period = sp.get('income_period')
            evaluate = sp.get('evaluate')

            fl = []  # filter list
            if executors:
                fl.append(ss('t."Id" IN (SELECT "TaskId" from'
                             ' "Executors" where "UserId" in {})').format(sl(tuple(executors))))
            if frame == 'open':  # not closed for now, period should be ignored
                fl.append(ss('t.{} is NULL').format(si('Closed')))
            elif frame == 'no_exec':  # not closed for now, no executors assigned yet, period should be ignored
                fl.append(ss('t.{} is NULL').format(si('Closed')))
                fl.append(ss('t."Id" not in (select "TaskId" from "Executors")'))
            elif frame == 'no_deadline':  # not closed for now, no executors assigned yet, period should be ignored
                fl.append(ss('t.{} is NULL').format(si('Closed')))
                fl.append(ss('t."Deadline" is NULL'))
            elif frame == 'created':  # for the period
                fl.append(ss('t."Created" BETWEEN {} AND {}').format(sl(ffrom), sl(tto)))
            elif frame == 'closed':
                fl.append(ss('t.{} BETWEEN {} AND {}').format(si('Closed'), sl(ffrom), sl(tto)))
            if evaluate:
                fl.append(ss('t.{} = {}').format(si('EvaluationId'), sl(evaluate)))
            if len(services):
                fl.append(ss('(t."ServiceId" in {0} OR ps."ParentId" in {0})').format(sl(services)))

            return ss(' AND ').join(fl)

        query = sql.SQL('select t."Id" as task_id, '
                        ' t."Name" as task_name, '
                        ' t."Description" as task_descr, '
                        ' t."Created" as created, '
                        ' t."Closed" as closed, '
                        ' uc."Name" as creator, '
                        ' t."Deadline" as deadline, '
                        ' s."Name" as service,'
                        ' pps."Name" as parent_service,'
                        ' t."StatusId" as status, '
                        ' t."EvaluationId" as eval, '
                        ' CASE WHEN exp.minutes IS NULL THEN 0 ELSE exp.minutes END as minutes,'
                        ' \'\' as executors '  # fill it bellow
                        ' from "Tasks" t '
                        ' left join "Services" ps on t."ServiceId" = ps."Id" '
                        ' left join "Services" pps on ps."ParentId" = pps."Id" '
                        ' left join "Users" uc on t."CreatorId" = uc."Id" '
                        ' left join "Services" s on t."ServiceId" = s."Id" '
                        ' left join (select "TaskId" as task_id, sum("Minutes") as minutes '
                        '    from "Expenses" group by "TaskId") exp ON t."Id"=exp.task_id'
                        ' where {} order by created desc').format(_filter())

        def _fact(rec, res):
            res[rec.task_id] = rec

        body = {}
        se(conn, query, body, _fact, named_result=True)

        # Executor column update
        if len(body):
            query = sql.SQL('SELECT e."TaskId", u."Name" FROM "Executors" e '
                            'LEFT JOIN "Users" u ON u."Id"= e."UserId"'
                            'WHERE e."TaskId" IN {}').format(
                sl(tuple(body.keys()))
            )

            def _fact(rec, res):
                val = body[rec[0]].executors
                if val == '':
                    val = list()
                    body[rec[0]] = body[rec[0]]._replace(executors=val)
                val.append(rec[1])

            se(conn, query, None, _fact)
        return {'body': body}


class ExpensesReport(Report):
    """
        Expenses Report Class
            frame: closed
                params:
                    executors: list
                    services: list
                    close_period: dict
    """

    def set_up(self, params_url):
        pass

    def get_template(self):
        return "exp.html"

    def _prepare_data(self, conn):
        sp = self._params
        ss = sql.SQL
        sc = sql.Composed
        si = sql.Identifier
        sl = sql.Literal
        se = self.__class__._sql_exec

        def _filter():
            executors = sp.get('executors')
            ffrom = sp.get('from')
            tto = sp.get('to')
            services = sp.get('services')
            frame = sp.get('frame')

            fl = []  # filter list
            if executors:
                fl.append(ss('ex."UserId" in {}').format(sl(tuple(executors))))
                fl.append(ss('ex."DateExp" BETWEEN {} AND {}').format(sl(ffrom), sl(tto)))
            if frame == 'closed':
                fl.append(ss('t."Closed" BETWEEN {} AND {}').format(sl(ffrom), sl(tto)))
            if len(services):
                fl.append(ss('(t."ServiceId" in {0} OR ps."ParentId" in {0})').format(sl(tuple(services))))
            return ss(' AND ').join(fl) if len(fl) else sl(True)

        query = sql.SQL('SELECT t."Id" AS task_id, t."Name" AS task_name, t."Description" AS task_descr, '
                        ' t."Created" AS created, t."Closed" AS closed,'
                        ' uc."Name" AS creator, SUM(ex."Minutes") AS Minutes, '
                        ' s."Name" AS service, '
                        ' pps."Name" AS parent_service, '
                        ' ue."Name" AS executor'
                        ' FROM "Expenses" ex'
                        ' LEFT JOIN "Tasks" as t ON ex."TaskId" = t."Id"'
                        ' LEFT JOIN "Services" ps ON t."ServiceId" = ps."Id" '
                        ' LEFT JOIN "Services" pps ON ps."ParentId" = pps."Id" '
                        ' LEFT JOIN "Users" uc ON t."CreatorId"=uc."Id"'
                        ' LEFT JOIN "Users" ue ON ex."UserId"=ue."Id"'
                        ' LEFT JOIN "Services" s ON t."ServiceId"=s."Id" where {}'
                        ' GROUP BY t."Id", creator, parent_service, service, ue."Name" '
                        ' ORDER BY t."Created" desc').format(_filter())

        def _fact(rec, res):
            res[rec.task_id] = rec

        body = []
        se(conn, query, body, None, named_result=True)
        return {'body': body}
