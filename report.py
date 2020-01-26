from datetime import datetime, date, timedelta
from unittest import TestCase
import json
import hashlib
from collections import namedtuple

import psycopg2
from psycopg2 import sql

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

    def set_up(self):  # safety plug, optional override in subclass
        pass

    def _D(self):
        """ Jinja call for get_data() method """
        if self._data:
            return self._data
        start = datetime.now()
        self._data = self._prepare_data()
        self._EXT = datetime.now() - start
        return self._data

    def _P(self):
        """ Jinja call for _params attribute """
        return self._params

    def __init__(self, db_conn, params=None):
        self._data = None
        self._db_conn = db_conn
        self._params = params if params else {}
        self._navigate = {}
        self._EXT = 0  # get_data execution time, jinja profiling
        self.set_up()

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
    def factory(cls, conn, params):
        """ Create report object from params dictionary"""
        params = cls._idx_to_params(conn, params['idx'])
        if not params:
            return None

        report_class = cls._get_map().get(params.get('type'))
        if not report_class:
            return None

        return report_class(conn, params)

    @classmethod
    def default_map(cls, conn):
        result = {}
        for name, report_class in cls._get_map().items():
            result[name] = report_class.get_idx(conn)
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

        # try find params in storage
        query = sql.SQL('SELECT {}, {} FROM {} WHERE {} = {}').format(
            sql.Identifier('last_touch'), sql.Identifier('touch_count'),
            sql.Identifier('Params'),
            sql.Identifier('id'), sql.Literal(value_hash)
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
            cls._sql_exec(conn, query)
        else:
            query = sql.SQL('UPDATE {} SET {} = {} WHERE {}={}').format(
                sql.Identifier('Params'),
                sql.Identifier('last_touch'),
                sql.Literal(datetime.now()),
                sql.Identifier('id'),
                sql.Literal(value_hash)
            )
            cls._sql_exec(conn, query)
        return {'idx': value_hash}

    @classmethod
    def _idx_to_params(cls, conn, idx):
        query = sql.SQL('SELECT {} FROM {} WHERE {}={}').format(
            sql.Identifier('params'),
            sql.Identifier('Params'),
            sql.Identifier('id'),
            sql.Literal(idx)
        )
        result = cls._sql_exec(conn, query)
        if not len(result):
            return None

        # increment touch count
        query = sql.SQL('UPDATE {} SET {}={}+1 WHERE {}={}').format(
            sql.Identifier('Params'),
            sql.Identifier('touch_count'),
            sql.Identifier('touch_count'),
            sql.Identifier('id'),
            sql.Literal(idx)
        )
        cls._sql_exec(conn, query)

        return cls._json_to_dict(result[0][0])

    @classmethod
    def _get_def_params(cls):
        """ Override it in subclasses for default report link generation """
        return {}

    @classmethod
    def get_idx(cls, conn, params=None):
        if not params:
            params = cls._get_def_params()
        params['type'] = cls.get_type()
        return cls._params_to_idx(conn, params)

    @classmethod
    def _sql_exec(cls, conn, query, result=None, result_factory=None, auto_commit=True):
        # todo: result need named tuple implementation
        cursor = conn.cursor()
        sql_str = query.as_string(conn)
        cursor.execute(sql_str)
        if result is None:
            result = []
        try:
            rows = cursor.fetchall()
        except Exception:
            # empty query result
            if auto_commit:
                conn.commit()
            return result

        if result_factory:
            for row in rows:
                result_factory(row, result)
        else:
            result = rows
        cursor.close()
        if auto_commit:
            conn.commit()
        return result

    def get_navigate(self):
        return self._navigate

    def _add_navigate_point(self, caption: str, params: dict):
        self._navigate[caption] = self.get_idx(self._db_conn, params)

    def get_navigate(self):
        return self._navigate


class DiagReport(Report):

    def set_up(self):
        if not self._params.get('from'):
            self._params['from'] = (datetime.today() - timedelta(days=1)).date()
        if not self._params.get('to'):
            self._params['to'] = self._params['from']

        prev_point = _get_period(self._params['from'], 'day', -1)
        next_point = _get_period(self._params['from'], 'day', 1)
        self._add_navigate_point('<< Prev day', {'from': prev_point['from'], 'to': prev_point['to']})
        self._add_navigate_point('Next day >>', {'from': next_point['from'], 'to': next_point['to']})

    def get_data(self):
        start = datetime.combine(self._params['from'], datetime.min.time())
        fin = datetime.combine(self._params['to'], datetime.max.time())

        fields = 'id type status start finish duration params result'.split()
        query = sql.SQL('SELECT {} FROM {} WHERE {} ORDER BY {} DESC').format(
            sql.SQL(', ').join(sql.Identifier(f) for f in fields),
            sql.Identifier('Loader'),
            sql.SQL('{} BETWEEN {} AND {}').format(
                sql.Identifier('start'),
                sql.Literal(start),
                sql.Literal(fin)
            ),
            sql.Identifier('start')
        )

        def factory(row, res):
            res.append([val for val in row])

        result = []
        result = self._sql_exec(self._db_conn, query, result, factory)
        return {'header': fields, 'body': result}

    def get_template(self):
        return 'diag.html'

    def get_caption(self):
        return "Diagnostic report on {}".format(
            self._params['from']
        )


class HelpdeskReport(Report):

    def set_up(self):
        def _dflt(key, def_value):
            r = self._params.get(key)
            if not r:
                r = self._params[key] = def_value
            return r

        frame = _dflt('frame', 'daily')  # str: daily, weekly, monthly, qtrly
        delta = _dflt('frame_delta', 0)  # int: 0 -- current period
        report_date = _dflt('report_date', date.today())  # date: report on date, default -- today()

        m = {'daily': 'day', 'weekly': 'week', 'monthly': 'month', 'qtr': 'qtr'}
        if frame == 'daily':
            p = _get_period(report_date - timedelta(days=delta), 'week', 0, with_time=True)
        else:
            p = _get_period(report_date, m[frame], delta - 1, with_time=True)

        self._params['from'] = p['from']
        self._params['to'] = p['to']

        self._add_navigate_point('Daily', {'frame': 'daily', 'executors': self._params['executors']})
        self._add_navigate_point('Last week', {'frame': 'weekly', 'executors': self._params['executors']})
        self._add_navigate_point('Last month', {'frame': 'monthly', 'executors': self._params['executors']})

        # next_point = _get_period(self._params['from'], 'day', 1)
        # self._add_navigate_point('<< Prev day', {'from': prev_point['from'], 'to': prev_point['to']})
        # self._add_navigate_point('Next day >>', {'from': next_point['from'], 'to': next_point['to']})

    def get_template(self):
        return 'hdesk.html'

    @classmethod
    def _get_def_params(cls):
        # todo: delete after default test
        return {
            'frame': 'daily',
            'executors': (7162, 9131, 8724, 9070),
            'from': date.today()
        }

    def _prepare_data(self):
        data = {}

        exs = tuple(self._params['executors'])  # tuple() for correct sql.Literal list converting
        frame = self._params['frame']
        body = {i: [] for i in exs}

        ss = sql.SQL
        si = sql.Identifier
        sl = sql.Literal
        se = self.__class__._sql_exec
        cn = self._db_conn

        def _fact(row, res):
            res[row[0]] = row[1]

        def _seq(params):
            return {ex: seq for ex, seq in zip(exs, range(1, len(body)))}

        def _get_detail(report: Report.__class__, params: dict):
            res = {}
            for ex_id in exs:
                params['executors'] = [ex_id]
                res[ex_id] = report.get_idx(cn, params)
            return res

        def _do_query(query):
            res = {}
            se(cn, query, res, _fact)
            return res

        def _names(params):
            return _do_query(ss('SELECT {}, {} FROM {} WHERE {} IN {}').format(
                si('Id'), si('Name'), si('Users'),
                si('Id'), sl(exs))
            )

        def _own_tasks_d(params):
            return _get_detail(TaskReport, {'frame': 'opened'})

        def _own_tasks(params):
            return _do_query(ss('SELECT e.{} as id, count(t.{}) as cc FROM {} t, {} e '
                                'WHERE e.{}=t.{} AND t.{} is NULL AND e.{} IN {} GROUP BY id').format(
                si('UserId'), si('Id'), si('Tasks'), si('Executors'),
                si('TaskId'), si('Id'), si('Closed'), si('UserId'), sl(exs))
            )

        def _dnt_d(params):
            return _get_detail(TaskReport, {'frame': 'closed', 'from': params['from'], 'to': params['to']})

        def _dnt(params):
            q = ss('SELECT e.{} as id, count(t.{}) FROM {} t, {} e '
                   'WHERE e.{}=t.{} AND t.{} BETWEEN {} AND {} '
                   'AND e.{} IN {} GROUP BY id').format(
                si('UserId'), si('Id'), si('Tasks'), si('Executors'),
                si('TaskId'), si('Id'), si('Closed'), sl(params['from']), sl(params['to']),
                si('UserId'), sl(exs)
            )
            return _do_query(q)

        def _dnu_d(params):
            return _get_detail(ExpensesReport, {'from': params['from'], 'to': params['to']})

        def _dnu(params):
            q = ss('SELECT {} as id, sum({}) FROM {} '
                   'WHERE {} BETWEEN {} AND {} '
                   'AND {} IN {} GROUP BY id').format(
                si('UserId'), si('Minutes'), si('Expenses'),
                si('DateExp'), sl(params['from']), sl(params['to']),
                si('UserId'), sl(exs)
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
            return _get_detail(TaskReport, detail)

        def _eval(evaluate: int):
            q = sql.SQL('select e."UserId", count(e."TaskId") '
                        'from "Executors" e '
                        'left join "Tasks" t on e."TaskId" = t."Id" '
                        'where t."EvaluationId"={} and e."UserId" in {} and t."Closed" between {} and {} '
                        'group by e."UserId"').format(
                sl(evaluate), sl(exs), sl(self._params['from']), sl(self._params['to'])
            )
            return _do_query(q)

        def _ev_header(evaluate: int, selector):
            return {
                'params': _eval_map[evaluate],
                'selector': selector,
            }

        def _sm_header(selector):  # simple header
            return {'params': None, 'selector': selector}

        head_map = (
            ('*', {
                'seq': _sm_header(_seq),
                'name': _sm_header(_names),
                'detail_own_tasks': _sm_header(_own_tasks_d),
                'own_tasks': _sm_header(_own_tasks),
            }),
            ('daily weekly', {
                'd1t': _dn_header(0, 'day', _dnt),
                'd1t_d': _dn_header(0, 'day', _dnt_d),
                'd1u': _dn_header(0, 'day', _dnu),
                'd1u_d': _dn_header(0, 'day', _dnu_d),

                'd2t': _dn_header(-1, 'day', _dnt),
                'd2t_d': _dn_header(-1, 'day', _dnt_d),
                'd2u': _dn_header(-1, 'day', _dnu),
                'd2u_d': _dn_header(-1, 'day', _dnu_d),

                'd3t': _dn_header(-2, 'day', _dnt),
                'd3t_d': _dn_header(-2, 'day', _dnt_d),
                'd3u': _dn_header(-2, 'day', _dnu),
                'd3u_d': _dn_header(-2, 'day', _dnu_d),

                'd4t': _dn_header(-3, 'day', _dnt),
                'd4t_d': _dn_header(-3, 'day', _dnt_d),
                'd4u': _dn_header(-3, 'day', _dnu),
                'd4u_d': _dn_header(-3, 'day', _dnu_d),

                'd5t': _dn_header(-4, 'day', _dnt),
                'd5t_d': _dn_header(-4, 'day', _dnt_d),
                'd5u': _dn_header(-4, 'day', _dnu),
                'd5u_d': _dn_header(-4, 'day', _dnu_d),

                'd6t': _dn_header(-5, 'day', _dnt),
                'd6t_d': _dn_header(-5, 'day', _dnt_d),
                'd6u': _dn_header(-5, 'day', _dnu),
                'd6u_d': _dn_header(-5, 'day', _dnu_d),

                'd7t': _dn_header(-6, 'day', _dnt),
                'd7t_d': _dn_header(-6, 'day', _dnt_d),
                'd7u': _dn_header(-6, 'day', _dnu),
                'd7u_d': _dn_header(-6, 'day', _dnu_d),
            }),
            ('weekly', {
                'w1t': _dn_header(-0, 'week', _dnt),
                'w1t_d': _dn_header(-0, 'week', _dnt_d),
                'w1u': _dn_header(-0, 'week', _dnu),
                'w1u_d': _dn_header(-0, 'week', _dnu_d),

                'w2t': _dn_header(-1, 'week', _dnt),
                'w2t_d': _dn_header(-1, 'week', _dnt_d),
                'w2u': _dn_header(-1, 'week', _dnu),
                'w2u_d': _dn_header(-1, 'week', _dnu_d),

                'w3t': _dn_header(-2, 'week', _dnt),
                'w3t_d': _dn_header(-2, 'week', _dnt_d),
                'w3u': _dn_header(-2, 'week', _dnu),
                'w3u_d': _dn_header(-2, 'week', _dnu_d),
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
        head = {}
        for r in head_map:
            if frame in r[0].split() or '*' in r[0].split():
                head.update(r[1])

        fields = [key for key in head.keys()]
        RU = namedtuple('RU', fields)  # record utilization type

        for val in head.values():
            column = val['selector'](val['params'])
            for eid in exs:
                body[eid].append(column.get(eid, 0))

        # namedtuplizer
        named_body = []
        for rec in body.values():
            named_body.append(RU(*rec))

        # todo: need to do service filter
        data['utl'] = {'head': head, 'body': named_body}
        return data


class TaskReport(Report):
    """
    Task Report Class
        frame: opened
            params:
                executors: list
                services: list
        frame: closed
            params:
                executors: list
                services: list
                close_period: dict
        frame: income
            params:
                services: list
                income_period: dict
        frame: no_executor
            params:
                services: list
        frame: no_deadline
            params:
                services: list
        frame: evaluate
            params:
                executor: list
                evaluate: int
    """

    def set_up(self):
        pass

    def get_template(self):
        return "task.html"

    def get_data(self):
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
            services = sp.get('services')
            close_period = sp.get('close_period')
            income_period = sp.get('income_period')
            evaluate = sp.get('evaluate')

            fl = []  # filter list
            if executors:
                fl.append(ss('ue.{} in {}').format(si('Id'), sl(tuple(executors))))
            if frame == 'opened':
                fl.append(ss('t.{} is NULL').format(si('Closed')))
            if frame == 'closed':
                fl.append(ss('t.{} BETWEEN {} AND {}').format(si('Closed'), sl(ffrom), sl(tto)))
            if evaluate:
                fl.append(ss('t.{} = {}').format(si('EvaluationId'), sl(evaluate)))
            return ss(' AND ').join(fl)

        query = sql.SQL('select e."TaskId" as task_id, '
                        't."Name" as task_name, '
                        't."Description" as task_descr, '
                        't."Created" as created, '
                        't."Closed" as closed, '
                        'uc."Name" as creator, '
                        't."Deadline" as deadline, '
                        's."Name" as service,'
                        't."StatusId" as status, '
                        't."EvaluationId" as eval, '
                        '\'\' as executors '  # fill it later
                        'from "Executors" e '
                        'left join "Tasks" t on e."TaskId" = t."Id" '
                        'left join "Users" ue on e."UserId" = ue."Id" '
                        'left join "Users" uc on t."CreatorId" = uc."Id" '
                        'left join "Services" s on t."ServiceId" = s."Id" '
                        'where {} order by t."Created" desc').format(_filter())
        fields = "task_id task_name task_descr created closed creator " \
                 "deadline service status eval executors"
        R = namedtuple("R", fields)

        def _fact(rec, res):
            res[rec[0]] = R(*rec)

        body = {}
        se(self._db_conn, query, body, _fact)

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

            se(self._db_conn, query, None, _fact)
        return {'body': body}

    @classmethod
    def _get_def_params(cls):
        # todo: delete after default test
        return {
            'frame': 'opened',
            'executors': [7162]
        }


class ExpensesReport(Report):
    def get_template(self):
        return "exp.html"

    def get_data(self):
        if self._data:
            return self._data

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

            fl = []  # filter list
            fl.append(ss('ex."UserId" in {}').format(sl(tuple(executors))))
            fl.append(ss('ex."DateExp" BETWEEN {} AND {}').format(sl(ffrom), sl(tto)))
            return ss(' AND ').join(fl)

        query = sql.SQL('select t."Id", t."Name", t."Description", t."Created", t."Closed", '
                        'uc."Name" as creator, q.ss, s."Name" from "Tasks" t '
                        'right join (select ex."TaskId" as id, sum(ex."Minutes") as ss '
                        'from "Expenses" ex where {} group by ex."TaskId" '
                        ') q on q.id = t."Id" '
                        'left join "Users" uc ON t."CreatorId"=uc."Id" '
                        'left join "Services" s ON t."ServiceId"=s."Id" '
                        'order by t."Created" desc').format(_filter())

        fields = "task_id task_name task_descr created closed creator expenses service"
        R = namedtuple("R", fields)

        def _fact(rec, res):
            res[rec[0]] = R(*rec)

        body = {}
        se(self._db_conn, query, body, _fact)
        return {'body': body}


###########################
# Unittest section
###########################
from keys import KeyChain


class TestReports(TestCase):
    def setUp(self):
        acc_key = KeyChain.PG_KEY
        self._conn = psycopg2.connect(dbname=acc_key["db_name"], user=acc_key["user"],
                                      password=acc_key["pwd"], host=acc_key["host"])

    def test_diag_report(self):
        rep = DiagReport(self._conn)
        rep.get_caption()
        rep.get_navigate()
        rep.get_data()

    def test_util_report(self):
        rep = HelpdeskReport(self._conn)
        rep.get_caption()
        rep.get_navigate()
        rep.get_data()

    def test_task_report(self):
        rep = TaskReport(self._conn)
        rep.get_caption()
        rep.get_navigate()
        rep.get_data()

    def test_expenses_report(self):
        rep = ExpensesReport(self._conn)
        rep.get_caption()
        rep.get_navigate()
        rep.get_data()


if __name__ == '__main__':
    unittest.main()
