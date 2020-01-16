from datetime import datetime, date, timedelta
from unittest import TestCase
import json

import psycopg2
from psycopg2 import sql


def _get_period(now: date, period_type: str, delta: int):
    """
    Возвращает словарь с параметрами периода
    :param now: текущая дата
    :param period_type: тип периода Week, Month, Qtr
    :param delta: сколько периодов отнять или прибавить
    :return: параметры периода:
        'from' -- дата начала
        'to' -- дата окончания
        'id' -- идентификатор периода 1945 (45 неделя 19-го года)
        'type' -- тип периода (см: period_type)
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
        result['from'] = datetime.date(y, m, 1)
        y, m = (y, m + 1) if m != 12 else (y + 1, 1)
        next_month = datetime.date(y, m, 1)
        result['to'] = next_month - timedelta(days=1)
        result['id'] = result['from'].strftime('%y%m')
    elif period_type is 'week':
        result['from'] = now + timedelta(days=-now.isoweekday() + 1, weeks=delta)
        result['to'] = result['from'] + timedelta(days=6)
        result['id'] = result['from'].strftime('%y%W')
    elif period_type is 'day':
        result['from'] = now + timedelta(days=delta)
        result['to'] = result['from']
        result['id'] = result['from'].strftime('%m%d')
    return result


class Report:
    _report_map = None

    def __init__(self, db_conn, params=None):
        self._db_conn = db_conn
        self._params = params if params else {}
        self._navigate = {}
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
        return cls._report_map

    @classmethod
    def factory(cls, conn, params_id):
        """ Create report object from params dictionary"""
        params = cls._idx_to_params(conn, params_id)
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
            result[name] = report_class.default_url(conn)
        return result

    @classmethod
    def _params_to_idx(cls, conn, params):
        def converter(o):
            if isinstance(o, datetime):
                return o.strftime('datetime:%Y-%m-%d %H:%M:%S.%f')
            elif isinstance(o, date):
                return o.strftime('date:%Y-%m-%d')
        return {'idx': json.dumps(params, default=converter)}

    @classmethod
    def _idx_to_params(cls, conn, idx):
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
        return json.loads(idx['idx'], object_hook=dict_converter)

    @classmethod
    def _def_params(cls, params=None):
        if not params:
            params = {}
        params['type'] = cls.get_type()
        return params

    @classmethod
    def default_url(cls, conn):
        return cls._params_to_idx(None, cls._def_params())

    def _sql_exec(self, query, result=None, result_factory=None, auto_commit=True):
        # todo: result need named tuple implementation
        cursor = self._db_conn.cursor()
        sql_str = query.as_string(self._db_conn)
        cursor.execute(sql_str)
        if result is None:
            result = []
        try:
            rows = cursor.fetchall()
        except Exception:
            # empty query result
            if auto_commit:
                self._db_conn.commit()
            return result

        if result_factory:
            for row in rows:
                result_factory(row, result)
        else:
            result = rows
        cursor.close()
        if auto_commit:
            self._db_conn.commit()
        return result

    def get_navigate(self):
        return self._navigate

    def _add_navigate_point(self, caption: str, params: dict):
        self._navigate[caption] = self._params_to_idx(self._db_conn, self.__class__._def_params(params))

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
            sql.Identifier('id')
        )

        def factory(row, res):
            res.append([val for val in row])

        result = []
        result = self._sql_exec(query, result, factory)
        return {'header': fields, 'body': result}

    def get_template(self):
        return 'diag.html'

    def get_caption(self):
        return "Diagnostic report on {}".format(
            self._params['from']
        )


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


if __name__ == '__main__':
    unittest.main()
