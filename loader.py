import io
import simplejson as json
import sys
import traceback
from contextlib import redirect_stdout
from datetime import datetime
from datetime import timedelta
from unittest import TestCase

import psycopg2
from psycopg2 import sql
from croniter import croniter

from connector import PGConnector
from connector import ISConnector
from activities import reg


class Loader:
    _table_name = "Loader"

    def __init__(self, key_chain):
        self._registry = {}
        reg.init_ldr(self)
        self.key_chain = key_chain
        self.get_PG_connector = lambda: PGConnector(key_chain.PG_KEY)
        self.get_IS_connector = lambda: ISConnector(key_chain.IS_KEY)
        key = key_chain.PG_KEY
        self._db_conn = psycopg2.connect(dbname=key["db_name"], user=key["user"],
                                         password=key["pwd"], host=key["host"])

    def register(self, factory):
        """ Activities register method for activities produce and schedule control
        """
        activity = factory(self)
        self._registry[activity.get_type()] = {
            'factory': factory,
            'crontab': activity.get_crontab(),
        }

    def sql_commit(self):
        self._db_conn.commit()

    def sql_exec(self, query, result=None, result_factory=None, auto_commit=True, named_result=False):
        # todo: result need named tuple implementation
        if named_result:
            cursor = self._db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        else:
            cursor = self._db_conn.cursor()

        sql_str = query.as_string(self._db_conn)
        cursor.execute(sql_str)
        if result is None:
            result = []
        try:
            rows = cursor.fetchall()
        except Exception:
            # empty query result case
            if auto_commit:
                self.sql_commit()
            return result

        if result_factory:
            for row in rows:
                result_factory(row, result)
        else:
            result = rows
        cursor.close()
        if auto_commit:
            self.sql_commit()
        return result

    def _sql_compose(self, cmd: str, params: dict, conditions=None):
        result = ''
        if cmd is 'insert':
            result = sql.SQL("INSERT INTO {}({}) VALUES ({})").format(
                sql.Identifier(self._table_name),
                sql.SQL(', ').join(sql.Identifier(field) for field in params.keys()),
                sql.SQL(', ').join(sql.Literal(str(value)) for value in params.values()))
        elif cmd is 'select':
            result = sql.SQL("SELECT {} FROM {} WHERE {}").format(
                sql.SQL(', ').join(sql.Identifier(field) for field in params.keys()),
                sql.Identifier(self._table_name),
                sql.SQL(' AND ').join(sql.Composed([sql.Identifier(key), sql.SQL(value[0]), sql.Literal(value[1])])
                                      for key, value in conditions.items()))
        elif cmd is 'update':
            result = sql.SQL("UPDATE {} SET ({}) = ({}) WHERE {}").format(
                sql.Identifier(self._table_name),
                sql.SQL(', ').join(sql.Identifier(field) for field in params.keys()),
                sql.SQL(', ').join(sql.Literal(value) for value in params.values()),
                sql.SQL(' AND ').join(sql.Composed([sql.Identifier(key), sql.SQL(value[0]), sql.Literal(value[1])])
                                      for key, value in conditions.items()))

        else:
            raise ValueError
        return result

    def to_plan(self, activity, due_date=None):

        def converter(o):
            if isinstance(o, datetime):
                return o.strftime('datetime:%Y-%m-%d %H:%M:%S.%f')

        query_params = {
            'type': activity.get_type(),
            'status': 'todo',
            'start': due_date if due_date else datetime.now(),
            'params': json.dumps(activity.get_params(), default=converter)
        }
        query = sql.SQL("INSERT INTO {}({}) VALUES ({}) RETURNING {}").format(
            sql.Identifier(self._table_name),
            sql.SQL(', ').join(sql.Identifier(field) for field in query_params.keys()),
            sql.SQL(', ').join(sql.Literal(str(value)) for value in query_params.values()),
            sql.Identifier('id')
        )

        return self.sql_exec(query)[0]

    def json_to_params(self, json_str):

        def dict_converter(source):
            # scan json-source tree and replace datetime strings to datetime objects
            def date_converter(s):
                try:
                    return datetime.strptime(s, 'Datetime:%Y-%m-%d %H:%M:%S.%f')
                except:
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

            # res[0][2] if not res[0][2] else json.loads(res[0][2], object_hook=dict_converter)
        return json_str if not json_str else json.loads(json_str, object_hook=dict_converter)

    def id_to_params(self, activity_id: int):
        result = {}
        query = sql.SQL('SELECT "params" FROM "Loader" WHERE "id"={}').format(sql.Literal(activity_id))
        res = self.sql_exec(query, auto_commit=False, named_result=True)
        if len(res):
            result = self.json_to_params(res[0].params)
        return result

    def track_schedule(self):
        # check crontab activities
        query = sql.SQL('SELECT {} FROM {} WHERE {}').format(
            sql.SQL(', ').join(map(sql.Identifier, ('id', 'type', 'start'))),
            sql.Identifier(self._table_name),
            sql.SQL('{} = {} AND {} > {} AND {} is Null').format(
                sql.Identifier('status'), sql.Literal('todo'),
                sql.Identifier('start'), sql.Literal(datetime.now()),
                sql.Identifier('params')
            )
        )

        schedule = []

        def factory(_row, _res):
            # add record (id, type, start, valid)
            _res.append([_row[0], _row[1], _row[2], False])

        schedule = self.sql_exec(query, schedule, factory, auto_commit=False)

        for activity_type, params in self._registry.items():
            if params['crontab'] is None:
                continue
            next_start = croniter(params['crontab'], datetime.now()).get_next(datetime)
            need_to_plan = True
            for rec in schedule:
                if rec[1] == activity_type:
                    if next_start == rec[2]:
                        rec[3] = True
                        need_to_plan = False
                        break
            if need_to_plan:
                query_params = {
                    'type': activity_type,
                    'status': 'todo',
                    'start': next_start,
                }
                query = self._sql_compose('insert', query_params)
                self.sql_exec(query, auto_commit=False)

        rec_for_delete = []
        for rec in schedule:
            if not rec[3]:
                rec_for_delete.append(rec[0])
        if len(rec_for_delete):
            # todo: delete only crontab activity
            query = sql.SQL('DELETE FROM {} WHERE {} IN ({})').format(
                sql.Identifier(self._table_name),
                sql.Identifier('id'),
                sql.SQL(', ').join(sql.Literal(_id) for _id in rec_for_delete)
            )
            self.sql_exec(query, auto_commit=False)

        self.sql_commit()

        # main execution loop
        while True:
            # params = {
            #     'id': None,
            #     'type': None,
            #     'params': None,
            # }
            # conditions = {
            #     'status': ('=', 'todo'),
            #     'start': ('<=', datetime.now())
            # }
            #
            # query = self._sql_compose('select', params, conditions)
            query = sql.SQL('SELECT "id", "type", "params" FROM "Loader" WHERE "status"={} AND "start"<={}').\
                format(sql.Literal('todo'), sql.Literal(datetime.now()))
            res = self.sql_exec(query, auto_commit=False, named_result=True)

            if len(res) is 0:
                break

            rec = res[0]
            act_id = rec.id
            act_type = rec.type
            json_params = rec.params

            # set in progress status
            self.sql_exec(self._sql_compose('update', {'status': 'working'}, {'id': ('=', act_id)}))

            # def dict_converter(source):
            #     # scan json-source tree and replace datetime strings to datetime objects
            #     def date_converter(s):
            #         try:
            #             return datetime.strptime(s, 'Datetime:%Y-%m-%d %H:%M:%S.%f')
            #         except:
            #             return s
            #
            #     def list_converter(lst):
            #         for i in range(len(lst)):
            #             r = lst[i]
            #             if isinstance(r, str):
            #                 lst[i] = date_converter(r)
            #             elif isinstance(r, dict):
            #                 dict_converter(r)
            #             elif isinstance(r, list):
            #                 list_converter(r)
            #
            #     for k, v in source.items():
            #         if isinstance(v, list):
            #             list_converter(v)
            #         elif isinstance(v, dict):
            #             dict_converter(v)
            #         elif isinstance(v, str):
            #             source[k] = date_converter(v)
            #     return source
            #

            # stdout dispatch
            prn_stream = io.StringIO()
            # prn_stream = sys.stdout

            with redirect_stdout(prn_stream):
                start = datetime.now()
                try:
                    act_params = self.json_to_params(json_params)
                    factory = self._registry[act_type]['factory']
                    activity = factory(self, act_params)
                    activity.run()
                except Exception:
                    print('Fail:\n', traceback.format_exc())
                    status = 'fail'
                else:
                    status = 'finish'

            duration = (datetime.now() - start).seconds
            self.sql_exec(self._sql_compose('update', {'status': status,
                                                       'duration': duration,
                                                       'result': prn_stream.getvalue(),
                                                       'finish': datetime.now()}, {'id': ('=', act_id)}))

    def get_activity_status(self, activity_id: int):
        query = sql.SQL('SELECT {} FROM {} WHERE {}={}').format(
            sql.Identifier('status'),
            sql.Identifier('Loader'),
            sql.Identifier('id'),
            sql.Literal(activity_id),
        )
        result = self.sql_exec(query, auto_commit=False)
        return None if not len(result) else result[0][0]

    def get_state(self, date: datetime = None):
        result = []
        if not date:
            date = datetime.today() - timedelta(days=1)
        start = datetime(date.year, date.month, date.day)
        finish = start + timedelta(days=1)
        fields = 'id type status start finish duration params result'.split()
        query = sql.SQL('SELECT {} FROM {} WHERE {} ORDER BY {} DESC').format(
            sql.SQL(', ').join(sql.Identifier(f) for f in fields),
            sql.Identifier(self._table_name),
            sql.SQL('{} BETWEEN {} AND {}').format(
                sql.Identifier('start'),
                sql.Literal(start),
                sql.Literal(finish)
            ),
            sql.Identifier('id')
        )

        def factory(row, res):
            res.append([val for val in row])

        result = self.sql_exec(query, result, factory)
        return {'actual_date': date, 'header': fields, 'data': result}

    def get_PG_connector(self):  # __init__ defined
        pass

    def get_IS_connector(self):  # __init__ defined
        pass

from keys import KeyChain
class TestLoader(TestCase):
    def test_loader(self):
        ldr = Loader(KeyChain)
        ldr.track_schedule()