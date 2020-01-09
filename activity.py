import io
import json
import smtplib
import sys
import traceback
from contextlib import redirect_stdout
from datetime import datetime
from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest import TestCase
from html import escape

import psycopg2
from psycopg2 import sql

from keys import KeyChain
from croniter import croniter

class Loader:
    _table_name = "Loader"

    def __init__(self, acc_key: dict):
        self._key = acc_key
        self._db_conn = psycopg2.connect(dbname=acc_key["db_name"], user=acc_key["user"],
                                         password=acc_key["pwd"], host=acc_key["host"])
        self._registry = {}

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

    def sql_exec(self, query, result=None, result_factory=None, auto_commit=True):
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
        query = self._sql_compose('insert', query_params)
        self._sql_exec(query)

    def track_schedule(self):
        # check crontab activities
        query = sql.SQL('SELECT {} FROM {} WHERE {}').format(
            sql.SQL(', ').join(map(sql.Identifier, ('id', 'type', 'start'))),
            sql.Identifier(self._table_name),
            sql.SQL('{} = {} AND {} > {}').format(
                sql.Identifier('status'), sql.Literal('todo'),
                sql.Identifier('start'), sql.Literal(datetime.now()),
            )
        )

        schedule = []

        def factory(_row, _res):
            # add record (id, type, start, valid)
            _res.append([_row[0],_row[1], _row[2], False])

        schedule = self._sql_exec(query, schedule, factory, auto_commit=False)

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
                self._sql_exec(query, auto_commit=False)

        rec_for_delete = []
        for rec in schedule:
            if not rec[3]:
                rec_for_delete.append(rec[0])
        if len(rec_for_delete):
            query = sql.SQL('DELETE FROM {} WHERE {} IN ({})').format(
                sql.Identifier(self._table_name),
                sql.Identifier('id'),
                sql.SQL(', ').join(sql.Literal(_id) for _id in rec_for_delete)
            )
            self._sql_exec(query, auto_commit=False)

        self._db_conn.commit()

        # main execution loop
        while True:
            params = {
                'id': None,
                'type': None,
                'params': None,
            }
            conditions = {
                'status': ('=', 'todo'),
                'start': ('<=', datetime.now())
            }

            query = self._sql_compose('select', params, conditions)
            res = self._sql_exec(query, auto_commit=False)

            if len(res) is 0:
                break
            act_id = res[0][0]
            act_type = res[0][1]

            # set in progress status
            self._sql_exec(self._sql_compose('update', {'status': 'working'}, {'id': ('=', act_id)}))

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

            act_params = res[0][2] if not res[0][2] else json.loads(res[0][2], object_hook=dict_converter)

            # stdout dispatch
            prn_stream = io.StringIO()
            # prn_stream = sys.stdout

            with redirect_stdout(prn_stream):
                start = datetime.now()
                try:
                    factory = self._registry[act_type]['factory']
                    activity = factory(self, act_params)
                    activity.run()
                except Exception:
                    print('Fail:\n', traceback.format_exc())
                    status = 'fail'
                else:
                    status = 'finish'

            duration = (datetime.now() - start).seconds
            self._sql_exec(self._sql_compose('update', {'status': status,
                                                        'duration': duration,
                                                        'result': prn_stream.getvalue(),
                                                        'finish': datetime.now()}, {'id': ('=', act_id)}))

    def get_state(self, date: datetime = None):
        result = []
        if not date:
            date = datetime.today()-timedelta(days=1)
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
        result = self._sql_exec(query, result, factory)
        return {'actual_date': date, 'header': fields, 'data': result}


class Activity:
    def __init__(self, ldr: Loader, params=None):
        if params:
            self._params = params
        else:
            self._params = {}.fromkeys(self._fields())
        self._ldr = ldr

    def __setitem__(self, key, value):
        # key legal check
        if key not in self._fields():
            raise KeyError
        self._params[key] = value

    def __getitem__(self, key):
        return self._params[key]
        pass

    def get_params(self):
        return self._params

    def _fields(self):
        return []

    def get_type(self):
        return self.__class__.__name__

    def get_crontab(self):
        return None

    def apply(self, due_date=None):
        self._ldr.to_plan(self, due_date)

    def run(self):
        pass


class FakeEmail(Activity):
    def run(self):
        print(f'Mail send successfully. Data: {str(self._params)}')

    def get_crontab(self):
        return '*/5 * * * *'


class Email(Activity):
    def _fields(self):
        return ('subject from to cc body').split()

    def run(self):
        acc_key = KeyChain.SMTP_KEY
        msg = MIMEMultipart('alternative')
        msg['Subject'] = self['subject']
        msg['From'] = acc_key['user']
        msg['To'] = ", ".join(self['to']) if self['to'] else ''
        msg['Cc'] = ", ".join(self['cc']) if self['cc'] else ''

        # Record the MIME type of html - text/html.
        body = MIMEText(self['body'], 'html')

        # Attach HTML part into message container.
        msg.attach(body)

        try:
            server = smtplib.SMTP_SSL(host=acc_key['host'], port=acc_key['port'])
            server.login(acc_key['user'], acc_key['pwd'])
            server.sendmail(msg["From"], msg["To"].split(",") +
                            msg["Cc"].split(","), msg.as_string())
            server.quit()
            print("Successfully sent email")
        except smtplib.SMTPException as error:
            print(f"Unable to send email\nError: {error}")
            raise error


class LoaderStateReporter(Activity):
    def get_crontab(self):
        return '0 3 * * *'
    
    def run(self):
        report = self._ldr.get_state()
        actual = report['actual_date']
        thead = report['header']
        tbody = report['data']
        small_size = 50

        def long_cut(s):
            if s and len(s) > small_size:
                return f'{s[:small_size]}...'

        def date_cut(s):
            if s and s is not '':
                return str(s)[11:19]

        adapter_map = {
            'start': (date_cut,),
            'finish': (date_cut, ),
            'params': (long_cut, ),
            'result': (long_cut, ),
        }

        for row in tbody:
            for field, adapters in adapter_map.items():
                for adapter in adapters:
                    row[thead.index(field)] = adapter(row[thead.index(field)])

        email = Email(self._ldr)
        email['to'] = ('belov78@gmail.com',)
        email['subject'] = 'Loader daily report'

        tab_caption = f"Loader Report on {actual.strftime('%Y-%m-%d')}"

        tab_head = '{}{}{}'.format(
            '<tr class="table-dark">',
            ''.join(f'<th>{i}</th>' for i in thead),
            '</tr>\n'
        )
        status_idx = thead.index('status')

        def get_row_class(_row):
            style = {
                'todo': '',
                'working': 'table-warning',
                'finish': 'table-success',
                'fail': 'table-danger',
            }
            return style[_row[status_idx]]

        tab_rows = ''.join(
            f'<tr class="{get_row_class(row)}">{"".join(f"<td>{escape(str(col))}</td>" for col in row)}</tr>\n'
            for row in report['data'])

        html_table = f'<table class="table">' \
                     f'<thead>{tab_head}</thead><tbody>{tab_rows}</tbody></table>'

        html_head = '<head>' \
                    '   <meta name="viewport" content="width=device-width, initial-scale=1">' \
                    '   <meta charset="utf-8">' \
                    '   <link rel="stylesheet" ' \
                    '       href="https://bootswatch.com/4/spacelab/bootstrap.css">' \
                    '</head>'

        html_report = f'<html>\n{html_head}\n<body>' \
                      f'<div class="container">\n' \
                      f'<h3>{tab_caption}<h3>'\
                      f'{html_table}\n</div>' \
                      f'</body>\n</html>\n'

        email['body'] = html_report
        # f = open("mail.html", "w")
        # f.write(html_report)
        # f.close()
        email.apply()


class ISActualizer(Activity):

    def get_crontab(self):
        return '*/10 * * * *'

    def run(self):
        pass



class TestLoader(TestCase):
    def setUp(self):
        ldr = Loader(KeyChain.TEST_LOADER_KEY)
        ldr.register(FakeEmail)
        ldr.register(Email)
        ldr.register(LoaderStateReporter)
        self.ldr = ldr

    def test_to_plan(self):
        self.ldr.track_schedule()

    def test_get_activity_status(self):
        self.ldr.get_activity_status(1335)

if __name__ == '__main__':
    unittest.main()
