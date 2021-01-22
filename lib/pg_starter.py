import io
from contextlib import redirect_stdout
from datetime import datetime
from datetime import timedelta
from croniter import croniter
import traceback


import psycopg2
from psycopg2 import sql
from psycopg2 import extras

from lib.schedutils import Starter
from lib.schedutils import Activity
import lib.tablesync as tablesync

from activities import reg


class PGStarter(Starter):
    _table_name = "Loader"

    def __init__(self, pg_key):
        super().__init__(pg_key)
        self._db_conn = psycopg2.connect(dbname=pg_key["db_name"], user=pg_key["user"],
                                         password=pg_key["pwd"], host=pg_key["host"], port=pg_key.get("port", None))

        self.external_tabs = {}
        # download external crontabs setting
        _tab = tablesync.download_table(pg_key['cron_tabs'])
        for rec in _tab['data']:
            _type = rec[0]
            _cron = rec[1]
            if croniter.is_valid(_cron):
                self.external_tabs[_type]=_cron

        self._registry = {}
        reg.init_ldr(self)

    def register(self, factory):
        """ Activities register method for activities produce and schedule control
        """
        _act = factory(self)
        _type = _act.get_type()
        _cron = self.external_tabs.get(_type) or _act.get_crontab()
        self._registry[_type] = {
            'factory': factory,
            'crontab': _cron,
        }

    def to_plan(self, activity: Activity, due_date=None) -> int:
        query_params = {
            'type': activity.get_type(),
            'status': self.JobStatus.TODO,
            'plan': due_date if due_date else datetime.now(),
            'params': activity.dump_params()
        }
        insert_query = sql.SQL("INSERT INTO {}({}) VALUES ({}) RETURNING {}").format(
            sql.Identifier(self._table_name),
            sql.SQL(', ').join(sql.Identifier(field) for field in query_params.keys()),
            sql.SQL(', ').join(sql.Literal(value) for value in query_params.values()),
            sql.Identifier('id')
        )
        cursor = self._db_conn.cursor()
        cursor.execute(insert_query)
        self._db_conn.commit()
        return cursor.fetchone()[0]

    def _id_to_dump(self, activity_id: int):
        result = {}
        select_params_query = sql.SQL('SELECT {} FROM {} WHERE {}={}').format(
            sql.Identifier('params'), sql.Identifier(self._table_name),
            sql.Identifier('id'), sql.Literal(activity_id)
        )
        cursor = self._db_conn.cursor()
        cursor.execute(select_params_query)
        res = cursor.fetchone()
        if len(res):
            result = res[0]
        return result

    def _update_current_crontab_schedule(self):
        # select not execute record from journal
        query = sql.SQL('SELECT {} FROM {} WHERE {}').format(
            sql.SQL(', ').join(map(sql.Identifier, ('id', 'type', 'plan'))),
            sql.Identifier(self._table_name),
            sql.SQL('{} = {} AND {} > {} AND {} is Null').format(
                sql.Identifier('status'), sql.Literal(self.JobStatus.TODO),
                sql.Identifier('plan'), sql.Literal(datetime.now()),
                sql.Identifier('params')
            )
        )

        cursor = self._db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        cursor.execute(query)
        rows = cursor.fetchall()
        rows = rows if rows else list()  # change None to empty list

        # id, type, start and valid state (used later)
        schedule = [[row.id, row.type, row.plan, False] for row in rows]

        # check current crontab schedule from script
        for activity_type, params in self._registry.items():
            cron_tab = params['crontab'] or ''
            if not croniter.is_valid(cron_tab):
                continue
            next_plan = croniter(params['crontab'], datetime.now()).get_next(datetime)
            need_to_plan = True
            for rec in schedule:
                if rec[1] == activity_type:
                    if next_plan == rec[2]:
                        rec[3] = True
                        need_to_plan = False
                        break
            if need_to_plan:
                insert_params = {
                    'type': activity_type,
                    'status': 'todo',
                    'plan': next_plan,
                }
                insert_new_schedule_query =\
                    sql.SQL('insert into {}({}) values({})').format(
                        sql.Identifier(self._table_name),
                        sql.SQL(', ').join(sql.Identifier(field) for field in insert_params.keys()),
                        sql.SQL(', ').join(sql.Literal(value) for value in insert_params.values())
                    )
                cursor.execute(insert_new_schedule_query)
                self._db_conn.commit()

        # prepare journal record list for removing
        rec_for_delete = [rec[0] for rec in schedule if not rec[3]]
        # and removing them
        if len(rec_for_delete):
            delete_not_valid_record_query = sql.SQL('DELETE FROM {} WHERE {} IN ({})').format(
                sql.Identifier(self._table_name),
                sql.Identifier('id'),
                sql.SQL(', ').join(sql.Literal(_id) for _id in rec_for_delete)
            )
            cursor.execute(delete_not_valid_record_query)
            self._db_conn.commit()

    def clear_schedule(self):
        """ WARNING: Delete all schedule records Using for test fixture ONLY """
        q_clear_all = sql.SQL('delete from {}').format(sql.Identifier(self._table_name))
        cursor = self._db_conn.cursor()
        cursor.execute(q_clear_all)
        self._db_conn.commit()

    def track_schedule(self):
        self._update_current_crontab_schedule()

        # infinity loop: try to execute all actual activities
        while True:
            # select next journal record for execute
            select_next_record_query = \
                sql.SQL('SELECT {}, {}, {} FROM {} WHERE {}={} AND {}<={} LIMIT 1').format(
                    sql.Identifier('id'), sql.Identifier('type'),
                    sql.Identifier('params'), sql.Identifier(self._table_name),
                    sql.Identifier('status'), sql.Literal(self.JobStatus.TODO),
                    sql.Identifier('plan'), sql.Literal(datetime.now())
                )
            cursor = self._db_conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
            cursor.execute(select_next_record_query)
            record = cursor.fetchone()

            if not record:
                break  # no record for execution any more

            act_id = record.id
            act_type = record.type
            act_dump = record.params

            start = datetime.now()
            update_params = {
                'start': start,
                'status': self.JobStatus.WORKING,
            }

            update_progress_status_query = sql.SQL('UPDATE {} SET ({}) = ({}) WHERE {} = {}').format(
                sql.Identifier(self._table_name),
                sql.SQL(', ').join([sql.Identifier(key) for key in update_params.keys()]),
                sql.SQL(', ').join([sql.Literal(value) for value in update_params.values()]),
                sql.Identifier('id'), sql.Literal(act_id),
            )
            cursor.execute(update_progress_status_query)
            self._db_conn.commit()

            # stdout redirect
            prn_stream = io.StringIO()  # sys.stdout
            with redirect_stdout(prn_stream):
                try:
                    factory = self._registry[act_type]['factory']
                    activity = factory(self)
                    activity.update_params(act_dump)
                    activity.run()
                except Exception:
                    print('Fail:\n', traceback.format_exc())
                    status = self.JobStatus.FAIL
                else:
                    status = self.JobStatus.DONE
            finish = datetime.now()
            duration = (finish - start).seconds

            update_params = {
                'status': status,
                'duration': duration,
                'result': prn_stream.getvalue(),
                'finish': finish
            }
            update_progress_status_query = sql.SQL('UPDATE {} SET ({}) = ({}) WHERE {} = {}').format(
                sql.Identifier(self._table_name),
                sql.SQL(', ').join([sql.Identifier(key) for key in update_params.keys()]),
                sql.SQL(', ').join([sql.Literal(value) for value in update_params.values()]),
                sql.Identifier('id'), sql.Literal(act_id),
            )
            cursor.execute(update_progress_status_query)
            self._db_conn.commit()

    def get_activity_status(self, activity_id: int):
        select_activity_status_query = sql.SQL('SELECT {} FROM {} WHERE {}={}').format(
            sql.Identifier('status'),
            sql.Identifier(self._table_name),
            sql.Identifier('id'),
            sql.Literal(activity_id),
        )
        cursor = self._db_conn.cursor()
        cursor.execute(select_activity_status_query)
        result = cursor.fetchone()
        return None if not result else result[0]

    def get_state(self, on_day: datetime = None, status_filter: list = None) -> dict:
        if not on_day:
            on_day = datetime.today() - timedelta(days=1)

        start = datetime(on_day.year, on_day.month, on_day.day)
        finish = start + timedelta(days=1)

        fields = 'id type status plan start finish duration params result logs'.split()
        query_state_data = sql.SQL('SELECT {} FROM {} WHERE {} ORDER BY {} DESC').format(
            sql.SQL(', ').join(sql.Identifier(f) for f in fields),
            sql.Identifier(self._table_name),
            sql.SQL('plan BETWEEN {} AND {} {}').format(
                sql.Literal(start),
                sql.Literal(finish),
                sql.SQL('') if not status_filter else sql.SQL('AND {} IN ({})').format(
                    sql.Identifier('status'), sql.Literal(status_filter)
                )
            ),
            sql.Identifier('id')
        )

        cursor = self._db_conn.cursor()
        cursor.execute(query_state_data)
        result = cursor.fetchall()

        return {'actual_date': on_day, 'header': fields, 'data': result}


from unittest import TestCase
from keys import KeyChain


class PGStarterTest(TestCase):
    class TestActivity(Activity):
        def _fields(self) -> str:
            return 'result'

        def get_crontab(self):
            return None  # '0/5 * * * *'

        def run(self):
            self._ldr.run_result = self['result']

    def setUp(self) -> None:
        self._starter = PGStarter(KeyChain.PG_STARTER_KEY)
        self._starter.clear_schedule()
        self._starter.register(self.TestActivity)

    def test_to_plan(self):
        starter = self._starter
        starter.run_result = None
        a = PGStarterTest.TestActivity(starter)
        result = datetime.now()
        a['result'] = result
        ida = starter.to_plan(a)
        dump = starter._id_to_dump(ida)
        starter.track_schedule()
        b = PGStarterTest.TestActivity(starter)
        b.update_params(dump)
        for key, value in a.get_params().items():
            self.assertEqual(value, b[key])
        self.assertEqual(starter.run_result, result)
        report = starter.get_state(datetime.now())
        for record in report['data']:
            print(' | '.join([str(v) for v in record]))

    def test_get_activity_status(self):
        a = PGStarterTest.TestActivity(self._starter)
        ida = a.apply()
        self._starter.track_schedule()
        self._starter.track_schedule()
        status = self._starter.get_activity_status(ida)
        self.assertEqual(status, PGStarter.JobStatus.DONE)

    def test_track_schedule(self):
        self._starter.track_schedule()

    def test_external_crontabs(self):
        print(self._starter._registry)


from activities.intraservice import ISActualizer

class ISActualizerTest(TestCase):
    def test_run(self):
        a = ISActualizer(PGStarter(KeyChain.PG_STARTER_KEY))
        a.run()




