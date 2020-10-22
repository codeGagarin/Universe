from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from psycopg2 import extras

from keys import KeyChain
from lib.schedutils import Activity
from connector import ISConnector
from connector import PGConnector


class PGActivity(Activity):
    def __init__(self, ldr, params=None):
        super().__init__(ldr, params)
        pg_key = KeyChain.PG_KEY
        self._db_conn = psycopg2.connect(dbname=pg_key["db_name"], user=pg_key["user"],
                                         password=pg_key["pwd"], host=pg_key["host"], port=pg_key.get("port", None))

    def sql_exec(self, query, result=None, result_factory=None, auto_commit=True, named_result=False):
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


class ISSync(PGActivity):
    def _fields(self):
        return 'from to'

    def run(self):
        pg_con = PGConnector(KeyChain.PG_KEY)
        is_con = ISConnector(KeyChain.IS_KEY)

        update_pack = is_con.get_update_pack(self['from'], self['to'])
        for task in update_pack['Tasks'].values():
            pg_con.delete_task_actuals(task)
            pg_con.update(task)

        for user in update_pack['Users'].values():
            pg_con.update(user)

        for actual in update_pack['Actuals']:
            pg_con.update(actual)

        for service in update_pack['Services'].values():
            pg_con.update(service)

        for executor in update_pack['Executors']:
            pg_con.update(executor)

        print(f"Ts:{len(update_pack['Tasks'])}, "
              f"Us:{len(update_pack['Users'])}, "
              f"Ac:{len(update_pack['Actuals'])}, "
              f"Sr:{len(update_pack['Services'])}, "
              f"Ex:{len(update_pack['Executors'])}.")


class ISActualizer(PGActivity):

    def get_crontab(self):
        return '0 */1 * * *'

    def _add_job(self, from_date: datetime, to_date: datetime, activity_id: int):
        query = sql.SQL('INSERT INTO {}({},{},{}) VALUES ({},{},{}) RETURNING {}').format(
            sql.Identifier('SyncJobs'),
            sql.Identifier('from'), sql.Identifier('to'), sql.Identifier('activity_id'),
            sql.Literal(from_date), sql.Literal(to_date), sql.Literal(activity_id),
            sql.Identifier('id')
        )
        return self.sql_exec(query, auto_commit=False)[0][0]

    def run(self):
        query = sql.SQL('SELECT {}, {} FROM {} WHERE {} IS NOT NULL ORDER BY {} DESC LIMIT 1').format(
            sql.Identifier('to'),
            sql.Identifier('activity_id'),
            sql.Identifier('SyncJobs'),
            sql.Identifier('activity_id'),
            sql.Identifier('to')
        )
        result = self.sql_exec(query, auto_commit=False)
        if not len(result):
            last_update_tic = datetime(2019, 10, 1, 0, 0, 0)
            print("First launch detected")
        else:
            state = self._ldr.get_activity_status(result[0][1])
            if state != "finish":
                return  # actualization in progress
            last_update_tic = result[0][0]

        from_date = last_update_tic
        d = timedelta(hours=6)
        to_date = from_date + d
        if to_date > datetime.now():
            to_date = datetime.now()

        m = ISSync(self._ldr)
        m['from'] = from_date
        m['to'] = to_date
        activity_id = m.apply()

        job_id = self._add_job(from_date, to_date, activity_id)
        print(f'Job id:{job_id} added.')
        self._db_conn.commit()


class IS404TaskCloser(PGActivity):
    """Closed all union 404 url tasks"""
    def get_crontab(self):
        return '30 */1 * * *'

    def run(self):
        # mark new open task
        query = sql.SQL('UPDATE "Tasks" SET "m_lastClosedTouch"="Created" '
                        ' WHERE "Closed" IS NULL AND "m_lastClosedTouch" IS NULL')
        self.sql_exec(query)

        # get 1/24 opened task count
        query = sql.SQL('SELECT COUNT(*) AS cc FROM "Tasks" WHERE "Closed" IS NULL')
        rows = self.sql_exec(query, named_result=True)
        open_tasks_count = rows[0].cc
        limit = open_tasks_count/12

        # select older closed touch tasks
        query = sql.SQL('SELECT "Id" AS idx FROM "Tasks" WHERE "Closed" IS NULL ORDER BY "m_lastClosedTouch" LIMIT 10')
        rows = self.sql_exec(query, named_result=True)

        # check 404 url task error
        is_conn = ISConnector(KeyChain.IS_KEY)
        for rec in rows:
            if is_conn.is_404(rec.idx):
                # closed 404 tasks
                print(f"#{rec.idx} ")
                query = sql.SQL('UPDATE "Tasks" SET "Closed"={}, "m_lastClosedTouch"={} '
                                ' WHERE "Id"={}').format(
                    sql.Literal(datetime.now()),
                    sql.Literal(datetime(1111, 11, 11)),  # manual closed marker
                    sql.Literal(rec.idx)
                )
            else:
                # update last_touch param
                query = sql.SQL('UPDATE "Tasks" SET "m_lastClosedTouch"={} '
                                ' WHERE "Id"={}').format(sql.Literal(datetime.now()), sql.Literal(rec.idx))
            self.sql_exec(query)


from unittest import TestCase
from lib.schedutils import NullStarter


class ISActualizerTest(TestCase):
    def test_run(self):
        a = ISActualizer(NullStarter())
        a.run()
