from datetime import datetime

from lib.schedutils import Activity
from lib.pg_utils import PGMix, sql
from keys import KeyChain
from connector import PGConnector, ISConnector, Task


class ClosedFix(Activity, PGMix):
    def __init__(self, ldr, params=None):
        Activity.__init__(self, ldr, params)
        PGMix.__init__(self, KeyChain.PG_KOMTET_KEY)

    def get_crontab(self):
        return '30 */2 * * *'

    def run(self):
        cursor = self._cursor(named=True)
        is_connector = ISConnector(KeyChain.IS_KEY)
        pg_connector = PGConnector(KeyChain.PG_KOMTET_KEY)
        now = datetime.now()

        # mark new open task
        cursor.execute(
            sql.SQL(
                'UPDATE "Tasks" SET "m_lastClosedTouch"={} '
                ' WHERE "Closed" IS NULL AND "m_lastClosedTouch" IS NULL').format(
                sql.Literal(now)
            )
        )
        self._commit()

        # get 1/12 of all opened task count
        cursor.execute('SELECT COUNT(*) AS cc FROM "Tasks" WHERE "Closed" IS NULL')
        open_tasks_count = cursor.fetchone().cc
        session_limit = round(open_tasks_count/12)

        # get tasks for current session
        open_task_query = 'SELECT "Id" AS task_id FROM "Tasks" WHERE "Closed" IS Null' \
                          ' ORDER BY "m_lastClosedTouch" LIMIT {}'.format(session_limit)
        cursor.execute(open_task_query)
        task_list = [record.task_id for record in cursor]

        # mark current session tasks
        mark_task_query = sql.SQL(
            'UPDATE "Tasks" SET "m_lastClosedTouch"={} where "Id" in ({})').format(
            sql.Literal(now),
            sql.SQL(', ').join(map(sql.Literal, task_list))
        )
        cursor.execute(mark_task_query)
        self._commit()

        for task_id in task_list:
            task = Task({'Id': task_id})
            if is_connector.is_404(task['Id']):
                # closed 404 tasks
                print("#{}".format(task_id))
                manual_task_close_query = sql.SQL(
                    'UPDATE "Tasks" SET "Closed"={}, "m_lastClosedTouch"={} WHERE "Id"={}').format(
                        sql.Literal(datetime.now()),
                        sql.Literal(datetime(1111, 11, 11)),  # manual closed marker
                        sql.Literal(task_id)
                    )
                self._cursor().execute(manual_task_close_query)
            else:
                is_connector.select(task)
                if task['Closed']:
                    print(',{}'.format(task['Id']))
                    pg_connector.update(task)

        self._commit()


from unittest import TestCase
from lib.schedutils import NullStarter


class TestClosedFix(TestCase):
    def setUp(self) -> None:
        self.a = ClosedFix(NullStarter())
        self.isc = ISConnector(KeyChain.IS_KEY)
        self.pgc = PGConnector(KeyChain.PG_KOMTET_KEY)

    def test_run(self):
        self.a.run()

    def test_pack(self):
        pg_con = self.pgc
        is_con = self.isc
        b = e = datetime(2020, 7, 2)
        update_pack = is_con.get_update_pack(b, e)
        for task in update_pack['Tasks'].values():
            pg_con.delete_task_actuals(task)
            pg_con.delete_task_executors(task)
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

