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

        # mark new open task
        cursor.execute('UPDATE "Tasks" SET "m_lastClosedTouch"="Created" '
                       ' WHERE "Closed" IS NULL AND "m_lastClosedTouch" IS NULL')
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
        mark_task_query = 'UPDATE "Tasks" SET "m_lastClosedTouch"=now() where "Id" in ({})'.format(
            ', '.join(map(str, task_list))
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

    def test_run(self):
        self.a.run()


