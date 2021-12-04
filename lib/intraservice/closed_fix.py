from datetime import datetime

from lib.schedutils import Activity
from lib.pg_utils import PGMix, sql
from keys import KeyChain
from lib.connectors.connector import PGConnector, ISConnector, Task


class ClosedFix(Activity, PGMix):
    PG_KEY = KeyChain.PG_IS_SYNC_KEY

    @classmethod
    def get_crontab(cls):
        return '30 */2 * * *'

    def remove_task_expenses(self, task_id, cursor):
        cursor.execute(
            sql.SQL(
                'delete from {} where {}={} returning {}'
            ).format(
                sql.Identifier('Expenses'),
                sql.Identifier('TaskId'),
                sql.Literal(task_id),
                sql.Identifier('Minutes')
            )
        )
        minutes_sum = minutes_count = 0
        for minutes in cursor.fetchall():
            minutes_sum += minutes[0]
            minutes_count += 1

        print("#{}{}".format(
            task_id,
            '(c:{}-s:{})'.format(minutes_count, minutes_sum) if minutes_count else ''
        ))
        manual_task_close_query = sql.SQL(
            'UPDATE "Tasks" SET "Closed"={}, "m_lastClosedTouch"={} WHERE "Id"={}').format(
            sql.Literal(datetime.now()),
            sql.Literal(datetime(1111, 11, 11)),  # manual closed marker
            sql.Literal(task_id)
        )
        cursor.execute(manual_task_close_query)

    def run(self):
        is_connector = ISConnector(KeyChain.IS_KEY)
        pg_connector = PGConnector(self.PG_KEY)
        now = datetime.now()

        # mark new open task
        with self.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    'UPDATE "Tasks" SET "m_lastClosedTouch"={} '
                    ' WHERE "Closed" IS NULL AND "m_lastClosedTouch" IS NULL').format(
                    sql.Literal(now)
                )
            )
            self.commit()

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
            self.commit()

            for task_id in task_list:
                task = Task({'Id': task_id})
                if is_connector.is_404(task['Id']):  # closed 404 tasks
                    self.remove_task_expenses(task_id, cursor)
                else:
                    is_connector.select(task)
                    if task['Closed']:
                        print(',{}'.format(task['Id']))
                        pg_connector.update(task)

            self.commit()


from unittest import TestCase
from lib.schedutils import NullStarter


class _ClosedFixTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_remove_expenses(self):
        task_id = 133738
        a = ClosedFix(NullStarter)
        with a.cursor() as cursor:
            a.remove_task_expenses(task_id, cursor)
