from lib.schedutils import Activity
from lib.pg_utils import PGMix
from keys import KeyChain
from connector import ISConnector, Task


class ClosedFix(Activity, PGMix):
    def __init__(self, ldr, params=None):
        Activity.__init__(self, ldr, params)
        PGMix.__init__(self, KeyChain.PG_KOMTET_KEY)

    def get_crontab(self):
        return '35 15 * * *'

    def run(self):
        is_connector = ISConnector(KeyChain.IS_KEY)
        open_task_query = 'SELECT "Id" AS task_id FROM "Tasks" WHERE "Closed" IS Null ORDER BY "Id"'
        cursor = self._cursor(named=True)
        cursor.execute(open_task_query)
        print('Open task count:{}'.format(cursor.rowcount))
        for record in cursor:
            task = Task({'Id': record.task_id})
            is_connector.select(task)
            if task['Closed']:
                print('#{}'.format(task['Id']))






from unittest import TestCase
from lib.schedutils import NullStarter


class TestClosedFix(TestCase):
    def setUp(self) -> None:
        self.a = ClosedFix(NullStarter())

    def test_run(self):
        self.a.run()


