import sys
from unittest import TestCase

import jinja2
from flask import url_for


from keys import TestKeyChain
from loader import Loader
from activities.reg import *
from report import *
from connector import *


class TestActivities(TestCase):
    ldr: Loader
    @classmethod
    def setUpClass(cls):
        cls.ldr = Loader(TestKeyChain)

    def tearDown(self):
        self.ldr.track_schedule()

    def test_LoaderStateReporter2(self):
        rep = LoaderStateReporter2(self.ldr)
        rep.run()


class TestReports(TestCase):
    def setUp(self):
        pass

    def render_report(self, report_cls, params=None):
        cn = report_cls.get_connection(TestKeyChain.PG_KEY)
        report = report_cls(params)
        report.request_data(cn)
        template_ldr = jinja2.FileSystemLoader(searchpath="./templates")
        template_env = jinja2.Environment(loader=template_ldr)
        template_name = report.get_template()
        template = template_env.get_template(template_name)
        return template.render(rpt=report, url_for=url_for)

    def test_diag_report(self):
        self.render_report(DiagReport)



TEST_DATA = {
    'task': {
        'Id': 127112,
        'ServiceId': 178,
        'StatusId': 28,
        'ParentId': None,
        'Name': 'Интранет сайт для WiFi отелей',
        'Description': None,
        'Created': datetime(2018, 6, 27, 16, 29, 56),
        'CreatorId': 8225,
        'Closed': datetime(2019, 12, 17, 13, 13, 54),
        'Deadline': None,
        'FeedbackId': None,
    },
    'user': {
        'Id': 7154,
        'Name': 'Степанов Максим  ',
        'Email': 'stepanov.maxim@gmail.com'
    },
    'executor': {
        'TaskId': 12,
        'UserId': 100,
    },
    'actual': {
        'Id': 71352,
        'TaskId': 151593,
        'UserId': 5372,
        'Date': datetime(2019, 12, 13, 00, 00, 00),
        'Minutes': 10,
        'Rate': 0.0,
        'Comments': ''
    },
    'service': {
        'Id': 75,
        'Code': 'INT_wp',
        'Name': 'Рабочее место',
        'Description': '',
        'IsArchive': False,
        'IsPublic': False,
        'ParentId': 66,
        'Path': '66|75|',
    }

}

class TestPGConnector(TestCase):
    def setUp(self):
        pass

    def test_task(self):
        self._entity_test(Task)

    def test_user(self):
        self._entity_test(User)

    def test_executor(self):
        self._entity_test(Executor)

    def test_actual(self):
        self._entity_test(Actual)

    def test_service(self):
        self._entity_test(Service)

    def _entity_test(self, entity_factory):
        pg_con = PGConnector(KeyChain.PG_KEY)
        entity = entity_factory()
        test_data = TEST_DATA[entity.get_type()]
        entity = entity_factory(test_data)

        # constructor test
        for key, value in test_data.items():
            self.assertEqual(entity[key], value)

        # check item set/get
        test_val = 'Example nest content'
        entity['Description'] = test_val
        self.assertEqual(entity['Description'], test_val)

        # check fields mapping integrity
        f = entity.get_fields()
        m = list(pg_con.get_fields_map(entity).keys())
        # check field size
        self.assertEqual(len(f), len(m))
        for i in f:
            self.assertTrue(i in m, i)

        if pg_con.is_read_only():
            entity = entity_factory()
            entity['Id'] = test_data['Id']
            pg_con.select(entity)
            for key in entity.keys():
                if key is "Description":
                    continue
                self.assertEqual(entity[key], test_data[key])

        else:
            # DELETE statement check
            entity = entity_factory(test_data)
            pg_con.delete(entity)
            self.assertFalse(pg_con.is_exist(entity))

            # INSERT statement check
            pg_con.update(entity)
            self.assertTrue(pg_con.is_exist(entity))
            entity = entity_factory()
            entity.set_id(test_data)
            pg_con.select(entity)
            for key in test_data.keys():
                if key is "Description":
                    continue
                self.assertEqual(entity[key], test_data[key], key)

            # UPDATE statement check
            entity = entity_factory()
            entity.set_id(test_data)
            pg_con.delete(entity)
            pg_con.update(entity)
            entity = entity_factory(test_data)
            pg_con.update(entity)
            pg_con.select(entity)
            for key in test_data.keys():
                if key is "Description":
                    continue
                self.assertEqual(entity[key], test_data[key])

    def test_delete_task_actuals(self):
        pg_con = PGConnector(TestKeyChain.PG_KEY)
        a = Actual(TEST_DATA['actual'])
        task_id = a['TaskId']
        pg_con.update(a)
        t = Task()
        t['Id'] = task_id
        pg_con.delete_task_actuals(t)
        self.assertFalse(pg_con.is_exist(a))

class TestISConnector(TestCase):
    def setUp(self):
        # test db_key
        self.connector = ISConnector(TestKeyChain.IS_KEY)

    def test_select_task(self):
        factory = Task
        test_data = TEST_DATA['task']
        entity = factory(test_data)
        entity_id = entity['Id']
        entity = factory({'Id': entity_id})
        self.connector.select(entity)
        for key in test_data.keys():
            if key is 'Description':
                continue
            self.assertEqual(entity[key], test_data[key])

    def test_select_user(self):
        factory = User
        test_data = TEST_DATA[factory().get_type()]
        entity = factory(test_data)
        entity_id = entity['Id']
        entity = factory({'Id': entity_id})
        self.connector.select(entity)
        for key in test_data.keys():
            self.assertEqual(entity[key], test_data[key])

    def test_select_actual(self):
        factory = Actual
        test_data = TEST_DATA[factory().get_type()]
        entity = factory(test_data)
        entity_id = entity['Id']
        entity = factory({'Id': entity_id})
        self.connector.select(entity)
        for key in test_data.keys():
            self.assertEqual(entity[key], test_data[key], key)

    def test_select_service(self):
        factory = Service
        test_data = TEST_DATA[factory().get_type()]
        entity = factory(test_data)
        entity_id = entity['Id']
        entity = factory({'Id': entity_id})
        self.connector.select(entity)
        for key in test_data.keys():
            self.assertEqual(entity[key], test_data[key])

    def test_get_update_pack(self):
        pg_con = PGConnector(KeyChain.PG_KEY)
        is_con = self.connector
        start = datetime(2019, 12, 18, 12, 00)
        finish = datetime(2019, 12, 18, 12, 30)

        update_pack = is_con.get_update_pack(start, finish)
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

        pass



if __name__ == '__main__':
    unittest.main()
