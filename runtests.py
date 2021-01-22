import sys
from unittest import TestCase

import jinja2
from flask import url_for

from keys import KeyChain
from lib.pg_starter import PGStarter
from activities.reg import *
from report import *
from connector import *


class TestActivities(TestCase):
    ldr: PGStarter
    @classmethod
    def setUpClass(cls):
        cls.ldr = PGStarter(KeyChain.PG_STARTER_KEY)

    def tearDown(self):
        self.ldr.track_schedule()

    def test_LoaderStateReporter2(self):
        rep = LoaderStateReporter2(self.ldr)
        rep.run()

    def test_HelpdeskWeekly(self):
        rep = HelpdeskWeekly(self.ldr)
        rep.run()

    def test_ISActualizer(self):
        rep = ISActualizer(self.ldr)
        rep.run()

    def test_IS404TaskCloser(self):
        rep = IS404TaskCloser(self.ldr)
        rep.run()

    def teest_EMail(self):
        activity_id = 5489
        params = self.ldr.id_to_params(activity_id)
        activity = EmailActivity(self.ldr, params)
        activity.run()

    def test_EMail(self):
        cn = HelpdeskReport.get_connection(KeyChain.PG_KEY)
        li = ('it@bglogistic.ru',)
        to = HelpdeskReport.users_to_email(cn, li)

        mail = EmailActivity(self.ldr)
        mail['to'] = to
        mail.apply()



    def test_SMTP(self):
        params = {
            'smtp': 'RRT',
            'to': ('belov78@gmail.com',),
            # 'cc': ('i.belov@prosto12.ru',),
            'subject': 'smpt test',
            'body': 'Body'
        }
        activity = EmailActivity(self.ldr, params)
        activity.run()



class TestReports(TestCase):
    def setUp(self):
        pass

    def render_report(self, report_cls, param_idx):
        cn = report_cls.get_connection(KeyChain.PG_KEY)
        report = Report.factory(cn, {'idx': param_idx})
        report.request_data(cn)
        template_ldr = jinja2.FileSystemLoader(searchpath="./templates")
        template_env = jinja2.Environment(loader=template_ldr)
        template_name = report.get_template()
        template = template_env.get_template(template_name)
        return template.render(rpt=report)

    def test_users_to_email(self):
        cn = HelpdeskReport.get_connection(KeyChain.PG_KEY)
        li = ('it@bglogistic.ru',)
        print(HelpdeskReport.users_to_email(cn, li))

    def test_DiagReport(self):
        self.render_report(DiagReport, 15706847338409224)

    def test_HelpdeskReport(self):
        self.render_report(HelpdeskReport, 316531349939618110)

    def test_TaskReport(self):
        self.render_report(TaskReport, 256482489741668139)

    def test_ExpensesReport(self):
        self.render_report(ExpensesReport, 322920820642275206)


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
        pg_con = PGConnector(KeyChain.PG_KEY)
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
        self.connector = ISConnector(KeyChain.IS_KEY)

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
