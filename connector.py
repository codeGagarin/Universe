from datetime import datetime
import requests
import unittest

from abc import abstractmethod
from unittest import TestCase
from urllib3.util.retry import Retry
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter

import psycopg2

from keys import KeyChain


class DataEntity:

    def __init__(self, data=None):
        if not data:
            data = {}
        self._data = {}
        for i in self.get_fields():
            self._data[i] = data.get(i)

    def get_fields(self):
        return list(self._fields.keys())

    def get_field_type(self, field_name: str):
        return self._fields[field_name]

    def get_key_fields(self):
        return 'Id',

    def set_id(self, data: dict):
        for key in self.get_key_fields():
            self[key] = data[key]

    def __getitem__(self, key: str):
        return self._data[key]

    def __setitem__(self, key: str, value):
        self._data[key] = value

    def get_type(self):
        return self._type

    # ToDo: add methods GetFieldWrapper() or DataEntity decorator for data connectors

    def __repr__(self):
        return f"Type: {self._type}, Data: {self._data.__repr__()}"


class DataConnector:

    def is_exist(self, entity: DataEntity):
        pass

    def delete(self, entity: DataEntity):
        pass

    def update(self, entity: DataEntity):
        pass

    def select(self, entity: DataEntity):
        pass

    # ToDo: deprecated, use get_resource_name instead
    def _get_entity_table(self, entity: DataEntity):
        return self._tables_map[entity.get_type()]

    def get_resource_name(self, entity: DataEntity):
        return self._tables_map[entity.get_type()]

    # ToDo: deprecated, use get_resource_field instead
    def get_fields_map(self, entity: DataEntity):
        t = entity.get_type()
        return self._fields_map[t]

    def get_resource_field(self, entity: DataEntity, entity_field: str):
        t = entity.get_type()
        return self._fields_map[t][entity_field]

    def is_read_only(self):
        return True


def _pgw_d_get(value):
    return f"'{value}'" if value else 'NULL'


def _pgw_d_set(value):
    return value


def _pgw_dc_get(value):
    return f"'{value}'" if value is not None else 'NULL'


def _pgw_dc_set(value):
    return value


def _pgw_s_get(value: str):
    return f"'{value}'" if value is not None else 'NULL'


def _pgw_i_get(value: int):
    return f"{value}" if value else 'NULL'


def _pgw_i_set(value):
    return int(value) if value else None


def _pgw_b_get(value):
    return f"{value}" if value is not None else 'NULL'


def _pgw_b_set(value):
    return value


def _pgw_transparent(value):
    return value


# Todo: need super-class for connector wrapper
class _PGW:
    """ Entity value to Postgres column adapter """

    _adapter_map = {
        # type / get / set
        'd': (_pgw_d_get, _pgw_d_set),
        's': (_pgw_s_get, None),
        'i': (_pgw_i_get, None),
        'b': (_pgw_b_get, _pgw_b_set),
        'dc': (_pgw_dc_get, _pgw_dc_set)
    }

    def _get_adapter_get(self, key: str):
        record = self._adapter_map.get(self._entity.get_field_type(key))
        if record:
            adapter = record[0]
            if adapter:
                return adapter
        return _pgw_transparent

    def _get_adapter_set(self, key: str):
        record = self._adapter_map.get(self._entity.get_field_type(key))
        if record:
            adapter = record[1]
            if adapter:
                return adapter
        return _pgw_transparent

    def __init__(self, entity: DataEntity):
        self._entity = entity

    def __setitem__(self, key, value):
        self._entity[key] = self._get_adapter_set(key)(value)

    def __getitem__(self, key):
        value = self._entity[key]
        return self._get_adapter_get(key)(value)


_PG_CONNECTOR_MAPPING = {
    'tables_map': {
        'task': 'Tasks',
        'user': 'Users',
        'executor': 'Executors',
        'actual': 'Expenses',
        'service': 'Services',
    },
    # declare translation map [entity fields <-> table column]
    'fields_map': {
        'task': {
            'Id': 'Id',
            'ServiceId': 'ServiceId',
            'StatusId': 'StatusId',
            'ParentId': 'ParentId',
            'Name': 'Name',
            'Description': 'Description',
            'Created': 'Created',
            'CreatorId': 'CreatorId',
            'Closed': 'Closed',
            'Deadline': 'Deadline',
            'FeedbackId': 'EvaluationId',
        },
        'user': {
            'Id': 'Id',
            'Name': 'Name',
            'Email': 'Email',
        },
        'executor': {
            'TaskId': 'TaskId',
            'UserId': 'UserId',
        },
        'actual': {
            'Id': 'Id',
            'TaskId': 'TaskId',
            'UserId': 'UserId',
            'Date': 'DateExp',
            'Minutes': 'Minutes',
            'Rate': 'Rate',
            'Comments': 'Comments'
        },
        'service': {
            'Id': 'Id',
            'Code': 'Code',
            'Name': 'Name',
            'Description': 'Description',
            'IsArchive': 'IsArchive',
            'IsPublic': 'IsPublic',
            'ParentId': 'ParentId',
            'Path': 'Path',
        },
    }
}


class PGConnector(DataConnector):
    # declare tables translation map [core <-> storage]

    def __init__(self, acc_key: dict):
        self._tables_map = _PG_CONNECTOR_MAPPING['tables_map']
        self._fields_map = _PG_CONNECTOR_MAPPING['fields_map']
        self._key = acc_key
        self._db_conn = psycopg2.connect(dbname=acc_key["db_name"], user=acc_key["user"],
                                         password=acc_key["pwd"], host=acc_key["host"])

    def is_read_only(self):
        return False

    def _field_to_col(self, field_name: str, entity: DataEntity):
        """ Convert entity field to database table column"""
        m = self.get_fields_map(entity)
        return m[field_name]

    def __del__(self):
        self._db_conn.commit()
        self._db_conn.close()

    def _sql_exec(self, sql_str: str):
        cursor = self._db_conn.cursor()
        cursor.execute(sql_str)
        try:
            result = cursor.fetchall()
        except Exception:
            result = None

        cursor.close()
        self._db_conn.commit()
        return result

    def select(self, entity: DataEntity):
        table = self._get_entity_table(entity)
        field_map = self.get_fields_map(entity)
        fields_str = ', '.join(['"' + f + '"' for f in field_map.values()])
        sql_str = f'SELECT {fields_str} FROM "{table}" WHERE {self._get_where_str(entity)}'
        records = self._sql_exec(sql_str)

        result = True
        if len(records) is 0:
            result = False
        else:
            row = records[0]
            keys = list(entity.get_fields())
            col_names = field_map.values()
            for i in range(0, len(col_names)):
                _PGW(entity)[keys[i]] = row[i]

        return result

    def _get_where_str(self, entity: DataEntity):
        """ return where sql-string for multi ids entity"""
        return ' AND '.join(
            [f'"{self._field_to_col(key, entity)}" = {_PGW(entity)[key]}' for key in entity.get_key_fields()]
        )

    def is_exist(self, entity: DataEntity):
        table = self._get_entity_table(entity)
        where_str = self._get_where_str(entity)
        sql_str = f'SELECT * from "{table}" WHERE {where_str}'
        records = self._sql_exec(sql_str)

        if len(records) is 0:
            return False
        elif len(records) is 1:
            return True
        else:
            print(f'\nDatabase Layer Warning!!!')
            print(f'Multiple ID store detected.')
            print(f'table: {table}, id: {where_str}, times: {len(records)}')
            print(f'Database Layer Warning!!!\n')
            return True

    def delete(self, entity: DataEntity):
        table = self._get_entity_table(entity)
        where_str = self._get_where_str(entity)
        sql_str = f'DELETE FROM "{table}" WHERE {where_str}'
        self._sql_exec(sql_str)

    def update(self, entity: DataEntity):
        field_map = self.get_fields_map(entity)

        table = self._get_entity_table(entity)
        if not self.is_exist(entity):
            # INSERT statement
            param_list_sql = ', '.join(['"' + field_map[f] + '"' for f in entity.get_fields()])
            value_list_sql = ', '.join([_PGW(entity)[key] for key in entity.get_fields()])
            sql_str = f'INSERT INTO "{table}" ({param_list_sql}) VALUES ({value_list_sql})'
        else:
            # UPDATE statement
            param_list_sql = ', '.join([f'"{field_map[key]}"={_PGW(entity)[key]}' for key in entity.get_fields()])
            sql_str = f'UPDATE "{table}" SET {param_list_sql} WHERE {self._get_where_str(entity)}'

        self._sql_exec(sql_str)

    def delete_task_actuals(self, task: DataEntity):
        actual = Actual()
        resource = self.get_resource_name(actual)
        field = 'TaskId'
        resource_field = self.get_resource_field(Actual(), field)
        value = _PGW(task)['Id']
        sql_str = f'DELETE FROM "{resource}" WHERE "{resource_field}" = {value}'
        self._sql_exec(sql_str)


# ToDo: need to rename field 'Expenses' to 'Actuals' in database
_INTRA_CONNECTOR_MAPPING = {
    'tables_map': {
        'task': 'Task',
        'user': 'User',
        'executor': 'Executors',
        'actual': 'taskexpenses',
        'service': 'Service'
    },
    # declare translation map [entity fields <-> table column]
    'fields_map': {
        'task': {
            'Id': 'Id',
            'ServiceId': 'ServiceId',
            'StatusId': 'StatusId',
            'ParentId': 'ParentId',
            'Name': 'Name',
            'Description': 'Description',
            'Created': 'Created',
            'CreatorId': 'CreatorId',
            'Closed': 'Closed',
            'Deadline': 'Deadline',
            'FeedbackId': 'EvaluationId',
        },
        'user': {
            'Id': 'Id',
            'Name': 'Name',
            'Email': 'Email',
        },
        'executor': {
            'TaskId': 'TaskId',
            'UserId': 'UserId',
        },
        'actual': {
            'Id': 'Id',
            'TaskId': 'TaskId',
            'UserId': 'UserId',
            'Date': 'Date',
            'Minutes': 'Minutes',
            'Rate': 'Rate',
            'Comments': 'Comments'
        },
        'service': {
            'Id': 'Id',
            'Code': 'Code',
            'Name': 'Name',
            'Description': 'Description',
            'IsArchive': 'IsArchive',
            'IsPublic': 'IsPublic',
            'ParentId': 'ParentId',
            'Path': 'Path',
        },
    }
}


def _inw_d_set(value: str):
    return value if not value else datetime.strptime(value[0: 19], '%Y-%m-%dT%H:%M:%S')


def _inw_d_get(value: datetime):
    return value if not value else value.strftime('%Y-%m-%d %H:%M:%S')


class _INW:
    """ Entity value to Intraservice field adapter """
    adapter_map = {
        # type / get / set
        'd': (_inw_d_get, _inw_d_set),
    }

    def __init__(self, entity: DataEntity):
        self._entity = entity

    def __setitem__(self, key, value):
        data_type = self._entity.get_field_type(key)
        data_adapter = self.adapter_map.get(data_type)
        self._entity[key] = data_adapter[1](value) if data_adapter else value


class IntraConnector(DataConnector):
    def _api_request_get(self, resource: str, params: dict, result_factory, result=None):
        if not result:
            result = {}

        base_url = self._acc_key['url']
        session = requests.Session()
        retries = Retry(total=25,
                        backoff_factor=0.0001,
                        status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        # Make API request
        url = f"{base_url}{resource}"
        r = session.get(url=url, auth=self._auth, params=params)
        session.close()
        raw_data = dict(r.json())
        result_factory(result, raw_data)
        if raw_data.get('Paginator'):
            page_count = raw_data['Paginator']['PageCount']

            if page_count > 1:
                for page in range(page_count, 0, -1):
                    params.update({'Page': page})
                    r = session.get(url=url, auth=self._auth, params=params)
                    raw_data = dict(r.json())
                    result_factory(result, raw_data)

        return result

    def __init__(self, acc_key: dict):
        self._tables_map = _INTRA_CONNECTOR_MAPPING['tables_map']
        self._fields_map = _INTRA_CONNECTOR_MAPPING['fields_map']
        self._acc_key = acc_key
        self._auth = HTTPBasicAuth(acc_key['user'], acc_key['pwd'])

    def select(self, entity: DataEntity):
        """ Work for Task, User, Actuals """
        entity.get_fields()
        resource = self._get_entity_table(entity)
        translation_map = self.get_fields_map(entity)
        fields = list(translation_map.values())

        if entity.get_type() in ('service', 'actual', 'user'):  # special API for service, task data request
            resource = f"{resource}/{entity['Id']}"
            params = {}

            def factory(res: dict, raw: dict):
                for k, val in raw.items():
                    res[k] = val

        elif entity.get_type() in 'task':
            resource = f"{resource}/{entity['Id']}"
            params = {}

            def factory(res: dict, raw: dict):
                for k, val in raw['Task'].items():
                    res[k] = val

        else:
            fields_str = ','.join(i for i in fields)
            params = {
                'Id': entity['Id'],
                'Fields': fields_str,
            }

            def factory(res: dict, raw: dict):
                for k, val in raw[resource].items():
                    res[k] = val

        data = self._api_request_get(resource, params, factory)
        for key in entity.get_fields():
            api_field = translation_map[key]
            _INW(entity)[key] = data[api_field]

    def _update_entity_from_raw(self, entity, raw):
        field_map = self.get_fields_map(entity)
        for k, val in field_map.items():
            if k is 'Comments':
                val = 'Comment'  # Intraservice API bug!
            entity[k] = raw[val]

    def get_update_pack(self, start: datetime, finish: datetime):
        """ Returns updated tasks for the period """
        result = {
            'Tasks': {},
            'Executors': [],
            'Services': {},
            'Users': {},
            'Actuals': [],
        }

        entity = Task()
        field_map = self.get_fields_map(entity)
        params = {
            'ChangedMoreThan': _inw_d_get(start),
            'ChangedLessThan': _inw_d_get(finish),
            'fields': ','.join(value for value in field_map.values()) + ',ExecutorIds, Changed',
            'include': 'USER, SERVICE, STATUS',
        }

        def factory(res, raw):
            # read tasks info
            for task_raw in raw['Tasks']:
                task = Task()
                self._update_entity_from_raw(task, task_raw)
                task_id = task['Id']
                res['Tasks'][task_id] = task

                # read executors info
                executor_list = task_raw['ExecutorIds'].split(',')
                for user_id in executor_list:
                    if user_id:
                        executor = Executor({'TaskId': task_id, 'UserId': user_id})
                        res['Executors'].append(executor)

            # read users info
            for user_raw in raw['Users']:
                user = User()
                self._update_entity_from_raw(user, user_raw)
                user_id = user['Id']
                res['Users'][user_id] = user

            # read service info

            for service_raw in raw['Services']:
                service = Service()
                self._update_entity_from_raw(service, service_raw)
                service_id = service['Id']
                res['Services'][service_id] = service
                # check parent service
                parent_id = service['ParentId']
                if parent_id and parent_id not in res['Services'].keys():
                    parent = Service({'Id': parent_id})
                    self.select(parent)
                    res['Services'][parent_id] = parent

        result = self._api_request_get('task', params, factory, result)

        # read actual (expenses) info

        def factory(res: dict, raw: dict):
            for actual_raw in raw['Expenses']:
                actual = Actual()
                self._update_entity_from_raw(actual, actual_raw)
                res['Actuals'].append(actual)

        for task_id in result['Tasks']:
            field_map = self.get_fields_map(Actual())
            params = {
                'taskid': task_id,
                'fields': ','.join(value for value in field_map.values()),
            }
            result = self._api_request_get('taskexpenses', params, factory, result)

        return result


class Task(DataEntity):
    def __init__(self, data=None):
        self._type = 'task'
        self._fields = {
            'Id': 'i',
            'ServiceId': 'i',
            'StatusId': 'i',
            'ParentId': 'i',
            'Name': 's',
            'Description': 's',
            'Created': 'd',
            'CreatorId': 'i',
            'Closed': 'd',
            'FeedbackId': 'i',
            'Deadline': 'd',
        }
        # todo: add information about task union
        super().__init__(data)


class User(DataEntity):
    def __init__(self, data=None):
        self._type = 'user'
        self._fields = {
            'Id': 'i',
            'Name': 's',
            'Email': 's',
        }
        super().__init__(data)


class Executor(DataEntity):
    def __init__(self, data=None):
        self._type = 'executor'
        self._fields = {
            'TaskId': 'i',
            'UserId': 'i',
        }
        super().__init__(data)

    def get_key_fields(self):
        return ('TaskId',
                'UserId')


class Actual(DataEntity):
    def __init__(self, data=None):
        self._type = 'actual'
        self._fields = {
            'Id': 'i',
            'TaskId': 'i',
            'UserId': 'i',
            'Date': 'd',
            'Minutes': 'i',
            'Rate': 'dc',
            'Comments': 's',
        }
        super().__init__(data)


class Service(DataEntity):
    def __init__(self, data=None):
        self._type = 'service'
        self._fields = {
            'Id': 'i',
            'Code': 's',
            'Name': 's',
            'Description': 's',
            'IsArchive': 'b',
            'IsPublic': 'b',
            'ParentId': 'i',
            'Path': 's',
        }
        super().__init__(data)


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
        pg_con = PGConnector(KeyChain.TEST_PG_KEY)
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
        pg_con = PGConnector(KeyChain.TEST_PG_KEY)
        a = Actual(TEST_DATA['actual'])
        task_id = a['TaskId']
        pg_con.update(a)
        t = Task()
        t['Id'] = task_id
        pg_con.delete_task_actuals(t)
        self.assertFalse(pg_con.is_exist(a))


class TestIntraConnector(TestCase):
    def setUp(self):
        # test db_key
        self.connector = IntraConnector(KeyChain.TEST_INTRA_KEY)

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
        pg_con = PGConnector(KeyChain.TEST_PG_KEY)
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
