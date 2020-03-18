from datetime import datetime
import requests
import unittest

from abc import abstractmethod
from unittest import TestCase
from urllib3.util.retry import Retry
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter

import psycopg2
from psycopg2 import sql

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
        # try:
        cursor.execute(sql_str)
        # except Exception:
        #     pass
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
        sql_str = sql.SQL('SELECT {} FROM {} WHERE {}').format(
            sql.SQL(', ').join(sql.Identifier(name) for name in field_map.values()),
            sql.Identifier(table),
            self._get_where_sql(entity)
        ).as_string(self._db_conn)
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

    def _get_where_sql(self, entity: DataEntity):
        return sql.SQL(' AND ').join(
            sql.Composed([
                sql.Identifier(self.get_fields_map(entity)[key]),
                sql.SQL(' = '),
                sql.Literal(entity[key])
            ]) for key in entity.get_key_fields()
        )

    def is_exist(self, entity: DataEntity):
        table = self._get_entity_table(entity)
        field_map = self.get_fields_map(entity)

        sql_str = sql.SQL('SELECT * FROM {} WHERE {}').format(
            sql.Identifier(table),
            self._get_where_sql(entity)
        ).as_string(self._db_conn)

        records = self._sql_exec(sql_str)

        if len(records) is 0:
            return False
        elif len(records) is 1:
            return True
        else:
            print(f'\nDatabase Layer Warning!!!')
            print(f'Multiple ID store detected.')
            print(f'table: {table}, id: ?, times: {len(records)}')
            print(f'Database Layer Warning!!!\n')
            return True

    def delete(self, entity: DataEntity):
        table = self._get_entity_table(entity)

        sql_str = sql.SQL('DELETE FROM {} WHERE {}').format(
            sql.Identifier(table),
            self._get_where_sql(entity)
        ).as_string(self._db_conn)

        self._sql_exec(sql_str)

    def update(self, entity: DataEntity):
        field_map = self.get_fields_map(entity)

        table = self._get_entity_table(entity)
        if not self.is_exist(entity):
            # INSERT statement
            sql_str = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                sql.Identifier(table),
                sql.SQL(', ').join(sql.Identifier(field_map[f]) for f in entity.get_fields()),
                sql.SQL(', ').join(sql.Literal(entity[key]) for key in entity.get_fields())
            ).as_string(self._db_conn)
        else:
            # UPDATE statement
            sql_str = sql.SQL('UPDATE {} SET ({}) = ({}) WHERE {}').format(
                sql.Identifier(table),
                sql.SQL(', ').join(sql.Identifier(field_map[key]) for key in entity.get_fields()),
                sql.SQL(', ').join(sql.Literal(entity[key]) for key in entity.get_fields()),
                self._get_where_sql(entity)
            ).as_string(self._db_conn)

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


class ISConnector(DataConnector):
    _certVerify = True  # connection SSL cert verify
    def is_404(self, task_id):

        base_url = self._acc_key['url']
        session = requests.Session()
        retries = Retry(total=25,
                        backoff_factor=0.0001,
                        status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        # Make API request
        url = f"{base_url}task/{task_id}"
        r = session.get(url=url, auth=self._auth, verify=self._certVerify)

        session.close()
        return True if r.status_code == 404 else False

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
        r = session.get(url=url, auth=self._auth, params=params, verify=self._certVerify)
        raw_data = dict(r.json())
        session.close()
        if r.status_code != 200:
            print(f"IS API Request error: {url}\r Response: {raw_data}")
            raise EnvironmentError

        result_factory(result, raw_data)
        if raw_data.get('Paginator'):
            page_count = raw_data['Paginator']['PageCount']

            if page_count > 1:
                for page in range(page_count, 0, -1):
                    params.update({'Page': page})
                    r = session.get(url=url, auth=self._auth, params=params, verify=self._certVerify)
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
                    # todo: IS API BUG, don't forget checkup ticket 135965
                    try:
                        self.select(parent)
                    except EnvironmentError:
                        continue
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


