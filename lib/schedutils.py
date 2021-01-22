"""
    Where Starter & Activity Interfaces Only
    Use it for inherit in your own starter or/and activities implementation
    NullStarter implementation use for custom activities testing
"""
from dataclasses import dataclass
from datetime import datetime

import simplejson as json


class Starter:
    @dataclass
    class JobStatus:
        DONE = 'finish'
        FAIL = 'fail'
        TODO = 'todo'
        WORKING = 'working'

    def __init__(self, db_key):
        """ Default constructor """
        self._registry = {}

    def register(self, factory):
        """ Activities register method for activities produce and schedule control """
        activity = factory(self)
        self._registry[activity.get_type()] = {
            'factory': factory,
            'crontab': activity.get_crontab(),
        }

    def to_plan(self, activity, due_date=None) -> int:
        """ Should returns activity ID """
        pass

    def track_schedule(self):
        """ Where you should to run all actual activities """
        pass

    def get_activity_status(self, activity_id: int):
        """ Return activity status DONE/FAIL etc... """
        pass

    def get_state(self, on_day: datetime = None, status_filter: list = None) -> dict:
        """ Return state table with follow column
        id - activity ID, which was run at period
        type - activity type (class)
        status -- actual activity status
        start -- when activity was planed
        finish -- when activity execution was finished
        duration -- activity execution real duration
        params -- params for activity run
        result -- activity execution result
        """


class Activity:
    def __init__(self, ldr, params=None):
        if params:
            self._params = params
        else:
            self._params = {}.fromkeys(self._fields().split())
        self._ldr = ldr

    def __setitem__(self, key, value):
        # key legal check
        if key not in self._fields().split():
            raise KeyError(f'Key [{key}] not found for activity [{self.get_type()}]')
        self._params[key] = value

    def __getitem__(self, key):
        return self._params.get(key, None)
        pass

    def _fields(self) -> str:
        """ Override it for declare build in field
            Example: return 'id name descr'
        """
        return ''

    def get_params(self):
        return self._params

    def dump_params(self):
        def converter(o):
            if isinstance(o, datetime):
                return o.strftime('datetime:%Y-%m-%d %H:%M:%S.%f')
        return json.dumps(self.get_params(), default=converter)

    def update_params(self, dump):
        def dict_converter(source):
            # scan json-source tree and replace datetime strings to datetime objects
            def date_converter(s):
                try:
                    return datetime.strptime(s, 'Datetime:%Y-%m-%d %H:%M:%S.%f')
                except:
                    return s

            def list_converter(lst):
                for i in range(len(lst)):
                    r = lst[i]
                    if isinstance(r, str):
                        lst[i] = date_converter(r)
                    elif isinstance(r, dict):
                        dict_converter(r)
                    elif isinstance(r, list):
                        list_converter(r)

            for k, v in source.items():
                if isinstance(v, list):
                    list_converter(v)
                elif isinstance(v, dict):
                    dict_converter(v)
                elif isinstance(v, str):
                    source[k] = date_converter(v)
            return source

            # res[0][2] if not res[0][2] else json.loads(res[0][2], object_hook=dict_converter)
        if dump:
            json_params = json.loads(dump, object_hook=dict_converter)
            for key, value in json_params.items():
                self[key] = value

    def get_type(self):
        return self.__class__.__name__

    def get_crontab(self):
        return None

    def apply(self, due_date=None):
        return self._ldr.to_plan(self, due_date)

    def run(self):
        pass


class NullStarter(Starter):
    def __init__(self):
        super().__init__(None)

    def register(self, factory):
        pass

    def to_plan(self, activity, due_date=None):
        pass

    def track_schedule(self):
        pass

    def get_activity_status(self, activity_id: int):
        return self.JobStatus.DONE


from unittest import TestCase


class TestActivity(TestCase):
    def setUp(self) -> None:
        self._starter = NullStarter()

    def test_dump_update_params(self):
        fields = 'int float str data'
        field_list = fields.split()
        values = (10, 23.22, 'test_string', datetime.now())
        params = {field_list[i]: values[i] for i in range(0, len(field_list))}

        def get_fields():
            return fields

        a = Activity(self._starter, params)
        dump = a.dump_params()
        b = Activity(self._starter)
        b._fields = get_fields
        b.update_params(dump)
        for key, value in params.items():
            self.assertEqual(value, b[key])

    def test_empty_dump_update(self):
        a = Activity(self._starter)
        a._fields = lambda: 'descr'
        a['descr'] = 'whf'
        dump = a.dump_params()
        b = Activity(self._starter)
        b._fields = lambda: 'name descr'
        b.update_params(dump)

