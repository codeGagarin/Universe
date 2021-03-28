from dataclasses import dataclass
from datetime import date
from typing import Union

from ...period_utils import Period
from ...report_classes import ISReport

from ..expenses_details import ExpensesDetails


class TaskReport(ISReport):
    @classmethod
    def anchor_path(cls):
        return __file__

    @dataclass
    class Locals:
        SERVICE_FILTER = None
        USER_FILTER = None

    @dataclass()
    class Fields:
        CREATOR = 'creator'
        CREATED = 'created'
        CLOSED = 'closed'
        SERVICE = 'service'
        PLANED = 'planed'
        EVALUATION = 'evaluate'

    @dataclass
    class Params:
        CLOSED_IS_NULL: [bool, None]
        CLOSED_FROM: Union[date, None]
        CLOSED_TO: Union[date, None]
        CREATED_FROM: Union[date, None]
        CREATED_TO: Union[date, None]
        SERVICE_FILTER: Union[tuple, None]  # None, if empty
        USER_FILTER: Union[tuple, None]  # None, if empty
        EVALUATION_FILTER: Union[tuple, None]  # None, if empty
        FIELD_LIST: Union[tuple, None]  # None, if empty
        NO_EXEC: Union[bool, None]  # use True: if need display only tasks w/o executors
        PLANED_IS_NULL: Union[bool, None]  # use True: if need display only tasks w/o deadline

    _presets = {
        'DRIM2': Params(
            CLOSED_IS_NULL=None,
            CLOSED_FROM=Period(date.today(), Period.Type.MONTH, -1).begin,
            CLOSED_TO=Period(date.today(), Period.Type.MONTH, -1).end,
            CREATED_FROM=None,
            CREATED_TO=None,
            SERVICE_FILTER=(193,),
            USER_FILTER=None,
            FIELD_LIST=(Fields.CREATED, Fields.CREATOR, Fields.PLANED, Fields.SERVICE, Fields.CLOSED),
            NO_EXEC=None,
            PLANED_IS_NULL=None,
            EVALUATION_FILTER=None,
        )
    }

    def update_locals(self, _params, _locals) -> None:
        _locals.SERVICE_FILTER = [
            rec.name for rec in
            self.query_mark(self.MarkTypes.SERVICES, _params.SERVICE_FILTER)
        ]

        _locals.USER_FILTER = [
            rec.name for rec in
            self.query_mark(self.MarkTypes.USERS, _params.USER_FILTER)
        ]

    def update_details(self, _params, _locals, _data) -> None:
        for rec in _data['task_list']:
            self.add_detail(
                rec.task_id,
                ExpensesDetails.Params(
                    TASK_ID=rec.task_id,
                    EXECUTOR_ID=None,
                    FIELD_LIST=(ExpensesDetails.Fields.EXECUTOR,)
                ),
                kind='tasks'
            )

        for rec in _data['user_list']:
            self.add_detail(
                '{}:{}'.format(rec.task_id, rec.user_id),  # artificial key
                ExpensesDetails.Params(
                    TASK_ID=rec.task_id,
                    EXECUTOR_ID=rec.user_id,
                    FIELD_LIST=None
                ),
                kind='users'
            )

    def update_data(self, _params, _locals, _data) -> None:
        with self.cursor() as cursor:
            cursor.execute(
                self.load_query('query_tasks.sql', _params)
            )
            _data['task_list'] = cursor.fetchall()

        _data['user_list'] = {}  # default result
        if len(_data['task_list']):
            with self.cursor() as cursor:
                cursor.execute(
                    self.load_query(
                        'query_executors.sql',
                        {
                            'TASK_LIST': map(lambda rec: rec.task_id, _data['task_list'])
                        }
                    )
                )
                _data['user_list'] = cursor.fetchall()
