from dataclasses import dataclass
from datetime import date
from typing import Union

from ...report_classes import ISReport
from ..expenses_details import ExpensesDetails


class ExpensesReport(ISReport):
    @classmethod
    def anchor_path(cls):
        return __file__

    @dataclass
    class Locals:
        SERVICE_FILTER = None
        USER_FILTER = None

    @dataclass
    class Fields:
        CREATOR = 'creator'
        CREATED = 'created'
        CLOSED = 'closed'
        SERVICE = 'service'
        EXECUTOR = 'executor'

    @dataclass
    class Params:
        CLOSED_IS_NULL: [bool, None]
        CLOSED_FROM: Union[date, None]
        CLOSED_TO: Union[date, None]
        DATE_EXP_FROM: Union[date, None]
        DATE_EXP_TO: Union[date, None]
        SERVICE_FILTER: Union[tuple, None]  # None, if empty
        USER_FILTER: Union[tuple, None]  # None, if empty
        FIELD_LIST: Union[tuple, None]  # None, if empty

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
        for rec in _data['lines']:
            self.add_detail(
                rec.details_key,
                ExpensesDetails.Params(
                    TASK_ID=rec.task_id,
                    EXECUTOR_ID=rec.executor_id,
                    FIELD_LIST=(ExpensesDetails.Fields.EXECUTOR,)
                )
            )

    def update_data(self, _params, _locals, _data) -> None:
        with self.cursor() as cursor:
            cursor.execute(self.load_query('query.sql', _params))
            _data['lines'] = cursor.fetchall()
