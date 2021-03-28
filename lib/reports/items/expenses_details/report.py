from dataclasses import dataclass

from ...report_classes import ISReport


class ExpensesDetails(ISReport):
    @classmethod
    def anchor_path(cls):
        return __file__  # NEVER delete this! Using for correct Jinja templates path resolving

    @dataclass
    class Locals:
        TASK_NAME = None
        TASK_DESCR = None

    @dataclass
    class Fields:
        EXECUTOR = 'executor'

    @dataclass
    class Params:
        TASK_ID: [int, None]
        EXECUTOR_ID: [int, None]
        FIELD_LIST: [list, None]

    _presets = {
        'Some test': Params(
            TASK_ID=161905,
            EXECUTOR_ID=None,
            FIELD_LIST=(Fields.EXECUTOR,)
        )
    }

    def update_locals(self, _params, _locals) -> None:
        mark_data = self.query_mark(self.MarkTypes.TASKS, (self._params.TASK_ID,))[0]
        _locals.TASK_NAME = mark_data.name
        _locals.TASK_DESCR = mark_data.descr

    def update_data(self, _params, _locals, _data) -> None:
        with self.cursor() as cursor:
            cursor.execute(self.load_query('query.sql', _params))
            _data['lines'] = cursor.fetchall()
