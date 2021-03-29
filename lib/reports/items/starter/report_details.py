from dataclasses import dataclass

from lib.schedutils import Starter
from keys import KeyChain
from lib.reports.report_classes import PGReport, sql


class JobDetails(PGReport):
    @classmethod
    def anchor_path(cls):
        return __file__

    _DEFAULT_VIEW = 'view_job'

    @dataclass
    class Params:
        ID: int
        BACK_PAGE_IDX: int

    @dataclass
    class Locals:
        TYPES = Starter.JobStatus
        FIELD_LIST = (
            'id',
            'type',
            'status',
            'plan',
            'start',
            'finish',
            'duration',
            'params',
            'result'
        )

    @classmethod
    def need_pg_key(cls):
        return KeyChain.PG_STARTER_KEY

    def redo(self):
        with self.cursor() as cursor:
            cursor.execute(
                sql.SQL('UPDATE {} SET status={} where id={}').format(
                    sql.Identifier('Loader'),
                    sql.Literal(self._locals.TYPES.TODO),
                    sql.Literal(self._params.ID),
                )
            )
            self.commit()

    def update_data(self, _params, _locals, _data) -> None:
        with self.cursor() as cursor:
            cursor.execute(
                sql.SQL('SELECT {} FROM "Loader" WHERE id={}').format(
                    sql.SQL(', ').join([sql.SQL(field) for field in _locals.FIELD_LIST]),
                    sql.Literal(_params.ID)
                )
            )
            _data['job'] = cursor.fetchone()

    def do_action(self, action, form, flash) -> int:
        if action == 'Close':
            return self.get_params().BACK_PAGE_IDX
        if action == 'Redo':
            self.redo()
            flash('Job restart!')
            return self.idx
        return 0
