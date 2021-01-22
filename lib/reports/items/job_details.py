from lib.schedutils import Starter
from .._report import PGReport, _Params, sql


class JobDetails(PGReport):
    JobStatus = Starter.JobStatus

    field_list = [
        'id',
        'type',
        'status',
        'plan',
        'start',
        'finish',
        'duration',
        'params',
        'result'
    ]

    @classmethod
    def detail_query(cls, starter_id: int):
        return sql.SQL('SELECT {} FROM "Loader" WHERE id={}').format(
            sql.SQL(', ').join([sql.SQL(field) for field in cls.field_list]),
            sql.Literal(starter_id)
        )

    def redo(self):
        with self._cursor(named=True) as cursor:
            cursor.execute(
                sql.SQL('UPDATE {} SET status={} where id={}').format(
                    sql.Identifier('Loader'),
                    sql.Literal(self.JobStatus.TODO),
                    sql.Literal(self._D().id),
                )
            )
            self._commit()

    def _prepare_data(self, params: _Params) -> dict:
        starter_id = params['job_id']

        with self._cursor(named=True) as cursor:
            cursor.execute(self.detail_query(starter_id))
            return cursor.fetchone()

    def do_action(self, action, params, flash) -> int:
        if action == 'Close':
            return params['back']
        if action == 'Redo':

            self.redo()
            flash('Job restart!')
            return params['idx']
        return 0


