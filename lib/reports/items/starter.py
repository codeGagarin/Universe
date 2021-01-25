from datetime import datetime, timedelta

from lib.schedutils import Starter as StarterClass

from .._report import PGReport, _Params, sql
from ..period import get_period
from .job_details import JobDetails


class Starter(PGReport):
    JobStatus = StarterClass.JobStatus  # using for FAIL, DONE, etc job status constants in Jinja template

    def prepare_navigation(self):
        prev_point = get_period(self._params['from'], 'day', -1)
        next_point = get_period(self._params['from'], 'day', 1)
        self.add_nav_point('Prev day', _Params({'from': prev_point['from'], 'to': prev_point['to']}))
        self.add_nav_point('Next day', _Params({'from': next_point['from'], 'to': next_point['to']}))

        # configure types navigation layer
        type_nav_query = sql.SQL(
            'SELECT type AS type, COUNT(*) AS count, SUM(CASE WHEN status={} THEN 1 ELSE 0 END) AS fail'
            '  FROM "Loader" WHERE plan >= {} AND plan < {} GROUP BY type'
            '  ORDER BY type').format(
            sql.Literal(self.JobStatus.FAIL),
            sql.Literal(self._params['from']),
            sql.Literal(self._params['to'] + timedelta(days=1)),
        )

        with self._cursor(named=True) as cursor:
            cursor.execute(type_nav_query)
            for record in cursor:
                nav_params = _Params({
                    'from': self._params['from'], 'to': self._params['to'],
                })
                if self._params.get('act_type') != record.type:
                    nav_params['act_type'] = record.type
                self.add_nav_point((record.type, record.count, record.fail), nav_params, 'types')

    def prepare_body(self, report_data):
        result = {
            'content': report_data,
            'url': {}
        }

        for record in report_data:
            result['url'][record.id] = self.params_to_idx(
                {
                    'job_id': record.id,
                    'back': self.idx
                },
                report_class=JobDetails
            )

        return result

    @staticmethod
    def gen_data_query(params: _Params):
        period_filter = sql.SQL('plan >= {} AND plan < {}').format(
            sql.Literal(params['from']),
            sql.Literal(params['to'] + timedelta(days=1))
        )

        activity_filter = sql.SQL(' AND type = {}').format(sql.Literal(params.get('act_type'))) \
            if params.get('act_type') else sql.SQL('')

        return sql.SQL('SELECT id, type, status, plan, start, finish, duration, params, result'
                       ' FROM "Loader" WHERE {} ORDER BY plan DESC').format(
            sql.Composed([period_filter, activity_filter]))

    def _prepare_data(self, params: _Params):
        if not params.get('from'):
            # welcome to default params generate section
            default_report_date = (datetime.today() - timedelta(days=1)).date()
            params['from'] = default_report_date
            params['to'] = default_report_date

        self.prepare_navigation()

        # prepare main report data
        report_query = self.gen_data_query(params)
        with self._cursor(named=True) as cursor:
            cursor.execute(report_query)
            return {
                'body': self.prepare_body(cursor.fetchall()),
                'from': params['from'],
                'act_type': params.get('act_type')
            }

