from datetime import timedelta, date
from dataclasses import dataclass
from keys import KeyChain
from lib.schedutils import Starter as StarterClass
from typing import Union

from ...report_classes import PGReport
from .report_details import JobDetails


class Starter(PGReport):
    @classmethod
    def anchor_path(cls):
        return __file__

    _DEFAULT_VIEW = 'view_starter'


    @dataclass
    class Params:
        ON_DAY: date
        JOB_TYPE_FILTER: Union[str, None]

    @dataclass
    class PresetTypes:
        LAST_DAY = 'Last Day Report'

    @dataclass
    class Locals:
        TYPES = StarterClass.JobStatus  # using for FAIL, DONE, etc job status constants in Jinja template

    _presets = {
        PresetTypes.LAST_DAY: Params(
            ON_DAY=date.today() - timedelta(days=1),
            JOB_TYPE_FILTER=None
        )
    }

    @classmethod
    def need_pg_key(cls):
        return KeyChain.PG_STARTER_KEY

    def update_navigation(self, _params, _locals, _data) -> None:
        # timeline navigation configure
        self.add_nav_point('Prev day', Starter.Params(
            ON_DAY=_params.ON_DAY - timedelta(days=1),
            JOB_TYPE_FILTER=_params.JOB_TYPE_FILTER)
        )
        self.add_nav_point('Next day', Starter.Params(
            ON_DAY=_params.ON_DAY + timedelta(days=1),
            JOB_TYPE_FILTER=_params.JOB_TYPE_FILTER)
        )

        # types bar navigation configure
        with self.cursor() as cursor:
            cursor.execute(
                self.load_query(
                    'query_navigation.sql',
                    {
                        'ON_DAY': _params.ON_DAY,
                        'FAIL': _locals.TYPES.FAIL
                    }
                )
            )
            for record in cursor:
                self.add_nav_point(
                    (record.type, record.count, record.fail),
                    Starter.Params(
                        ON_DAY=_params.ON_DAY,
                        JOB_TYPE_FILTER=None if record.type == _params.JOB_TYPE_FILTER else record.type
                    ),
                    'types'
                )

    def update_details(self, _params, _locals, _data) -> None:
        for rec in _data['jobs']:
            self.add_detail(
                rec.id,
                JobDetails.Params(
                    ID=rec.id,
                    BACK_PAGE_IDX=self.idx
                )
            )

    def update_data(self, _params, _locals, _data) -> None:
        with self.cursor() as cursor:
            cursor.execute(
                self.load_query('query_job_list.sql', _params)
            )
            _data['jobs'] = cursor.fetchall()
