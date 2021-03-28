from dataclasses import dataclass
from datetime import date

from ...report_classes import PGReport

from keys import KeyChain


class ExampleReport(PGReport):
    """ Don't forget register SomeReport class into lib.reports.activity_reg.py """
    @classmethod
    def anchor_path(cls):
        return __file__  # NEVER delete this! Using for correct Jinja templates path resolving

    """ Uncomment this line when view name differ from [view.html] extension should be omitted """
    # _DEFAULT_VIEW = 'custom_view'

    @classmethod
    def need_pg_key(cls):
        """ Don't forget specify database key"""
        return KeyChain.PG_KEY

    @dataclass
    class Locals:
        """ Specify locals report params here """
        SOME_LOCAL_PARAM = None  # initialization here is mandatory

    @dataclass
    class Fields:
        USELESS_FIELD = 'useless_field'
        pass

    @dataclass
    class Params:
        SOME_BOOL_PARAM: [bool, None]
        SOME_DATE_PARAM: [date, None]
        FIELD_LIST: [list, None]

    _presets = {
        'Preset example': Params(
            SOME_BOOL_PARAM=True,
            SOME_DATE_PARAM=date(2021, 1, 1),
            FIELD_LIST=(Fields.USELESS_FIELD,)
        )
    }

    def update_details(self, _params, _locals, _data) -> None:
        """ self.add_detail(key, PARAMS, kind=some_kind) """
        pass

    def update_locals(self, _params, _locals) -> None:
        """ _locals.SOME_FIELD = ... """
        pass

    def update_navigation(self, _params, _locals, _data) -> None:
        """ self.add_nav_point(caption, params, kind) """
        pass

    def update_data(self, _params, _locals, _data) -> None:
        """ Example for PGReport: """
        with self.cursor() as cursor:
            cursor.execute(self.load_query('query.sql', _params))
            _data['lines'] = cursor.fetchall()

    def do_action(self, action, form, flash) -> int:  # return target_idx
        """ Override bellow for report action handling """
        pass
