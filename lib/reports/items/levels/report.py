from dataclasses import dataclass
from collections import namedtuple
from datetime import datetime, timedelta
import requests
import re

from lib.reports.report_classes import PGReport
from keys import KeyChain


class LevelsReport(PGReport):
    """ Don't forget register SomeReport class into lib.reports.activity_reg.py """
    @classmethod
    def anchor_path(cls):
        return __file__  # NEVER delete this! Using for correct Jinja templates path resolving

    """ Uncomment this line when view name differ from [view.html] extension should be omitted """
    # _DEFAULT_VIEW = 'custom_view'

    @classmethod
    def need_pg_key(cls):
        """ Don't forget specify database key"""
        return KeyChain.PG_LEVEL_KEY

    @dataclass
    class Locals:
        """ Specify locals report params here """
        HDR_MINUTES = 30  # initialization here is mandatory
        REPORT_DATE = None

    @dataclass
    class Fields:
        # USELESS_FIELD = 'useless_field'
        pass

    @dataclass
    class Params:
        STAMP: datetime or None
        # SOME_DATE_PARAM: [date, None]
        # FIELD_LIST: [list, None]
        ...

    _presets = {
        'FZLevels': Params(
            STAMP=None  # Now
        )
    }

    def update_params(self, _params: Params) -> None:
        if not _params.STAMP:
            _params.STAMP = datetime.now().replace(second=0, microsecond=0)

    def update_details(self, _params, _locals, _data) -> None:
        """ self.add_detail(key, PARAMS, kind=some_kind) """
        pass

    def update_locals(self, _params, _locals) -> None:
        """ _locals.SOME_FIELD = ... """
        pass

    def update_navigation(self, _params, _locals, _data) -> None:
        """ self.add_nav_point(caption, params, kind) """
        pass

    def get_data_from_site(self):
        Setting = namedtuple('Setting', ['url', 're', 'names'])
        parse_setting = (
            Setting(
                'http://spun.fkpkzs.ru/Level/C2',
                r"<td class=\"timestampvalue\"><span>(.+)<\/span><\/td>\s+<td.+?>"
                r"<span>(.+)<\/span><\/td>\s+<td.+?><span>(.+)<\/span><\/td",
                ('Горбатый Город', 'Горбатый Море')
            ),
            Setting(
                'http://spun.fkpkzs.ru/Level/C1',
                r"<td class=\"timestampvalue\"><span>(.+)<\/span><\/td>\s+<td.+?>"
                r"<span>(.+)<\/span><\/td>\s+<td.+?><span>(.+)<\/span><\/td",
                ('Тунель Город', 'Тунель Море')
            ),
            Setting(
                'http://spun.fkpkzs.ru',
                r'<td class="timestampvalue"><span>(.+)<\/span><\/td>\s+<td.+?><span>(.+)<\/span>',
                ('Горный институт', )
            ),
        )

        LevelDataRecord = namedtuple('LevelDataRecord', ('point', 'stamp', 'value'))

        def data_adapter(_data, field_names: list):
            result = []
            for rec in _data:
                stamp = datetime.strptime(rec[0], "%d.%m.%Y %H:%M")
                for i in range(0, len(field_names)):
                    result.append(LevelDataRecord(field_names[i], stamp, int(rec[i + 1])))
            return result

        scanned_data = []
        for record in parse_setting:
            response = requests.get(record.url)
            response.encoding = 'windows-1251'
            html = response.text
            scanned_data += data_adapter(
                re.findall(record.re, html, re.MULTILINE), record.names
            )
        return scanned_data


    def update_data(self, _params: Params, _locals: Locals, _data) -> None:

        # with self.cursor() as cursor:
        #     cursor.execute(self.load_query('query.sql', _params))
        #     raw = cursor.fetchall()

        raw = self.get_data_from_site()

        point_map = {
            'Горный институт': 'C0',
            'Горбатый Город': ('C2', 'pre'),
            'Горбатый Море': ('C2', 'post'),
            'Тунель Город': ('C1', 'pre'),
            'Тунель Море': ('C1', 'post'),
        }
        point_map = {v: k for k, v in point_map.items()}  # revert key <-> value in dict

        def gen(index: tuple or str):
            return {
                rec.stamp: rec.value
                for rec
                in raw
                if rec.point == point_map[index]
            }

        index_minutes = {
            _params.STAMP - timedelta(minutes=i): i
            for i in range(_locals.HDR_MINUTES)
        }

        _data['index'] = index_minutes.values()

        _data['c2_delta'] = {
            index:
                gen(('C2', 'pre')).get(stamp, 0) - gen(('C2', 'post')).get(stamp, 0)  # levels delta
            for stamp, index in index_minutes.items()
        }

        _data['c1_delta'] = {
            index:
                gen(('C1', 'pre')).get(stamp, 0) - gen(('C1', 'post')).get(stamp, 0)  # levels delta
            for stamp, index in index_minutes.items()
        }

        _data['c2_c1_delta'] = {
            index:
                gen(('C2', 'pre')).get(stamp, 0) - gen(('C1', 'pre')).get(stamp, 0)  # levels delta
            for stamp, index in index_minutes.items()
        }

        _data['c0_level'] = {
            index:
                gen('C0').get(stamp, 0)
            for stamp, index in index_minutes.items()
        }

    def do_action(self, action, form, flash) -> int:  # return target_idx
        """ Override bellow for report action handling """
        pass

import unittest


class ReportTest(unittest.TestCase):
    def test_request(self):
        LevelsReport.get_data_from_site(None)