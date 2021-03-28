import hashlib
from typing import List, Union
from datetime import datetime
from dataclasses import asdict, fields

from keys import KeyChain
from lib.pg_utils import PGMix

from .param_utils import json_to_dict, dict_to_json
from .report_classes import Report, sql


def _hash(s: str):
    """ bigint hash for random string, presents params ID for params storage """
    return int(hashlib.shake_128(s.encode()).hexdigest(7), 18)


class Manager(PGMix):
    def __init__(self, report_list):
        PGMix.__init__(self, KeyChain.PG_REPORT_KEY)
        self._param2report = {}
        self._regs = {}
        self.idx_map = {}

        for report in report_list:
            self._regs[report.get_type()] = report
            self._param2report[report.Params] = report

    def preset_map(self) -> dict:
        result = {}
        for report_type in self._regs.values():
            presets = report_type.presets()

            for name, preset in presets.items():
                result[f'{report_type.get_type()} :: {name}'] = \
                    self.params_to_idx(preset)

        self.flush()
        return result

    def report_factory(self, idx: int) -> Union[Report, None]:
        params = self.idx_to_params(idx)
        report_class = self._param2report[params.__class__]

        if not report_class:
            return None

        report = report_class(self)
        report.request_data(params, idx)
        return report

    def params_to_idx(self, params: Report.Params) -> int:
        report_class = self._param2report[params.__class__]
        params_dict = asdict(params)
        params_dict['__type__'] = report_class.get_type()

        json_params = dict_to_json(params_dict)
        hash_value = _hash(json_params)
        self.idx_map[hash_value] = json_params
        return hash_value

    def idx_to_params(self, idx: int):
        with self.cursor() as cursor:
            cursor.execute(
                sql.SQL('UPDATE "Params" SET last_touch = {}, touch_count = touch_count+1'
                        ' WHERE id={} RETURNING "params"').format(
                    sql.Literal(datetime.now()),
                    sql.Literal(idx)
                )
            )
            self.commit()
            result = cursor.fetchone()

        if result:
            json_params = result[0]
            params_dict = json_to_dict(json_params)
            report_class = self._regs[params_dict['__type__']]
            field_list = [field.name for field in fields(report_class.Params)]
            field_dict = {field: params_dict.get(field) for field in field_list}
            return report_class.Params(**field_dict)
        else:
            return None

    def flush(self):
        """ submit all params to database """
        # if they exist
        if not len(self.idx_map):
            return

        check_query = sql.SQL('SELECT idx, p.id AS pid FROM unnest(ARRAY[{}]) AS idx'
                              ' LEFT JOIN "Params" AS p ON p.id=idx').format(
            sql.SQL(', ').join([
                sql.Literal(hash_value) for hash_value in self.idx_map.keys()
            ])
        )

        insert_query = sql.SQL(
            'INSERT INTO "Params" (id, params, last_touch, touch_count) VALUES (%s, %s, {}, {})').format(
            sql.Literal(datetime.now()),
            sql.Literal(0)
        )

        update_query = sql.SQL(
            'UPDATE "Params" SET last_touch = {}, touch_count = touch_count+1 WHERE id=%s').format(
            sql.Literal(datetime.now())
        )

        insert_batch_params = []
        update_batch_params = []

        cursor = self.cursor(named=True)
        cursor.execute(check_query)
        for check in cursor:
            idx = check.idx
            if check.pid:  # need update
                update_batch_params.append((idx,))
            else:  # need insert
                insert_batch_params.append((idx, self.idx_map[idx]))

        if len(insert_batch_params):
            self._extras.execute_batch(cursor, insert_query, insert_batch_params)
        if len(update_batch_params):
            self._extras.execute_batch(cursor, update_query, update_batch_params)
        self.commit()
        self.idx_map.clear()


from unittest import TestCase


class ManagerTest(TestCase):
    class AReport(Report):
        pass

    class BReport(Report):
        pass

    def setUp(self) -> None:
        self.man = Manager()

    def test(self):
        report_list: List[Report.__class__] = [self.AReport, self.BReport]
        self.man.register(report_list)
        _map = self.man.preset_map()
        report = self.man.report_factory(_map['AReport'])
        self.assertEqual(report.__class__, self.AReport)
