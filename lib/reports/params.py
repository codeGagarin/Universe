import hashlib
from datetime import datetime
from typing import Union

from lib.pg_utils import PGMix, sql

from .json_utils import json_to_dict, dict_to_json


def _hash(s: str):
    """ bigint hash for random string, presents params ID for params storage """
    return int(hashlib.shake_128(s.encode()).hexdigest(7), 18)


class _Params(dict):
    pass


class ParamsBox(PGMix):
    def __init__(self, params_store_key):
        PGMix.__init__(self, params_store_key)
        self.idx_map = {}

    def params_to_idx(self, params: _Params) -> int:
        json_params = dict_to_json(params)
        value_hash = _hash(json_params)
        self.idx_map[value_hash] = json_params
        return value_hash

    def idx_to_params(self, idx: int) -> Union[_Params, None]:
        local_stored_params = self.idx_map.get(idx)
        if local_stored_params:
            return json_to_dict(local_stored_params)

        query = sql.SQL('UPDATE "Params" SET last_touch = {}, touch_count = touch_count+1'
                        ' WHERE id={} RETURNING "params"').format(
            sql.Literal(datetime.now()),
            sql.Literal(idx)
        )

        with self._cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

        if result:
            self._commit()
            return json_to_dict(result[0])
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

        cursor = self._cursor(named=True)
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
        self._commit()
        self.idx_map.clear()


from unittest import TestCase
from keys import KeyChain
from typing import List


class ParamsBoxTest(TestCase):
    def setUp(self) -> None:
        self.box = ParamsBox(KeyChain.PG_REPORT_KEY)
        self.params: List[_Params] = [
            _Params({'str': '1', 'num': 1, 'bool': True, 'dt': datetime.now()}),
            _Params({'str': '0', 'num': 0, 'bool': False, 'dt': datetime.now()})
        ]

    def test(self):
        idx0 = self.box.params_to_idx(self.params[0])
        idx1 = self.box.params_to_idx(self.params[1])
        local_stored_params = self.box.idx_to_params(idx0)
        self.assertEqual(self.params[0], local_stored_params)
        self.box.flush()
        global_stored_params = self.box.idx_to_params(idx1)
        self.assertEqual(self.params[1], global_stored_params)
