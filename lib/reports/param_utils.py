import hashlib
from datetime import datetime, date
import json


def _hash(s: str):
    """ bigint hash for random string, presents params ID for params storage """
    return int(hashlib.shake_128(s.encode()).hexdigest(7), 18)


def json_to_dict(value: str):
    def dict_converter(source):
        # scan json-source tree and replace datetime strings to datetime objects
        def date_converter(s):
            try:
                return datetime.strptime(s, 'Datetime:%Y-%m-%d %H:%M:%S.%f')
            except:
                pass
            try:
                return datetime.strptime(s, 'Date:%Y-%m-%d').date()
            except:
                pass
            return s

        def list_converter(lst):
            for i in range(len(lst)):
                r = lst[i]
                if isinstance(r, str):
                    lst[i] = date_converter(r)
                elif isinstance(r, dict):
                    dict_converter(r)
                elif isinstance(r, list):
                    list_converter(r)

        for k, v in source.items():
            if isinstance(v, list):
                list_converter(v)
            elif isinstance(v, dict):
                dict_converter(v)
            elif isinstance(v, str):
                source[k] = date_converter(v)
        return source

    return json.loads(value, object_hook=dict_converter)


def dict_to_json(value: dict):
    def converter(o):
        if isinstance(o, datetime):
            return o.strftime('Datetime:%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(o, date):
            return o.strftime('Date:%Y-%m-%d')

    return json.dumps(value, default=converter)


# TODO: cool param box tests, should be replace to testing area
# class ParamsBoxTest(TestCase):
#     def setUp(self) -> None:
#         self.box = ParamsBox(KeyChain.PG_REPORT_KEY)
#         self.params: List[_Params] = [
#             _Params({'str': '1', 'num': 1, 'bool': True, 'dt': datetime.now()}),
#             _Params({'str': '0', 'num': 0, 'bool': False, 'dt': datetime.now()})
#         ]
#
#     def test(self):
#         idx0 = self.box.params_to_idx(self.params[0])
#         idx1 = self.box.params_to_idx(self.params[1])
#         local_stored_params = self.box.idx_to_params(idx0)
#         self.assertEqual(self.params[0], local_stored_params)
#         self.box.flush()
#         global_stored_params = self.box.idx_to_params(idx1)
#         self.assertEqual(self.params[1], global_stored_params)
