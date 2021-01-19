from datetime import datetime, date
import json


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
