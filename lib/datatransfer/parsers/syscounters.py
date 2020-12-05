"""
    System counter parses
"""
import re
import csv
from datetime import datetime, timedelta


def _get_hdr_params(header_line):
    pure_hdr = header_line[1: len(header_line)]

    header_data = re.findall(r'\\\\(.+)\\(.+)\\(.+)', '\n'.join(pure_hdr))
    result = {
        _i: {
            'id': header_data[_i],
            'host': header_data[_i][0],
            'context': header_data[_i][1],
            'type': header_data[_i][2],
        } for _i in range(len(header_data))
    }

    return result


def parse(local_path: str, origin_file_name: str, gmt_time_zone: int):
    counter_file = open(local_path, encoding="utf-16")
    with counter_file:

        counter_data = iter(csv.reader(counter_file))
        hdr_line = next(counter_data)
        params = _get_hdr_params(hdr_line)

        for counter_line in counter_data:
            stamp = datetime.strptime(counter_line[0], '%m/%d/%Y %H:%M:%S.%f') \
                    - timedelta(hours=3) + timedelta(gmt_time_zone)  # GMT 0 correction

            counter_values = counter_line[1: len(counter_line)]
            for i in range(len(counter_values)):
                str_value = counter_values[i].replace(' ', '')

                line = {
                    'stamp': stamp,
                    'counter': params[i]['id'],
                    'host': params[i]['host'],
                    'context': params[i]['context'],
                    'type': params[i]['type'],
                    'flt_value': float(str_value) if str_value else None,
                    'str_value': str_value,
                }
                yield line
