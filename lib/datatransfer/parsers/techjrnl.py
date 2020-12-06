"""
    1S: Enterprise Technical Journal Parser
"""

from datetime import datetime, timedelta
import re


def _get_meta(file_name: str):
    re_params = r'_(\d+)_(\d\d)(\d\d)(\d\d)(\d\d)'
    prm = re.findall(re_params, file_name)[0]
    return {
        'rphost': int(prm[0]),
        'yy': int(prm[1]),
        'mm': int(prm[2]),
        'dd': int(prm[3]),
        'hh': int(prm[4])
    }


def _parse_line(line: str, name_meta: dict, time_zone_adjust: int):
    re_header = r'^(\d\d):(\d\d)\.(\d+)-(\d+),(\w+),(\d+),'
    header = re.findall(re_header, line)
    re_params = r'([\w:]+)=([^,\r]+)'
    params = {g[0]: g[1] for g in re.findall(re_params, line)}

    stamp = datetime(
        2000 + int(name_meta['yy']),
        int(name_meta['mm']),
        int(name_meta['dd']),
        int(name_meta['hh']),
        int(header[0][0]),  # minutes
        int(header[0][1]),  # seconds
        int(header[0][2])  # milliseconds
    ) + timedelta(hours=time_zone_adjust)

    record = {
        'rphost': name_meta['rphost'],
        'dur': header[0][3],
        'event': header[0][4], 'lvl': header[0][5],
        'osthread': params.get('OSThread'),
        'exception': params.get('Exception'),
        'descr': params.get('Descr'),
        'stamp': stamp,
        'source': line
    }
    return record


def parse(local_path: str, origin_file_name: str, gmt_time_zone: int):
    re_hdr = r'^\d\d:\d\d\.\d+-'
    accumulate_line = ''
    name_meta = _get_meta(origin_file_name)

    with open(local_path, encoding='utf-16') as log_file:
        while True:
            log_line = log_file.readline()

            if not log_line:
                if len(accumulate_line):
                    yield _parse_line(accumulate_line, name_meta, gmt_time_zone)
                break
            elif re.match(re_hdr, log_line):  # new line tag found
                if len(accumulate_line) == 0:  # no line accumulated
                    accumulate_line += log_line
                else:
                    yield _parse_line(accumulate_line, name_meta, gmt_time_zone)
                    accumulate_line = log_line
                    continue
            else:
                accumulate_line += log_line


import unittest


class _TJParserTest(unittest.TestCase):
    def setUp(self) -> None:
        self._path = '/var/folders/_h/try61xjj22b6hxn72f84f9s00000gn/T/rphost_10368_20092116.log'
        self._origin = 'rphost_1020_20092316.log'

    def test_parse(self):
        for line in parse(self._path, self._origin, +3):
            print(line)

# if __name__ == '__main__':
#     unittest.main()