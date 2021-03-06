"""
    1C:Apdex XML  format parser
    Need calculate Apdex indicators after file processing
"""

from datetime import datetime, timedelta
from xml.etree import ElementTree


def parse(local_path: str, origin_file_name: str, time_zone_adjust: int):
    apdex_file = open(local_path)
    with apdex_file:
        root = ElementTree.parse(apdex_file).getroot()
        for ops in root:
            for measure in ops:
                ops_attribute = ops.attrib
                measure_attribute = measure.attrib
                line = {
                    'ops_uid': ops_attribute['uid'],
                    'ops_name': ops_attribute['name'],
                    'duration': float(measure_attribute['value']),
                    'user': measure_attribute['userName'],
                    'start':
                        datetime.strptime(measure_attribute['tSaveUTC'], '%Y-%m-%dT%H:%M:%S') +
                        timedelta(hours=time_zone_adjust),
                    'session': int(measure_attribute['sessionNumber']),
                    'fail': not bool(measure_attribute['runningError']),
                    'target': float(ops_attribute['targetValue']),
                    'priority': ops_attribute['priority'],
                }
                line['status'] = 'NS' if line['target'] >= line['duration'] else 'NT'
                yield line
