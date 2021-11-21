import os
from datetime import datetime, timedelta
import time
import tarfile

from lib.schedutils import Activity

ARC_EXTENSION = 'tar.gz'


def gen_pack_name(year: int, month: int) -> str:
    return f'{year}-{month:02}.{ARC_EXTENSION}'


def _path_to_pack(file_path) -> str:
    modify_date = datetime.strptime(
        time.ctime(os.path.getmtime(file_path)), "%a %b %d %H:%M:%S %Y"
    )
    return gen_pack_name(modify_date.year, modify_date.month)


def _pack_list(file_list: list) -> dict:
    result = {}
    for file_path in file_list:
        pack_name = _path_to_pack(file_path)
        pack_node = result[pack_name] = result.get(pack_name, [])
        pack_node.append(file_path)
    return result


def _file_list(path: str, days_ignore_count):
    return [
        file for file in
        [
            os.path.join(path, file) for file in os.listdir(path)
            if file[-len(ARC_EXTENSION):] != ARC_EXTENSION
        ]
        if os.path.isfile(file) and datetime.strptime(
                    time.ctime(os.path.getmtime(file)), "%a %b %d %H:%M:%S %Y"
                    ) < datetime.now() - timedelta(days=days_ignore_count)
    ]


def to_pack(target_path: list, days_ignore_count: int = 0):
    for path in target_path:
        path = os.path.expanduser(path)
        print(f'Target path: {path}')
        packs = _pack_list(_file_list(path, days_ignore_count))
        for pack_name, file_list in packs.items():
            with tarfile.open(os.path.join(path, pack_name), 'w:gz') as tar:
                for file in file_list:
                    file_name = os.path.basename(file)
                    print(f'{pack_name} <- {file_name}')
                    tar.add(file, arcname=file_name)
                    os.remove(file)


class Archiver(Activity):
    TARGET_PATH = [
        '~/ftp/vgunf/logs/done',
        '~/ftp/vgunf/cntr/done',
        '~/ftp/vgunf/apdx/done',
        '~/ftp/skzup/logs',
    ]

    def run(self):
        to_pack(self.TARGET_PATH, 7)

    @classmethod
    def get_crontab(cls):
        return '0 0 * * 0'


from unittest import TestCase
import unittest
from lib.schedutils import NullStarter


class TestArchiver(TestCase):
    def setUp(self) -> None:
        self.a = Archiver(NullStarter())
        self.a.TARGET_PATH = ['../../test_data/files']

    def test_run(self):
        self.a.run()


if __name__ == '__main__':
    unittest.main()