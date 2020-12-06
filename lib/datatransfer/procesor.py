"""
    File processor utils
"""

from dataclasses import dataclass
from typing import List, Callable
import traceback

from ._session import TransferSession, StorageSession, FileBadge


@dataclass()
class ParserJob:
    parser: Callable
    data_type: str
    transfer_path: str
    store_place: str
    time_zone_adjust: int = +3
    max_files: int = 500
    line_validator: Callable[[dict], None] = None


class _Log:
    def __init__(self):
        self._data = {}

    def register_type(self, data_type: str) -> None:
        self._data[data_type] = {'f': 0, 'd': 0}

    def done(self, badge: FileBadge) -> None:
        self._data[badge.data_type]['d'] += 1

    def fail(self, badge: FileBadge) -> None:
        self._data[badge.data_type]['f'] += 1

    def __repr__(self) -> str:
        return '-'.join(
            [
                '[{}]:d.{}:f.{}'.format(
                    key,
                    self._data[key]['d'],
                    self._data[key]['f']
                )
                for key in self._data.keys()
            ]
        )


class Processor:
    def __init__(self, transfer: TransferSession, storage: StorageSession):
        self._transfer = transfer
        self._storage = storage
        self.parser_jobs: List[ParserJob] = []
        self._log = _Log()

    def add_parser_job(self, job: ParserJob):
        self.parser_jobs.append(job)
        self._transfer.add_transfer_rule(job.data_type, job.transfer_path)
        self._storage.add_store_rule(job.data_type, job.store_place)
        self._log.register_type(job.data_type)

    def execute(self):
        for job in self.parser_jobs:
            for i in range(job.max_files):
                trans_id = self._transfer.attach_file(job.data_type)

                if not trans_id:
                    break

                store_badge = FileBadge(
                    self._transfer.file_name(trans_id),
                    job.data_type
                )
                batch_id = self._storage.attach_file(store_badge)

                try:
                    path = self._transfer.local_path(trans_id)

                    for line in job.parser(path, store_badge.name, job.time_zone_adjust):
                        if job.line_validator:
                            job.line_validator(line)
                        self._storage.submit_line(batch_id, line)

                except Exception:
                    self._transfer.to_fail(trans_id)
                    self._storage.update_file_status(batch_id, False, traceback.format_exc())
                    self._log.fail(store_badge)
                else:
                    self._transfer.to_done(trans_id)
                    self._storage.update_file_status(batch_id, True)
                    self._log.done(store_badge)

        print(self._storage.filter(), self._log, sep=':')


from unittest import TestCase

from keys import KeyChain
from .pg_session import PGSession, DataFilter
from .ftp_session import FtpSession
from .parsers import techjrnl, apdex, syscounters


class _ProcessorTest(TestCase):
    def setUp(self) -> None:
        ftp_key = KeyChain.FTP_TJ_KEYS['tjtest']
        self.transfer = FtpSession(ftp_key)
        self.storage = PGSession(
            KeyChain.PG_PERF_KEY,
            DataFilter().add('base1s', ftp_key['user'])
        )

    def test_execute(self):
        processor = Processor(transfer=self.transfer, storage=self.storage)
        processor.add_parser_job(
            ParserJob(
                parser=techjrnl.parse,
                data_type='logs',
                transfer_path='logs',
                store_place='TJLines',
                time_zone_adjust=+1,
                max_files=1,
            )
        )
        processor.add_parser_job(
            ParserJob(
                parser=apdex.parse,
                data_type='apdx',
                transfer_path='apdx',
                store_place='ApdexLines',
                time_zone_adjust=+3,
                max_files=30,
            )
        )
        processor.add_parser_job(
            ParserJob(
                parser=syscounters.parse,
                data_type='cntr',
                transfer_path='cntr',
                store_place='CounterLines',
                time_zone_adjust=+3,
                max_files=1,
            )
        )

        # # detect counter_lines timeline bounds
        # left_bound: datetime = None
        # right_bound: datetime = None
        # def counter_line_validator(line: dict):
        #     now =
        #

        processor.execute()


class _LogTest(TestCase):
    def setUp(self) -> None:
        self.log = _Log()
        _types = 'type1 type2 type3'.split()
        [self.log.register_type(t) for t in _types]
        badges = [FileBadge('', t) for t in _types]
        for i in range(len(badges)):
            for k in range(i+1):
                self.log.done(badges[i])
                self.log.fail(badges[i])

    def test_repr(self):
        print(self.log)
