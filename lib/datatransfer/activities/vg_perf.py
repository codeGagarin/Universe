"""
    Activities for VG performance data transfer
"""

from keys import KeyChain
from lib.schedutils import Activity
from .._index import Processor, DataFilter, FtpSession, PGSession, ParserJob
from ..parsers import techjrnl, syscounters, apdex
from .apdex_calc import ApdexCalc


class VGPerf(Activity):

    DEFAULT_MAX_FILES = 300

    def get_crontab(self):
        return '*/30 * * * *'

    def run(self):
        ftp_key = KeyChain.FTP_TJ_KEYS['vgunf']
        base1s = ftp_key['user']
        transfer = FtpSession(ftp_key)
        storage = PGSession(
            KeyChain.PG_PERF_KEY,
            DataFilter().add('base1s', base1s)
        )

        processor = Processor(transfer=transfer, storage=storage)
        processor.add_parser_job(
            ParserJob(
                parser=techjrnl.parse,
                data_type='logs',
                transfer_path='logs',
                store_place='TJLines',
                gmt_time_zone=+3,
                max_files=self.DEFAULT_MAX_FILES,
            )
        )
        processor.add_parser_job(
            ParserJob(
                parser=apdex.parse,
                data_type='apdx',
                transfer_path='apdx',
                store_place='ApdexLines',
                gmt_time_zone=+3,
                max_files=self.DEFAULT_MAX_FILES,
            )
        )
        processor.add_parser_job(
            ParserJob(
                parser=syscounters.parse,
                data_type='cntr',
                transfer_path='cntr',
                store_place='CounterLines',
                gmt_time_zone=+3,
                max_files=self.DEFAULT_MAX_FILES,
            )
        )
        processor.execute()

        calc = ApdexCalc(self._ldr)
        calc['base1s'] = base1s
        calc.apply()


from unittest import TestCase
from lib.schedutils import NullStarter


class _VGPerfTest(TestCase):

    def setUp(self) -> None:
        self.ns = NullStarter()
        self.a = VGPerf(NullStarter())

    def test_run(self):
        a = self.a
        a.DEFAULT_MAX_FILES = 1
        a.run()
