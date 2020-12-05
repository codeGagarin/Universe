import ftplib
import os
import tempfile

from ._session import TransferSession


class FtpSession(TransferSession):

    PARSING: str = 'pars'
    DONE: str = 'done'
    FAIL: str = 'fail'

    def __init__(self, key):
        super().__init__()
        self._con = ftplib.FTP(key['host'])
        self._con.login(key['user'], key['pwd'])
        self._file_name = None
        self._local_path = None
        self._home_dir = None
        self._file_type = None

    def __del__(self):
        self._con.close()

    def attach_file(self, data_type: str) -> int:  # -> file_id, None if home dir is empty
        self._home_dir = self._transfer_rule[data_type]
        files = self._con.mlsd(self._home_dir)

        self._file_type = data_type
        self._file_name = None
        for name, facts in files:
            if facts['type'] == 'file':
                self._file_name = name
                break

        if not self._file_name:
            return 0

        process_dir = f'{self._home_dir}/{self.PARSING}'

        self._con.rename(
            f'{self._home_dir}/{self._file_name}',
            f'{process_dir}/{self._file_name}'
        )

        tmp_dir = tempfile.gettempdir()
        self._local_path = f'{tmp_dir}/{self._file_name}'
        parse_file = open(self._local_path, 'wb')
        self._con.retrbinary("RETR " + f'{process_dir}/{self._file_name}', parse_file.write)
        parse_file.close()
        return 1  # threading plug

    def to_fail(self, file_id: int):
        self._con.rename(
            f'{self._home_dir}/{self.PARSING}/{self._file_name}',
            f'{self._home_dir}/{self.FAIL}/{self._file_name}'
        )
        os.remove(self._local_path)

    def to_done(self, file_id: int):
        self._con.rename(
            f'{self._home_dir}/{self.PARSING}/{self._file_name}',
            f'{self._home_dir}/{self.DONE}/{self._file_name}'
        )
        os.remove(self._local_path)

    def local_path(self, file_id) -> str:
        return self._local_path

    def file_name(self, file_id) -> str:
        return self._file_name


from unittest import TestCase
from keys import KeyChain


class _FtpSessionTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_attach_file(self):
        s = FtpSession(KeyChain.FTP_TJ_KEYS['tjtest'])
        s.add_transfer_rule('logs', 'logs')
        _id = s.attach_file('logs')
        print(s.file_name(_id))
        print(s.local_path(_id))
        s.to_fail(_id)


