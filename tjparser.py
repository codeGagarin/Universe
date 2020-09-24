import re
import os
from ftplib import FTP
import tempfile

# from psycopg2 import psycopg2

from keys import KeyChain

fk = KeyChain.FTP_SK_KEY
fkh = fk['host']
fku = fk['user']
fkp = fk['pwd']

parse_file = 'rphost_6716_20040413.log'

def download_log(log_name):
    tmp_dir = tempfile.gettempdir()
    out_name = f"{tmp_dir}/{log_name}"
    ftp = FTP(fkh)
    ftp.login(fku, fkp)
    ftp.cwd('logs')
    ftp.retrbinary("RETR " + parse_file, open(out_name, 'wb').write)
    ftp.quit()
    return out_name

class LogLine:
    def __init__(self, log_line: str):
        self.log_line = log_line

        re_header = r'^(\d\d):(\d\d)\.(\d+)-(\d+),(\w+),(\d+),'
        header = re.findall(re_header, log_line)

        re_params = r'([\w:]+)=([^,\r]+)'
        params = re.findall(re_params, log_line)

        self.mm = None
        self.ss = None
        self.ms = None
        self.params = None


class LogFileParser:

    def __init__(self, log_file_name):
        self.eof = False
        self.file_name = log_file_name
        self.file_descriptor = None
        self.file = open(log_file_name, encoding='utf-16')
        self.accumulate_line = ''

    def __del__(self):
        self.file.close()
        os.remove(self.file_name)

    def get_descriptor(self):
        if self.file_descriptor:
            return self.file_descriptor
        return 'TODO: add descriptor'

    def read_line(self):
        result = None
        re_hdr = r'^\d\d:\d\d\.\d+-'

        while True:
            log_line = self.file.readline()

            if not log_line:
                result = LogLine(self.accumulate_line)
                self.eof = True
                break
            elif re.match(re_hdr, log_line):  # new line tag found
                if self.accumulate_line == '':  # no line accumulated
                    self.accumulate_line += log_line
                else:
                    result = LogLine(self.accumulate_line)
                    self.accumulate_line = log_line
                    break
            else:
                self.accumulate_line += log_line

        return result


class LogStoreFile:
    def __init__(self, file_name: str):
        pass

    def add_line(self, line: LogLine):
        pass


def main():
    file_name = download_log(parse_file)
    print(file_name)
    file_store = LogStoreFile(file_name)
    parser = LogFileParser(file_name)
    num = 1
    while not parser.eof:
        line = parser.read_line()
        print(f"{num}-hh:{line.mm}:mm:{line.ss}:mc{line.ms}")
        num += 1


main()