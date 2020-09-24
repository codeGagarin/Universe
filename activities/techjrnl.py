from activities.activity import Activity

from keys import KeyChain
import lib.ftptjparser as parser

class TJSync(Activity):
    def run(self):
        ftp_key = KeyChain.FTP_TJ_KEYS['vgunf']

        ftp_con = parser.connect_server(ftp_key)
        log_files = parser.get_files_for_sync(ftp_con, 25)
        adapter = parser.PGAdapter(KeyChain.PG_PERF_KEY, ftp_key['user'])
        for file in log_files:
            is_ok = parser.parse_log_file(ftp_con, file, adapter)
            print(f"{ftp_key['user']}: {'Ok' if is_ok else 'Fail'}")
        ftp_con.close()


    def get_crontab(self):
        return '*/5 * * * *'


