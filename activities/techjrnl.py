from activities.activity import Activity

from keys import KeyChain
import lib.perf_utils as utils


class TJSync(Activity):
    def run(self):
        ftp_key = KeyChain.FTP_TJ_KEYS['vgunf']

        ftp_con = utils.connect_server(ftp_key)
        log_files = utils.get_tj_files_for_sync(ftp_con, 50)
        adapter = utils.PGAdapter(KeyChain.PG_PERF_KEY, ftp_key['user'])
        for file in log_files:
            is_ok = utils.parse_log_file(ftp_con, file, adapter)
        print(f"{ftp_key['user']}: {'Ok' if is_ok else 'Fail'}")
        ftp_con.close()


    def get_crontab(self):
        return '*/5 * * * *'


