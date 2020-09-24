from activities.activity import Activity

from keys import KeyChain
import lib.perf_utils as utils


class TJSync(Activity):
    def run(self):
        ftp_key = KeyChain.FTP_TJ_KEYS['vgunf']

        ftp_con = utils.connect_server(ftp_key)
        log_files = utils.get_tj_files_for_sync(ftp_con, 500)
        adapter = utils.PGAdapter(KeyChain.PG_PERF_KEY, ftp_key['user'])
        files_ok = 0
        files_fail = 0
        for file in log_files:
            is_ok = utils.parse_log_file(ftp_con, file, adapter)
            if is_ok:
                files_ok += 1
            else:
                files_fail += 1

        print(f"{ftp_key['user']}: [TJ] f:{files_fail}.k:{files_ok}")
        ftp_con.close()


    def get_crontab(self):
        return '*/5 * * * *'


