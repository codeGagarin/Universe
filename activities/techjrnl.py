from activities.activity import Activity


from keys import KeyChain
import lib.perfutils as utils

import lib.levelscan as scan


class TJSync(Activity):
    def run(self):
        ftp_key = KeyChain.FTP_TJ_KEYS['vgunf']

        adapter = utils.PGAdapter(KeyChain.PG_YANDEX_PERF_KEY, ftp_key['user'])
        utils.process_logs(ftp_key, adapter, max_files=500)
        utils.process_apdx(ftp_key, adapter, max_files=500)
        utils.process_cntr(ftp_key, adapter, max_files=100)

        print(adapter.get_log_str())

    def get_crontab(self):
        return '*/30 * * * *'


class LevelScan(Activity):
    def run(self):
        scan.scan_levels(KeyChain.PG_YANDEX_PERF_KEY)
        scan.scan_levels(KeyChain.PG_YANDEX_SSD_CPU)


    def get_crontab(self):
        return '15 * * * *'

