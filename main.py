from lib.pg_starter import PGStarter
from keys import KeyChain
from lib.reports.report_reg import report_list
from lib.activity_reg import activity_list


starter = PGStarter(activity_list=activity_list,  report_list=report_list)
starter.track_schedule()
