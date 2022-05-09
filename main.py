from lib.pg_starter import PGStarter
from keys import KeyChain
from lib.reports.report_reg import report_list
from lib.activity_reg import activity_list
from lib.telebots.starter_alarm import TelegramAlarmer


def make_starter():
    return PGStarter(activity_list=activity_list,  report_list=report_list, alarmer=TelegramAlarmer())


make_starter().track_schedule()
