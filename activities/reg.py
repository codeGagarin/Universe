from lib.mail import EmailActivity
from activities.intraservice import ISActualizer, ISSync, IS404TaskCloser
from activities.reports import LoaderStateReporter2, HelpdeskWeekly
from lib.perfutils import TJSync
from lib.tablesync import TableSyncActivity
from lib.levelscan import LevelScan, FZLevelScan
from lib.monitutils import Monitoring
from lib.datarollup import CounterLinesRoll
from lib.datatransfer import VGPerf, ApdexCalc
from lib.intraservice import ClosedFix


def init_ldr(ldr):
    ldr.register(EmailActivity)
    ldr.register(ISActualizer)
    ldr.register(ISSync)
    ldr.register(LoaderStateReporter2)
    ldr.register(HelpdeskWeekly)
    ldr.register(IS404TaskCloser)
    ldr.register(TJSync)
    ldr.register(TableSyncActivity)
    ldr.register(LevelScan)
    ldr.register(FZLevelScan)
    ldr.register(Monitoring)
    ldr.register(CounterLinesRoll)
    ldr.register(VGPerf)
    ldr.register(ApdexCalc)
    ldr.register(ClosedFix)


