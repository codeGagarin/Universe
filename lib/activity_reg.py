from .mail import EmailActivity
from .perfutils import TJSync
from .tablesync import TableSyncActivity
from .levelscan import LevelScan, FZLevelScan
from .monitutils import Monitoring
from .datarollup import CounterLinesRoll
from .datatransfer import VGPerf, ApdexCalc
from .intraservice import ClosedFix
from .archiver import Archiver
from .reports.report_reg import activity_list as report_activity_list

activity_list = [
    EmailActivity,
    TJSync,
    TableSyncActivity,
    LevelScan,
    FZLevelScan,
    Monitoring,
    CounterLinesRoll,
    VGPerf,
    ApdexCalc,
    ClosedFix,
    Archiver,
]

activity_list.extend(report_activity_list)

__all__ = [
    activity_list
]
