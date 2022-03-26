from .mail import EmailActivity
from .perfutils import TJSync
from .tablesync import TableSyncActivity
from .levelscan import LevelScan, FZLevelScan
from .monitutils import Monitoring
from .datarollup import CounterLinesRoll
from .datatransfer import VGPerf, ApdexCalc
from .intraservice import ClosedFix, ISServiceUpdater
from .archiver import Archiver
from .reports.report_reg import activity_list as report_activity_list
from .intraservice.sync_lib import ISSync, ISActualizer
from .clientbase.cost_transfer import CostTransfer

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
    ISActualizer,
    ISSync,
    ISServiceUpdater,
    CostTransfer
]

activity_list.extend(report_activity_list)

__all__ = [
    activity_list
]
