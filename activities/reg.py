from activities.activity import Email
from activities.intraservice import ISActualizer, ISSync, IS404TaskCloser
from activities.reports import LoaderStateReporter2, HelpdeskWeekly
from activities.techjrnl import TJSync, LevelScan


def init_ldr(ldr):
    ldr.register(Email)
    ldr.register(ISActualizer)
    ldr.register(ISSync)
    ldr.register(LoaderStateReporter2)
    ldr.register(HelpdeskWeekly)
    ldr.register(IS404TaskCloser)
    ldr.register(TJSync)
    ldr.register(LevelScan)




