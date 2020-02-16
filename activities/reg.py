from activities.activity import Email
from activities.intraservice import ISActualizer, ISSync
from activities.reports import LoaderStateReporter2, HelpdeskWeekly


def init_ldr(ldr):
    ldr.register(Email)
    ldr.register(ISActualizer)
    ldr.register(ISSync)
    ldr.register(LoaderStateReporter2)
    ldr.register(HelpdeskWeekly)

