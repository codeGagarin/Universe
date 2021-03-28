from activities.intraservice import ISActualizer, ISSync, IS404TaskCloser
from activities.reports import LoaderStateReporter2, HelpdeskWeekly
from lib.activity_reg import activity_list

activity_list.extend(
    [
        ISActualizer,
        ISSync,
        LoaderStateReporter2,
        HelpdeskWeekly,
        IS404TaskCloser,
    ]
)




