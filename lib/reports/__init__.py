from typing import List

from .manager import Manager
from ._report import Report
from .items.starter import Starter
from .items.job_details import JobDetails

_report_list = [
    Starter,
    JobDetails
]

__all__ = [report.__name__ for report in _report_list]
__all__.append('manager')

manager = Manager()
manager.register(_report_list)
