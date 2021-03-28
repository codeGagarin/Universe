from keys import KeyChain

from .items.starter import Starter, JobDetails
from .items.service_board.report import ServiceBoard, ServiceBoardSender
from .items.task_report import TaskReport
from .items.expenses_report import ExpensesReport
from .items.presets import PresetReport
from .items.expenses_details import ExpensesDetails

report_list = [
    PresetReport,
    Starter,
    JobDetails,
    ServiceBoard,
    TaskReport,
    ExpensesReport,
    ExpensesDetails
]

activity_list = [
    ServiceBoardSender,
]

if KeyChain.FLASK_DEBUG_MODE:
    from lib.reports.items.__example__ import ExampleReport
    report_list.append(ExampleReport)

__all__ = [report_list]
