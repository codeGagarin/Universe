from typing import List, Union


from keys import KeyChain
from .params import ParamsBox
from ._report import Report


class Manager:
    def __init__(self):
        self._params_box = ParamsBox(KeyChain.PG_REPORT_KEY)
        self._regs = {}

    def register(self, reports: list):
        for report in reports:
            self._regs[report.get_type()] = report

    def reports_map(self) -> dict:
        result = {}
        for report in self._regs.values():
            result[report.get_type()] = self._params_box.params_to_idx(report.default_params())
        self._params_box.flush()
        return result

    def report_factory(self, idx: int) -> Union[Report, None]:
        params = self._params_box.idx_to_params(idx)
        report_class = self._regs.get(params['type'])

        if not report_class:
            return None

        report = report_class(self._params_box)
        report.request_data(params, idx)
        return report


from unittest import TestCase


class ManagerTest(TestCase):
    class AReport(Report):
        pass

    class BReport(Report):
        pass

    def setUp(self) -> None:
        self.man = Manager()

    def test(self):
        report_list: List[Report.__class__] = [self.AReport, self.BReport]
        self.man.register(report_list)
        _map = self.man.reports_map()
        report = self.man.report_factory(_map['AReport'])
        self.assertEqual(report.__class__, self.AReport)

