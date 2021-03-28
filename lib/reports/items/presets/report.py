from ...report_classes import Report


class PresetReport(Report):
    @classmethod
    def anchor_path(cls):
        return __file__  # NEVER delete this! Using for correct Jinja templates path resolving


