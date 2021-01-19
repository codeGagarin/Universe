from pathlib import Path
import logging
from io import StringIO

import jinja2
import premailer

from lib.schedutils import Activity
from activities.mail import Email
from report import DiagReport, HelpdeskReport
from keys import KeyChain


class ReportActivity(Activity):

    @classmethod
    def get_report_html(cls, report, web_path):
        proj_root = str(Path(__file__).parent.parent)
        templates_root = proj_root+"/templates"
        template_ldr = jinja2.FileSystemLoader(searchpath=templates_root)
        template_env = jinja2.Environment(loader=template_ldr)
        template_name = report.get_template()
        template = template_env.get_template(template_name)
        report.web_server_name = web_path
        html_text = template.render(rpt=report, eml=True)
        css_path = f'{proj_root}/static/css/bootstrap.css'
        f = open(css_path)
        css_text = f.read()
        f.close()
        premailer_log = StringIO()
        premailer_log_hander = logging.StreamHandler(premailer_log)
        return premailer.Premailer(
            cssutils_logging_handler=premailer_log_hander,
            cssutils_logging_level=logging.CRITICAL,
            remove_classes=True,
            css_text=css_text
        ).transform(html_text)


class LoaderStateReporter2(ReportActivity):
    def get_crontab(self):
        return '0 3 * * *'

    def run(self):
        report = DiagReport()
        conn = report.get_connection(KeyChain.PG_STARTER_KEY)
        report.request_data(conn)

        email = Email(self._ldr)
        email['to'] = ('belov78@gmail.com',)
        email['subject'] = 'Loader daily report'
        email['body'] = self.get_report_html(report, KeyChain.WEB_PATH)
        email.apply()


class HelpdeskWeekly(ReportActivity):
    def get_crontab(self):
        return '0 7 * * 1'

    @classmethod
    def get_report_params(cls):
        return HelpdeskReport.get_report_map()

    def run(self):
        params = self.get_report_params()
        for prm in params.values():
            report = HelpdeskReport(prm['params'])
            conn = report.get_connection(KeyChain.PG_KEY)
            report.request_data(conn)

            email = Email(self._ldr)
            email['smtp'] = prm['smtp']
            email['to'] = report.users_to_email(conn, prm.get('to', []))
            email['cc'] = report.users_to_email(conn, prm.get('cc', []))
            email['subject'] = prm.get('subj', '')
            email['body'] = self.get_report_html(report, KeyChain.WEB_PATH)
            email.apply()


from unittest import TestCase
from lib.schedutils import NullStarter


class LoaderStateReporterTest(TestCase):
    def setUp(self) -> None:
        self.stater = NullStarter()
        self.lsr2 = HelpdeskWeekly(self.stater)

    def test_run(self):
        self.lsr2.run()


class HelpdeskWeeklyTest(TestCase):
    def setUp(self) -> None:
        self.stater = NullStarter()
        self.hdw = HelpdeskWeekly(self.stater)

    def test_run(self):
        self.hdw.run()


