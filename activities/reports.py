from pathlib import Path

import jinja2
from premailer import transform

from activities.activity import Activity, Email
from report import DiagReport, HelpdeskReport


class ReportActivity(Activity):

    @classmethod
    def get_report_html(cls, report, key_chain):
        proj_root = str(Path(__file__).parent.parent)
        templates_root = proj_root+"/templates"
        template_ldr = jinja2.FileSystemLoader(searchpath=templates_root)
        template_env = jinja2.Environment(loader=template_ldr)
        template_name = report.get_template()
        template = template_env.get_template(template_name)
        report.web_server_name = key_chain.WEB_PATH
        html_text = template.render(rpt=report, eml=True)
        css_path = f'{proj_root}/static/css/bootstrap.css'
        f = open(css_path)
        css_text = f.read()
        f.close()
        return transform(html_text, css_text=css_text)


class LoaderStateReporter2(ReportActivity):
    def get_crontab(self):
        return '0 3 * * *'

    def run(self):
        report = DiagReport()
        conn = report.get_connection(self._ldr.key_chain.PG_KEY)
        report.request_data(conn)

        email = Email(self._ldr)
        email['to'] = ('belov78@gmail.com',)
        email['subject'] = 'Loader daily report'
        email['body'] = self.get_report_html(report, self._ldr.key_chain)
        email.run()


class HelpdeskWeekly(ReportActivity):
    def get_crontab(self):
        return '0 7 * * 1'

    @classmethod
    def get_report_params(cls):
        return {
            "StationITWeekly": {
                'to': (7162, 9131, 8724, 9070),
                'cc': ('alexey.makarov@station-hotels.ru', 'igor.belov@station-hotels.ru'),
                'subj': '[Weekly] Недельный отчет Helpdesk',
                'params': {
                    'services': (139,),
                    'executors': (7162, 9131, 8724, 9070),
                    'frame': 'weekly'
                }
            },
            'Prosto12': {
                'to': ('v.ulianov@prosto12.ru', 'i.belov@prosto12.ru'),
                'cc': (),
                'subj': '[Weekly] Недельный отчет Helpdesk',
                'params': {
                    'services': (),
                    'executors': (396, 5994, 405, 402, 5995, 390, 43),
                    'frame': 'weekly',
                },
            },
        }

    def run(self):
        params = self.get_report_params()
        for prm in params.values():
            report = HelpdeskReport(prm['params'])
            conn = report.get_connection(self._ldr.key_chain.PG_KEY)
            report.request_data(conn)

            email = Email(self._ldr)
            email['to'] = report.users_to_email(conn, prm['to'])
            email['cc'] = report.users_to_email(conn, prm['cc'])
            email['subject'] = prm['subj']
            email['body'] = self.get_report_html(report, self._ldr.key_chain)
            email.apply()
