from io import StringIO
import logging
from dataclasses import dataclass

import premailer

from lib.schedutils import Activity
from lib.pg_utils import PGMix
from lib.mail import EmailActivity
from static import WEB_STATIC_PATH
from keys import KeyChain


class ReportSender(Activity, PGMix):
    @dataclass
    class MailerParams:
        PRESET_NAME: str
        SMTP: str or None  # name of KeyChain.SMTP_KEY dict
        TO: list or None
        CC: list or None
        SUBJECT: str or None

    REPORT_TYPE = None
    IMMEDIATE_SEND = False
    MAIL_LIST = (
        # MailerParams(
        #     PRESET_NAME=report.PresetTypes.STATION,
        #     SMTP='sth',
        #     TO=ServiceBoard.presets()[ServiceBoard.PresetTypes.STATION].EXECUTORS,
        #     CC=['sn@rrt.ru', 'igor.belov@rrt.ru']
        # ),
    )

    def run(self):
        manager = self._ldr.report_manager
        for mail_params in self.MAIL_LIST:
            report = self.REPORT_TYPE(manager)
            report.request_data(self.REPORT_TYPE.presets()[mail_params.PRESET_NAME])
            email = EmailActivity(self._ldr)
            email['smtp'] = mail_params.SMTP
            email['subject'] = mail_params.SUBJECT
            email['to'] = report.validate_email_list(mail_params.TO)
            email['cc'] = report.validate_email_list(mail_params.CC)
            email['body'] = report_to_html(report)
            if self.IMMEDIATE_SEND:
                email.run()
            else:
                email.apply()


def report_to_html(report):
    template = report.get_template()
    report_env = report.environment()
    report_env['EML_RENDER'] = True  # for exclude scripts and css-styles from message body
    html_text = template.render(**report_env)

    premailer_log = StringIO()
    premailer_log_handler = logging.StreamHandler(premailer_log)

    return premailer.Premailer(
        cssutils_logging_handler=premailer_log_handler,
        cssutils_logging_level=logging.CRITICAL,
        remove_classes=True,
        base_url=KeyChain.WEB_PATH,
        base_path=WEB_STATIC_PATH,
        external_styles=f'css/bootstrap.css',
    ).transform(html_text)
