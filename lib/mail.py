from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

from lib.schedutils import Activity
from keys import KeyChain


class EmailActivity(Activity):
    def _fields(self):
        return 'subject from to cc body smtp'

    def run(self):
        html_body = self['body']

        acc_key = KeyChain.SMTP_KEY[self['smtp']]

        msg = MIMEMultipart('alternative')
        msg['Subject'] = self['subject']
        msg['From'] = acc_key['from'] if acc_key.get('from', None) else acc_key['user']
        test_address = KeyChain.SMTP_KEY.get('DEBUG')
        if test_address == 'Nope':
            print("Successfully sent [Nope] email")
            return
        if test_address:
            msg['Subject'] += ' TO:{' + (", ".join(self['to']) if self['to'] else '') + '}' + \
                              ' CC:{' + (", ".join(self['cc']) if self['cc'] else '') + '}'
            msg['To'] = test_address
            msg['Cc'] = ''
        else:
            msg['To'] = ", ".join(self['to']) if self['to'] else ''
            msg['Cc'] = ", ".join(self['cc']) if self['cc'] else ''

        # Record the MIME type of html - text/html.
        body = MIMEText(html_body or '', 'html')

        # Attach HTML part into message container.
        msg.attach(body)

        try:
            server = smtplib.SMTP_SSL(host=acc_key['host'], port=acc_key['port'])
            if acc_key['port'] == 587:
                server.starttls()
            server.login(acc_key['user'], acc_key['pwd'])
            server.sendmail(msg["From"], msg["To"].split(",") +
                            msg["Cc"].split(","), msg.as_string())
            server.quit()
            print("Successfully sent email")
        except smtplib.SMTPException as error:
            print(f"Unable to send email\nError: {error}")
            raise error


from unittest import TestCase
from lib.schedutils import NullStarter


class EmailActivityTest(TestCase):
    def setUp(self) -> None:
        self.a = EmailActivity(NullStarter())
        self.a['to'] = tuple('i.belov@prosto12.ru')
        self.a['subject'] = 'test'

    def test_send(self):
        self.a.run()
