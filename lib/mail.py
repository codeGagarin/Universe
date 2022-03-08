from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.utils import COMMASPACE
import smtplib

from lib.schedutils import Activity
from keys import KeyChain


class EmailActivity(Activity):
    def _fields(self):
        return 'subject from to cc body smtp attachment'

    def run(self):
        acc_key = KeyChain.SMTP_KEY[self['smtp']]

        msg = MIMEMultipart()
        msg['Subject'] = self['subject']
        msg['From'] = acc_key['from'] if acc_key.get('from', None) else acc_key['user']
        test_address = KeyChain.SMTP_KEY.get('DEBUG')

        def address_adapter(address_line): return COMMASPACE.join(
            [address_line] if isinstance(address_line, str) else address_line or []
        )

        to = address_adapter(self['to'])
        cc = address_adapter(self['cc'])

        if test_address == 'Nope':
            print("Successfully sent [Nope] email")
            return
        if test_address:
            msg['Subject'] += f' TO:{to} CC:{cc}'
            msg['To'] = test_address
            msg['Cc'] = ''
        else:
            msg['To'] = to
            msg['Cc'] = cc

        # Attach HTML part into message container.
        msg.attach(MIMEText(self['body'] or '', 'html'))

        for name, data in (self['attachment'] or {}).items():
            part = MIMEApplication((data if isinstance(data, str) else data.getbuffer()), Name=name)
            part['Content-Disposition'] = f'attachment; filename={name}'
            msg.attach(part)
        try:
            server = None
            if acc_key['port'] == 465:
                server = smtplib.SMTP_SSL(host=acc_key['host'], port=acc_key['port'])
            elif acc_key['port'] == 587:
                server = smtplib.SMTP(host=acc_key['host'], port=acc_key['port'])
                server.starttls()
            else:
                assert True, 'SMTP:{} default port not found'.format(acc_key['host'])

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

    def test_send(self):
        self.a['to'] = ['belov78@gmail.com']
        self.a['subject'] = 'test'
        self.a['smtp'] = 'P12'
        self.a['body'] = 'FInd it enclosure'
        self.a['attachment'] = {
            'hello.txt': 'I\'m file from email attachment'
        }
        self.a.run()
