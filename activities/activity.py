from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


class Activity:
    def __init__(self, ldr, params=None):
        if params:
            self._params = params
        else:
            self._params = {}.fromkeys(self._fields().split())
        self._ldr = ldr

    def __setitem__(self, key, value):
        # key legal check
        if key not in self._fields().split():
            raise KeyError
        self._params[key] = value

    def __getitem__(self, key):
        return self._params.get(key, None)
        pass

    def get_params(self):
        return self._params

    def _fields(self):
        return ''

    def get_type(self):
        return self.__class__.__name__

    def get_crontab(self):
        return None

    def apply(self, due_date=None):
        return self._ldr.to_plan(self, due_date)

    def run(self):
        pass


class Email(Activity):
    def _fields(self):
        return 'subject from to cc body smtp'

    def run(self):
        html_body = self['body']

        key = self['smtp'] if self['smtp'] else 'DEF'

        acc_key = self._ldr.key_chain.SMTP_KEY.get(key if key else self._ldr.key_chain.SMTP_KEY['DEF'])
        msg = MIMEMultipart('alternative')
        msg['Subject'] = self['subject']
        msg['From'] = acc_key['from'] if acc_key.get('from', None) else acc_key['user']
        test_address = acc_key.get('test_address')
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
        body = MIMEText(html_body, 'html')

        # Attach HTML part into message container.
        msg.attach(body)

        try:
            server = smtplib.SMTP_SSL(host=acc_key['host'], port=acc_key['port'])
            server.login(acc_key['user'], acc_key['pwd'])
            server.sendmail(msg["From"], msg["To"].split(",") +
                            msg["Cc"].split(","), msg.as_string())
            server.quit()
            print("Successfully sent email")
        except smtplib.SMTPException as error:
            print(f"Unable to send email\nError: {error}")
            raise error
