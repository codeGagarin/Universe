import jinja2

from activities.activity import Activity, Email
from report import DiagReport

class LoaderStateReporter2(Activity):
    # def get_crontab(self):
    #     return '0 3 * * *'

    def get_report_html(self):
        kch = self._ldr.key_chain
        template_ldr = jinja2.FileSystemLoader(searchpath="./templates")
        template_env = jinja2.Environment(loader=template_ldr)
        template_name = "diag.html"
        template = template_env.get_template(template_name)
        report = DiagReport()
        conn = report.get_connection(kch.PG_KEY)
        report.request_data(conn)
        report.web_server_name = kch.WEB_PATH
        return template.render(rpt=report)

    def run(self):
        email = Email(self._ldr)
        email['to'] = ('belov78@gmail.com',)
        email['subject'] = 'Loader daily report'
        email['body'] = self.get_report_html()
        email.apply()

class LoaderStateReporter(Activity):
    def get_crontab(self):
        return '0 3 * * *'

    def run(self):
        report = self._ldr.get_state()
        actual = report['actual_date']
        thead = report['header']
        tbody = report['data']
        small_size = 50

        def long_cut(s):
            if s and len(s) > small_size:
                return f'{s[:small_size]}...'

        def date_cut(s):
            if s and s is not '':
                return str(s)[11:19]

        adapter_map = {
            'start': (date_cut,),
            'finish': (date_cut,),
            'params': (long_cut,),
            'result': (long_cut,),
        }

        for row in tbody:
            for field, adapters in adapter_map.items():
                for adapter in adapters:
                    row[thead.index(field)] = adapter(row[thead.index(field)])

        email = Email(self._ldr)
        email['to'] = ('belov78@gmail.com',)
        email['subject'] = 'Loader daily report'

        tab_caption = f"Loader Report on {actual.strftime('%Y-%m-%d')}"

        tab_head = '{}{}{}'.format(
            '<tr class="table-dark">',
            ''.join(f'<th>{i}</th>' for i in thead),
            '</tr>\n'
        )
        status_idx = thead.index('status')

        def get_row_class(_row):
            style = {
                'todo': '',
                'working': 'table-warning',
                'finish': 'table-success',
                'fail': 'table-danger',
            }
            return style[_row[status_idx]]

        tab_rows = ''.join(
            f'<tr class="{get_row_class(row)}">{"".join(f"<td>{escape(str(col))}</td>" for col in row)}</tr>\n'
            for row in report['data'])

        html_table = f'<table class="table">' \
                     f'<thead>{tab_head}</thead><tbody>{tab_rows}</tbody></table>'

        html_head = '<head>' \
                    '   <meta name="viewport" content="width=device-width, initial-scale=1">' \
                    '   <meta charset="utf-8">' \
                    '   <link rel="stylesheet" ' \
                    '       href="https://bootswatch.com/4/spacelab/bootstrap.css">' \
                    '</head>'

        html_report = f'<html>\n{html_head}\n<body>' \
                      f'<div class="container">\n' \
                      f'<h3>{tab_caption}<h3>' \
                      f'{html_table}\n</div>' \
                      f'</body>\n</html>\n'

        email['body'] = html_report
        # f = open("mail.html", "w")
        # f.write(html_report)
        # f.close()
        email.apply()
