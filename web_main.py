from flask import Flask
from flask import request
from flask import render_template
from flask import url_for
import psycopg2
from psycopg2 import sql

from keys import KeyChain
from report import Report
from activities.reports import ReportActivity
app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/report/')
def report():
    conn = Report.get_connection(KeyChain.PG_KEY)
    index_page = None

    rpt = Report.factory(conn, dict(request.args))
    if not rpt:
        index_page = render_template('index.html')
    else:
        rpt.request_data(conn)
    conn['cn'].close()
    return index_page if index_page else render_template(rpt.get_template(), rpt=rpt)


@app.route('/reportm/')
def reportm():
    conn = Report.get_connection(KeyChain.PG_KEY)
    index_page = None

    rpt = Report.factory(conn, dict(request.args))
    if not rpt:
        index_page = render_template('index.html')
    else:
        rpt.request_data(conn)
    conn['cn'].close()
    #  rpt.web_server_name = "http://127.0.0.1:5000"  # delete this now! for transform testing
    return index_page if index_page else ReportActivity.get_report_html(rpt, KeyChain)


@app.route('/default/')
def params():
    conn = Report.get_connection(KeyChain.PG_KEY)
    result = "<h1>Defaults map</h1>"
    for name, params_dict in Report.default_map(conn).items():
        url = url_for('report', **params_dict)
        result += f'<a href="{url}">{name}</a><br>'
    conn['cn'].commit()
    return result


if __name__ == '__main__':
    app.run(debug=True)
