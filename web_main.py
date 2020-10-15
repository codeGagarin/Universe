from flask import Flask
from flask import request
from flask import render_template
from flask import url_for
import psycopg2
from psycopg2 import sql

from keys import KeyChain
from report import Report
from activities.reports import ReportActivity
from lib.tablesync import TableSyncActivity
from loader import Loader
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


@app.route('/tablesync/')
def tablesync():
    ldr = Loader(KeyChain)
    act = TableSyncActivity(ldr)
    act['index'] = request.args['idx']
    act.apply()
    return f'{str(act.due_date)}'


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
