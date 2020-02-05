from flask import Flask
from flask import request
from flask import render_template
from flask import url_for
import psycopg2
from psycopg2 import sql

from keys import KeyChain
from report import Report

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


def _get_conn():
    acc_key = KeyChain.PG_KEY
    cn = psycopg2.connect(dbname=acc_key["db_name"], user=acc_key["user"],
                          password=acc_key["pwd"], host=acc_key["host"])
    cn.autocommit = False
    return cn


@app.route('/report/')
def report():
    conn = _get_conn()
    index_page = None
    with conn:
        rpt = Report.factory(conn, dict(request.args)['idx'])
        if not rpt:
              index_page = render_template('index.html')
        else:
            rpt.request_data(conn)
        conn.commit()
    conn.close()
    return index_page if index_page else render_template(rpt.get_template(), rpt=rpt)



@app.route('/default/')
def params():
    conn = _get_conn()
    with conn:
        result = "<h1>Defaults map</h1>"
        for name, params_dict in Report.default_map(conn).items():
            url = url_for('report', **params_dict)
            result += f'<a href="{url}">{name}</a><br>'
        conn.commit()
    conn.close()
    return result


@app.route('/profile/')
def profile():
    import cProfile
    from report import HelpdeskReport
    from report import Report
    from web_main import _get_conn

    cn = _get_conn()
    rpt = Report.factory(cn, HelpdeskReport._get_idx(cn))
    cProfile.run("rpt._D()")


if __name__ == '__main__':
    app.run(debug=True)
