from flask import Flask
from flask import request
from flask import render_template
from flask import url_for
import psycopg2

from keys import KeyChain
from report import Report

app = Flask(__name__)


@app.route('/')
def index():
    return '.'


def _get_con():
    acc_key = KeyChain.PG_KEY
    return psycopg2.connect(dbname=acc_key["db_name"], user=acc_key["user"],
                            password=acc_key["pwd"], host=acc_key["host"])


@app.route('/report/')
def report():
    rpt = Report.factory(_get_con(), dict(request.args))
    return render_template(rpt.get_template(), rpt=rpt)


@app.route('/default/')
def params():
    result = "<h1>Defauts map</h1>"
    for name, params_dict in Report.default_map(_get_con()).items():
        url = url_for('report', **params_dict)
        result += f'<a a href="{url}">{name}</a>'
    return result


if __name__ == '__main__':
    app.run(debug=True)
