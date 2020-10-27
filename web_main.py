from flask import Flask, request, render_template, url_for, jsonify

from keys import KeyChain
from report import Report

from lib.tablesync import TableSyncActivity
from loader import Loader
import lib.telebots as bots

app = Flask(__name__)
path_root = 'bot_root'


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
    """ Table Sync Webhook """
    ldr = Loader(KeyChain)
    act = TableSyncActivity(ldr)
    act['index'] = request.args['idx']
    act.apply()
    return f'{str(act.due_date.strftime("%H:%M:%S"))}'


@app.route('/default/')
def defualt():
    conn = Report.get_connection(KeyChain.PG_KEY)
    result = "<h1>Defaults map</h1>"
    for name, params_dict in Report.default_map(conn).items():
        url = url_for('report', **params_dict)
        result += f'<a href="{url}">{name}</a><br>'
    conn['cn'].commit()
    return result


@app.route(f'/{path_root}/<token>', methods=['GET', 'POST'])
def bots_update(token):
    if request.method == 'POST':
        params = request.get_json()
        bots.update(token, params)
        return jsonify({'statusCode': 400})


if __name__ == '__main__':
    bots.init(path_root)
    app.run(debug=True)
