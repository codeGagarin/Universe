from flask import Flask, request, render_template, url_for, jsonify, redirect, flash

from keys import KeyChain
from report import Report

from lib.reports import manager

from lib.tablesync import TableSyncActivity
import lib.telebots as bots

app = Flask(__name__)
app.secret_key = KeyChain.FLASK_SECRET_KEY
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


@app.route('/report_v2/')
def report_v2():
    rpt = manager.report_factory(request.args['idx'])

    if not rpt:
        return render_template('index.html')

    return render_template(rpt.get_template(), rpt=rpt)


@app.route('/action/', methods=['POST'])
def action():
    if request.method == 'POST':
        rpt = manager.report_factory(request.form['idx'])
        target_idx = rpt.do_action(request.form['action'], request.form, flash)
        return redirect(rpt.report_url_for(target_idx))


@app.route('/tablesync/')
def tablesync():
    # """ Table Sync Webhook """
    # ldr = Loader(KeyChain)
    # act = TableSyncActivity(ldr)
    # act['index'] = request.args['idx']
    # act.apply()
    # return f'{str(act.due_date.strftime("%H:%M:%S"))}'
    pass


@app.route('/default/')
def default():
    result = "<h1>NG Defaults map</h1>"
    for name, idx in manager.reports_map().items():
        url = url_for('report_v2', **{'idx': idx})
        result += f'<a href="{url}">{name}</a><br>'

    conn = Report.get_connection(KeyChain.PG_KEY)
    result += "<h1>Defaults map</h1>"
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
