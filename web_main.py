from flask import Flask, request, render_template, jsonify, redirect, flash, get_flashed_messages, g

from keys import KeyChain
from report import Report

from lib.reports.manager import Manager
from lib.reports.report_reg import report_list
from lib.reports.items.presets import PresetReport
from lib.clientbase.cost_transfer import CostTransfer
from lib.schedutils import NullStarter

import lib.telebots as bots

app = Flask(__name__)
app.secret_key = KeyChain.FLASK_SECRET_KEY
path_root = 'bot_root'


def get_report_manager():
    manager = getattr(g, '_manager', None)
    if manager is None:
        manager = g._manager = Manager(report_list)
    return manager


@app.route(KeyChain.SSL_CRT_VALIDATION['path'])
def ssl_crt_validation():
    return KeyChain.SSL_CRT_VALIDATION['key']


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/transfer/')
def transfer():
    ct = CostTransfer(NullStarter())
    ct['early_opened'] = True
    ct['period_delta'] = -2
    cp = ct.get_cost_pack()
    return f"""<html> {ct.cost_total_htm(cp)} </html>"""


@app.route('/transfer/csv')
def transfer_csv():
    ct = CostTransfer(NullStarter())
    ct['early_opened'] = True
    ct['period_delta'] = -2
    cp = ct.get_cost_pack()
    return cp.to_csv()


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
    rpt = get_report_manager().report_factory(request.args['idx'])
    return report_v2_render(rpt) if rpt else render_template('index.html')


def report_v2_render(rpt):
    env = rpt.environment()
    env['get_flashed_messages'] = get_flashed_messages
    return render_template(rpt.get_template(), **env)


@app.route('/action/', methods=['POST'])
def action():
    if request.method == 'POST':
        rpt = get_report_manager().report_factory(request.form['idx'])
        target_idx = rpt.do_action(request.form['action'], request.form, flash)
        return redirect(rpt.report_url_for(target_idx))


@app.route('/default/')
def default():
    def update_data(_params, _locals, _data):
        _data['NG'] = {_name: _idx for _name, _idx in get_report_manager().preset_map().items()}
        _data['OS'] = {_name: _params_dict for _name, _params_dict in Report.default_map(conn).items()}

    rpt = PresetReport(get_report_manager())
    rpt.update_data = update_data

    os_report = Report()

    def os_url_for(idx):
        return os_report.url_for('report', idx)

    rpt.OS_REPORT_URL = os_url_for

    conn = Report.get_connection(KeyChain.PG_KEY)  # Old school style
    rpt.request_data()
    conn['cn'].commit()  # Old school style

    return report_v2_render(rpt)


@app.route(f'/{path_root}/<token>', methods=['GET', 'POST'])
def bots_update(token):
    if request.method == 'POST':
        params = request.get_json()
        bots.update(token, params)
        return jsonify({'statusCode': 400})


Flask.jinja_options = {'extensions': ['jinja2.ext.autoescape', 'jinja2.ext.with_'], 'line_statement_prefix': '%'}


@app.context_processor
def inject_debug():
    return dict(debug=app.debug)


if __name__ == '__main__':
    bots.init(path_root)
    app.run(debug=KeyChain.FLASK_DEBUG_MODE)
