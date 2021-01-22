from pathlib import Path
from io import StringIO
import logging

import jinja2
import premailer


def report_to_html(report, web_path):
    proj_root = str(Path(__file__).parent.parent)
    templates_root = proj_root + "/templates"
    template_ldr = jinja2.FileSystemLoader(searchpath=templates_root)
    template_env = jinja2.Environment(loader=template_ldr)
    template_name = report.get_template()
    template = template_env.get_template(template_name)
    report.web_server_name = web_path
    html_text = template.render(rpt=report, eml=True)
    css_path = f'{proj_root}/static/css/bootstrap.css'
    f = open(css_path)
    css_text = f.read()
    f.close()
    premailer_log = StringIO()
    premailer_log_handler = logging.StreamHandler(premailer_log)
    return premailer.Premailer(
        cssutils_logging_handler=premailer_log_handler,
        cssutils_logging_level=logging.CRITICAL,
        remove_classes=True,
        css_text=css_text
    ).transform(html_text)
