import io
from contextlib import redirect_stdout

import logging
import traceback
from datetime import datetime
import json

import requests

import reg_api
import win_api
import common
from keys import KeyChain

trace_function_id = 'd4ep1mofq0uq39tvjc76'

report = {  # to be sent on report base
    'zone_ip': None,
    'zone_before': None,
    'zone_after': None,
    'diff': [],
    'trace': None,
    'is_ok': False,
    'exec_time': 0
}


def _line(_type, msg):
    print(
        datetime.now().strftime(
            f'%M:%S {_type}: {msg}'
        )
    )


def info(msg):
    _line('INFO', msg)


def debug(msg):
    _line('DEBUG', msg)


def error(msg):
    _line('ERROR', msg)


def diff(int_records, ext_records, sorter=None) -> list:
    """ Return difference between two records for update record_list1 later
        list((host, old_ip, new_ip), ...)
    """
    exclude_hosts = ('@', 'DomainDnsZones', 'ForestDnsZones')

    ext_index = {host: ip for host, ip in ext_records}
    int_index = {host: ip for host, ip in int_records}

    result = []
    for host, old_ip in int_records:
        if host in exclude_hosts:
            continue

        new_ip = ext_index.get(host)
        if not new_ip or new_ip == old_ip:
            continue

        result.append(
            (host, old_ip, new_ip)
        )

    for host, new_ip in ext_records:
        if host in exclude_hosts:
            continue

        old_ip = int_index.get(host)
        if new_ip == old_ip:
            continue

        result.append(
            (host, old_ip, new_ip)
        )

    sorter = sorter or (lambda v: v)
    return sorter(result)


def update_internal_a_records(updates):
    win_api.add_internal_a_record = lambda _host, ip: info(f'Fake add host:{host} ip:{ip}')
    win_api.remove_internal_a_record = lambda _host, ip: info(f'Fake remove host:{host} ip:{ip}')

    for host, old_ip, new_ip in updates:
        if old_ip:
            win_api.remove_internal_a_record(host, old_ip)
        win_api.add_internal_a_record(host, new_ip)
        info(f'Update record for host:{host}  old_ip:{old_ip} new_ip:{new_ip}')
    info(f'Update {len(list(updates))} record(s)')


def zone_ip(ext_records):
    return {
        host: ip for host, ip in ext_records
    }['@']


def _host_sorter(v):
    return sorted(v, key=lambda i: i[0].lower())


def main():
    info('Request external records.')
    ext_records = reg_api.get_external_a_records(sorter=_host_sorter)

    info('Request internal records:')
    int_records = report['zone_before'] = win_api.get_internal_a_records(sorter=_host_sorter)

    report['zone_ip'] = zone_ip(ext_records)

    report['diff'] = diff(int_records, ext_records, sorter=_host_sorter)
    update_internal_a_records(
        report['diff']
    )

    report['zone_after'] = win_api.get_internal_a_records(sorter=_host_sorter)
    report['is_ok'] = True


if __name__ == '__main__':
    log_stream = io.StringIO()
    with redirect_stdout(log_stream):
        begin = datetime.now()
        try:
            main()
            info('Sync status: Ok')
        except:
            error(f'Sync status: Fail\n{traceback.format_exc()}')
    print(log_stream.getvalue())

    report['trace'] = log_stream.getvalue()
    report['exec_time'] = (datetime.now() - begin).seconds

    response = requests.post(
        **common.request_params(
            common.LAMBDA_TRACE,
            KeyChain.YCF_STH_DNS_SYNC,
        ),
        data=json.dumps(report)
    )

    assert response.status_code == 200, response.text
    info('Report has been sent')

