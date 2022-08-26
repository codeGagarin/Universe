import logging
from logging import info, debug, warning
import sys
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
    'dif': [],
    'trace': None,
    'is_ok': False,
    'exec_time': 0
}


def configure_logging(level):
    root = logging.getLogger()
    root.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
    handler.setFormatter(formatter)

    root.addHandler(handler)


def dif(int_records, ext_records) -> list:
    """ Return difference between two records for update record_list1 later """
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

    return result


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


def main():
    info('Request external records.')
    ext_records = reg_api.get_external_a_records()

    info('Request internal records:')
    int_records = report['zone_before'] = win_api.get_internal_a_records()

    report['zone_ip'] = zone_ip(ext_records)

    report['dif'] = dif(int_records, ext_records)
    update_internal_a_records(
        report['dif']
    )

    report['zone_after'] = win_api.get_internal_a_records()
    report['is_ok'] = True


if __name__ == '__main__':
    begin = datetime.now()
    configure_logging(logging.INFO)
    try:
        main()
        info('Sync status: Ok')
    except:
        report['trace'] = traceback.format_exc()
        info('Sync status: Fail')
        traceback.print_exc()
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
