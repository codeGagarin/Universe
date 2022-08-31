from logging import info

import winrm

import common
from keys import KeyChain

__pwsh_session__ = None


def _session():
    global __pwsh_session__

    if not __pwsh_session__:
        __pwsh_session__ = winrm.Session(
            *KeyChain.WIN_STH_DOMAIN, transport='ntlm'
        )
    return __pwsh_session__


def _api_execute(pwsh_command: str):
    response = _session().run_ps(
        pwsh_command
    )

    assert response.status_code == 0, response.std_err.decode('utf-8')

    return response.std_out.decode('utf-8')


def get_internal_a_records(sorter=None):
    raw_out = _api_execute(
        'Get-DnsServerResourceRecord -ZoneName "station-hotels.ru" -RRType "A"'
        ' | Select HostName, @{n="IP";E={$_.RecordData.IPV4Address}}'
    )

    """ Convert raw out result to list(pair(host_name, ip)) format"""
    result = list(
        line.split() for line in
        raw_out.split('\r\n')
        if line != ''
    )[2:]  # skip two header lines

    sorter = sorter or (lambda v: v)
    return sorter(result)


def add_internal_a_record(host, ip):
    raw_out = _api_execute(
        f'Add-DnsServerResourceRecordA -Name "{host}" -ZoneName "{common.ZONE_NAME}" '
        f'-AllowUpdateAny -IPv4Address "{ip}" -TimeToLive 01:00:00'
    )
    info(raw_out)


def remove_internal_a_record(host, ip):
    raw_out = _api_execute(
        f'Remove-DnsServerResourceRecord -ZoneName "{common.ZONE_NAME}" -RRType "A" '
        f'-Name "{host}" -RecordData "{ip}" -Force '
    )
    info(raw_out)

