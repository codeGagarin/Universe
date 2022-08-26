import json

import requests

from keys import KeyChain

RG_KEY = KeyChain.RG_KEY_STATION


def adapt_str(**kvargs):
    return '&'.join(
        f'{k}={v}' for k, v in kvargs.items()
    )


def adapt_json(**kvargs):
    return json.dumps(kvargs)


def _ext_ip():
    """ returned my global IPv4-address """

    import requests
    response = requests.get(
        'https://ifconfig.me/ip'
    )
    assert response.status_code == 200, response.text

    return response.text


def api_request(cmd: str, **params) -> dict or str:
    end_point = 'https://api.reg.ru/api/regru2'

    auth = {
        k: v for k, v in RG_KEY.items()
        if not k.startswith('_')
    }

    response = requests.put(
        f'{end_point}/{cmd}?input_data={adapt_json(**auth, **params or {})}&input_format=json'
    )

    response_body = json.loads(
        response.text
    )
    assert response_body['result'] == 'success', f'Global IP: {_ext_ip()} Response message: {response.text}'

    return response_body


def _extract_subdomains(raw_response: dict, RRT: str = 'A'):
    result = []
    for domain in raw_response['answer']['domains']:
        for record in domain['rrs']:
            if record['rectype'] == RRT:
                result.append(
                    (record['subname'], record['content'])
                )
    return result


def get_external_a_records():
    target_domain = RG_KEY['_ext_domain']
    response: dict = api_request(
        cmd='zone/get_resource_records',
        domains=[
            {
                'dname': target_domain
            }
        ]
    )

    return _extract_subdomains(response, 'A')
