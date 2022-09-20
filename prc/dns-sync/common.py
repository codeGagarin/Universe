LAMBDA_GROUP = 'sth_dns_sync'
TABLE_FOR_REPORT_STORING = f'{LAMBDA_GROUP}_reports'
ZONE_NAME = 'station-hotels.ru'

LAMBDA_TRACE = "d4e9frgkrgbeiis1hm4g"


def request_params(function_id: str, api_key: str):
    return {
        'url': f'https://functions.yandexcloud.net/{function_id}',
        'headers': {
            'Authorization': f'Api-Key {api_key}',
            'Content-Type': 'application/json'
        }
    }