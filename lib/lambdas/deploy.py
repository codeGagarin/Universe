import yaml
from unittest import TestCase

import requests

from .config import Lambda, LambdasConfig
from .sdk import Clients
from .ops import get_function_id, create_function, update_function, set_function_access, create_version
from keys import KeyChain


def log(msg: str):
    print(msg)


def load_config(config_path: str) -> LambdasConfig:
    with open(config_path, mode='rt') as f:
        return LambdasConfig(
            **yaml.safe_load(f)
        )


def deploy_lambda(lambda_params: Lambda):
    fs_client = Clients.function_service()
    lambda_name = lambda_params.get_full_name()

    log(f'Begin deploying lambda: {lambda_name}')
    function_id = get_function_id(lambda_name, fs_client)

    if not function_id:
        log(f'Lambda not found: ctrate new one: {lambda_name}')
        function_id = create_function(lambda_params, fs_client)
    else:
        log(f'Update lambda data: {lambda_name}')
        update_function(function_id, lambda_params, fs_client)
    log(f'Done. id: {function_id}')

    set_function_access(function_id, lambda_params.is_public)

    log(f'Create new version with entry_point: {lambda_params.handler}')
    version_id = create_version(function_id, lambda_params, fs_client)
    log(f'Done. Version id: {version_id}')

    if lambda_params.test:
        log(f'Begin testing. Params: {lambda_params.test}')
        response = do_lambda_test(function_id, lambda_params.test)
        log(f'Test - Ok. Response: {response.text}')

    return function_id


def check_lambda(lambda_params: Lambda, check_params=None):
    lambda_name = lambda_params.get_full_name()
    function_id = get_function_id(
        lambda_name, Clients.function_service())

    assert function_id is not None, \
        f'Function with name not found: {lambda_name}'

    return do_lambda_test(
        function_id, lambda_params.test or check_params or {
             'get': {}  # default empty test params
         }
    )


def deploy_config(cfg: LambdasConfig):
    for _lambda in cfg.lambdas:
        deploy_lambda(_lambda)


def do_lambda_test(function_id: str, test_params: dict):
    common_section = {
        'url': f'https://functions.yandexcloud.net/{function_id}',
        'headers': {'Authorization': f'Api-Key {KeyChain.YC_DEPLOY_API_KEY[1]}'},
    }

    method, params_or_data = list(
        test_params.items()
    )[0]

    switch_method = {
        'get': {
            'params': params_or_data
        },
        'put': {
            'data': params_or_data
        }
    }

    response = requests.request(
        **common_section,
        method=method,
        **switch_method[method]
    )

    assert response.status_code == 200, response.text
    return response


class _Loader(TestCase):
    cfg: LambdasConfig = None
    cfg_path: str = 'lambda.yml'  # default config path

    def load_config(self):
        assert self.cfg_path is not None, 'self.cfg_path is not defined'
        return load_config(self.cfg_path)

    def setUp(self):
        self.cfg = self.load_config()


class Deployer(_Loader):
    def deploy(self, lambda_name: str):
        deploy_lambda(
            self.cfg.by_name(lambda_name)
        )


class Tester(_Loader):
    def extern_check(self, lambda_name: str):
        return check_lambda(
            self.cfg.by_name(lambda_name)
        )