from unittest import TestCase, mock
from unittest.mock import patch, Mock
import yaml
import json
from tempfile import mkstemp

import requests

from deploy import deploy_lambda, log
from config import LambdasConfig, Lambda
from ops import service_account_name_to_id, pretty_file_size
from keys import KeyChain


config_for_tests = """
    deploy_key: YC_DEPLOY_ACC
    role: ycf-deploy-acc
    name_prefix: test
    triggers:
        tick:
            rule: timer
            cron: 0.0.12
            invoke_retry_fun: 
    lambdas:        
      deploy-flow:
        description: Uses for testing deploy flow 
        service_account_key: AWS_FAILS_KEY
        secrets: 
          - TEST_KEY_FOR_INJECT
        handler: common.endpoint_for_test
        env:
          TEST_ENV: test_env_value
        test:
          get:
            param1: 1
            param2: bla-bla
        req:
          - psycopg2
        manual_data:
          common.py: |
            import os
            import psycopg2
            from keys import KeyChain
            def endpoint_for_test(event, context):
                return {
                    'statusCode': 200,
                    'body': {
                        'env': os.getenv('TEST_ENV'),
                        'data': event['queryStringParameters'],
                        'secret': KeyChain.TEST_KEY_FOR_INJECT,
                        'req': psycopg2.__name__                  
                    }
                }          

"""


class ConfigTest(TestCase):

    def test_config_validation(self):
        LambdasConfig(
            **yaml.safe_load(config_for_tests)
        )

    def test_proxy_field(self):
        raw_data = yaml.safe_load(config_for_tests)
        cfg = LambdasConfig(
            **raw_data
        )
        self.assertEqual(raw_data['name_prefix'], cfg.lambdas[0].name_prefix)

    def test_size_to_bytes(self):
        raw = (
            ('256Kb', 256*1024),
            ('236gb', 236*1024*1024*1024),
            ('256Gb', 256*1024*1024*1024),
            ('223mb', 223*1024*1024),
            ('26gB', 26*1024*1024*1024),
        )

        for size, _bytes in raw:
            self.assertEqual(
                _bytes, Lambda.size_to_bytes(size))


class SecretsTest(TestCase):
    def test_get_key(self):
        from key_chain import KeyChain as _KeyChain
        with patch.object(_KeyChain, '_get_env', lambda name: '{"who you": "Im a json-like"}'):
            self.assertEqual(_KeyChain.Anything['who you'], 'Im a json-like')


class OpsTest(TestCase):
    def test_check_public_access(self):
        import sdk
        # class FakeClients(sdk.Clients)
        m = Mock()
        try:
            with patch.object(sdk, 'Clients', m) as mocked:
                from ops import check_public_access
                check_public_access('function_id')
        except Exception:
            ...
        ...


class TestProcessor(TestCase):

    def test_deploy(self):
        function_id = deploy_lambda(
            LambdasConfig(
                **yaml.safe_load(config_for_tests)
            ).lambdas[0]
        )
        test_data = {'param': 'value'}
        response = requests.get(
            url=f'https://functions.yandexcloud.net/{function_id}',
            headers={'Authorization': f'Api-Key {KeyChain.YC_DEPLOY_API_KEY[1]}'},
            params=test_data
        )
        self.assertTrue(
            response.ok, f'[{response.status_code}] Bad response: {response.text}')
        function_result = json.loads(response.content)
        self.assertEqual(function_result['env'], 'test_env_value')
        log('Test env: Ok')
        self.assertEqual(function_result['req'], 'psycopg2')
        log('Test req: Ok')
        self.assertEqual(function_result['data'], test_data)
        log('Test parameter: Ok')
        self.assertEqual(function_result['secret'], KeyChain.TEST_KEY_FOR_INJECT)
        log('Test secret: Ok')



    def test_pretty_file_size(self):
        print(pretty_file_size(1438671102))

    def test_service_account_name_to_id(self):
        print(service_account_name_to_id('ycf-deploy-acc'))

    def test_keys_injection(self):
        from key_chain import _KeyChainMeta as KeyChainMeta

        class TestKeyChain(KeyChainMeta):
            @staticmethod
            def _get_env(name):
                return json.dumps(
                    {'Key': 11, 'Secret': 'top secret information'}
                )

        key_chain = TestKeyChain()
        key = key_chain.Bla


