from unittest import TestCase, mock
from unittest.mock import patch, Mock
import yaml
import json

import requests

from deploy import deploy_lambda, log
from config import LambdasConfig, Lambda
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
        files: 
          - local_keys.py
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
        with patch.object(_KeyChain, 'get_env', lambda name: '{"who you": "Im a json-like"}'):
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


    def _test_s3_upload(self):
        import boto3
        from pathlib import Path
        from zipfile import ZipFile
        import hashlib

        bucket_id = 'Base-copies'
        # bucket_id = 'test-bucket-828729889298'

        def client_s3():
            # return session.Session().client(
            #     service_name='s3',
            #     **KeyChain.AWS_SELECTEL_S3
            # )
            return boto3.resource('s3', **KeyChain.AWS_SELECTEL_S3)

        def md5(path: str):
            hash_md5 = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        def make_archive_file(path) -> str:
            log(f'Make archive for path: {path}')
            path = path if isinstance(path, Path) else Path(path)
            zip_path, _ = mkstemp()

            with ZipFile(zip_path, mode='w') as zip_file:
                if path.is_dir():
                    dir_content = path.glob('**/*')
                    for sub_path in dir_content:
                        if sub_path.name == '.DS_Store':
                            continue
                        relative_path = sub_path.relative_to(path)
                        zip_file.write(
                            sub_path, relative_path
                        )
                        log(f'add file: {relative_path}')
                else:
                    zip_file.write(path)
                    log(f'add file: {path.name}')

                log(
                    'Zipped size: {}'.format(
                        pretty_file_size(
                            Path(zip_path).stat().st_size
                        )
                    )
                )
            return zip_path

        class Progress:
            def __init__(self, file_path):
                self._total_bytes = Path(file_path).stat().st_size
                self._submitted_bytes = 0

            def __call__(self, _bytes):
                self._submitted_bytes += _bytes
                log(
                    '{} of {}'.format(
                        pretty_file_size(self._submitted_bytes),
                        pretty_file_size(self._total_bytes),
                    )
                )

        def put(s3, prefix: str, local: str, base_type: str, company: str):
            """" submit local file to s3 storage """
            pack_path = make_archive_file(local)
            key = f'{prefix}/{Path(local).name}.zip'
            log(f'S3:Put {key}')
            metadata = {
                'Base-type': base_type,
                'Company': company
            }
            s3.Bucket(bucket_id).upload_file(
                pack_path,
                Key=key,
                ExtraArgs={
                    'Metadata': metadata
                },
                Callback=Progress(pack_path),
            )
            log('Done')
            assert s3.Object(bucket_id, key).e_tag[1:33] == md5(pack_path), 'Error: the checksum does not match!'

            # delete local pack file
            Path(pack_path).unlink()

        files = (
            {  # template
                'prefix': '',
                'local': '',
                'base_type': '',
                'company': ''
            },
            # {
            #     'prefix': 'DG_AUTO',
            #     'local': '/Users/igor/Downloads/Stancia/1C_DG_auto',
            #     'base_type': 'buh',
            #     'company': 'DG_AUTO'
            # },
            # {
            #     'prefix': 'Roschinskaya/zup',
            #     'local': '/Users/igor/Downloads/Stancia/1C_Roschinskaya_ZUP',
            #     'base_type': 'zup',
            #     'company': 'Roschinskaya'
            # },
            # {
            #     'prefix': 'tmp',
            #     'local': '/Users/igor/Desktop/спеки/',
            #     'base_type': 'buh',
            #     'company': 'DG_AUTO'
            # },
        )

        _s3 = client_s3()
        for f in files:
            if f['local']:  # skip template record
                put(_s3, **f)

    def test_pretty_file_size(self):
        print(pretty_file_size(1438671102))

    def test_service_account_name_to_id(self):
        print(service_account_name_to_id('ycf-deploy-acc'))

    def test_keys_injection(self):
        from local_keys import _KeyChainMeta as KeyChainMeta

        class TestKeyChain(KeyChainMeta):
            @staticmethod
            def _get_env(name):
                return json.dumps(
                    {'Key': 11, 'Secret': 'top secret information'}
                )

        key_chain = TestKeyChain()
        key = key_chain.Bla


