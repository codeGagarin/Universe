from typing import List, Dict

from pydantic import BaseModel, validator, root_validator

from keys import KeyChain

LAMBDA_PROXY_FIELDS = (
    'name_prefix',
    'role'
)


def _explicit(d: dict):
    """ Exclude keys start with _ """
    return {
        key: value
        for key, value
        in d.items()
        if not key.startswith('_')
    }


class Lambda(BaseModel):
    name: str
    role: str
    handler: str
    description: str = ''
    env: dict = None
    files: List[str] = None
    manual_data: Dict[str, str] = None
    """ Memory usage size. Example: 10Gb, 10mb, 1024KB """
    mem_usage: int = 128*1024*1024  # 128MB default
    runtime = 'python38'
    is_public = False
    execute_duration: int = 300  # ms
    req: List[str] = None
    secrets: List[str] = None
    name_prefix: str = None
    test: dict = None

    def get_full_name(self):
        return f'{self.name_prefix}-{self.name}' if self.name_prefix else self.name

    @validator('test', pre=False)
    def test_field_valid_check(cls, test_dict: dict):
        assert len(test_dict.keys()) == 1
        method, params_or_data = list(test_dict.items())[0]
        assert method in ('get', 'post')
        return test_dict

    @validator('mem_usage', pre=True)
    def size_to_bytes(cls, size: str):
        size_types = {'kb': 1024, 'mb': 1024 * 1024, 'gb': 1024 * 1024 * 1024}
        for t in size_types:
            if size.lower().find(t) != -1:
                return int(size[:-len(t)])*size_types[t]


class LambdasConfig(BaseModel):
    lambdas: List[Lambda]
    deploy_key: dict

    @root_validator(pre=True)
    def proxy_fields_update(cls, values):
        for proxy_field_name in LAMBDA_PROXY_FIELDS:
            proxy_field_value = values.get(proxy_field_name)
            if proxy_field_value:
                for lambda_data in values['lambdas'].values():
                    if not lambda_data.get(proxy_field_name):
                        lambda_data[proxy_field_name] = proxy_field_value
        return values

    @validator('deploy_key', pre=True)
    def deploy_key_validator(cls, value: str):
        return _explicit(
            getattr(KeyChain, value)
        )

    @validator('lambdas', pre=True)
    def lambdas_dict_to_list(cls, lambdas: dict):
        return [
            Lambda(name=k, **v)
            for k, v in lambdas.items()
        ]

    def by_name(self, lambda_name: str):
        return {
            lm.name: lm for lm in self.lambdas
        }[lambda_name]
