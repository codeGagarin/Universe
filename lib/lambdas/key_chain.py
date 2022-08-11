import json
import os


class _KeyChainMeta:
    @staticmethod
    def get_env(name):
        return os.getenv(f'SECRET_{name}')

    def __getattr__(self, name):
        key_value = self.get_env(name)
        assert key_value, f'Key {name} does not exist'
        return json.loads(key_value)

    @staticmethod
    def dump(key: dict):
        return json.dumps(key)


KeyChain = _KeyChainMeta()
