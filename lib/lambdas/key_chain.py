import json
import os


class _KeyChainMeta:
    @staticmethod
    def _get_env(name):
        return os.getenv(f'SECRET_{name}')

    def __getattr__(self, name):
        key_value = self._get_env(name)
        assert key_value, f'Key {name} does not exist'
        return json.loads(key_value)

    @classmethod
    def dump(cls, key_name: str):
        return json.dumps(getattr(cls, key_name))


KeyChain = _KeyChainMeta()
