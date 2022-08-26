import json
import os


class _KeyChain:
    @staticmethod
    def _get_env(name):
        return os.getenv(f'SECRET_{name}')

    def __getattr__(self, name):
        key_value = self._get_env(name)
        assert key_value, f'Key {name} does not exist'
        return json.loads(key_value)

    def dump(self, key_name: str):
        return json.dumps(self.__getattr__(key_name))


KeyChain = _KeyChain()
