from .params import ParamsBox, _Params


class Navigator:
    def __init__(self, box: ParamsBox):
        self.box = box
        self._data = {}

    def add_point(self, caption: str, params: _Params, kind=None):
        self._data[kind] = self.get(kind)
        self._data[kind][caption] = {'idx': self.box.params_to_idx(params)}

    def get(self, kind=None):
        return self._data.get(kind, {})
