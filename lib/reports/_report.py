from html import escape
from typing import Union

from .params import ParamsBox, _Params
from lib.pg_utils import PGMix as _PGMix, sql
from keys import KeyChain

# external dependency anchor
sql = sql
_Params = _Params


class Report:
    def __init__(self, params_box: ParamsBox):
        self.web_server_direct_url = ''
        self._params_box = params_box
        self._nav_data = {}
        self._data = None  # report data
        self._params = None  # report params
        self.idx = None  # report params idx
        self.set_up()

    def set_up(self):
        pass

    def _D(self):
        """ Jinja call for get_data() method """
        return self._data

    def _N(self, kind=None):
        return self._nav_data.get(kind, {})

    def _P(self):
        return self._params

    def do_action(self, action, params, flash) -> int:  # return target_idx
        return 0

    @classmethod
    def default_params(cls) -> _Params:
        return _Params({'type': cls.get_type()})

    def request_data(self, params: _Params, idx: int):
        self.idx = idx
        self._params = params
        self._data = self._prepare_data(params)
        self._params_box.flush()

    def _prepare_data(self, params: _Params) -> dict:
        """
            return report data
        """
        return {}

    def add_nav_point(self, caption: Union[str, tuple], params: _Params, kind=None):
        self._nav_data[kind] = self._nav_data.get(kind, {})
        self._nav_data[kind][caption] = self.params_to_idx(params)

    def ixd_to_params(self, idx: int) -> _Params:
        return self._params_box.idx_to_params(idx)

    def params_to_idx(self, params: dict, report_class=None) -> int:
        report_class = report_class or self.__class__
        params['type'] = report_class.get_type()
        return self._params_box.params_to_idx(_Params(params))

    @classmethod
    def url_target(cls):
        return 'report_v2'

    def url_for_static(self, res_path):
        return '{}/static/{}'.format(
            self.web_server_direct_url,
            res_path
        )

    def report_url_for(self, idx: int) -> str:
        return '{}/{}/?idx={}'.format(
            self.web_server_direct_url,
            self.url_target(),
            idx
        )

    def action_url(self) -> str:
        return '{}/{}/'.format(
            self.web_server_direct_url,
            'action'
        )

    # todo: delete after migration: deprecated, use report_url_for
    def url_for(self, target, params: _Params = None):
        if not params:
            params = {}
        # Version 2 reporting compatibility
        target = 'report_v2' if target == 'report' else target
        return \
            '{}/{}/?{}'.format(
                self.web_server_direct_url, target, '&'.join(
                    '{}={}'.format(
                        escape(str(key)), escape(str(value))
                    ) for key, value in params.items()
                )
            )

    @classmethod
    def get_type(cls):
        return cls.__name__

    @classmethod
    def get_template(cls):
        return '{}.html'.format(cls.get_type())


class PGReport(Report, _PGMix):
    def __init__(self, box: ParamsBox):
        Report.__init__(self, box)
        _PGMix.__init__(self, KeyChain.PG_REPORT_KEY)
