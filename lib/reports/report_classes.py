from typing import Union
from dataclasses import dataclass, asdict, is_dataclass
import pathlib
from jinja2 import Environment, FileSystemLoader

from lib.pg_utils import PGMix as _PGMix, sql

from .flask_common import FLASK_COMMON_PATH

from keys import KeyChain

# external dependency anchor
sql = sql


class Report:
    _DEFAULT_VIEW = 'view'

    ABSOLUTE_WEB_PATH = ''

    @classmethod
    def anchor_path(cls) -> str:
        assert False, f'Must override in class {cls.__name__}  [ @classmethod def file(cls): return __file__ ]'
        pass

    @dataclass
    class Params:
        pass

    @dataclass
    class Locals:
        pass

    def update_locals(self, _params, _locals) -> None:
        """ Override it bellow for updates report locals """
        pass

    def update_details(self, _params, _locals, _data) -> None:
        """ Override it bellow for prepare report details """
        pass

    def update_data(self, _params, _locals, _data) -> None:
        """ Override it bellow for prepare report data """
        pass

    def update_navigation(self, _params, _locals, _data) -> None:
        """ Override it bellow for prepare report navigation """

    def do_action(self, action, form, flash) -> int:  # return target_idx
        """ Override bellow for report action handling """
        return 0

    _presets = {}

    @classmethod
    def presets(cls) -> dict:
        return cls._presets

    @dataclass
    class Details:
        KEYS: list
        PARAMS: any  # Params
        CHANGER: callable

    def __init__(self, manager):
        self._manager = manager
        self._nav_data = {}
        self._data = {}  # report data
        self._params = None  # report params
        self._locals = self.__class__.Locals()
        self._details = {}  # report details
        self.idx = None  # this idx
        self._query_map = {}  # map of report query
        self._marks = {}  # mark details

    #  BEGIN FLASK CALL SECTION  #
    def get_data(self):
        """ Jinja call for report data """
        return self._data

    def get_navigation(self, kind=None):
        """ Jinja call for navigation data """
        return self._nav_data.get(kind, {})

    def get_params(self) -> Params:
        """ Jinja call for report params data """
        return self._params

    def get_locals(self):
        """ Jinja call for local params data """
        return self._locals

    def get_details(self, key, kind=None):
        return self._details.get(kind)[key]

    def get_marks(self, kind=None):
        """ Jinja call for marks details """
        return self._marks.get(kind, {})

    def debug_data(self):
        """ Jinja call for query list """
        return self._query_map

    def environment(self):
        return {
            'rpt': self,
            'DT': self.get_data,
            'NV': self.get_navigation,
            'PR': self.get_params,
            'LC': self.get_locals,
            'DL': self.get_details,
            'MR': self.get_marks,
            'DB': self.debug_data,
            'REPORT_URL': self.report_url_for,
        }

    #  END FLASK CALL SECTION  #

    def request_data(self, params: Params = None, idx=None):
        self.idx = idx
        self._params = params

        self.update_locals(self._params, self._locals,)
        self.update_data(self._params, self._locals, self._data)
        self.update_details(self._params, self._locals, self._data)
        self.update_navigation(self._params, self._locals, self._data)

        self._manager.flush()

    def add_detail(self, key, value, kind=None):
        self._details[kind] = self._details.get(kind, {})
        self._details[kind][key] = self.params_to_idx(value)

    def add_mark(self, key, value, kind=None):
        self._marks[kind] = self._marks.get(kind, {})
        self._marks[kind][key] = value

    def add_nav_point(self, caption: Union[str, tuple], params, kind=None):
        self._nav_data[kind] = self._nav_data.get(kind, {})
        self._nav_data[kind][caption] = self.params_to_idx(params)

    def ixd_to_params(self, idx: int):
        return self._manager.idx_to_params(idx)

    def params_to_idx(self, params: Params) -> int:
        return self._manager.params_to_idx(params)

    @classmethod
    def url_target(cls):
        return 'report_v2'

    def url_for_static(self, res_path):
        return '{}/static/{}'.format(
            self.ABSOLUTE_WEB_PATH,
            res_path
        )
        # ) if self.ABSOLUTE_WEB_PATH else 'static/{}'.format(
        #     res_path
        # )

    def report_url_for(self, idx: int) -> str:
        return '{}/{}/?idx={}'.format(
            self.ABSOLUTE_WEB_PATH,
            self.url_target(),
            idx
        )

    def action_url(self) -> str:
        return '{}/{}/'.format(
            self.ABSOLUTE_WEB_PATH,
            'action'
        )

    @classmethod
    def get_type(cls):
        return cls.__name__

    @classmethod
    def get_template(cls):
        file_name = '{}.html'.format(cls._DEFAULT_VIEW)
        home = pathlib.Path(cls.anchor_path()).parent

        env = Environment(loader=FileSystemLoader([home, FLASK_COMMON_PATH]), line_statement_prefix='%')
        return env.get_template(file_name)

    def _add_to_query_map(self, file_name, query):
        removed_empty_lines_query = '\n'.join(
            [s for s in query.split('\n') if s.strip()]
        )

        try_no = 0
        safe_name = file_name
        while self._query_map.get(safe_name):
            try_no += 1
            safe_name = f'{file_name} - [{try_no}]'

        self._query_map[safe_name] = removed_empty_lines_query

    def load_query(self, query_path: str, params):
        """ support query_path like ../../../some_template.sql """
        target_path = pathlib.Path(self.anchor_path()).parent.joinpath(query_path).resolve()
        current_folder = target_path.parent
        file_name = target_path.name
        env = Environment(loader=FileSystemLoader(str(current_folder)), line_statement_prefix='%')
        template = env.get_template(file_name)
        params = asdict(params) if is_dataclass(params) else params
        query = template.render(**params)

        self._add_to_query_map(file_name, query)
        return query

    def validate_email_list(self, email_list) -> list:
        """ Helper to convert user_id to personal email  """
        """ Override it bellow for customize """
        return email_list


class PGReport(Report, _PGMix):
    @classmethod
    def need_pg_key(cls):
        assert False, f'Should override in class {cls.__name__}' \
                      f'[ @classmethod def need_pg_key(cls): return KeyChain... ]'

    def __init__(self, manager):
        Report.__init__(self, manager)
        _PGMix.__init__(self, self.need_pg_key())


class ISReport(PGReport):
    @dataclass
    class MarkTypes:
        SERVICES = 'services'
        USERS = 'users'
        TASKS = 'tasks'

    @classmethod
    def need_pg_key(cls):
        return KeyChain.PG_IS_SYNC_KEY

    def query_mark(self, mark_type, filter_list):
        with self.cursor() as cursor:
            cursor.execute(
                self.load_query(
                    '../../mark_template.sql',
                    {
                        'MARK_TYPE': mark_type,
                        'FILTER_LIST': filter_list
                    }
                )
            )
            return cursor.fetchall()

    def validate_email_list(self, email_list_mixed_with_ids) -> list:
        """ Replace user_id to personal email """

        if not email_list_mixed_with_ids:
            return []

        ids_list = list(filter(lambda val: isinstance(val, int), email_list_mixed_with_ids))
        if not len(ids_list):
            return email_list_mixed_with_ids  # nothing to replace

        with self.cursor(named=False) as cursor:
            cursor.execute(
                sql.SQL('select "Id", "Email" from "Users" where "Id" in {}').format(
                    sql.Literal(tuple(ids_list))
                )
            )
            ids_email_dict = {key: value for key, value in cursor.fetchall()}  # {Id1: Email1, ...} format

        pure_lowercase_email_list = map(
            lambda val:
                ids_email_dict[val].lower()
                if isinstance(val, int) else
                val.lower(),
            email_list_mixed_with_ids
        )

        unique_pure_email_list = list()
        for email in pure_lowercase_email_list:
            if email not in unique_pure_email_list:
                unique_pure_email_list.append(email)

        return unique_pure_email_list
