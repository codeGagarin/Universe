from datetime import datetime

from lib.pg_utils import PGMix, sql

from ._session import StorageSession, DataFilter, FileBadge


# generate where filter subquery: and [field1 = value1] and [field2 = value2] ...
def _gen_where_for(_filter: DataFilter):
    if not len(_filter):
        return sql.SQL('')  # safe subquery result
    else:
        return sql.Composed(
            [
                sql.SQL(' and  {} = {} ').format(
                    sql.Identifier(item.field), sql.Literal(item.value)
                ) for item in _filter
            ]
        )


# generate fields filter subquery: , field1, field2 ...
def _gen_fields_for(_filter: DataFilter):
    if not len(_filter):
        return sql.SQL('')  # safe subquery result
    else:
        return sql.Composed(
            [
                sql.SQL(', {}').format(
                    sql.Identifier(item.field)
                ) for item in _filter
            ]
        )


# generate values filter subquery: , value1, value2 ...
def _gen_values_for(_filter: DataFilter):
    if not len(_filter):
        return sql.SQL('')  # safe subquery result
    else:
        return sql.Composed(
            [
                sql.SQL(', {}').format(
                    sql.Literal(item.value)
                ) for item in _filter
            ]
        )


class PGSession(StorageSession, PGMix):
    _file_table = 'TJFiles'

    def __init__(self, key, _filter: DataFilter = None):
        StorageSession.__init__(self, _filter)
        PGMix.__init__(self, key)

        # file badges local cash
        self._badges = {}

        # batch submit helpers
        self._batch_params = []
        self._batch_hdr = []

        # Use for processing duration calculate:
        #   timestamp::<update file status> - timestamp::<attach file>
        self._attach_begin = None

    def _check_file_exist(self, badge: FileBadge):
        result = None  # in case file not exist

        check_query = sql.SQL('SELECT id FROM {} WHERE {}={} and {}={} {}').format(
            sql.Identifier(self._file_table),
            sql.Identifier('name'), sql.Literal(badge.name),
            sql.Identifier('type'), sql.Literal(badge.data_type),
            _gen_where_for(self._filter)
        )
        cursor = self._cursor(named=True)
        cursor.execute(check_query)
        rows = cursor.fetchall()

        if len(rows) != 0:
            result = rows[0].id
            self._badges[result] = badge
        return result

    def _id_to_badge(self, file_id):
        return self._badges[file_id]

    def _clear_file_data(self, file_id):
        delete_query = sql.SQL('DELETE FROM {} WHERE {}={}').format(
            sql.Identifier(self._storage_rule[self._id_to_badge(file_id).data_type]),
            sql.Identifier('file_id'), sql.Literal(file_id)
        )
        params = {
            'lines_count': 0,
            'duration': 0,
            'status': 'update',
            'fail_descr': None,
            'last_update': datetime.now()
        }
        update_query = sql.SQL('UPDATE {} SET ({}) = ({}) WHERE {}={}').format(
            sql.Identifier(self._file_table),
            sql.SQL(', ').join(sql.Identifier(key) for key in params.keys()),
            sql.SQL(', ').join(sql.Literal(value) for value in params.values()),
            sql.Identifier('id'),
            sql.Literal(file_id)
        )
        cursor = self._cursor()
        cursor.execute(delete_query)
        cursor.execute(update_query)
        self._commit()

    def _create_file(self, badge: FileBadge) -> int:
        # return new id for file badge
        params = {
            'lines_count': 0,
            'duration': 0,
            'status': 'new',
            'fail_descr': None,
            'last_update': datetime.now(),
            'name': badge.name,
            'type': badge.data_type,
        }

        insert_query = sql.SQL('INSERT INTO {}({} {}) VALUES ({} {}) RETURNING {}').format(
            sql.Identifier(self._file_table),
            sql.SQL(', ').join(map(sql.Identifier, params.keys())),
            _gen_fields_for(self._filter),
            sql.SQL(', ').join(map(sql.Literal, params.values())),
            _gen_values_for(self._filter),
            sql.Identifier('id'),
        )

        cursor = self._cursor(named=True)
        cursor.execute(insert_query)
        self._commit()
        file_id = cursor.fetchone().id
        self._badges[file_id] = badge  # time for local cache

        return file_id

    def attach_file(self, badge: FileBadge):
        self._attach_begin = datetime.now()  # duration time begin

        file_id = self._check_file_exist(badge)

        if file_id:
            # file already submitted, clear data for update
            self._clear_file_data(file_id)
        else:
            file_id = self._create_file(badge)
        self._batch_params.clear()

        return file_id

    def update_file_status(self, file_id: int, is_ok: bool, fail_reason: str = None):
        cursor = self._cursor(named=True)
        badge = self._id_to_badge(file_id)

        if len(self._batch_params):
            batch_query = sql.SQL('insert into {} ({}, file_id {}) values ({}, {} {})').format(
                sql.Identifier(self._storage_rule[badge.data_type]),
                sql.SQL(', ').join(map(sql.Identifier, self._batch_hdr)),
                _gen_fields_for(self._filter),
                sql.SQL(', ').join(sql.SQL('%s') for _ in enumerate(self._batch_hdr)),
                sql.Literal(file_id),
                _gen_values_for(self._filter),
            )

            self._extras.execute_batch(cursor, batch_query, self._batch_params)

        lines_count = len(self._batch_params)
        self._batch_params.clear()

        params = {
            'lines_count': lines_count,
            'duration': (datetime.now() - self._attach_begin).seconds,
            'status': 'done' if is_ok else 'fail',
            'fail_descr': fail_reason,
        }
        update_query = sql.SQL('UPDATE {} SET ({})=({}) WHERE {}={}').format(
            sql.Identifier(self._file_table),
            sql.SQL(', ').join(sql.Identifier(key) for key in params.keys()),
            sql.SQL(', ').join(sql.Literal(value) for value in params.values()),
            sql.Identifier('id'), sql.Literal(file_id),
        )

        cursor.execute(update_query)
        self._commit()

    def submit_line(self, file_id: int, line_data: dict):
        self._batch_params.append(list(line_data.values()))
        self._batch_hdr = list(line_data.keys())


from unittest import TestCase
from keys import KeyChain


class _PGSessionTest(TestCase):
    def setUp(self) -> None:
        self.badge = FileBadge('some_data_file.xml', 'apdx')
        self.session = PGSession(KeyChain.PG_PERF_KEY, DataFilter().add('base1s', 'test_filter'))
        self.session.add_store_rule('apdx', 'ApdexLines')

    def test_main_usage(self):
        _id = self.session.attach_file(self.badge)
        line = {
            'ops_uid': '100',
            'ops_name': 'Cool operation',
            'user': 'Donald',
        }
        for i in range(10):
            line['start'] = datetime.now()
            line['duration'] = i
            self.session.submit_line(_id, line)

        self.session.update_file_status(_id, True)
