""" Abstract classes for transfer session
        -- StorageSession class for data storage service (DBMS, etc...)
        -- TransferSession class for transfer services (FTP, HTTP, etc..)
"""

from dataclasses import dataclass
from typing import List


@dataclass
class FileBadge:
    name: str
    data_type: str


@dataclass
class _DataFilterItem:
    field: str
    value: str

    def __repr__(self):
        return '{}={}'.format(
            self.field,
            self.value
        )


class DataFilter(List[_DataFilterItem]):
    def add(self, field: str, value: str):
        self.append(_DataFilterItem(field, value))
        return self

    def __repr__(self):
        return '|'.join(map(repr, self))


class StorageSession:
    def __init__(self, _filter: DataFilter = None):
        self._filter = _filter or DataFilter()
        self._storage_rule = {}

    def attach_file(self, badge: FileBadge) -> int:  # -> file_id
        pass

    def submit_line(self, file_id: int, line_data: dict):
        pass

    def update_file_status(self, file_id, is_ok, fail_reason=None):
        pass

    # specify where to store data for types
    def add_store_rule(self, data_type: str, storage_place: str):
        self._storage_rule[data_type] = storage_place

    def filter(self) -> DataFilter:
        return self._filter


class TransferSession:
    def __init__(self):
        self._transfer_rule = {}

    def attach_file(self, data_type: str) -> int:  # -> file_id, None if home dir is empty
        pass

    def to_fail(self, file_id: int):
        pass

    def to_done(self, file_id: int):
        pass

    def local_path(self, file_id) -> str:
        pass

    def file_name(self, file_id) -> str:
        pass

    # specify where from transfer data for types
    def add_transfer_rule(self, data_type: str, transfer_path: str):
        self._transfer_rule[data_type] = transfer_path


from unittest import TestCase


class DataFilterTest(TestCase):
    def setUp(self):
        self.filter = DataFilter()
        self.filter.add('base1s', 'tvgunf').add('bla', 'bla_bla')

    def test_repr(self):
        print(self.filter)
