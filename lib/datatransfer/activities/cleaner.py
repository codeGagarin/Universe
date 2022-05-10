from typing import List
import yaml

from pydantic import BaseModel, validator

from lib.schedutils import Activity
from cfg import CONFIG_COMMON_PATH
from lib.pg_utils import PGMix, sql, DataBaseKey
from keys import KeyChain

cfg_path = f'{CONFIG_COMMON_PATH}/cleaner.yaml'


class CleanJob(BaseModel):
    table_name: str
    stamp_field: str
    db_key: DataBaseKey
    storage_depth: str

    @validator('db_key', pre=True)
    def convert_db_key_name_to_dict(cls, value):
        return getattr(KeyChain, value)

    @validator('storage_depth', pre=True)
    def convert_storage_depth_to_interval(cls, value):
        period_type = value[-1:]
        period_count = value[:-1]
        try:
            return '{} {}'.format(
                int(period_count),
                {'d': 'days', 'm': 'month', 'y': 'year'}[period_type]
            )
        except (ValueError, KeyError):
            raise ValueError(f'Wrong field: storage_depth={value}')


class Config(BaseModel):
    jobs: List[CleanJob]


def load_cfg(file_path):
    """ Return config dict """
    with open(file_path, mode='rt') as file:
        return yaml.safe_load(file)


class Cleaner(Activity):
    config: Config = None

    @classmethod
    def get_crontab(cls):
        return '0 2 * * *'

    @staticmethod
    def do(job: CleanJob):
        mix = PGMix(job.db_key)
        with mix.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    WITH 
                        deleted as (
                            DELETE FROM {} WHERE {} < now()-{}::interval RETURNING *
                        )
                    SELECT COUNT(*) FROM deleted  
                    """
                ).format(
                    sql.Identifier(job.table_name),
                    sql.Identifier(job.stamp_field),
                    sql.Literal(job.storage_depth)
                )
            )
            scope = cursor.fetchone()[0]
            print(f'{job.table_name}:{scope} ')
        mix.commit()

    def run(self):
        if not self.config:
            self.config = Config(
                **load_cfg(file_path=cfg_path)
            )
        for job in self.config.jobs:
            self.do(job)


from unittest import TestCase


class TestConfig(TestCase):
    def test_validate(self):
        print(load_cfg(cfg_path))
        Config(**load_cfg(cfg_path))


class TestCleaner(TestCase):
    def test_run(self):
        Cleaner().run()

    def test_run_adv(self):
        from datetime import datetime, timedelta

        storage_depth = 10  # days

        job = CleanJob(
            db_key='PG_PERF_KEY',
            table_name='TestTable',
            stamp_field='stamp',
            storage_depth=f'{storage_depth}d'
        )

        create_query = sql.SQL(
            """ create table {} (stamp timestamp, data  varchar) """
        ).format(
            sql.Identifier(job.table_name)
        )

        dump_query = sql.SQL(
            """ select * from {} """
        ).format(
            sql.Identifier(job.table_name)
        )

        insert_query = sql.SQL(
            """ insert into {} (stamp, data) values (%s::date, %s)"""
        ).format(
            sql.Identifier(job.table_name)
        )

        drop_query = sql.SQL(
            """ drop table if exists {} """
        ).format(
            sql.Identifier(job.table_name)
        )

        insert_test_data_bar = [
            (f'{datetime.now()-timedelta(days=n):%Y-%m-%d}', str(n))
            for n in range(storage_depth)
        ]

        insert_test_data_foo = [
            (f'{datetime.now()-timedelta(days=n):%Y-%m-%d}', str(n))
            for n in range(storage_depth, storage_depth*2)
        ]

        mix = PGMix(job.db_key)
        with mix.cursor() as cursor:
            cursor.execute(drop_query)
            cursor.execute(create_query)
            cursor.executemany(insert_query, insert_test_data_bar)
            result_before = mix.to_csv(dump_query).read()
            cursor.executemany(insert_query, insert_test_data_foo)
            cleaner = Cleaner()
            cleaner.config = Config(jobs=[job])
            mix.commit()
            cleaner.run()
            result_after = mix.to_csv(dump_query).read()
            cursor.execute(drop_query)

        self.assertEqual(result_before, result_after)



