from enum import Enum
from typing import List, Dict

import pandas as pd
import yaml
from pydantic import BaseModel, validator


from cfg import CONFIG_COMMON_PATH
from lib.schedutils import Activity
from lib.period_utils import Period
from keys import KeyChain
from lib.pg_utils import PGMix, sql
from lib.mail import EmailActivity

CONFIG_PATH_NEW = 'cost_config.yaml'


class RateEnum(str, Enum):
    EK1 = 'EK1'
    EK2 = 'EK2'
    EK3 = 'EK3'
    PR3 = 'PR3'


class Agent(BaseModel):
    name: str
    intra_id: int
    default_rate_type: RateEnum


class Client(BaseModel):
    name: str
    intra_id: int
    sheet_prefix: str
    rates: Dict[RateEnum, int]
    report_group: str


class ReportColumn(BaseModel):
    query_field_id: str
    caption: str
    column_size: int


class Config(BaseModel):
    agents: List[Agent]
    clients: List[Client]
    rate_description: Dict[RateEnum, str]
    xls: List[ReportColumn]

    @validator('agents', pre=True)
    def agents_dict_to_list(cls, value):
        return [Agent(
            name=k, **v
        ) for k, v in value.items()]

    @validator('clients', pre=True)
    def clients_dict_to_list(cls, value):
        return [Client(
            name=k, **v
        ) for k, v in value.items()]

    @validator('xls', pre=True)
    def xls_dict_to_list(cls, value):
        return [ReportColumn(
            query_field_id=v[0], caption=k, column_size=v[1]
        ) for k, v in value.items()]


class CostTransfer(Activity, PGMix):
    PG_KEY = KeyChain.PG_COST_KEY

    def _fields(self) -> str:
        return 'period_delta early_opened'

    def __init__(self, ldr, params=None, config_root=None):
        """
        @param config_root: specify root for config
        """
        Activity.__init__(self, ldr, params)
        self.CONFIG_ROOT = config_root or CONFIG_COMMON_PATH
        with open(f'{CONFIG_COMMON_PATH}/{CONFIG_PATH_NEW}', 'r') as cfg_file:
            cfg_dict = yaml.safe_load(cfg_file)
            self.cfg = Config(**cfg_dict)

    def get_service_filter(self):
        with self.cursor() as cursor:
            raw_services = {}
            for client in self.cfg.clients:
                cursor.execute(
                    sql.SQL(
                        """ SELECT "Id" FROM "Services" WHERE "Description" LIKE {} """
                    ).format(
                        sql.Literal(f'%[{client.report_group}]%')
                    )
                )
                for rec in cursor:
                    raw_services[rec.Id] = client.intra_id

            unfold_services = {}
            for service_id, client_id in raw_services.items():
                cursor.execute(
                    sql.SQL(
                        """ SELECT "Id" FROM "Services" WHERE "ParentId"={} """
                    ).format(
                        sql.Literal(service_id)
                    )
                )
                child_services = cursor.fetchall()
                if len(child_services):
                    for rec in child_services:
                        unfold_services[rec.Id] = client_id
                else:
                    unfold_services[service_id] = client_id
        return unfold_services

    def get_actual_period(self) -> Period:
        return Period(period_type=Period.Type.MONTH, delta=self['period_delta'] or -1)

    def work_sheets(self) -> dict:
        return {
            client.name: f'{client.sheet_prefix}{self.get_actual_period().begin:%y%m}'
            for client in self.cfg.clients
        }

    def work_hours_xls(self, cost_pack):
        """ Make readable: rename, adapt and resize xls-columns """
        work_hours = pd.DataFrame()
        column_adapters = {
            'rate': lambda enum_rate: self.cfg.rate_description[enum_rate]
        }

        for column in self.cfg.xls:
            target_column = cost_pack[column.query_field_id]
            work_hours[column.caption] = target_column.apply(column_adapters[column.query_field_id]) \
                if column_adapters.get(column.query_field_id) \
                else target_column

        xls_sheet_name = 'Expenses'
        xls_io = io.BytesIO()
        writer = pd.ExcelWriter(xls_io, engine='xlsxwriter')
        work_hours.to_excel(writer, sheet_name=xls_sheet_name, index=False)

        # adjust column width
        xls_sheet = writer.sheets[xls_sheet_name]

        for column_id, size in enumerate(i.column_size for i in self.cfg.xls):
            xls_sheet.set_column(column_id, column_id, size)  # set column size

        writer.save()
        return xls_io

    def cost_total_htm(self, cost_pack: pd.DataFrame):
        total = cost_pack.append(  # add empty record for all client names
            pd.DataFrame.from_dict({
                'work sheet': self.work_sheets().values()
            })
        )
        total_clients = total.groupby('work sheet', as_index=False).agg(
            {'minutes': 'sum', 'task_id': 'count', 'value': 'sum'}
        ).rename(columns={'task_id': 'tasks count'})
        total_clients['work hours'] = total_clients['minutes'].apply(lambda m: round(m/60, 1))

        total_agents = total.groupby('agent_name', as_index=False).agg(
            {'minutes': 'sum', 'task_id': 'count'}
        ).rename(columns={'task_id': 'tasks count'})
        total_agents['work hours'] = total_agents['minutes'].apply(lambda m: round(m/60, 1))

        return f"""
            <div>{total_clients.drop(columns=['minutes']).to_html(border=2, index=False)}</div>
            <br>
            <div>{total_agents.drop(columns=['minutes']).to_html(border=2, index=False)}</div>
        """

    def get_cost_pack(self):
        p = self.get_actual_period()
        filter_services = self.get_service_filter()
        with self.cursor() as cursor:
            cost_query = sql.SQL(
                """
                    WITH
                         current_expenses AS (
                            SELECT e."TaskId" task_id, e."UserId" agent_id, e."DateExp" date_exp, e."Id" exp_id,
                            e."Minutes" minutes, t."ServiceId" service_id
                            FROM "Expenses" e LEFT JOIN "Tasks" t ON t."Id"=e."TaskId"
                            WHERE t."ServiceId" IN ({0})
                            AND e."UserId" IN ({1})
                            AND e."DateExp" BETWEEN {2} AND {3}
                         ),
                         prev_expenses AS (
                            SELECT e."TaskId" task_id, e."UserId" agent_id, e."DateExp" date_exp, e."Id" exp_id,
                            e."Minutes" minutes, t."ServiceId" service_id
                            FROM "Expenses" e LEFT JOIN "Tasks" t ON t."Id"=e."TaskId"
                            WHERE t."ServiceId" IN ({0})
                            AND t."Closed" IS NULL
                            AND e."UserId" IN ({1})
                            AND e."DateExp" < {2}
                            AND TRUE = {4}
                         ),
                         union_expenses AS (
                            SELECT * FROM current_expenses
                                UNION ALL
                            SELECT * FROM prev_expenses
                         ),
                         task_exp AS (
                            SELECT service_id, task_id, agent_id, sum(minutes) minutes, 
                               array_agg(right(to_char(date_part('day', date_exp)::integer, '00'), 
                               2)||'-'||minutes/60||':'||right(to_char(minutes%60, '00'), 
                               2) order by exp_id) exp_details
                            FROM union_expenses
                            GROUP BY service_id, task_id, agent_id
                        )
                    SELECT task_exp.*, t."Name" AS name, ex."Name" AS agent_name,
                        cr."Name" AS creator, cr."Id" AS creator_id, cr."CompanyName" AS client_name
                        FROM task_exp
                        LEFT JOIN "Tasks" t ON t."Id"=task_id
                        LEFT JOIN "Users" ex ON ex."Id"=agent_id
                        LEFT JOIN "Users" cr ON cr."Id"=t."CreatorId"
                """
            ).format(
                sql.SQL(', ').join(map(sql.Literal, filter_services)),
                sql.SQL(', ').join(map(sql.Literal, (agent.intra_id for agent in self.cfg.agents))),
                sql.Literal(p.begin), sql.Literal(p.end), sql.Literal(self['early_opened'] or False)
            )

            raw = pd.read_csv(
                self.to_csv(cost_query)
            )

        def to_hr_mm(minutes: int) -> str:
            """ Convert minutes sum to HH:MM format
            >>> to_hr_mm(122)
            '2:02' """
            return f'{minutes // 60}:{minutes % 60:02}'

        def adapter(value) -> str:
            """ Convert postgress array value to Python tuple
            >>> adapter('{1-2:20,2-1:13,3-0:45}')
            1-2:20, 2-1:13, 3-0:45 """
            return value.replace('{', '').replace('}', '').replace(',', ', ')

        raw['client_name'].fillna('', inplace=True)
        raw['client_id'] = raw['service_id'].apply(filter_services.get)
        raw['client'] = raw['client_id'].apply(
            {c.intra_id: c.name for c in self.cfg.clients}.get
        )
        raw['agent'] = raw['agent_id'].apply(
            {a.intra_id: a.name for a in self.cfg.agents}.get
        )
        raw['rate'] = raw['agent'].apply(
            {a.name: a.default_rate_type for a in self.cfg.agents}.get
        )

        raw['rate_value'] = raw.apply(
            lambda row: {c.name: c.rates for c in self.cfg.clients}[row.client][row.rate]
            # lambda r: self.cfg.get_rate_value(r['client'], r['rate'])
            , axis=1
        )
        raw['hours'] = raw['minutes'].apply(to_hr_mm)
        raw['value'] = raw.apply(lambda r: round(r['rate_value']*r['minutes']/60), axis=1)
        raw['exp_details'] = raw['exp_details'].apply(adapter)

        raw['work sheet'] = raw['client'].apply(
            lambda client: self.work_sheets()[client]
        )
        return raw

    @staticmethod
    def work_sheet_data(cost_pack, client):
        return cost_pack[(cost_pack['client'] == client)]

    def run(self):
        cost_pack = self.get_cost_pack()

        attachment = {}
        for client, sheet_name in self.work_sheets().items():
            client_cost_pack = self.work_sheet_data(cost_pack, client)
            if not client_cost_pack.empty:
                attachment[f'{sheet_name}.xlsx'] = self.work_hours_xls(client_cost_pack)

        e = EmailActivity(self._ldr)
        e['to'] = 'belov78@gmail.com'
        e['subject'] = 'Orbita report'
        e['smtp'] = 'P12'
        e['body'] = f"<html>{self.cost_total_htm(cost_pack)}</html>"
        e['attachment'] = attachment
        e.run()


from datetime import datetime as dt
from unittest import TestCase
from lib.schedutils import NullStarter as NS


def update_users(user_list):
    from lib.connectors.connector import ISConnector, PGConnector, User
    pg_con = PGConnector(KeyChain.PG_COST_KEY)
    is_con = ISConnector(KeyChain.IS_KEY)
    for user_id in user_list:
        cr = User(
            data={
                'Id': user_id
            }
        )
        is_con.select(cr)
        pg_con.update(cr)
        print(cr)


class TestCostTransfer(TestCase):
    def setUp(self) -> None:
        self.t = CostTransfer(NS())
        pass

    def test_get_period(self):
        print(self.t.get_actual_period())

    def test_get_service_filter(self):
        print(self.t.get_service_filter())

    def test_srv_upd(self):
        # from lib.intraservice.service_updater import ISServiceUpdater
        # u = ISServiceUpdater(NS())
        # u.run()
        pass

    def test_cost_upd(self):
        from lib.intraservice.sync_lib import ISSync
        ISSync(NS(), {
            'from': dt(2021, 2, 1),
            'to': dt(2021, 2, 1),
        }).run()
        pass

    def test_user_update(self):
        cp = self.t.get_cost_pack()
        task_list = cp[(cp['client_name'].isnull())]['creator_id'].unique()
        update_users(task_list)

    def test_parquet(self):
        cp = self.t.get_cost_pack()
        cp.to_parquet(path='~/downloads/cost_pack.pq', engine='fastparquet')

    def test_get_cost_pack(self):
        t = self.t

        cp = t.get_cost_pack()
        # cp.to_parquet('~/downloads/pq', engine='fastparquet')
        sheet = cp[cp.client_id == 8]
        xls = t.work_hours_xls(sheet)

        e = EmailActivity(NS())
        e['to'] = 'belov78@gmail.com'
        e['subject'] = 'Worksheet'
        e['smtp'] = 'P12'
        e['attachment'] = {'sheet.xlsx': xls}
        e['body'] = sheet.style.to_html(doctype_html=True)
        e.run()

    def test_send_report(self):
        self.t['period_delta'] = -2
        self.t['early_opened'] = False
        self.t.run()












