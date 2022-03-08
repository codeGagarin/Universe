import io

import pandas as pd
import yaml

from lib.schedutils import Activity
from lib.period_utils import Period
from keys import KeyChain
from lib.pg_utils import PGMix, sql
from lib.mail import EmailActivity

CONFIG_PATH = 'cost_config.yaml'


class _Config:
    def __init__(self, cfg_path):
        with open(cfg_path, 'r') as cfg_file:
            cfg = yaml.safe_load(cfg_file)
        self._clients = cfg['clients']
        self._groups = cfg['groups']
        self._agents = cfg['agents']
        self._xls = cfg['xls']

    def groups_clients(self):
        return {
            key: self._clients[value]['IS']
            for key, value
            in self._groups.items()
        }

    def ids_clients(self):
        return {
            self._clients[key]['IS']: key
            for key, value
            in self._clients.items()
        }

    def clients_params(self):
        return self._clients

    def ids_agents(self):
        return {
            params['IS']: agent
            for agent, params
            in self._agents.items()
        }

    def agents_list(self):
        return list(
            self.ids_agents().keys()
        )

    def agents_rates(self):
        return {
            agent: params['rate']
            for agent, params
            in self._agents.items()
        }

    def get_rate_value(self, client, rate):
        return self._clients[client]['rates'][rate]

    def aliases_columns(self):
        return {alias: params[0] for alias, params in self._xls.items()}.items()

    def aliases_ids_sizes(self):
        return enumerate((params[1] for params in self._xls.values()))


class CostTransfer(Activity, PGMix):
    PG_KEY = KeyChain.PG_COST_KEY

    def _fields(self) -> str:
        return 'period_delta early_opened'

    def __init__(self, ldr, params=None, config_root=None):
        """
        @param config_root: specify root for config
        """
        self.CONFIG_ROOT = config_root or ''

        Activity.__init__(self, ldr, params)
        self.cfg = _Config(f'{self.CONFIG_ROOT}{CONFIG_PATH}')

    def get_service_filter(self):
        with self.cursor() as cursor:
            raw_services = {}
            for group_name, client_id in self.cfg.groups_clients().items():
                cursor.execute(
                    sql.SQL(
                        """ SELECT "Id" FROM "Services" WHERE "Description" LIKE {} """
                    ).format(
                        sql.Literal(f'%[{group_name}]%')
                    )
                )
                for rec in cursor:
                    raw_services[rec.Id] = client_id

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
            client: f'{params["sheet_prefix"]}{self.get_actual_period().begin:%y%m}'
            for client, params in self.cfg.clients_params().items()
        }

    def work_hours_xls(self, cost_pack):
        work_hours = pd.DataFrame()
        for alias, column in self.cfg.aliases_columns():
            work_hours[alias] = cost_pack[column]

        xls_sheet_name = 'Expenses'
        xls_io = io.BytesIO()
        writer = pd.ExcelWriter(xls_io, engine='xlsxwriter')
        work_hours.to_excel(writer, sheet_name=xls_sheet_name, index=False)

        # adjust column width
        xls_sheet = writer.sheets[xls_sheet_name]
        for column_id, size in self.cfg.aliases_ids_sizes():  # loop through all columns
            xls_sheet.set_column(column_id, column_id, size)  # set column width

        writer.save()
        return xls_io

    def report_total(self, cost_pack: pd.DataFrame):
        total = cost_pack.append(  # add empty record for all client names
            pd.DataFrame.from_dict({
                'work sheet': self.work_sheets().values()
            })
        )
        total_clients = total.groupby('work sheet', as_index=False).agg(
            {'minutes': 'sum', 'task_id': 'count'}
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
        filter_agents = self.cfg.agents_list()
        filter_services = self.get_service_filter()
        with self.cursor() as cursor:
            cost_query = sql.SQL(
                """
                    WITH
                         current_expenses AS (
                            SELECT e."TaskId" task_id, e."UserId" agent_id,
                            e."Minutes" minutes, t."ServiceId" service_id
                            FROM "Expenses" e LEFT JOIN "Tasks" t ON t."Id"=e."TaskId"
                            WHERE t."ServiceId" IN ({0})
                            AND e."UserId" IN ({1})
                            AND e."DateExp" BETWEEN {2} AND {3}
                         ),
                         prev_expenses AS (
                            SELECT e."TaskId" task_id, e."UserId" agent_id,
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
                            SELECT service_id, task_id, agent_id, sum(minutes) AS minutes FROM union_expenses
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
                sql.SQL(', ').join(map(sql.Literal, filter_agents)),
                sql.Literal(p.begin), sql.Literal(p.end), sql.Literal(self['early_opened'] or False)
            )
            csv_query = sql.SQL("COPY ({0}) TO STDOUT WITH CSV HEADER").format(cost_query)

            with io.StringIO() as data:
                cursor.copy_expert(csv_query, data)
                data.seek(0)
                raw = pd.read_csv(data)

        raw['client_id'] = raw['service_id'].apply(filter_services.get)
        raw['client'] = raw['client_id'].apply(self.cfg.ids_clients().get)
        raw['agent'] = raw['agent_id'].apply(self.cfg.ids_agents().get)
        raw['rate'] = raw['agent'].apply(self.cfg.agents_rates().get)
        raw['rate_value'] = raw.apply(
            lambda r: self.cfg.get_rate_value(r['client'], r['rate']), axis=1
        )
        raw['hours'] = raw['minutes'].apply(lambda mm: f'{mm // 60}:{mm % 60:02}')
        raw['value'] = raw.apply(lambda r: round(r['rate_value']*r['minutes']/60), axis=1)
        raw['work sheet'] = raw['client'].apply(
            lambda client: self.work_sheets()[client]
        )
        return raw



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


