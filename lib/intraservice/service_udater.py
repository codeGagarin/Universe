from lib.schedutils import Activity
from lib.connectors.connector import PGConnector, ISConnector
from keys import KeyChain


class ISServiceUpdater(Activity):
    @classmethod
    def get_crontab(cls):
        return '0 0 * * *'

    def run(self):
        is_con = ISConnector(KeyChain.IS_KEY)
        pg_con = PGConnector(KeyChain.PG_KEY)

        is_srv_list = {srv['Id']: srv for srv in is_con.get_service_list()}
        pg_srv_list = {srv['Id']: srv for srv in pg_con.get_service_list()}

        is_ids = set(is_srv_list.keys())
        pg_ids = set(pg_srv_list.keys())

        ids_for_insert = is_ids - pg_ids
        ids_for_update = is_ids - ids_for_insert

        pretty_srv_names = {}
        for _id in is_srv_list:
            pretty_name = ''
            srv = is_srv_list[_id]
            pid = srv['ParentId']
            if pid:
                pretty_name += f"[{is_srv_list[pid]['Name']}]"+': '
            pretty_name += f"[{srv['Name']}]"
            pretty_srv_names[_id] = pretty_name

        for _id in ids_for_insert:
            pg_con.update(is_srv_list[_id])

        for _id in ids_for_insert:
            print('+ '+pretty_srv_names[_id])

        # update only changed data
        ids_for_real_update = {
            _id: diff for _id, diff in (
                (srv1['Id'], srv1.diff(srv2)) for srv1, srv2 in (
                    ((is_srv_list[__id], pg_srv_list[__id]) for __id in ids_for_update)
                )
            ) if diff
        }

        for _id in ids_for_real_update:
            pg_con.update(is_srv_list[_id])

        for _id, diff in ids_for_real_update.items():
            print(f'* {pretty_srv_names[_id]}')
            for field_name, diff_values in diff.items():
                print(f'  {field_name}: (-){diff_values["another"]} (+){diff_values["own"]}')
