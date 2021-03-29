from datetime import datetime, timedelta

from keys import KeyChain
from lib.schedutils import Activity
from lib.schedutils import Starter
from lib.pg_utils import PGMix, sql
from connector import ISConnector
from connector import PGConnector


class ISSync(Activity, PGMix):
    PG_KEY = KeyChain.PG_IS_SYNC_KEY

    def _fields(self):
        return 'from to'

    def run(self):
        pg_con = PGConnector(KeyChain.PG_KEY)
        is_con = ISConnector(KeyChain.IS_KEY)

        update_pack = is_con.get_update_pack(self['from'], self['to'])
        for task in update_pack['Tasks'].values():
            pg_con.delete_task_actuals(task)
            pg_con.delete_task_executors(task)
            pg_con.update(task)

        for user in update_pack['Users'].values():
            pg_con.update(user)

        for actual in update_pack['Actuals']:
            pg_con.update(actual)

        for service in update_pack['Services'].values():
            pg_con.update(service)

        for executor in update_pack['Executors']:
            pg_con.update(executor)

        print(f"Ts:{len(update_pack['Tasks'])}, "
              f"Us:{len(update_pack['Users'])}, "
              f"Ac:{len(update_pack['Actuals'])}, "
              f"Sr:{len(update_pack['Services'])}, "
              f"Ex:{len(update_pack['Executors'])}.")


class ISActualizer(Activity, PGMix):
    PG_KEY = KeyChain.PG_IS_SYNC_KEY

    @classmethod
    def get_crontab(cls):
        return '0 */1 * * *'

    def _add_job(self, from_date: datetime, to_date: datetime, activity_id: int):
        with self.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    'INSERT INTO "SyncJobs"("from", "to", activity_id) VALUES ({},{},{}) RETURNING "id" as job_id'
                ).format(
                    sql.Literal(from_date), sql.Literal(to_date), sql.Literal(activity_id),
                )
            )
            self.commit()
            return cursor.fetchone().job_id

    def run(self):
        with self.cursor() as cursor:
            cursor.execute(
                """ 
                SELECT sj."to" AS "to", ld.status as status FROM "SyncJobs" AS sj
                    LEFT JOIN "Loader" AS ld ON ld.id=sj.activity_id
                    WHERE activity_id IS NOT NULL ORDER BY "to" DESC LIMIT 1                
                """
            )
            if not cursor.rowcount:
                last_update_tic = datetime(2019, 10, 1, 0, 0, 0)
                print("First ISActualizer launch detected")
            else:
                job_params = cursor.fetchone()
                if job_params.status != Starter.JobStatus.DONE:
                    return  # actualization in progress, comeback later
                last_update_tic = job_params.to

        from_date = last_update_tic
        d = timedelta(hours=6)
        to_date = from_date + d
        if to_date > datetime.now():
            to_date = datetime.now()

        new_job = ISSync(self._ldr)
        new_job['from'] = from_date
        new_job['to'] = to_date
        activity_id = new_job.apply()

        job_id = self._add_job(from_date, to_date, activity_id)
        print(f'Job id:{job_id} added.')
