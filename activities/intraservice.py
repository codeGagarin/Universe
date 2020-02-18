from datetime import datetime, timedelta
from psycopg2 import sql

from activities.activity import Activity


class ISSync(Activity):
    def _fields(self):
        return 'from to'

    def run(self):
        pg_con = self._ldr.get_PG_connector()
        is_con = self._ldr.get_IS_connector()

        update_pack = is_con.get_update_pack(self['from'], self['to'])
        for task in update_pack['Tasks'].values():
            pg_con.delete_task_actuals(task)
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


class ISActualizer(Activity):

    def get_crontab(self):
        return '0 */1 * * *'

    def _add_job(self, from_date: datetime, to_date: datetime, activity_id: int):
        query = sql.SQL('INSERT INTO {}({},{},{}) VALUES ({},{},{}) RETURNING {}').format(
            sql.Identifier('SyncJobs'),
            sql.Identifier('from'), sql.Identifier('to'), sql.Identifier('activity_id'),
            sql.Literal(from_date), sql.Literal(to_date), sql.Literal(activity_id),
            sql.Identifier('id')
        )
        return self._ldr.sql_exec(query, auto_commit=False)[0][0]

    def run(self):
        query = sql.SQL('SELECT {}, {} FROM {} ORDER BY {} DESC LIMIT 1').format(
            sql.Identifier('to'),
            sql.Identifier('activity_id'),
            sql.Identifier('SyncJobs'),
            sql.Identifier('to')
        )
        result = self._ldr.sql_exec(query, auto_commit=False)
        if not len(result):
            last_update_tic = datetime(2019, 10, 1, 0, 0, 0)
            print("First launch detected")
        else:
            state = self._ldr.get_activity_status(result[0][1])
            if state != "finish":
                return  # actualization in progress
            last_update_tic = result[0][0]

        from_date = last_update_tic
        d = timedelta(hours=3)
        to_date = from_date + d
        if to_date > datetime.now():
            to_date = datetime.now()

        m = ISSync(self._ldr)
        m['from'] = from_date
        m['to'] = to_date
        activity_id = m.apply()

        job_id = self._add_job(from_date, to_date, activity_id)
        print(f'Job id:{job_id} added.')
        self._ldr.sql_commit()


class IS404TaskCloser(Activity):
    """Closed all union 404 url tasks"""
    def get_crontab(self):
        return '30 */1 * * *'

    def run(self):
        # mark new open task
        query = sql.SQL('UPDATE "Tasks" SET "m_lastClosedTouch"="Created" '
                        ' WHERE "Closed" IS NULL AND "m_lastClosedTouch" IS NULL')
        self._ldr.sql_exec(query)

        # get 1/24 opened task count
        query = sql.SQL('SELECT COUNT(*) AS cc FROM "Tasks" WHERE "Closed" IS NULL')
        rows = self._ldr.sql_exec(query, named_result=True)
        open_tasks_count = rows[0].cc
        limit = open_tasks_count/12

        # select older closed touch tasks
        query = sql.SQL('SELECT "Id" AS idx FROM "Tasks" WHERE "Closed" IS NULL ORDER BY "m_lastClosedTouch" LIMIT 10')
        rows = self._ldr.sql_exec(query, named_result=True)

        # check 404 url task error
        is_conn = self._ldr.get_IS_connector()
        for rec in rows:
            if is_conn.is_404(rec.idx):
                # closed 404 tasks
                print(f"#{rec.idx} ")
                query = sql.SQL('UPDATE "Tasks" SET "Closed"={}, "m_lastClosedTouch"={} '
                                ' WHERE "Id"={}').format(
                    sql.Literal(datetime.now()),
                    sql.Literal(datetime(1111, 11, 11)),  # manual closed marker
                    sql.Literal(rec.idx)
                )
            else:
                # update last_touch param
                query = sql.SQL('UPDATE "Tasks" SET "m_lastClosedTouch"={} '
                                ' WHERE "Id"={}').format(sql.Literal(datetime.now()), sql.Literal(rec.idx))
            self._ldr.sql_exec(query)
