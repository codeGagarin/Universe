with
    pids as (select pid from pg_stat_activity where datname='mirror_test' and pid!=pg_backend_pid())
select pid, pg_terminate_backend(pid) from pids

--delete from "Loader" WHERE "status"='todo'