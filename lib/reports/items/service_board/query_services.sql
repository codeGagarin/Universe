with
    -- cut -- params header -- cut --
    _period as (select '{{ from }}'::timestamp as begin, '{{ to }}'::timestamp as finish),
    srv as (select id from unnest(Array[{{ service_list }}]) as id),
    -- cut -- params header -- cut --
    period as (select begin, finish + '1 day'::interval as finish from _period),
    expand_srv as (select "Id" as id from "Services" where "ParentId" in (select * from srv)),
    parent_srv as (select distinct "ParentId" as id from "Services"
        where
            "ParentId" is not NULL and
            "ParentId" not in (select * from srv) and
            "Id" in (select * from srv)
    ),
    union_srv as (select * from expand_srv union distinct select * from parent_srv union distinct select * from srv),
    unfold_srv as (select "Id" as id, "Name", "ParentId", "IsArchive", "Path" as path,
        case when "ParentId" is Null then 1 else 2 end as level
        from "Services" where ("Id" in (select * from union_srv) or "ParentId" in (select * from union_srv))),
    tasks_spid as (
        select t.*, s."ParentId" as spid from "Tasks" as t
        left join "Services" as s on s."Id"=t."ServiceId"
        where "ServiceId" in (select id from unfold_srv)),
    created_tasks_detail as (select "ServiceId" as id, "Id" as task, spid
        from tasks_spid
        where "Created">=(select begin from period)
        and "Created"<(select finish from period)),
    created_tasks as (select t.id, count(*) as count
        from (select uf.id as id, ctd.task from created_tasks_detail as ctd, unfold_srv as uf
            where ctd.id=uf.id or ctd.spid=uf.id) as t group by id),
    closed_tasks_detail as (select * from tasks_spid as ts
        where "Closed" >= (select begin from period)
        and "Closed" < (select finish from period)
        ),
    closed_tasks as (select t.id, count(*) as count
        from (select uf.id as id, ctd."Id" from closed_tasks_detail as ctd, unfold_srv as uf
            where ctd."ServiceId"=uf.id or ctd.spid=uf.id) as t group by t.id),
    closed_exp_detail as (select e."TaskId" as tid, e."Minutes" as minutes, ts."ServiceId" as sid, ts.spid as spid
        from "Expenses" as e
        left join tasks_spid ts on ts."Id" = e."TaskId"
        where ts."Closed">=(select begin from period)
        and ts."Closed"<(select finish from period)),
    closed_exp as (select t.id as id, sum(t.minutes) as sum
        from (select uf.id as id, ced.minutes as minutes from closed_exp_detail as ced, unfold_srv as uf
        where ced.sid=uf.id or ced.spid=uf.id) as t group by id),
    open_task_detail as (select "Id" as id, "ServiceId" as sid, spid from tasks_spid
        where "Closed" is NULL),
    open_tasks as (select t.id as id, count(*) as count
        from (select uf.id as id, otd.sid from open_task_detail as otd, unfold_srv as uf
            where otd.sid=uf.id or otd.spid=uf.id) as t group by t.id),
    open_exp_detail as (select e."TaskId" as tid, e."Minutes" as minutes, ts."ServiceId" as sid, ts.spid as spid
        from "Expenses" as e
        left join tasks_spid ts on ts."Id" = e."TaskId"
        where "Closed" is NULL),
    open_exp as (select t.id as id, sum(t.minutes) as sum
        from (select uf.id as id, oed.minutes as minutes from open_exp_detail as oed, unfold_srv as uf
        where oed.sid=uf.id or oed.spid=uf.id) as t group by id),
    open_task_with_exec as (select distinct e."TaskId" as id from "Executors" as e
        left join "Tasks" as t on t."Id"=e."TaskId"
        where t."ServiceId" in (select id from unfold_srv)
        and t."Closed" is NULL),
    deep_diff as (
        select id from open_task_detail
        except
        select id from open_task_with_exec),
    no_exec_detail as (select dd.id, t."ServiceId" as sid, s."ParentId" as spid from deep_diff as dd
        left join "Tasks" as t on t."Id"=dd.id
        left join "Services" as s on s."Id"=t."ServiceId"),
    no_exec as (select t.id as id, count(*) as count
        from (select uf.id as id, ned.sid from no_exec_detail as ned, unfold_srv as uf
        where ned.sid=uf.id or ned.spid=uf.id) as t group by t.id),
    no_planed_detail as (select "Id" as id, "ServiceId" as sid, spid from tasks_spid
        where "Closed" is NULL and "Deadline" is NULL),
    no_planed as (select t.id as id, count(*) as count
        from (select uf.id as id, npd.sid from no_planed_detail as npd, unfold_srv as uf
            where npd.sid=uf.id or npd.spid=uf.id) as t group by t.id),
    report as (
        select
            ufs.id as id, ufs.level as level, ufs."Name" as name, ufs."ParentId" as pid, ufs."IsArchive" as arc,
            coalesce(crt.count, 0) as created,
            coalesce(clt.count, 0) as closed,
            coalesce(cle.sum, 0) as closed_exp,
            coalesce(ot.count, 0) as open,
            coalesce(ote.sum, 0) as open_exp,
            coalesce(ne.count, 0) as no_exec,
            coalesce(np.count, 0) as no_planed        from unfold_srv as ufs
        left join created_tasks as crt on (crt.id=ufs.id)
        left join closed_tasks as clt on (clt.id=ufs.id)
        left join closed_exp as cle on (cle.id=ufs.id)
        left join open_tasks as ot on (ot.id=ufs.id)
        left join open_exp as ote on (ote.id=ufs.id)
        left join no_exec as ne on (ne.id=ufs.id)
        left join no_planed as np on (np.id=ufs.id)
        order by path)
select *, created+closed+closed_exp+open+open_exp+no_exec+no_planed as sum_all from report