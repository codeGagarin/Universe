% set history_period_name = {PeriodType.WEEK: 'whf', PeriodType.MONTH: 'mhf', PeriodType.QTR: 'qhf'}[frame_type]
with
-- params section
    _period as (select '{{ from }}'::timestamp as begin, '{{ to }}'::timestamp as finish),
    history_period_name as (select '{{ history_period_name }}'::text as name),
    user_list as (select id from unnest(Array[{{ user_list|join(', ') }}]) as id),
    % if service_list
        srv as (select id from unnest(Array[{{ service_list|join(', ') }}]) as id),
        unfold_srv as (select "Id" as id, "Name", "ParentId", "IsArchive",
            case when "ParentId" is Null then 1 else 2 end as level
            from "Services" where "Id" in (select id from srv) or "ParentId" in (select * from srv) order by "Id"),
        target_tasks as (select * from "Tasks" where "ServiceId" in (select id from unfold_srv)),
    % else
        target_tasks as (select * from "Tasks"),
    % endif
-- params section
    period as (select begin, finish + '1 day'::interval as finish,
        date_trunc('quarter', begin)::date as qtr_statr,
        date_trunc('quarter', begin)::date+'3 month'::interval as qtr_finish from _period),
    order_user_list as (select id, ROW_NUMBER() over() as ord from user_list),
    -- todo: unfold empty list
    raw_period_map as (select * from (
        values(1,1,'wh1','1 week'::interval, 1), (2,2,'wh2','1 week'::interval, 1),
            (3,3,'wh3', '1 week'::interval, 1), (3,1,'whf','1 week'::interval, 1),
            (1, 1, 'mh1', '1 month'::interval, 1), (2, 2, 'mh2', '1 month'::interval, 1),
            (3, 3, 'mh3', '1 month'::interval, 1), (3,1,'mhf','1 month'::interval, 1),
            (1, 1, 'qh1', '3 month'::interval, 0), (2, 2, 'qh2', '3 month'::interval, 0),
            (3, 3, 'qh3', '3 month'::interval, 0), (3,1,'qhf','3 month'::interval, 0)
        )
        as t(begin, finish, mark, mark_interval, start_mark)),
    period_map as (
        select rpm.*,
            case when rpm.start_mark=1 then p.begin
            else p.qtr_statr end as start_point,
            case when rpm.start_mark=1 then p.finish
            else p.qtr_finish end as finish_point
        from raw_period_map rpm, period p
    ),
    periods as (select
        (t.start_point-t.begin*t.mark_interval)::date as begin,
        (t.finish_point-t.finish*t.mark_interval)::date as finish,
        t.mark as mark
        from period_map as t),
    users as (select u."Id" as id, oul.ord as ord, "Name" as name from "Users" as u
        left join order_user_list as oul on oul.id=u."Id"
        where u."Id" in (select id from user_list)),
    own_tasks as (select e."UserId" as id, count(*) as count from "Executors" as e
        right join target_tasks as t on t."Id"=e."TaskId"
        where t."Closed" is Null and
        e."UserId" in (select id from user_list)
    group by id),
    full_closed_task as (select e."UserId" as uid, e."TaskId" as tid,
                                t."Closed" as dt, t."EvaluationId" as eval
        from "Executors" as e
        left join target_tasks as t on t."Id"=e."TaskId"
        where t."Closed">=(select begin from period)
        and t."Closed"<(select finish from period)
        and e."UserId" in (select * from user_list)),
    full_utilization as (select e."UserId" as uid, e."TaskId" as tid, e."DateExp" as dt,
        e."Minutes" as minutes
        from "Expenses" as e
        where e."DateExp">=(select begin from period)
        and e."DateExp"<(select finish from period)
        and e."TaskId" in (select "Id" from target_tasks)
        and e."UserId" in (select * from user_list)),
    eval_map as (select * from (values(4,5), (2,4), (5,3), (6,2), (1,1)) as ev(_from, _to)),
    evaluation as (select uid as id,
        sum(case when eval=(select _from from eval_map where _to=5) then 1 end) as ev5,
        sum(case when eval=(select _from from eval_map where _to=4) then 1 end) as ev4,
        sum(case when eval=(select _from from eval_map where _to=3) then 1 end) as ev3,
        sum(case when eval=(select _from from eval_map where _to=2) then 1 end) as ev2,
        sum(case when eval=(select _from from eval_map where _to=1) then 1 end) as ev1
        from full_closed_task group by id),
    closed_task as (select fct.uid as id,
        sum(case when extract(dow from fct.dt)=1 then 1 end) as mo,
        sum(case when extract(dow from fct.dt)=2 then 1 end) as tu,
        sum(case when extract(dow from fct.dt)=3 then 1 end) as we,
        sum(case when extract(dow from fct.dt)=4 then 1 end) as th,
        sum(case when extract(dow from fct.dt)=5 then 1 end) as fr,
        sum(case when extract(dow from fct.dt)=6 then 1 end) as sa,
        sum(case when extract(dow from fct.dt)=0 then 1 end) as su,
        sum(1) as ttl
        from full_closed_task as fct group by id),
    utilization as (select uid as id,
        sum(case when extract(dow from dt)=1 then minutes end) as mo,
        sum(case when extract(dow from dt)=2 then minutes end) as tu,
        sum(case when extract(dow from dt)=3 then minutes end) as we,
        sum(case when extract(dow from dt)=4 then minutes end) as th,
        sum(case when extract(dow from dt)=5 then minutes end) as fr,
        sum(case when extract(dow from dt)=6 then minutes end) as sa,
        sum(case when extract(dow from dt)=0 then minutes end) as su,
        sum(minutes) as ttl
        from full_utilization group by id),
    closed_task_history_details as (select e."UserId" as uid, e."TaskId" as tid, t."Closed" as dt
        from "Executors" as e
        left join target_tasks as t on t."Id"=e."TaskId"
        where t."Closed">=(select begin from periods where mark in (select name from history_period_name))
        and t."Closed"<(select finish from  periods where mark in (select name from history_period_name))
        and e."UserId" in (select * from user_list)),
    closed_task_history as (select ct.uid as id,
        sum(case when ct.dt >= (select begin from periods where mark='wh1') and
                      ct.dt < (select finish from periods where mark='wh1')then 1 end) as wh1,
        sum(case when ct.dt >= (select begin from periods where mark='wh2') and
                      ct.dt < (select finish from periods where mark='wh2')then 1 end) as wh2,
        sum(case when ct.dt >= (select begin from periods where mark='wh3') and
                      ct.dt < (select finish from periods where mark='wh3')then 1 end) as wh3,
        sum(case when ct.dt >= (select begin from periods where mark='mh1') and
                      ct.dt < (select finish from periods where mark='mh1')then 1 end) as mh1,
        sum(case when ct.dt >= (select begin from periods where mark='mh2') and
                      ct.dt < (select finish from periods where mark='mh2')then 1 end) as mh2,
        sum(case when ct.dt >= (select begin from periods where mark='mh3') and
                      ct.dt < (select finish from periods where mark='mh3')then 1 end) as mh3,
        sum(case when ct.dt >= (select begin from periods where mark='qh1') and
                      ct.dt < (select finish from periods where mark='qh1')then 1 end) as qh1,
        sum(case when ct.dt >= (select begin from periods where mark='qh2') and
                      ct.dt < (select finish from periods where mark='qh2')then 1 end) as qh2,
        sum(case when ct.dt >= (select begin from periods where mark='qh3') and
                      ct.dt < (select finish from periods where mark='qh3')then 1 end) as qh3

        from closed_task_history_details as ct
        group by id),
    utilization_history_details as (
        select e."UserId" as uid, e."TaskId" as tid, e."DateExp" as dt,
        e."Minutes" as minutes
        from "Expenses" as e
        where e."DateExp">=(select begin from periods where mark in (select name from history_period_name))
        and e."DateExp"<(select finish from periods where mark in (select name from history_period_name))
        and e."TaskId" in (select "Id" from target_tasks)
        and e."UserId" in (select * from user_list)),
    utilization_history as (select uid as id,
        sum(case when dt >= (select begin from periods where mark='wh1') and
                      dt < (select finish from periods where mark='wh1')then minutes end) as wh1,
        sum(case when dt >= (select begin from periods where mark='wh2') and
                      dt < (select finish from periods where mark='wh2')then minutes end) as wh2,
        sum(case when dt >= (select begin from periods where mark='wh3') and
                      dt < (select finish from periods where mark='wh3')then minutes end) as wh3,
        sum(case when dt >= (select begin from periods where mark='mh1') and
                      dt < (select finish from periods where mark='mh1')then minutes end) as mh1,
        sum(case when dt >= (select begin from periods where mark='mh2') and
                      dt < (select finish from periods where mark='mh2')then minutes end) as mh2,
        sum(case when dt >= (select begin from periods where mark='mh3') and
                      dt < (select finish from periods where mark='mh3')then minutes end) as mh3,
        sum(case when dt >= (select begin from periods where mark='qh1') and
                      dt < (select finish from periods where mark='qh1')then minutes end) as qh1,
        sum(case when dt >= (select begin from periods where mark='qh2') and
                      dt < (select finish from periods where mark='qh2')then minutes end) as qh2,
        sum(case when dt >= (select begin from periods where mark='qh3') and
                      dt < (select finish from periods where mark='qh3')then minutes end) as qh3
        from utilization_history_details group by id),
    report as (select users.id as id,users.name as name,ot.count as own_task,
                      ct.ttl   as ttlt,ut.ttl as ttlu,

                      ct.mo    as mot,ct.tu as tut,ct.we as wet,ct.th as tht,
                      ct.fr    as frt,ct.sa as sat,ct.su as sut,
                      ut.mo    as mou,ut.tu as tuu,ut.we as weu,ut.th as thu,
                      ut.fr    as fru,ut.sa as sau,ut.su as suu,
                      cth.wh1  as wh1t,cth.wh2 as wh2t,cth.wh3 as wh3t,
                      uh.wh1   as wh1u,uh.wh2 as wh2u,uh.wh3 as wh3u,

                      cth.mh1 as mh1t,cth.mh2 as mh2t,cth.mh3 as mh3t,
                      uh.mh1 as mh1u,uh.mh2 as mh2u,uh.mh3 as mh3u,

                      cth.qh1 as qh1t, cth.qh2 as qh2t,cth.qh3 as qh3t,
                      uh.qh1 as qh1u,uh.qh2 as qh2u,uh.qh3 as qh3u,

                      -- weekly frame end
                      ev.ev1 as ev1,ev.ev2 as ev2,ev.ev3 as ev3,ev.ev4 as ev4,ev.ev5 as ev5,
                      0 as fin
               from users
                        left join own_tasks as ot on ot.id=users.id
        -- weekly frame begin
                        left join closed_task as ct on ct.id = users.id
                        left join utilization as ut on ut.id = users.id
                        left join closed_task_history as cth on cth.id = users.id
                        left join utilization_history as uh on uh.id = users.id
        -- weekly frame end
                        left join evaluation as ev on ev.id = users.id
               order by ord)
    select * from report
