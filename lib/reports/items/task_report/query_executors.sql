with
     task_list as (select id from unnest(Array[{{ TASK_LIST|join(', ') }}]) as id),
     user_list as (
         select "UserId", 0 as minutes, "TaskId" from "Executors"
            where "TaskId" in (select id from task_list)
            union all
         select "UserId", "Minutes" as minutes, "TaskId"from "Expenses"
            where "TaskId" in (select id from task_list)
    )
select
    e."TaskId" as task_id,
    u."Name" as user_name,
    u."Id" as user_id,
    COALESCE(sum(e.minutes), 0) as minutes
from user_list as e
    left join "Users" u on u."Id"=e."UserId"
where e."TaskId" in (
        select id from task_list
    )
    group by task_id, user_name, user_id