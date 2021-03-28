with
    % if SERVICE_FILTER
    srv as (select id from unnest(Array[{{ SERVICE_FILTER }}]) as id),
    unfold_srv as (select "Id" as id, "Name", "ParentId", "IsArchive",
        case when "ParentId" is Null then 1 else 2 end as level
        from "Services" where "Id" in (select * from srv) or "ParentId" in (select * from srv) order by "Id"),
    % endif
    eval_map as (select * from (values(5,4), (4,2), (3,5), (2,6), (1,1)) as val(_ev, _id)), -- 5: 4, 4: 2, 3: 5, 2: 6, 1: 1
    fine as (select 'finite la ')
select t."Id" as task_id,
       t."Name" as task_name,
       t."Description" as task_descr,
       c."Name" as creator,
       t."Created" as created,
       t."Closed" as closed,
       t."Deadline" as planed,
       s."Name" as service,
       ps."Name" as parent_service,
       em._ev as evaluation,
       COALESCE(ex.minutes, 0) as minutes

from "Tasks" t
    left join eval_map em on em._id = t."EvaluationId"
    left join "Users" as c on c."Id"=t."CreatorId"
    left join "Services" as s on s."Id"=t."ServiceId"
    left join "Services" as ps on ps."Id"=s."ParentId"
    left join (
        select "TaskId" as tid, sum("Minutes") as minutes from "Expenses"
            group by "TaskId") as ex on ex.tid = t."Id"
    left join
        (select "TaskId", count(*) as count from "Executors" group by "TaskId") executors
            on executors."TaskId"=t."Id"

    where true

    % if USER_FILTER
    and t."Id" in (select "TaskId" from "Executors" where "UserId" in ({{ USER_FILTER|join(', ') }}))
    % endif

    % if EVALUATION_FILTER
        and em._ev in ({{ EVALUATION_FILTER|join(', ') }})
    % endif

    % if CLOSED_IS_NULL is not none
        % if CLOSED_IS_NULL
            and t."Closed" is NULL
        % else
            and t."Closed" is not NULL
        % endif
    % endif


    {% if CLOSED_FROM %}
        and t."Closed" >= '{{ CLOSED_FROM }}'::date
    {% endif %}

    {% if CLOSED_TO %}
        and t."Closed" < '{{ CLOSED_TO }}'::date + '1 day'::interval
    {% endif %}

    {% if CREATED_FROM %}
        and t."Created" >= '{{ CREATED_FROM }}'::date
    {% endif %}

    {% if CREATED_TO %}
        and t."Created" < '{{ CREATED_TO }}'::date + '1 day'::interval
    {% endif %}

    {% if SERVICE_FILTER %}
        and t."ServiceId" in (select id from unfold_srv)
    {% endif %}

    % if NO_EXEC is not none
        % if NO_EXEC is not none
            and executors.count is NULL
        % else
            and executors.count is not NULL
        % endif
    % endif

    % if PLANED_IS_NULL is not none
        % if PLANED_IS_NULL
            and t."Deadline" is NULL
        % else
            and t."Deadline" is not NULL
        % endif
    % endif
