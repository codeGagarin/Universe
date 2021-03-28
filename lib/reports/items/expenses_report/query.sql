WITH
     expenses_totals as (
         select "TaskId" , "UserId", sum("Minutes") as Minutes from "Expenses"
         % if DATE_EXP_FROM
            where "DateExp" >= '{{DATE_EXP_FROM}}'::date
                and "DateExp" < '{{DATE_EXP_TO}}'::date + '1 day'::interval
         % endif
         group by "TaskId", "UserId"),
    % if SERVICE_FILTER
        srv as (select id from unnest(Array[{{ SERVICE_FILTER|join(', ') }}]) as id),
        unfold_srv as (select "Id" as id, "Name", "ParentId", "IsArchive",
            case when "ParentId" is Null then 1 else 2 end as level
            from "Services" where "Id" in (select * from srv) or "ParentId" in (select * from srv) order by "Id"),
    % endif
    fine as (select 'finite la ')
SELECT t."Id" AS task_id,
       t."Name" AS task_name,
       t."Description" AS task_descr,
       t."Created" AS created,
       t."Closed" AS closed,
       uc."Name" AS creator,
       ex.Minutes as minutes,
       s."Name" AS service,
       pps."Name" AS parent_service,
       ue."Name" AS executor,
       ue."Id" as executor_id,
       concat(t."Id", ':', ue."Id") AS details_key
FROM expenses_totals as ex
    LEFT JOIN "Tasks" as t ON ex."TaskId" = t."Id"
    LEFT JOIN "Services" ps ON t."ServiceId" = ps."Id"
    LEFT JOIN "Services" pps ON ps."ParentId" = pps."Id"
    LEFT JOIN "Users" uc ON t."CreatorId"=uc."Id"
    LEFT JOIN "Users" ue ON ex."UserId"=ue."Id"
    LEFT JOIN "Services" s ON t."ServiceId"=s."Id"
WHERE true
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

    {% if SERVICE_FILTER %}
        and t."ServiceId" in (select id from unfold_srv)
    {% endif %}

    {% if USER_FILTER %}
        and ex."UserId" in ({{ USER_FILTER|join(', ') }})
    {% endif %}
