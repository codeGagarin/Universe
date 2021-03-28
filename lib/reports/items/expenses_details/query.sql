select
       e."DateExp"::date as date_exp,
       u."Name" as executor,
       e."Comments" as comment,
       e."Minutes" as minutes
from "Expenses" e
    left join "Users" as u on u."Id"=e."UserId"
    where e."TaskId" = {{ TASK_ID }}
    % if EXECUTOR_ID
        and e."UserId" = {{ EXECUTOR_ID }}
    % endif
    order by date_exp asc
