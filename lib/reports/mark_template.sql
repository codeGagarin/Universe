% if MARK_TYPE == 'services' and FILTER_LIST is not none and FILTER_LIST
    select "Id" as id, "Name" as name from "Services" where "Id" in ({{ FILTER_LIST|join(', ') }})
% elif MARK_TYPE == 'users' and FILTER_LIST is not none and FILTER_LIST
    select "Id" as id, "Name" as name from "Users" where "Id" in ({{ FILTER_LIST|join(', ') }})
% elif MARK_TYPE == 'tasks' and FILTER_LIST is not none and FILTER_LIST
    select "Id" as id, "Name" as name, "Description" as descr from "Tasks" where "Id" in ({{ FILTER_LIST|join(', ') }})
% else
    select Null where false=true
% endif
