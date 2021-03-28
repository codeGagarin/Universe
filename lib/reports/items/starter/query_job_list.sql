SELECT id, type, status, plan, start, finish, duration, params, result
    FROM "Loader" WHERE
    plan >= '{{ ON_DAY }}'::date
    AND plan < '{{ ON_DAY }}'::date + '1 day'::interval
    % if JOB_TYPE_FILTER
        AND type='{{ JOB_TYPE_FILTER }}'
    % endif
    ORDER BY plan DESC