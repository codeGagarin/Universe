SELECT type AS type, COUNT(*) AS count, SUM(CASE WHEN status='{{FAIL}}' THEN 1 ELSE 0 END) AS fail
    FROM "Loader" WHERE
        plan >= '{{ ON_DAY }}'::date AND
        plan < '{{ ON_DAY }}'::date + '1 day'::interval GROUP BY type
    ORDER BY type
