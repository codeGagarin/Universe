with
     data as (select * from (values
        ('record one', True, 'useless bar', '2021-01-01'::date, 15),
        ('record two', false, 'useless foo', '2021-01-02'::date, 45),
        ('record three', true, 'useless bar and foo', '2021-01-01'::date, 63)
        ) as D(caption, bool_field, useless, date_field, measure))
select * from data
    where
        bool_field = {{ SOME_BOOL_PARAM }}
        and date_field = '{{ SOME_DATE_PARAM }}'::date