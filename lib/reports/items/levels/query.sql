select point, stamp, value from "FZLevels"
    where stamp between '{{ STAMP }}'::timestamp-'15 minutes'::interval and '{{ STAMP }}'::timestamp
