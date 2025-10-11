{{ config(materialized="table") }}

with dedup as (
    select
        c.*,
        row_number() over (
            partition by c.id
            order by c.run_time desc
        ) as rn
    from {{ source('RAW', 'SM_CONTACTS') }} c
    {% if var('refresh_date', none) is not none %}
      where cast(c.run_time as date) = '{{ var("refresh_date") }}'
    {% endif %}
)

select *
from dedup
where rn = 1
