{{ config(materialized="table") }}

with dedup as (
    select
        ch.*,
        row_number() over (
            partition by ch.id
            order by ch.run_time desc
        ) as rn
    from {{ source('RAW', 'SM_CHANNELS') }} ch
    {% if var('refresh_date', none) is not none %}
      where cast(ch.run_time as date) = '{{ var("refresh_date") }}'
    {% endif %}
)

select *
from dedup
where rn = 1
