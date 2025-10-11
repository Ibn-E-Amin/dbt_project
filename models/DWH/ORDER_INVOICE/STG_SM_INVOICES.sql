{{ config(materialized='table') }}

-- Get latest record per invoice ID for the given load date
with dedup as (
    select
        i.*,
        row_number() over (
            partition by i.id
            order by i.run_time desc
        ) as rn
    from {{ source('RAW', 'SM_INVOICES') }} i
    {% if var('refresh_date', none) is not none %}
      where cast(i.run_time as date) = '{{ var("refresh_date") }}'
    {% endif %}
)
select *
from dedup
where rn = 1
