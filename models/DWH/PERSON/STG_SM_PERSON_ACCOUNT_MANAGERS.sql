{{ config(materialized='table') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by id
            order by run_time desc
        ) as rn
    from {{ source('RAW', 'SM_ORGANISATION_ACCOUNT_MANAGERS') }}
    where cast(run_time as date) = '{{ var("refresh_date") }}'
)

select *
from ranked
where rn = 1
