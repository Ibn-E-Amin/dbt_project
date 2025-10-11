{{ config(materialized='table') }}

with latest_orgs as (
    select *
         , row_number() over (partition by id order by run_time desc) as rn
    from {{ source('RAW','SM_ORGANISATIONS') }}
    where cast(run_time as date) = '{{ var("refresh_date", run_started_at.date()) }}'
)

select *
from latest_orgs
where rn = 1;
