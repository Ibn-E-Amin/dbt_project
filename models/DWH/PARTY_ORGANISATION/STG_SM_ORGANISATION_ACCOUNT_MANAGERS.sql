{{ config(materialized='table') }}

with ranked as (
    select *,
           row_number() over (partition by organization_id order by cast(run_time as datetime2) desc) as rn
    from {{ source('RAW','SM_ORGANISATION_ACCOUNT_MANAGERS') }}
)

select distinct
    organization_id,
    name,
    run_time
from ranked
where rn = 1;