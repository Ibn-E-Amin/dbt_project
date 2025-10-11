{{ config(materialized='table') }}

with contacts as (
    select
        organization_id,
        organization_name,
        min(cast(run_time as datetime2)) as run_time,
        min(cast(createdat as date)) as createdat,
        min(cast(updatedat as date)) as updatedat
    from {{ source('RAW','SM_CONTACTS') }}
    group by organization_id, organization_name
)

select *
from contacts;
