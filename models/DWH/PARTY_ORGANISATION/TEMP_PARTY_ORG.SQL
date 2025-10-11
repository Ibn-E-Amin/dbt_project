{{ config(materialized='view') }}

with orgs as (
    select * from {{ ref('STG_SM_ORGANISATIONS') }}
),

contacts as (
    select organization_id, organization_name, run_time, createdat, updatedat
    from {{ ref('STG_SM_CONTACTS') }}
),

managers as (
    select organization_id, name as account_manager, run_time
    from {{ ref('STG_SM_ORGANISATION_ACCOUNT_MANAGERS') }}
),

total_orgs as (
    select
        cast(coalesce(o.id, c.organization_id, ac.organization_id) as int) as id,
        o.publicname,
        coalesce(o.internalname, c.organization_name) as internalname,
        o.legalentityname,
        o.taxpayerid,
        o.timezone,
        o.opentimegranularity,
        o.canceldate,
        o.trialexpirationdate,
        o.suspended,
        o.billingplan,
        o.billingmethod,
        cast(c.createdat as datetime2) as open_since,
        ac.name as account_manager,
        o.region,
        cast(c.createdat as datetime2) as created_at,
        cast(c.updatedat as datetime2) as updated_at,
        cast(coalesce(o.run_time, c.run_time, ac.run_time) as datetime2) as run_time
    from {{ ref('STG_SM_ORGANISATIONS') }} o
    full outer join (
        select organization_id, organization_name , max(cast(run_time as datetime2)) as run_time, min(cast(createdat as date)) as createdat, min(cast(updatedat as date)) as updatedat
        from {{ ref('STG_SM_CONTACTS') }}
        group by organization_id, organization_name
    ) c on o.id = c.organization_id
    full outer join (
        select organization_id, name, max(cast(run_time as datetime2)) as run_time
        from {{ ref('STG_SM_ORGANISATION_ACCOUNT_MANAGERS') }}
        group by organization_id, name
    ) ac on coalesce(o.id, c.organization_id) = ac.organization_id
)

select
    id,
    publicname,
    internalname,
    legalentityname,
    taxpayerid,
    timezone,
    opentimegranularity,
    canceldate,
    trialexpirationdate,
    suspended,
    billingplan,
    billingmethod,
    created_at,
    updated_at,
    run_time,
    -- extra fields used downstream
    open_since,
    account_manager,
    region
from total_orgs
;
