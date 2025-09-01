{{ config(materialized='table') }}

with base as (
select distinct
    PROP.CJ_INTERNAL_PROP_ID     as PROPOSAL_ID,
    PROP.CJ_PROP_STATUS          as STATUS,
    PROP.CJ_PROP_DECLINE_REASON  as STATUS_REASON,
    PROP.CJ_PROP_START_DATE      as START_DATE,
    PROP.CJ_PROP_END_DATE        as END_DATE
from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_PROPOSALS') }} PROP
left join {{party_organization()}} PO
    on PO.PO_INTERNAL_PARTY_ID = PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID
where {{exclude_invalid_org('PO')}}
),

excl_deleted as (
    select *
    from base b
    where {{ exclude_deleted_invoices("b", 2, 'PROPOSAL_ID') }}
)

select * from excl_deleted
