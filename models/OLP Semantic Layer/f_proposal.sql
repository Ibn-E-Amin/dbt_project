{{ config(materialized='table') }}

with base as (
select distinct
    PROP.CJ_INTERNAL_PROP_ID         as PROPOSAL_ID,
    nullif(PROP.CJ_PROP_CREATED_DATE, '1900-01-01')   as CREATE_DATE,
    nullif(PROP.CJ_PROP_ACCEPTED_DATE, '1900-01-01')  as ACCEPTED_DATE,
    PROP.CJ_PROP_SUBTOTAL            as SUBTOTAL,
    PROP.CJ_PROP_VALUE               as PROPOSAL_VALUE,
    PROP.CJ_PROP_DEPOSIT             as DEPOSIT_VALUE,
    PROP.CJ_PROP_GROSS_SUBTOTAL      as GROSS_SUBTOTAL,
    PROP.CJ_PROP_MATERIAL_COST       as MATERIAL_COST,
    PROP.CJ_PROP_LABOR_COST          as LABOR_COST,
    PROP.CJ_PROP_EXPENSES            as EXPENSES
from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_PROPOSALS') }} PROP
left join {{party_organization()}} PO
    on PO.PO_INTERNAL_PARTY_ID = PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID
where {{exclude_invalid_org('PO')}}
  and PROP.CJ_PROP_END_DATE is null
),

excl_deleted as (
    select *
    from base b
    where {{ exclude_deleted_invoices("b", 2, 'PROPOSAL_ID') }}
)

select * from excl_deleted
