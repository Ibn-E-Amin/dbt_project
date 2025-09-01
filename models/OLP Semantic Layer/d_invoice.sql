{{ config(materialized='table') }}

with base as (

    select distinct
        inv.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID,
        inv.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID,
        COALESCE(inv.ORD_INV_ROOT_PROPOSAL_ID, inv.ORD_INV_INTERNAL_INVOICE_ID) AS  PROPOSAL_ID,
        inv.ORD_INV_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID,
        inv.ORD_INV_TYPE AS INVOICE_TYPE,
        inv.ORD_INV_SERVICE_NAME AS SERVICE,
        inv.ORD_INV_REVENUE_CATEGORY AS SERVICE_CATEGORY,
        inv.ORD_INV_OWNER AS TEAM_MEMBER
    from {{ source('dd_dwh', 'ORDER_INVOICE') }} inv
    left join {{ party_organization() }} po
        on po.PO_INTERNAL_PARTY_ID = inv.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    where inv.ORD_INV_END_DATE IS NULL
      and {{ exclude_invalid_org('po') }}
),

joined as (

    select
        b.INVOICE_ID,
        b.STORE_ID,
        b.PROPOSAL_ID,
        b.LEAD_ID,
        b.INVOICE_TYPE,
        null as CAMPAIGN_ID,
        s.SERVICE_ID,
        b.TEAM_MEMBER
    from base b
    left join {{ ref('d_services') }} s
        on s.SERVICE = b.SERVICE
        and s.SERVICE_CATEGORY = b.SERVICE_CATEGORY
),

deletion_event as (
    select *
    from joined j
    where {{exclude_deleted_invoices("j", 3, 'INVOICE_ID')}}
),

excl_unapproved as (
    select *
    from deletion_event d
    where {{ exclude_unapproved_invoices("d") }}
)

select *
from excl_unapproved

