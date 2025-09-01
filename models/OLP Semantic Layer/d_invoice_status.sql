
{{ config(materialized='table') }}

with filtered as (
    select
        i.ORD_INV_INTERNAL_INVOICE_ID as INVOICE_ID,
        i.ORD_INV_STATUS as STATUS,
        i.ORD_INV_START_DATE as START_DATE,
        i.ORD_INV_END_DATE as END_DATE
    from {{order_invoice()}} i
    left join {{party_organization()}} po
        on po.PO_INTERNAL_PARTY_ID = i.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    where i.ORD_INV_END_DATE is null
      and {{ exclude_invalid_org("po") }}
),

deletion_event as (
    select *
    from filtered f
    where {{exclude_deleted_invoices("f", 3, 'INVOICE_ID')}}
),

unapproved_voided_status as (
    select *
    from deletion_event
    where END_DATE is null
    and STATUS not in ('Unapproved', 'Voided')
)

select *
from unapproved_voided_status
