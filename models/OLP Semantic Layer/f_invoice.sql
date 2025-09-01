{{ config(materialized='table') }}

with order_invoice as (
    select *
    from {{ source('dd_dwh', 'ORDER_INVOICE') }}
),

valid_org as (
    select *
    from {{ source('dd_dwh', 'PARTY_ORGANIZATION') }}
    where PO_END_DATE is null
),

filtered as (
    select distinct
        inv.ORD_INV_INTERNAL_INVOICE_ID as INVOICE_ID,
        nullif(inv.ORD_INV_INVOICE_CREATED, '1900-01-01') as CREATE_DATE,
        nullif(inv.ORD_INV_INVOICE_DATE, '1900-01-01') as INVOICE_DATE,
        inv.ORD_INV_TOTAL_AMOUNT as TOTAL,
        inv.ORD_INV_BALANCE_DUE as BALANCE_DUE,
        inv.ORD_INV_SUBTOTAL as SUBTOTAL,
        inv.ORD_INV_TAX_TOTAL as TAX_TOTAL,
        inv.ORD_INV_QUANTITY as QUANTITY,
        inv.ORD_INV_GROSS_SUBTOTAL as GROSS_SUBTOTAL
    from order_invoice inv
    left join valid_org po
        on po.PO_INTERNAL_PARTY_ID = inv.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    where inv.ORD_INV_END_DATE is null
      and {{ exclude_invalid_org("po") }}
),

excl_deleted as (
    select *
    from filtered f
    where {{ exclude_deleted_invoices("f", 3, 'INVOICE_ID') }}
),

excl_unapproved as (
    select *
    from excl_deleted f
    where {{ exclude_unapproved_invoices("f") }}
)

select *
from excl_unapproved
