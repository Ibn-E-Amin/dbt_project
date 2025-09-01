{{ config(materialized='table') }}

with filtered as (
    SELECT DISTINCT
        INV.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID,
        NULLIF(INV.ORD_INV_INVOICE_CREATED, '1900-01-01') AS CREATE_DATE,
        NULLIF(INV.ORD_INV_INVOICE_DATE, '1900-01-01') AS INVOICE_DATE,
        INV.ORD_INV_TOTAL_AMOUNT AS TOTAL,
        INV.ORD_INV_BALANCE_DUE AS BALANCE_DUE,
        INV.ORD_INV_SUBTOTAL AS SUBTOTAL,
        INV.ORD_INV_TAX_TOTAL AS TAX_TOTAL,
        INV.ORD_INV_QUANTITY AS QUANTITY,
        INV.ORD_INV_GROSS_SUBTOTAL AS GROSS_SUBTOTAL
    from {{source('dd_dwh', 'ORDER_INVOICE')}} INV
    left join {{party_organization()}} PO
        on PO.PO_INTERNAL_PARTY_ID = INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    where INV.ORD_INV_END_DATE IS NULL
        and {{ exclude_invalid_org("PO") }}
),

deletion_event as (
    select *
    from filtered f
    where {{exclude_deleted_invoices("f", 3, 'INVOICE_ID')}}
),

unapproved_voided_status as (
    select *
    from deletion_event de
    where {{exclude_unapproved_invoices("de")}}
)

select *
from unapproved_voided_status