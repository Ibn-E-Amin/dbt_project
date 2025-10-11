


{{ config(materialized='table') }}

--===============================================--
--======== INVOICE FACT TABLE ===================--
--===============================================--
WITH FILTERED AS (
    SELECT DISTINCT
        INV.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID
        ,NULLIF(INV.ORD_INV_INVOICE_CREATED, '1900-01-01') AS CREATE_DATE
        ,NULLIF(INV.ORD_INV_INVOICE_DATE, '1900-01-01') AS INVOICE_DATE
        ,INV.ORD_INV_TOTAL_AMOUNT AS TOTAL
        ,INV.ORD_INV_BALANCE_DUE AS BALANCE_DUE
        ,INV.ORD_INV_SUBTOTAL AS SUBTOTAL
        ,INV.ORD_INV_TAX_TOTAL AS TAX_TOTAL
        ,INV.ORD_INV_QUANTITY AS QUANTITY
        ,INV.ORD_INV_GROSS_SUBTOTAL AS GROSS_SUBTOTAL
    FROM {{ source('DD_DWH', 'ORDER_INVOICE') }} AS INV
    LEFT JOIN {{ party_organization() }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE INV.ORD_INV_END_DATE IS NULL
      AND {{ exclude_invalid_org('PO') }}
),

DELETION_EVENT AS (
    SELECT *
    FROM FILTERED AS F
    WHERE {{ exclude_deleted_invoices('F', 3, 'INVOICE_ID') }}
),

UNAPPROVED_VOIDED_STATUS AS (
    SELECT *
    FROM DELETION_EVENT AS DE
    WHERE {{ exclude_unapproved_invoices('DE') }}
)

SELECT *
FROM UNAPPROVED_VOIDED_STATUS
;