
{{ config(materialized='table') }}

--===============================================--
--======== INVOICE STATUS FILTERED ===============--
--===============================================--
WITH FILTERED AS (
    SELECT
        I.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID
        ,I.ORD_INV_STATUS AS STATUS
        ,I.ORD_INV_START_DATE AS START_DATE
        ,I.ORD_INV_END_DATE AS END_DATE
    FROM {{ order_invoice() }} AS I
    LEFT JOIN {{ party_organization() }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = I.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE I.ORD_INV_END_DATE IS NULL
      AND {{ exclude_invalid_org('PO') }}
),

DELETION_EVENT AS (
    SELECT *
    FROM FILTERED AS F
    WHERE {{ exclude_deleted_invoices('F', 3, 'INVOICE_ID') }}
),

UNAPPROVED_VOIDED_STATUS AS (
    SELECT *
    FROM DELETION_EVENT
    WHERE END_DATE IS NULL
      AND STATUS NOT IN ('Unapproved', 'Voided')
)

SELECT *
FROM UNAPPROVED_VOIDED_STATUS
;
