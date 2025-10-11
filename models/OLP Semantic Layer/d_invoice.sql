


{{ config(materialized='table') }}

--===============================================--
--======== INVOICE BASE AND JOINED ==============--
--===============================================--
WITH BASE AS (
    SELECT DISTINCT
        INV.ORD_INV_INTERNAL_INVOICE_ID AS INVOICE_ID
        ,INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID
        ,COALESCE(INV.ORD_INV_ROOT_PROPOSAL_ID, INV.ORD_INV_INTERNAL_INVOICE_ID) AS PROPOSAL_ID
        ,INV.ORD_INV_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID
        ,INV.ORD_INV_TYPE AS INVOICE_TYPE
        ,INV.ORD_INV_SERVICE_NAME AS SERVICE
        ,INV.ORD_INV_REVENUE_CATEGORY AS SERVICE_CATEGORY
        ,INV.ORD_INV_OWNER AS TEAM_MEMBER
    FROM {{ source('DD_DWH', 'ORDER_INVOICE') }} AS INV
    LEFT JOIN {{ party_organization() }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = INV.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE INV.ORD_INV_END_DATE IS NULL
      AND {{ exclude_invalid_org('PO') }}
),

JOINED AS (
    SELECT
        B.INVOICE_ID
        ,B.STORE_ID
        ,B.PROPOSAL_ID
        ,B.LEAD_ID
        ,B.INVOICE_TYPE
        ,NULL AS CAMPAIGN_ID
        ,S.SERVICE_ID
        ,B.TEAM_MEMBER
    FROM BASE AS B
    LEFT JOIN {{ ref('D_SERVICES') }} AS S
        ON S.SERVICE = B.SERVICE
        AND S.SERVICE_CATEGORY = B.SERVICE_CATEGORY
),

DELETION_EVENT AS (
    SELECT *
    FROM JOINED AS J
    WHERE {{ exclude_deleted_invoices('J', 3, 'INVOICE_ID') }}
),

EXCL_UNAPPROVED AS (
    SELECT *
    FROM DELETION_EVENT AS D
    WHERE {{ exclude_unapproved_invoices('D') }}
)

SELECT *
FROM EXCL_UNAPPROVED
;

