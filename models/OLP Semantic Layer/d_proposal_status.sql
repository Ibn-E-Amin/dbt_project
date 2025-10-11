

{{ config(materialized='table') }}

--===============================================--
--======== PROPOSAL STATUS BASE =================--
--===============================================--
WITH BASE AS (
    SELECT DISTINCT
        PROP.CJ_INTERNAL_PROP_ID AS PROPOSAL_ID
        ,PROP.CJ_PROP_STATUS AS STATUS
        ,PROP.CJ_PROP_DECLINE_REASON AS STATUS_REASON
        ,PROP.CJ_PROP_START_DATE AS START_DATE
        ,PROP.CJ_PROP_END_DATE AS END_DATE
    FROM {{ source('DD_DWH', 'CUSTOMER_JOURNEY_PROPOSALS') }} AS PROP
    LEFT JOIN {{ party_organization() }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE {{ exclude_invalid_org('PO') }}
),

EXCL_DELETED AS (
    SELECT *
    FROM BASE AS B
    WHERE {{ exclude_deleted_invoices('B', 2, 'PROPOSAL_ID') }}
)

SELECT *
FROM EXCL_DELETED
;
