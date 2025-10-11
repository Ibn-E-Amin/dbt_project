

{{ config(materialized='table') }}

--===============================================--
--======== PROPOSALS AND FINAL ==================--
--===============================================--
WITH SOURCE_PROPOSALS AS (
    SELECT DISTINCT
        CJ_INTERNAL_PROP_ID AS PROPOSAL_ID
        ,CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID
        ,CJ_PROP_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID
        ,CJ_SERVICE_NAME AS SERVICE
        ,CJ_REVENUE_CATEGORY AS REVENUE_CATEGORY
        ,CJ_PROP_TYPE AS TYPE
        ,CJ_PROP_TITLE AS PROPOSAL_TITLE
        ,CJ_PROP_OWNER AS TEAM_MEMBER
    FROM {{ source('DD_DWH', 'CUSTOMER_JOURNEY_PROPOSALS') }} AS PROP
    LEFT JOIN {{ party_organization() }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = PROP.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID
    WHERE {{ exclude_invalid_org('PO') }}
      AND CJ_PROP_END_DATE IS NULL
),

FINAL AS (
    SELECT
        P.PROPOSAL_ID
        ,P.STORE_ID
        ,P.LEAD_ID
        ,S.SERVICE_ID
        ,L.CAMPAIGN_ID
        ,P.TYPE
        ,P.PROPOSAL_TITLE
        ,P.TEAM_MEMBER
    FROM SOURCE_PROPOSALS AS P
    LEFT JOIN {{ ref('D_SERVICES') }} AS S
        ON S.SERVICE = P.SERVICE
        AND S.SERVICE_CATEGORY = P.REVENUE_CATEGORY
    LEFT JOIN {{ ref('D_LEAD') }} AS L
        ON L.LEAD_ID = P.LEAD_ID
    -- deletion filter
    WHERE P.PROPOSAL_ID NOT IN (
        SELECT CAST(ENTITYID AS INT)
        FROM {{ source('PROD_BK', 'SM_DELETION_EVENTS') }}
        WHERE ENTITYTYPE = 2
    )
),

EXCL_DELETED AS (
    SELECT *
    FROM FINAL AS F
    WHERE {{ exclude_deleted_invoices('F', 2, 'PROPOSAL_ID') }}
)

SELECT *
FROM EXCL_DELETED
;
