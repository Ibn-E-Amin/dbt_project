

{{ config(materialized='table') }}

--===============================================--
--======== CAMPAIGN FILTERED ====================--
--===============================================--
WITH FILTERED AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY CAM_PARTY_ORGANIZATION_ID, CAM_CAMPAIGN_NAME, CAM_CHANNEL_NAME) AS CAMPAIGN_ID
        ,CAM_PARTY_ORGANIZATION_ID AS STORE_ID
        ,CAM_CHANNEL_NAME AS CHANNEL
        ,CAM_CAMPAIGN_NAME AS CAMPAIGN
    FROM {{ source('DD_DWH', 'CAMPAIGN') }} AS C
    LEFT JOIN {{ party_organization() }} AS PO
        ON PO.PO_INTERNAL_PARTY_ID = C.CAM_PARTY_ORGANIZATION_ID
    WHERE C.CAM_SOURCE = 'CONTACTS'
        AND C.CAM_PARTY_ORGANIZATION_ID IS NOT NULL
        AND {{ exclude_invalid_org('PO') }}
    GROUP BY C.CAM_PARTY_ORGANIZATION_ID
        ,C.CAM_CHANNEL_NAME
        ,C.CAM_CAMPAIGN_NAME
)

SELECT *
FROM FILTERED
;