{{ config(materialized='table') }}

with filtered as (
    select 
        ROW_NUMBER()OVER(ORDER BY CAM_PARTY_ORGANIZATION_ID, CAM_CAMPAIGN_NAME, CAM_CHANNEL_NAME) AS CAMPAIGN_ID,
        CAM_PARTY_ORGANIZATION_ID AS STORE_ID,
        CAM_CHANNEL_NAME AS CHANNEL,
        CAM_CAMPAIGN_NAME AS CAMPAIGN
    from {{source('dd_dwh', 'CAMPAIGN')}} c
    left join {{party_organization()}} po
        on po.PO_INTERNAL_PARTY_ID = c.CAM_PARTY_ORGANIZATION_ID
    where c.CAM_SOURCE = 'CONTACTS'
        and c.CAM_PARTY_ORGANIZATION_ID is not null
        and {{ exclude_invalid_org("po") }}
    group by c.CAM_PARTY_ORGANIZATION_ID,
             c.CAM_CHANNEL_NAME,
             c.CAM_CAMPAIGN_NAME
)

select * 
from filtered;