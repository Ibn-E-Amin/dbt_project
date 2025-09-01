{{ config(materialized='table') }}


select
    L.LEAD_ID              as CUSTOMER_ID,
    L.LEAD_NAME            as CUSTOMER_NAME,
    L.SERVICE_CITY         as SERVICE_CITY,
    L.SERVICE_STATE        as SERVICE_STATE,
    L.SERVICE_POSTAL_CODE  as SERVICE_POSTAL_CODE,
    L.LONGITUDE            as LONGITUDE,
    L.LATITUDE             as LATITUDE,
    L.LEAD_CATEGORY        as CUSTOMER_CATEGORY,
    min(cast(I.ORD_INV_INVOICE_CREATED as date)) as CUSTOMER_START_DATE,
    L.CAMPAIGN_ID          as CAMPAIGN_ID,
    L.PC_ORGANIZATION_ID
from {{ ref('d_lead') }} L
inner join {{ order_invoice() }} I
    on cast(I.ORD_INV_INTERNAL_CUSTOMER_PARTY_ID as float) = L.LEAD_ID
group by
    L.LEAD_ID,
    L.LEAD_NAME,
    L.SERVICE_CITY,
    L.SERVICE_STATE,
    L.SERVICE_POSTAL_CODE,
    L.LONGITUDE,
    L.LATITUDE,
    L.LEAD_CATEGORY,
    L.CAMPAIGN_ID,
    L.PC_ORGANIZATION_ID
;
