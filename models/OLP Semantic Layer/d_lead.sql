{{ config(materialized='table') }}

with contacts_leads as (
    select distinct
        PC.PC_INTERNAL_PARTY_ID as LEAD_ID,
        PC.PC_NAME as LEAD_NAME,
        PC.PC_SERVICE_CITY as SERVICE_CITY,
        PC.PC_SERVICE_STATE as SERVICE_STATE,
        PC.PC_SERVICE_POSTAL_CODE as SERVICE_POSTAL_CODE,
        PT.PT_ADDRESS_1 as SERVICE_ADDRESS_1,
        PT.PT_ADDRESS_2 as SERVICE_ADDRESS_2,
        nullif(PC.PC_LONGITUDE, 0) as LONGITUDE,
        nullif(PC.PC_LATITUDE, 0) as LATITUDE,
        PC.PC_CATEGORY as LEAD_CATEGORY,
        PC.PC_CREATED_AT as LEAD_DATE,
        CAM.CAMPAIGN_ID,
        PC.PC_ORGANIZATION_ID
    from {{ source('dd_dwh', 'PARTY_CONTACT') }} PC
    left join {{ party_organization() }} PO
        on PO.PO_INTERNAL_PARTY_ID = PC.PC_ORGANIZATION_ID
    left join (
                select *
                from {{source('dd_dwh', 'PARTY')}}
                where PT_END_DATE is null
              ) PT
        on PT.PT_INTERNAL_PARTY_ID = PC.PC_INTERNAL_PARTY_ID
    left join {{ ref('d_campaigns') }} CAM
        on CAM.CAMPAIGN = PC.PC_CAMPAIGN
       and CAM.CHANNEL = PC.PC_CHANNEL
       and CAM.STORE_ID = PC.PC_ORGANIZATION_ID
    where {{exclude_invalid_org('PO')}}
    and PC.PC_END_DATE is null
),

excl_deleted as (
    select *
    from contacts_leads cl
    where {{ exclude_deleted_invoices("cl", 0,'LEAD_ID') }}
)

select
    CL.LEAD_ID,
    CL.LEAD_NAME,
    CL.SERVICE_CITY,
    CL.SERVICE_STATE,
    CL.SERVICE_POSTAL_CODE,
    CL.SERVICE_ADDRESS_1,
    CL.SERVICE_ADDRESS_2,
    CL.LONGITUDE,
    CL.LATITUDE,
    CL.LEAD_CATEGORY,
    cast(CL.LEAD_DATE as date) as LEAD_DATE,
    CL.CAMPAIGN_ID,
    CL.PC_ORGANIZATION_ID
from contacts_leads CL
