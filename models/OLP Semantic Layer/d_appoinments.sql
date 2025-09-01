{{ config(materialized='table') }}

with appointment_base as (
    select 
        APPT.CJ_INTERNAL_APPT_ID AS APPOINTMENT_ID,
        APPT.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID AS STORE_ID,
        APPT.CJ_APPT_INVOICE_ID AS INVOICE_ID,
        APPT.CJ_APPT_INTERNAL_CUSTOMER_PARTY_ID AS LEAD_ID,
        APPT.CJ_APPT_SERVICE AS SERVICE,
        APPT.CJ_APPT_SERVICE_AGENT_ID AS SERVICE_AGENT_ID,
        APPT.CJ_APPT_RECURRING_TYPE AS APPOINTMENT_TYPE,
        APPT.CJ_APPT_PROPOSAL_ID AS PROPOSAL_ID
    from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_APPOINTMENTS') }} APPT
    left join {{party_organization()}} O
        on APPT.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID = O.PO_INTERNAL_PARTY_ID
    where APPT.CJ_APPT_END_DATE is null
      and {{exclude_invalid_org('O')}}
),

final as (
    select 
        A.APPOINTMENT_ID,
        A.STORE_ID,
        A.INVOICE_ID,
        A.PROPOSAL_ID,
        A.LEAD_ID,
        null as CAMPAIGN_ID,
        P.SERVICE_ID,
        A.SERVICE_AGENT_ID,
        A.APPOINTMENT_TYPE
    from appointment_base A
    left join {{ ref('d_proposal') }} P 
        on P.PROPOSAL_ID = A.PROPOSAL_ID
),

excl_deleted as (
    select *
    from final f
    where {{ exclude_deleted_invoices("f", 1,'APPOINTMENT_ID') }}
)

select * 
from excl_deleted
