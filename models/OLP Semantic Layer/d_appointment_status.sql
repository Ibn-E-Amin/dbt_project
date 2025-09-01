{{ config(
    materialized='table'
) }}

with appointment_status as (
    select
        appt.CJ_INTERNAL_APPT_ID as APPOINTMENT_ID,
        appt.CJ_APPT_STATUS as STATUS,
        appt.CJ_APPT_START_DATE as START_DATE,
        appt.CJ_APPT_END_DATE as END_DATE
    from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_APPOINTMENTS') }} appt
    left join {{party_organization()}} po
        on appt.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID = po.PO_INTERNAL_PARTY_ID
    where appt.CJ_INTERNAL_APPT_ID is not null
      and {{exclude_invalid_org('po')}}
),

excl_deleted as (
    select *
    from appointment_status appt
    where {{ exclude_deleted_invoices("appt", 1,'APPOINTMENT_ID') }}
)

select * 
from excl_deleted
