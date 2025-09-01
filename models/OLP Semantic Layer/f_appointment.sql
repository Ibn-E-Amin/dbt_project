{{ config(
    materialized='table'
) }}

with appointments as (
    select
        appt.CJ_INTERNAL_APPT_ID as APPOINTMENT_ID,
        appt.CJ_APPT_CLOCK_SCHEDULED_DURATION as SCHEDULED_DURATION,
        appt.CJ_APPT_CLOCK_ACTUAL_DURATION as ACTUAL_DURATION,
        nullif(appt.CJ_APPT_CREATED_AT, '1900-01-01') as SCHEDULE_DATE,
        appt.CJ_APPT_TOTAL as TOTAL,
        appt.CJ_APPT_COSTS as COSTS,
        appt.CJ_APPT_EXPENSES as EXPENSES,
        appt.CJ_APPT_TAX_RATE as TAX_RATE,
        appt.CJ_APPT_GROSS_SUBTOTAL as GROSS_SUBTOTAL,
        appt.CJ_APPT_SUBTOTAL as SUBTOTAL,
        appt.CJ_APPT_QUANTITY as QUANTITY,
        appt.CJ_APPT_MATERIAL_COST as MATERIALS_COST,
        appt.CJ_APPT_LABOR_COST as LABOR_COST
    from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_APPOINTMENTS') }} appt
    left join {{party_organization()}} po
        on appt.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID = po.PO_INTERNAL_PARTY_ID
    where appt.CJ_INTERNAL_APPT_ID is not null
      and appt.CJ_APPT_END_DATE is null
      and {{exclude_invalid_org('po')}}
),

excl_deleted as (
    select *
    from appointments a
    where {{ exclude_deleted_invoices("a", 1,'APPOINTMENT_ID') }}
)

select * 
from excl_deleted

