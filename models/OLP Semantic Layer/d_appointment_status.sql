

{{ config(materialized='table') }}

--===============================================--
--======== APPOINTMENT STATUS ===================--
--===============================================--
WITH APPOINTMENT_STATUS AS (
    SELECT
        APPT.CJ_INTERNAL_APPT_ID AS APPOINTMENT_ID
        ,APPT.CJ_APPT_STATUS AS STATUS
        ,APPT.CJ_APPT_START_DATE AS START_DATE
        ,APPT.CJ_APPT_END_DATE AS END_DATE
    FROM {{ source('DD_DWH', 'CUSTOMER_JOURNEY_APPOINTMENTS') }} AS APPT
    LEFT JOIN {{ party_organization() }} AS PO
        ON APPT.CJ_APPT_INTERNAL_ORGANIZATION_PARTY_ID = PO.PO_INTERNAL_PARTY_ID
    WHERE APPT.CJ_INTERNAL_APPT_ID IS NOT NULL
      AND {{ exclude_invalid_org('PO') }}
),

EXCL_DELETED AS (
    SELECT *
    FROM APPOINTMENT_STATUS AS APPT
    WHERE {{ exclude_deleted_invoices('APPT', 1, 'APPOINTMENT_ID') }}
)

SELECT *
FROM EXCL_DELETED
;
