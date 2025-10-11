--===============================================--
--======== DWH_PARTY_ORGANIZATION_PERSON MODEL ==--
--===============================================--

{% set SCRIPT = var('script', 'INITIAL') %}
{% set RUN_ID = var('run_id', 'manual_run') %}
{% set BRAND = var('brand', 'Wallaby') %}
{% set ALGORITHM = var('algorithm', 'SHA2_256') %}
{% set PARTY_TYPE_CODE = var('party_type_code', 1) %}

--===============================================--
--======== POST-HOOK FOR INCREMENTAL RUNS =======--
--===============================================--
{% set post_hook_sql %}
UPDATE target
SET 
    target.PA_END_DATE = u.RUN_TIME,
    target.PA_UPDATED_RUN_ID = '{{ RUN_ID }}'
FROM {{ this }} AS target
INNER JOIN (
    SELECT
        o.ID AS CHANGED_RECORD_ID
        ,MAX(CAST(o.RUN_TIME AS DATE)) AS RUN_TIME
    FROM {{ ref('STG_SM_PERSON_USERS') }} AS o
    LEFT JOIN {{ ref('STG_SM_PERSON_ACCOUNT_MANAGERS') }} AS a
      ON o.ID = a.ID
    INNER JOIN {{ this }} AS p
      ON o.ID = p.PA_INTERNAL_PARTY_ID
    WHERE
        (
            ISNULL(o.NAME, '') <> ISNULL(p.PA_NAME, '')
            OR ISNULL(CAST(o.ORGANIZATION_ID AS VARCHAR(50)), '') <> ISNULL(CAST(p.PA_ORGANIZATION_ID AS VARCHAR(50)), '')
            OR ISNULL(o.ACTIVE, '') <> ISNULL(p.PA_ACTIVE, '')
            OR ISNULL(o.[ROLE], '') <> ISNULL(p.PA_ROLE, '')
            OR ISNULL(CAST(o.RESULTCODE AS VARCHAR(50)), '') <> ISNULL(CAST(p.PA_RESULT_CODE AS VARCHAR(50)), '')
            OR ISNULL(NULLIF(o.MESSAGE, ''), '') <> ISNULL(p.PA_MESSAGE, '')
            OR ISNULL(NULLIF(o.APIKEY, ''), '') <> ISNULL(p.PA_API_KEY, '')
            OR (CASE WHEN a.ID IS NOT NULL THEN 1 ELSE 0 END) <> COALESCE(p.PA_ACCOUNT_MANAGER, 0)
        )
        AND p.PA_END_DATE IS NULL
        AND p.PA_START_DATE <= CAST(o.RUN_TIME AS DATE)
    GROUP BY o.ID
) AS u
  ON target.PA_INTERNAL_PARTY_ID = u.CHANGED_RECORD_ID
WHERE target.PA_END_DATE IS NULL;
{% endset %}

{{
  config(
    materialized='incremental',
    unique_key='PA_PARTY_ID',
    post_hook = [post_hook_sql] if SCRIPT == 'INCREMENTAL' else []
  )
}}

--===============================================--
--======== BUILD NEW ROWS FROM STAGING ==========--
--===============================================--

WITH BASE AS (

    -- --------------------------------------------------------------------
    -- New rows: users observed today that do not yet exist in DWH
    -- --------------------------------------------------------------------
    select
        hashbytes('{{ ALGORITHM }}',
            concat(cast(u.id as nvarchar(max)), '|', {{ PARTY_TYPE_CODE }}, '|', '{{ BRAND }}')
        ) as pa_party_id,
        cast(u.id as int) as pa_internal_party_id,
        {{ PARTY_TYPE_CODE }} as pa_party_type_code,
        cast(u.organization_id as int) as pa_organization_id,
        cast(u.name as varchar(255)) as pa_name,
        cast(u.active as varchar(5)) as pa_active,
        cast(u.[role] as varchar(100)) as pa_role,
        try_cast(u.resultcode as int) as pa_result_code,
        case when u.message is not null and u.message not in ('','nan','NONE') then cast(u.message as varchar(255)) else 'nan' end as pa_message,
        case when u.apikey is not null and u.apikey not in ('','nan','NONE') then cast(u.apikey as varchar(255)) else 'nan' end as pa_api_key,
        case when am.id is not null then 1 else 0 end as pa_account_manager,
        '{{ RUN_ID }}' as pa_created_run_id,
        min(cast(u.run_time as date)) as pa_start_date
    from {{ ref('STG_SM_PERSON_USERS') }} u
    left join {{ ref('STG_SM_PERSON_ACCOUNT_MANAGERS') }} am
      on u.id = am.id
    left join {{ this }} p
      on p.pa_internal_party_id = u.id
    where p.pa_internal_party_id is null
    group by u.id, u.organization_id, u.name, u.active, u.[role], u.resultcode, u.message, u.apikey, am.id

    {% if SCRIPT == 'INCREMENTAL' %}
    union all

    -- --------------------------------------------------------------------
    -- Re-insert rows for incremental runs where prior DWH row is closed
    -- (i.e. the existing DWH row has an end_date and we need a new row)
    -- --------------------------------------------------------------------
    select
        hashbytes('{{ ALGORITHM }}',
            concat(cast(u.id as nvarchar(max)), '|', {{ PARTY_TYPE_CODE }}, '|', '{{ BRAND }}')
        ) as pa_party_id,
        cast(u.id as int) as pa_internal_party_id,
        {{ PARTY_TYPE_CODE }} as pa_party_type_code,
        cast(u.organization_id as int) as pa_organization_id,
        cast(u.name as varchar(255)) as pa_name,
        cast(u.active as varchar(5)) as pa_active,
        cast(u.[role] as varchar(100)) as pa_role,
        try_cast(u.resultcode as int) as pa_result_code,
        case when u.message is not null and u.message not in ('','nan','NONE') then cast(u.message as varchar(255)) else 'nan' end as pa_message,
        case when u.apikey is not null and u.apikey not in ('','nan','NONE') then cast(u.apikey as varchar(255)) else 'nan' end as pa_api_key,
        case when am.id is not null then 1 else 0 end as pa_account_manager,
        '{{ RUN_ID }}' as pa_created_run_id,
        min(cast(u.run_time as datetime2)) as pa_start_date
    from {{ ref('STG_SM_PERSON_USERS') }} u
    left join {{ ref('STG_SM_PERSON_ACCOUNT_MANAGERS') }} am
      on u.id = am.id
    inner join {{ this }} p
      on p.pa_internal_party_id = u.id
    where p.pa_end_date is not null
    group by u.id, u.organization_id, u.name, u.active, u.[role], u.resultcode, u.message, u.apikey, am.id
    {% endif %}

),

--------------------------------------------------------------------------------
-- Step 2: Compute SCD (lead-based end dates + updated_run_id)
--------------------------------------------------------------------------------
scd as (
    select
        *,
        lead(pa_start_date) over (
            partition by pa_internal_party_id
            order by pa_start_date asc
        ) as pa_end_date,
        lead(pa_created_run_id) over (
            partition by pa_internal_party_id
            order by pa_start_date asc
        ) as pa_updated_run_id,
        '{{ BRAND }}' as pa_brand
    from base
)

select *
from scd
{% if is_incremental() %}
where 1=1
{% endif %}
;
