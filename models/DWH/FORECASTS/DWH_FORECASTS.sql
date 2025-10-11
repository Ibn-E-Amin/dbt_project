
{% set SCRIPT = var('script', 'INITIAL') %}
{% set RUN_ID = var('run_id', 'manual_run') %}
{% set BRAND = var('brand', 'Wallaby') %}
{% set ALGORITHM = var('algorithm', 'SHA2_256') %}
{% set PARTY_TYPE_CODE = var('party_type_code', 2) %}

--===============================================--
--======== UPDATE EXISTING FORECASTS ============--
--===============================================--
{% set post_hook_sql %}
    UPDATE target
    SET 
        target.FOR_END_DATE = u.RUN_TIME,
        target.FOR_UPDATED_RUN_ID = '{{ RUN_ID }}'
    FROM {{ this }} AS target
    INNER JOIN (
        SELECT
            f.ID AS CHANGED_RECORD_ID
            ,f.YEAR
            ,f.MONTH
            ,MAX(CAST(f.RUN_TIME AS DATE)) AS RUN_TIME
        FROM {{ ref('STG_SM_FORECASTS') }} AS f
        INNER JOIN {{ this }} AS forc
            ON f.ID = forc.FOR_INTERNAL_PARTY_ID
            AND COALESCE(f.YEAR, 1990) = COALESCE(forc.FOR_YEAR, 1990)
            AND COALESCE(f.MONTH, 0) = COALESCE(forc.FOR_MONTH, 0)
        WHERE
            CAST(f.TARGET AS FLOAT) <> COALESCE(forc.FOR_TARGET, 0)
            AND forc.FOR_END_DATE IS NULL
            AND forc.FOR_START_DATE <= CAST(f.RUN_TIME AS DATE)
        GROUP BY f.ID, f.YEAR, f.MONTH
    ) AS u
    ON target.FOR_INTERNAL_PARTY_ID = u.CHANGED_RECORD_ID
    AND COALESCE(target.FOR_YEAR, 1990) = COALESCE(u.YEAR, 1990)
    AND COALESCE(target.FOR_MONTH, 0) = COALESCE(u.MONTH, 0)
    WHERE target.FOR_END_DATE IS NULL;
{% endset %}

{{
  config(
    materialized='incremental',
    unique_key='FOR_ID',
    post_hook = [post_hook_sql] if SCRIPT == 'INCREMENTAL' else []
  )
}}

--===============================================--
--======== NEW ROWS NOT YET IN FORECAST =========--
--===============================================--
WITH MIN_DATE AS (
    SELECT
        HASHBYTES('{{ ALGORITHM }}',
            CONCAT(F.ID, '|', F.YEAR, '|', F.MONTH, '|', {{ PARTY_TYPE_CODE }}, '|', '{{ BRAND }}')
        ) AS FOR_ID
        ,CAST(F.ID AS INT) AS FOR_INTERNAL_PARTY_ID
        ,CAST(F.YEAR AS INT) AS FOR_YEAR
        ,CAST(F.MONTH AS INT) AS FOR_MONTH
        ,CAST(F.TARGET AS FLOAT) AS FOR_TARGET
        ,MIN(F.RUN_ID) AS FOR_CREATED_RUN_ID
        ,MIN(CAST(F.RUN_TIME AS DATE)) AS FOR_START_DATE
    FROM {{ ref('STG_SM_FORECASTS') }} AS F
    LEFT JOIN {{ this }} AS T
      ON T.FOR_INTERNAL_PARTY_ID = F.ID
     AND COALESCE(T.FOR_YEAR, 1990) = COALESCE(F.YEAR, 1990)
     AND COALESCE(T.FOR_MONTH, 0) = COALESCE(F.MONTH, 0)
    WHERE T.FOR_INTERNAL_PARTY_ID IS NULL
    GROUP BY F.ID, F.YEAR, F.MONTH, F.TARGET

    --===============================================--
    --======== INCREMENTAL RE-INSERTS ===============--
    --===============================================--
    {% if SCRIPT == 'INCREMENTAL' %}
    UNION ALL
    SELECT
        HASHBYTES('{{ ALGORITHM }}',
            CONCAT(F.ID, '|', F.YEAR, '|', F.MONTH, '|', {{ PARTY_TYPE_CODE }}, '|', '{{ BRAND }}')
        ) AS FOR_ID
        ,CAST(F.ID AS INT) AS FOR_INTERNAL_PARTY_ID
        ,CAST(F.YEAR AS INT) AS FOR_YEAR
        ,CAST(F.MONTH AS INT) AS FOR_MONTH
        ,CAST(F.TARGET AS FLOAT) AS FOR_TARGET
        ,MIN(F.RUN_ID) AS FOR_CREATED_RUN_ID
        ,MIN(CAST(F.RUN_TIME AS DATETIME2)) AS FOR_START_DATE
    FROM {{ ref('STG_SM_FORECASTS') }} AS F
    INNER JOIN {{ this }} AS T
      ON T.FOR_INTERNAL_PARTY_ID = F.ID
     AND COALESCE(T.FOR_YEAR, 1990) = COALESCE(F.YEAR, 1990)
     AND COALESCE(T.FOR_MONTH, 0) = COALESCE(F.MONTH, 0)
    WHERE T.FOR_END_DATE IS NOT NULL
    GROUP BY F.ID, F.YEAR, F.MONTH, F.TARGET
    {% endif %}
),

NEW_ROWS AS (
    SELECT
        *
        ,LEAD(FOR_START_DATE) OVER (
            PARTITION BY FOR_INTERNAL_PARTY_ID, FOR_MONTH, FOR_YEAR
            ORDER BY FOR_START_DATE ASC
        ) AS FOR_END_DATE
        ,LEAD(FOR_CREATED_RUN_ID) OVER (
            PARTITION BY FOR_INTERNAL_PARTY_ID, FOR_MONTH, FOR_YEAR
            ORDER BY FOR_START_DATE ASC
        ) AS FOR_UPDATED_RUN_ID
        ,'{{ BRAND }}' AS FOR_BRAND
    FROM MIN_DATE
)

SELECT *
FROM NEW_ROWS
{% if is_incremental() %}
WHERE 1=1
{% endif %}
;
