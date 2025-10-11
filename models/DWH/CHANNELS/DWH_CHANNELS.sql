
{% set SCRIPT = var('script', 'INITIAL') %}
{% set RUN_ID = var('run_id', 'manual_run') %}
{% set BRAND = var('brand', 'Wallaby') %}
{% set ALGORITHM = var('algorithm', 'SHA2_256') %}

--===============================================--
--======== UPDATE EXISTING CHANNELS =============--
--===============================================--
{% set update_sql %}
    UPDATE target
    SET 
        target.CHN_END_DATE = u.RUN_TIME,
        target.CHN_UPDATED_RUN_ID = '{{ RUN_ID }}'
    FROM {{ this }} AS target
    INNER JOIN (
        SELECT
            c.ID AS CHANGED_RECORD_ID
            ,MAX(CAST(c.RUN_TIME AS DATE)) AS RUN_TIME
        FROM {{ ref('STG_SM_CHANNELS') }} AS c
        INNER JOIN {{ this }} AS chn
            ON c.ID = chn.CHN_CHANNEL_INTERNAL_ID
        WHERE
            (
                ISNULL(c.NAME, '') <> ISNULL(chn.CHN_CHANNEL_NAME, '')
                OR ISNULL(c.TYPE, '') <> ISNULL(chn.CHN_CHANNEL_TYPE, '')
            )
            AND chn.CHN_END_DATE IS NULL
            AND chn.CHN_START_DATE <= CAST(c.RUN_TIME AS DATE)
        GROUP BY c.ID
    ) AS u
    ON target.CHN_CHANNEL_INTERNAL_ID = u.CHANGED_RECORD_ID
    WHERE target.CHN_END_DATE IS NULL;
{% endset %}

{{
  config(
    materialized='incremental',
    unique_key='CHN_CHANNEL_ID',
    post_hook=[update_sql] if SCRIPT == 'INCREMENTAL' else []
  )
}}

--===============================================--
--======== IDENTIFY NEW CHANNELS ================--
--===============================================--
WITH CHANNELS AS (
    SELECT
        CASE 
            WHEN C.CHANNEL NOT IN ('', 'NAN', 'NONE') 
            THEN CAST(C.CHANNEL AS VARCHAR(100)) 
        END AS CHANNEL
        ,NULL AS CHN_CHANNEL_INTERNAL_ID
        ,CAST(C.RUN_TIME AS DATE) AS RUN_TIME
    FROM {{ ref('STG_SM_CONTACT') }} AS C
    LEFT JOIN {{ ref('STG_SM_CHANNELS') }} AS CH
        ON C.CHANNEL = CH.NAME
    LEFT JOIN {{ this }} AS OCH
        ON C.CHANNEL = OCH.CHN_CHANNEL_NAME
    WHERE CH.NAME IS NULL
      AND OCH.CHN_CHANNEL_NAME IS NULL

    UNION

    SELECT
        CASE 
            WHEN CH.NAME NOT IN ('', 'NAN', 'NONE') 
            THEN CAST(CH.NAME AS VARCHAR(100)) 
        END AS CHANNEL
        ,CH.ID AS CHN_CHANNEL_INTERNAL_ID
        ,CAST(CH.RUN_TIME AS DATE) AS RUN_TIME
    FROM {{ ref('STG_SM_CHANNELS') }} AS CH
    LEFT JOIN {{ this }} AS OCH
        ON CH.ID = OCH.CHN_CHANNEL_INTERNAL_ID
    WHERE OCH.CHN_CHANNEL_INTERNAL_ID IS NULL
),

MIN_DATE AS (
    SELECT
        CHANNEL
        ,CHN_CHANNEL_INTERNAL_ID
        ,'{{ RUN_ID }}' AS CHN_CREATED_RUN_ID
        ,MIN(RUN_TIME) AS CHN_START_DATE
    FROM CHANNELS
    GROUP BY CHANNEL, CHN_CHANNEL_INTERNAL_ID
),

NEW_ROWS AS (
    SELECT
        HASHBYTES('{{ ALGORITHM }}', CONCAT(CHANNEL, '|', '{{ BRAND }}')) AS CHN_CHANNEL_ID
        ,CHN_CHANNEL_INTERNAL_ID
        ,CHANNEL AS CHN_CHANNEL_NAME
        ,CHN_CREATED_RUN_ID
        ,CHN_START_DATE
        ,LEAD(CHN_START_DATE) OVER (PARTITION BY CHANNEL ORDER BY CHN_START_DATE ASC) AS CHN_END_DATE
        ,LEAD(CHN_CREATED_RUN_ID) OVER (PARTITION BY CHANNEL ORDER BY CHN_START_DATE ASC) AS CHN_UPDATED_RUN_ID
        ,'{{ BRAND }}' AS CHN_BRAND
    FROM MIN_DATE
    WHERE CHANNEL IS NOT NULL
)

SELECT
    NR.CHN_CHANNEL_ID
    ,NR.CHN_CHANNEL_INTERNAL_ID     
    ,NR.CHN_CHANNEL_NAME
    ,NR.CHN_CREATED_RUN_ID
    ,NR.CHN_START_DATE
    ,NR.CHN_END_DATE
    ,NR.CHN_UPDATED_RUN_ID
    ,NR.CHN_BRAND
FROM NEW_ROWS NR
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ this }} PP
    WHERE PP.CHN_CHANNEL_NAME = NR.CHN_CHANNEL_NAME
);
