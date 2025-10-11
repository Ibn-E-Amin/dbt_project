{% set SCRIPT = var('script', 'INITIAL') %}
{% set RUN_ID = var('run_id', 'manual_run') %}
{% set BRAND = var('brand', 'Wallaby') %}
{% set ALGORITHM = var('algorithm', 'SHA2_256') %}
{% set ITEM_TYPE_SERVICE = 0 %}
{% set ITEM_TYPE_PART = 1 %}

--===============================================--
--======== UPDATE EXISTING ITEMS ================--
--===============================================--
{% set update_sql %}
    UPDATE target
    SET 
        target.ITM_END_DATE = u.RUN_TIME,
        target.ITM_UPDATED_RUN_ID = '{{ RUN_ID }}'
    FROM {{ this }} AS target
    INNER JOIN (
        SELECT
            b.ID AS CHANGED_RECORD_ID
            ,MAX(CAST(COALESCE(b.UPDATEDAT, b.RUN_TIME) AS DATE)) AS RUN_TIME
        FROM (
            SELECT *, {{ ITEM_TYPE_SERVICE }} AS ITM_TYPE FROM {{ ref('STG_SM_SERVICES') }}
            UNION ALL
            SELECT *, {{ ITEM_TYPE_PART }} AS ITM_TYPE FROM {{ ref('STG_SM_PARTS') }}
        ) AS b
        INNER JOIN {{ this }} AS itm
            ON b.ID = itm.ITM_INTERNAL_ID
           AND {{ ITEM_TYPE_SERVICE }} = itm.ITM_TYPE
        WHERE (
            ISNULL(itm.ITM_NAME,'nan') <> ISNULL(b.NAME,'nan')
            OR ISNULL(itm.ITM_DESCRIPTION,'nan') <> ISNULL(b.DESCRIPTION,'nan')
            OR ISNULL(itm.ITM_UNIT_PRICE,0.0) <> ISNULL(b.PRICE,0.0)
            OR ISNULL(itm.ITM_ACTIVE,0) <> ISNULL(b.ACTIVE,0)
        )
        AND itm.ITM_END_DATE IS NULL
        AND itm.ITM_START_DATE <= CAST(COALESCE(b.UPDATEDAT, b.RUN_TIME) AS DATE)
        GROUP BY b.ID
    ) AS u
    ON target.ITM_INTERNAL_ID = u.CHANGED_RECORD_ID
    WHERE target.ITM_END_DATE IS NULL;
{% endset %}

{{ 
  config(
    materialized='incremental',
    unique_key='ITM_INTERNAL_ID',
    post_hook=[update_sql] if SCRIPT == 'INCREMENTAL' else []
  ) 
}}

--===============================================--
--======== GATHER ALL ITEMS (SERVICES + PARTS) ==--
--===============================================--
WITH BASE AS (
    SELECT 
        ID, NAME, DESCRIPTION, PRICE, DURATION, COST,
        STOCK, REORDER_POINT, QUANTITY_ON_ORDER, VENDOR,
        VENDOR_PART_NO, REVENUE_CATEGORY, STORE_ID, TAXABLE,
        ACTIVE, INTERNAL, ROLE, EXCLUDE_FROM_QBO,
        FINISH_ACTION, INCOME_ACCOUNT, RUN_TIME, UPDATEDAT,
        {{ ITEM_TYPE_SERVICE }} AS ITM_TYPE
    FROM {{ ref('STG_SM_SERVICES') }}

    UNION ALL

    SELECT 
        ID, NAME, DESCRIPTION, PRICE, DURATION, COST,
        STOCK, REORDER_POINT, QUANTITY_ON_ORDER, VENDOR,
        VENDOR_PART_NO, REVENUE_CATEGORY, STORE_ID, TAXABLE,
        ACTIVE, INTERNAL, ROLE, EXCLUDE_FROM_QBO,
        FINISH_ACTION, INCOME_ACCOUNT, RUN_TIME, UPDATEDAT,
        {{ ITEM_TYPE_PART }} AS ITM_TYPE
    FROM {{ ref('STG_SM_PARTS') }}
),

--===============================================--
--======== MAP TO DWH COLUMNS ===================--
--===============================================--
MIN_DATE AS (
    select
        b.id as itm_internal_id,
        hashbytes('{{ ALGORITHM }}', concat(b.id, '|', b.revenue_category, '|', '{{ BRAND }}')) as itm_group_id,
        b.name as itm_name,
        b.revenue_category as itm_revenue_category,
        b.description as itm_description,
        b.itm_type,
        b.store_id as itm_store_id,
        b.price as itm_base_rpice,
        b.price as itm_unit_price,
        b.cost as itm_unit_cost,
        b.duration as itm_base_duration,
        b.duration as itm_unit_duration,
        b.taxable as itm_taxable,
        b.active as itm_active,
        b.internal as itm_internal,
        b.role as itm_role,
        b.stock as itm_stock,
        b.reorder_point as itm_reorder_point,
        b.quantity_on_order as itm_quantity_on_order,
        b.exclude_from_qbo as itm_exclude_from_qbo,
        b.finish_action as itm_finish_action,
        b.income_account as itm_income_account,
        b.vendor as itm_vendor,
        b.vendor_part_no as itm_vendor_part_no,
        cast(b.run_time as datetime2) as itm_queue_date,
        '{{ RUN_ID }}' as itm_created_run_id,
        min(cast(coalesce(b.updatedat, b.run_time) as date)) as itm_start_date
    from base b
    left join {{ this }} itm
        on itm.itm_internal_id = b.id
       and itm.itm_type = b.itm_type
    where itm.itm_internal_id is null
    group by 
        b.id, b.name, b.revenue_category, b.description, b.itm_type, b.store_id, b.price, b.cost,
        b.duration, b.taxable, b.active, b.internal, b.role, b.stock, b.reorder_point, b.quantity_on_order,
        b.exclude_from_qbo, b.finish_action, b.income_account, b.vendor, b.vendor_part_no, b.run_time
),

orgs_to_insert as (
    select * from min_date

    {% if SCRIPT == 'INCREMENTAL' %}
    union
    select
        b.id as itm_internal_id,
        hashbytes('{{ ALGORITHM }}', concat(b.id, '|', b.revenue_category, '|', '{{ BRAND }}')) as itm_group_id,
        b.name as itm_name,
        b.revenue_category as itm_revenue_category,
        b.description as itm_description,
        b.itm_type as itm_type,
        b.store_id as itm_store_id,
        b.price as itm_base_rpice,
        b.price as itm_unit_price,
        b.cost as itm_unit_cost,
        b.duration as itm_base_duration,
        b.duration as itm_unit_duration,
        b.taxable as itm_taxable,
        b.active as itm_active,
        b.internal as itm_internal,
        b.role as itm_role,
        b.stock as itm_stock,
        b.reorder_point as itm_reorder_point,
        b.quantity_on_order as itm_quantity_on_order,
        b.exclude_from_qbo as itm_exclude_from_qbo,
        b.finish_action as itm_finish_action,
        b.income_account as itm_income_account,
        b.vendor as itm_vendor,
        b.vendor_part_no as itm_vendor_part_no,
        cast(b.run_time as datetime2) as itm_queue_date,
        '{{ RUN_ID }}' as itm_created_run_id,
        min(cast(coalesce(b.updatedat, b.run_time) as date)) as itm_start_date
    from base b
    left join (
        select itm_internal_id, itm_type
        from {{ this }}
        where itm_end_date is null
    ) itm
        on itm.itm_internal_id = b.id
       and itm.itm_type = b.itm_type
    where itm.itm_internal_id is not null
    group by 
        b.id, b.name, b.revenue_category, b.description, b.itm_type, b.store_id, b.price, b.cost,
        b.duration, b.taxable, b.active, b.internal, b.role, b.stock, b.reorder_point, b.quantity_on_order,
        b.exclude_from_qbo, b.finish_action, b.income_account, b.vendor, b.vendor_part_no, b.run_time
    {% endif %}
),

-- ============================================================
-- Step 3: Add end dates + brand
-- ============================================================
new_rows as (
    select * ,
        lead(itm_start_date) over (partition by itm_type, itm_internal_id order by itm_internal_id, itm_start_date asc) as itm_end_date,
        lead(itm_created_run_id) over (partition by itm_type, itm_internal_id order by itm_internal_id, itm_start_date asc) as itm_updated_run_id,
        '{{ BRAND }}' as itm_brand
    from orgs_to_insert
)

-- ============================================================
-- Final Select
-- ============================================================
select * from new_rows;