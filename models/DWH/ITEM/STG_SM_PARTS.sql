{{ config(materialized='table') }}

with dedup as (
    select
        p.ID,
        p.NAME,
        p.DESCRIPTION,
        p.UNITPRICE as price,
        p.UNITCOST as cost,
        p.UNITDURATION as duration,
        p.QUANTITYONHAND as stock,
        p.REORDERPOINT as reorder_point,    -- ✅ fixed alias
        p.QTY_ON_ORDER as quantity_on_order,
        p.VENDOR as vendor,
        p.VENDOR_PART_NO as vendor_part_no,
        p.REVENUE_CATEGORY as revenue_category,
        p.ORGANIZATIONID as store_id,
        p.TAXABLE,
        p.ACTIVE,
        p.INTERNAL,
        null as role,                       -- not in parts
        p.EXCLUDE_FROM_QBO_SUMMARIZE as exclude_from_qbo,
        null as finish_action,
        p.INCOMEACCOUNT as income_account,
        cast(p.RUN_TIME as datetime2) as run_time,
        cast(p.UPDATED_AT as datetime2) as updatedat,
        row_number() over (partition by p.ID order by p.RUN_TIME desc) as rn
    from {{ source('RAW', 'SM_PARTS') }} p
    {% if var('refresh_date', none) is not none %}
      where cast(p.run_time as date) = '{{ var("refresh_date") }}'
    {% endif %}
)

select *
from dedup
where rn = 1
