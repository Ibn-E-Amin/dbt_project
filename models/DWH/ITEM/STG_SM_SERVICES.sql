{{ config(materialized='table') }}

with dedup as (
    select
        s.ID,
        s.NAME,
        s.DESCRIPTION,
        s.UNITPRICE as price,
        null as cost,                 -- not in services
        s.BASEDURATION as duration,
        null as stock,                -- not in services
        null as reorder_point,        -- not in services
        null as quantity_on_order,    -- not in services
        null as vendor,               -- not in services
        null as vendor_part_no,       -- not in services
        s.REVENUECATEGORY as revenue_category,
        s.ORGANIZATION_ID as store_id,
        s.TAXABLE,
        s.ACTIVE,
        1 as internal,                -- mark as service
        s.ROLE,
        null as exclude_from_qbo,     -- not in services
        s.FINISH_ACTION,
        null as income_account,       -- not in services
        cast(s.RUN_TIME as datetime2) as run_time,
        cast(s.UPDATED_AT as datetime2) as updatedat,
        row_number() over (partition by s.ID order by s.RUN_TIME desc) as rn
    from {{ source('RAW', 'SM_SERVICES') }} s
    {% if var('refresh_date', none) is not none %}
      where cast(s.run_time as date) = '{{ var("refresh_date") }}'
    {% endif %}
)

select *
from dedup
where rn = 1
