{{ config(materialized='table') }}

with deduplicated as (
    select *,
        -- id,
        -- try_cast(year as int) as year,
        -- try_cast(month as int) as month,
        -- try_cast(target as float) as target,
        -- run_id,
        -- cast(run_time as datetime2) as run_time,
        row_number() over (partition by id, year, month order by run_time desc) as rn
    from {{ source('RAW', 'SM_FORECASTS') }}
    where try_cast(run_time as date) = try_cast('{{ var("refresh_date") }}' as date)

    
)

select *
from deduplicated
where rn = 1