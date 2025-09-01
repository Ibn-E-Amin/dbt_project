{{ config(
    materialized='table'
) }}

with org_with_open_since as (
    select
        PO_INTERNAL_PARTY_ID,
        case 
            when min(case when RN = 1 then PO_OPEN_SINCE end) = '1900-01-01'
                then min(case when RN = 2 then PO_OPEN_SINCE end)
            else min(case when RN = 1 then PO_OPEN_SINCE end)
        end as PO_OPEN_SINCE
    from (
        select
            PO_INTERNAL_PARTY_ID,
            PO_OPEN_SINCE,
            dense_rank() over (
                partition by PO_INTERNAL_PARTY_ID 
                order by PO_OPEN_SINCE
            ) as RN
        from {{ source('dd_dwh', 'PARTY_ORGANIZATION') }}
    ) ab
    where ab.RN in (1, 2)
    group by PO_INTERNAL_PARTY_ID
),

latest_data as (
    select
        p_org.PO_INTERNAL_PARTY_ID as STORE_ID,
        1 as BRAND_ID,
        p_org.PO_INTERNAL_NAME as STORE_NAME,
        p_org.PO_ACCOUNT_MANAGER as ACCOUNT_MANAGER,
        null as COUNTRY,
        p_org.PO_REGION as REGION,
        org_os.PO_OPEN_SINCE as OPEN_SINCE,
        case 
            when PO_CANCEL_DATE is not null and PO_CANCEL_DATE not in ('None', 'nan') then 'TERMINATED'
            when PO_TRIAL_EXPIRATION_DATE is not null and PO_TRIAL_EXPIRATION_DATE not in ('None', 'nan') then 'TERMINATED'
            when PO_SUSPENDED is null or PO_SUSPENDED = 1 then 'TERMINATED'
            else 'ACTIVE'
        end as STATUS
    from {{ source('dd_dwh', 'PARTY_ORGANIZATION') }} p_org
    left join org_with_open_since org_os
        on p_org.PO_INTERNAL_PARTY_ID = org_os.PO_INTERNAL_PARTY_ID
    where p_org.PO_END_DATE is null
      and {{exclude_invalid_org('p_org')}}
)

select *
from latest_data
