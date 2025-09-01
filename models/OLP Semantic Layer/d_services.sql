{{config(materialized='table')}}

with itm_services as (
    select distinct
        itm.ITM_REVENUE_CATEGORY as SERVICE_CATEGORY,
        itm.ITM_NAME as SERVICE
    from {{ source('dd_dwh', 'ITEM') }} itm
    left join {{ party_organization() }} po
        on itm.ITM_STORE_ID = po.PO_INTERNAL_PARTY_ID
    where itm.ITM_TYPE = 0
      and {{ exclude_invalid_org('po') }}
      and itm.ITM_REVENUE_CATEGORY <> 'nan'
),

inv_services as (
    select distinct
        inv.ORD_INV_REVENUE_CATEGORY as SERVICE_CATEGORY,
        inv.ORD_INV_SERVICE_NAME as SERVICE
    from {{ source('dd_dwh', 'ORDER_INVOICE') }} inv
    left join {{ party_organization() }} po
        on inv.ORD_INV_INTERNAL_ORGANIZATION_PARTY_ID = po.PO_INTERNAL_PARTY_ID
    where {{ exclude_invalid_org('po') }}
      and inv.ORD_INV_REVENUE_CATEGORY <> 'nan'
),

prop_services as (
    select distinct
        prop.CJ_REVENUE_CATEGORY as SERVICE_CATEGORY,
        prop.CJ_SERVICE_NAME as SERVICE
    from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_PROPOSALS') }} prop
    left join {{ party_organization() }} po
        on prop.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID = po.PO_INTERNAL_PARTY_ID
    where {{ exclude_invalid_org('po') }}
      and prop.CJ_REVENUE_CATEGORY <> 'nan'
),

unioned as (
    select * from itm_services
    union
    select * from inv_services
    union
    select * from prop_services
),

deduped as (
    select distinct
        row_number() over(order by service_category, service) as SERVICE_ID,
        SERVICE_CATEGORY,
        SERVICE
    from unioned
)

select * 
from deduped