{{ config(materialized='table') }}

with source_proposals as (

    select distinct
        CJ_INTERNAL_PROP_ID as PROPOSAL_ID,
        CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID as STORE_ID,
        CJ_PROP_INTERNAL_CUSTOMER_PARTY_ID as LEAD_ID,
        CJ_SERVICE_NAME as SERVICE,
        CJ_REVENUE_CATEGORY as REVENUE_CATEGORY,
        CJ_PROP_TYPE as TYPE,
        CJ_PROP_TITLE as PROPOSAL_TITLE,
        CJ_PROP_OWNER as TEAM_MEMBER
    from {{ source('dd_dwh', 'CUSTOMER_JOURNEY_PROPOSALS') }} prop
    left join {{ party_organization() }} po
        on po.PO_INTERNAL_PARTY_ID = prop.CJ_PROP_INTERNAL_ORGANIZATION_PARTY_ID
    where {{ exclude_invalid_org('po') }}
    and CJ_PROP_END_DATE is null
),

final as (
    select
        p.PROPOSAL_ID,
        p.STORE_ID,
        p.LEAD_ID,
        s.SERVICE_ID,
        l.CAMPAIGN_ID,
        p.TYPE,
        p.PROPOSAL_TITLE,
        p.TEAM_MEMBER
    from source_proposals p
    left join {{ ref('d_services') }} s
        on s.SERVICE = p.SERVICE
       and s.SERVICE_CATEGORY = p.REVENUE_CATEGORY
    left join {{ ref('d_lead') }} l
        on l.LEAD_ID = p.LEAD_ID
    -- deletion filter
    where p.PROPOSAL_ID not in (
        select cast(ENTITYID as int)
        from {{ source('prod_bk', 'SM_DELETION_EVENTS') }}
        where ENTITYTYPE = 2
    )
),

excl_deleted as (
    select *
    from final f
    where {{ exclude_deleted_invoices("f", 2, 'PROPOSAL_ID') }}
)

select * from excl_deleted
