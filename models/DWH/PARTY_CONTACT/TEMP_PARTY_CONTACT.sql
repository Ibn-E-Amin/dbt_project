{{ config(materialized="table") }}

with deleted as (
    select *
    from {{ source('RAW', 'SM_DELETION_EVENTS') }}
    where EntityTYPE = 0
),

contacts as (
    select
        cast(c.id as int) as id,
        cast(coalesce(c.organization_id, 0) as int) as organization_id,
        'CONTACTS' as pc_party_origin,

        hashbytes('{{ var("algorithm", "SHA2_256") }}', concat(c.campaign, '|', '{{ var("brand", "Wallaby") }}')) as cam_campaign_id,
        hashbytes('{{ var("algorithm", "SHA2_256") }}', concat(c.channel, '|', '{{ var("brand", "Wallaby") }}')) as chn_channel_id,

        case when c.utm_campaign not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.utm_campaign, 'nan') as nvarchar(4000))
             else 'nan' end as utm_campaign,

        case when c.campaign not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.campaign, 'nan') as nvarchar(4000))
             else 'nan' end as campaign,

        case when c.channel not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.channel, 'nan') as nvarchar(4000))
             else 'nan' end as channel,

        case when c.accountingclass not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.accountingclass, 'nan') as nvarchar(4000))
             else 'nan' end as accounting_class,

        case when c.name not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.name, 'nan') as nvarchar(4000))
             else 'nan' end as name,

        case when c.company not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.company, 'nan') as nvarchar(4000))
             else 'nan' end as company,

        case when c.created_by not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.created_by, 'nan') as nvarchar(4000))
             else 'nan' end as created_by,

        case when c.[role] not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.[role], 'nan') as nvarchar(4000))
             else 'nan' end as role,

        case when c.category not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.category, 'nan') as nvarchar(4000))
             else 'nan' end as category,

        case when c.servicecity not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.servicecity, 'nan') as nvarchar(4000))
             else 'nan' end as service_city,

        case when c.servicestate not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.servicestate, 'nan') as nvarchar(4000))
             else 'nan' end as service_state,

        case when charindex('.', c.servicezip) > 0
             then left(c.servicezip, charindex('.', c.servicezip) - 1)
             else coalesce(c.servicezip, 'nan') end as service_postal_code,

        case when c.longitude not in ('', 'NAN', 'NONE')
             then cast(c.longitude as float)
             else 0 end as longitude,

        case when c.latitude not in ('', 'NAN', 'NONE')
             then cast(c.latitude as float)
             else 0 end as latitude,

        case when c.cardonfile not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.cardonfile, 0) as bit)
             end as cardonfile,

        case when c.payment_on_file_kind not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.payment_on_file_kind, 'nan') as nvarchar(4000))
             else 'nan' end as payment_on_file_kind,

        case when c.tags not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.tags, 'nan') as nvarchar(4000))
             else 'nan' end as tags,

        cast(c.createdat as datetime2) as createdat,
        cast(c.updatedat as datetime2) as updatedat,

        case when c.api_key not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.api_key, 'nan') as nvarchar(4000))
             else 'nan' end as api_key,

        case when c.lifetime_value not in ('', 'NAN', 'NONE')
             then cast(c.lifetime_value as float)
             else 0.0 end as lifetime_value,

        case when c.flash_message not in ('', 'NAN', 'NONE')
             then cast(coalesce(c.flash_message, 'nan') as nvarchar(4000))
             else 'nan' end as flash_message,

        cast(c.run_time as datetime2) as run_time,
        cast(d.queuedat as datetime2) as deleted_at,
        case when d.id is not null then 1 else 0 end as delete_flag,
        c.run_id

    from {{ ref('STG_SM_CONTACT') }} c
    left join deleted d
      on d.entityid = c.id
)

select *
from contacts
