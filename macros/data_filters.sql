{% macro order_invoice() %}
    (
        select *
        from {{ source('DD_DWH', 'ORDER_INVOICE') }}
    )
{% endmacro %}

{% macro party_organization() %}
    (
        select *
        from {{ source('DD_DWH', 'PARTY_ORGANIZATION') }}
        where PO_END_DATE is null
    )
{% endmacro %}

-- macros/filters.sql

{% macro exclude_invalid_org(alias) %}
    coalesce({{ alias }}.PO_INTERNAL_NAME, 'nan') not in (
        'Download limit reached'
        ,'WW - Test Account'
        ,'WW - Franchising'
        ,'WW - Special Projects'
        ,'WW - Holding'
        ,'WW - Parking Lot'
        ,'Wallaby Windows - Test Account'
        ,'Wallaby Windows - Franchising'
        ,'Wallaby Windows Franchising'
        ,'Wallaby Windows - Special Projects'
        ,'Wallaby Windows - Holding'
        ,'Wallaby Windows - Parking Lot'
    )
{% endmacro %}


{% macro exclude_deleted_invoices(alias, alias2, columnName) %}
    {{ alias }}.{{columnName}} not in (
        select cast(ENTITYID as int)
        from {{ source('PROD_BK', 'SM_DELETION_EVENTS') }}
        where ENTITYTYPE = {{alias2}}
    )
{% endmacro %}


{% macro exclude_unapproved_invoices(alias) %}
    {{ alias }}.INVOICE_ID not in (
        select i.INVOICE_ID
        from {{ ref('D_INVOICE_STATUS')  }} i
        where i.END_DATE is null
          and i.STATUS in ('Unapproved', 'Voided')
    )
{% endmacro %}
