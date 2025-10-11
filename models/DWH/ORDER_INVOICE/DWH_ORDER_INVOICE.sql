{% set SCRIPT = var('script', 'INITIAL') %}
{% set RUN_ID = var('run_id', 'manual_run') %}
{% set BRAND = var('brand', 'Wallaby') %}
{% set ALGORITHM = var('algorithm', 'SHA2_256') %}

{% set update_sql %}
    UPDATE target
    SET 
        target.ord_inv_end_date = u.run_time,
        target.ord_inv_updated_run_id = '{{ RUN_ID }}'
    FROM {{ this }} target
    INNER JOIN (
        SELECT
            i.id AS changed_record_id,
            MAX(CAST(COALESCE(i.updatedat, i.run_time) AS DATE)) AS run_time
        FROM {{ ref('STG_SM_INVOICES') }} i
        LEFT JOIN {{ source('RAW', 'SM_DELETION_EVENTS') }} d 
            ON d.entitytype = 3 AND CAST(d.entityid AS INT) = i.id
        LEFT JOIN {{ source('RAW', 'SM_DELETION_EVENTS') }} d_lead
            ON d_lead.entitytype = 0 AND CAST(d_lead.entityid AS INT) = CAST(CAST(i.contactid AS FLOAT) AS INT)
        INNER JOIN {{ this }} inv
            ON i.id = inv.ord_inv_internal_invoice_id
        WHERE inv.ord_inv_end_date IS NULL
          AND (
                cast(i.contactid as int) <> coalesce(inv.ord_inv_internal_customer_party_id, 0)
             or cast(i.organization_id as int) <> coalesce(inv.ord_inv_internal_organization_party_id, 0)
             or hashbytes('{{ ALGORITHM }}', concat(i.service_name, '|', '{{ BRAND }}')) <> inv.ord_inv_itm_id
             or isnull(i.status,'nan') <> isnull(inv.ord_inv_status,'nan')
             or isnull(i.type,'nan') <> isnull(inv.ord_inv_type,'nan')
             or isnull(i.revenue_category,'nan') <> isnull(inv.ord_inv_revenue_category,'nan')
             or isnull(i.service_name,'nan') <> isnull(inv.ord_inv_service_name,'nan')
             or isnull(i.owner,'nan') <> isnull(inv.ord_inv_owner,'nan')
             or isnull(i.invoice_number,'nan') <> isnull(inv.ord_inv_invoice_number,'nan')
             or cast(i.invoice_date as date) <> cast(coalesce(inv.ord_inv_invoice_date, '1900-01-01') as date)
             or cast(i.created as date) <> cast(coalesce(inv.ord_inv_invoice_created, '1900-01-01') as date)
             or cast(i.date_paid as date) <> cast(coalesce(inv.ord_inv_date_paid, '1900-01-01') as date)
             or cast(i.total as float) <> coalesce(inv.ord_inv_total_amount, 0)
             or cast(i.balance_due as float) <> coalesce(inv.ord_inv_balance_due, 0)
             or cast(i.subtotal as float) <> coalesce(inv.ord_inv_subtotal, 0)
             or cast(i.root_proposal_id as int) <> coalesce(inv.ord_inv_root_proposal_id, 0)
             or cast(i.gross_subtotal as float) <> coalesce(inv.ord_inv_gross_subtotal, 0)
             or cast(i.tax_total as float) <> coalesce(inv.ord_inv_tax_total, 0)
             or isnull(i.repeat,'nan') <> isnull(inv.ord_inv_repeat,'nan')
             or cast(i.qty as float) <> coalesce(inv.ord_inv_quantity, 0)
             or isnull(i.excluded_from_eop,'nan') <> isnull(inv.ord_inv_excluded_from_eop,'nan')
             or cast(i.customer_notes as varchar(255)) <> coalesce(inv.ord_inv_customer_notes,'nan')
             or case when d.entityid is not null or d_lead.entityid is not null then 1 else 0 end <> inv.ord_inv_delete_flag
             or cast(coalesce(d.queuedat, d_lead.queuedat) as date) <> cast(coalesce(inv.ord_inv_delete_date,'1900-01-01') as date)
          )
          AND inv.ord_inv_start_date <= cast(coalesce(i.updatedat, i.run_time) as date)
        GROUP BY i.id
    ) u
    ON target.ord_inv_internal_invoice_id = u.changed_record_id
    WHERE target.ord_inv_end_date IS NULL;
{% endset %}

{{ 
  config(
    materialized='incremental',
    unique_key='ord_inv_invoice_id',
    post_hook=[update_sql] if SCRIPT == 'INCREMENTAL' else []
  ) 
}}

-- ============================================================
-- Step 1: Source and Deletion data
-- ============================================================
with base as (
    select 
        i.id,
        cast(i.contactid as int) as contactid,
        cast(i.organization_id as int) as organization_id,
        hashbytes('{{ ALGORITHM }}', concat(i.service_name, '|', '{{ BRAND }}')) as ord_inv_itm_id,
        i.invoice_number,
        i.invoice_date,
        i.created,
        i.date_paid,
        i.status,
        i.type,
        i.revenue_category,
        i.service_name,
        i.owner,
        cast(i.total as float) as total,
        cast(i.balance_due as float) as balance_due,
        cast(i.subtotal as float) as subtotal,
        cast(i.root_proposal_id as int) as root_proposal_id,
        cast(i.gross_subtotal as float) as gross_subtotal,
        cast(i.tax_total as float) as tax_total,
        i.repeat,
        cast(i.qty as float) as qty,
        i.excluded_from_eop,
        cast(i.customer_notes as varchar(255)) as customer_notes,
        cast(coalesce(i.created, i.run_time) as date) as run_time,
        case when d.entityid is not null or d_lead.entityid is not null then 1 else 0 end as delete_flag,
        cast(coalesce(d.queuedat, d_lead.queuedat) as date) as delete_date
    from {{ ref('STG_SM_INVOICES') }} i
    left join {{ source('RAW', 'SM_DELETION_EVENTS') }} d 
        on d.entitytype = 3 and cast(d.entityid as int) = i.id
    left join {{ source('RAW', 'SM_DELETION_EVENTS') }} d_lead 
        on d_lead.entitytype = 0 
        and cast(d_lead.entityid as int) = cast(cast(i.contactid as float) as int)
),

-- ============================================================
-- Step 2: Filter new/changed rows
-- ============================================================
min_date as (
    select
        hashbytes('{{ ALGORITHM }}', concat(b.id, '|', '{{ BRAND }}')) as ord_inv_invoice_id,
        cast(b.id as int) as ord_inv_internal_invoice_id,
        b.contactid as ord_inv_internal_customer_party_id,
        b.organization_id as ord_inv_internal_organization_party_id,
        b.ord_inv_itm_id,
        b.invoice_number as ord_inv_invoice_number,
        b.invoice_date as ord_inv_invoice_date,
        b.created as ord_inv_invoice_created,
        b.date_paid as ord_inv_date_paid,
        b.status as ord_inv_status,
        b.type as ord_inv_type,
        b.revenue_category as ord_inv_revenue_category,
        b.service_name as ord_inv_service_name,
        b.owner as ord_inv_owner,
        b.total as ord_inv_total_amount,
        b.balance_due as ord_inv_balance_due,
        b.subtotal as ord_inv_subtotal,
        b.root_proposal_id as ord_inv_root_proposal_id,
        b.gross_subtotal as ord_inv_gross_subtotal,
        b.tax_total as ord_inv_tax_total,
        b.repeat as ord_inv_repeat,
        b.qty as ord_inv_quantity,
        b.excluded_from_eop as ord_inv_excluded_from_eop,
        b.customer_notes as ord_inv_customer_notes,
        b.delete_flag as ord_inv_delete_flag,
        b.delete_date as ord_inv_delete_date,
        '{{ RUN_ID }}' as ord_inv_created_run_id,
        min(b.run_time) as ord_inv_start_date
    from base b
    left join {{ this }} inv
        on inv.ord_inv_internal_invoice_id = b.id
    where inv.ord_inv_internal_invoice_id is null
    group by 
        b.id, b.contactid, b.organization_id, b.ord_inv_itm_id, b.invoice_number, b.invoice_date, 
        b.created, b.date_paid, b.status, b.type, b.revenue_category, b.service_name, b.owner, 
        b.total, b.balance_due, b.subtotal, b.root_proposal_id, b.gross_subtotal, b.tax_total, 
        b.repeat, b.qty, b.excluded_from_eop, b.customer_notes, b.delete_flag, b.delete_date
),

-- ============================================================
-- Step 3: Add end_date and brand
-- ============================================================
new_rows as (
    select *,
        lead(ord_inv_start_date) over (partition by ord_inv_internal_invoice_id order by ord_inv_start_date) as ord_inv_end_date,
        lead(ord_inv_created_run_id) over (partition by ord_inv_internal_invoice_id order by ord_inv_start_date) as ord_inv_updated_run_id,
        '{{ BRAND }}' as ord_brand
    from min_date
)

select * from new_rows
