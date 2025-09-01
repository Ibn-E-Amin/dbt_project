{{ config(materialized='table') }}

with forecast as (
    select
        FOR_INTERNAL_PARTY_ID as STORE_ID,
        FOR_YEAR as FORECAST_YEAR,
        FOR_MONTH as FORECAST_MONTH,
        datefromparts(FOR_YEAR, FOR_MONTH, 1) as FORECAST_DATE,
        FOR_TARGET as FORECAST_TARGET
    from {{ source('dd_dwh', 'FORECAST') }}
    where FOR_END_DATE is null
),
invoice_summary as (
    select
        D.STORE_ID,
        year(F.INVOICE_DATE) as INVOICE_YEAR,
        month(F.INVOICE_DATE) as INVOICE_MONTH,
        sum(F.SUBTOTAL) as SUBTOTAL,
        sum(F.BALANCE_DUE) as BALANCE_DUE
    from {{ ref('d_invoice') }} D
    inner join {{ ref('f_invoice') }} F
        on F.INVOICE_ID = D.INVOICE_ID
    group by D.STORE_ID, year(F.INVOICE_DATE), month(F.INVOICE_DATE)
)
select
    F.STORE_ID,
    F.FORECAST_YEAR,
    F.FORECAST_MONTH,
    F.FORECAST_DATE,
    F.FORECAST_TARGET,
    sum(isnull(I.SUBTOTAL, 0)) as SUBTOTAL,
    sum(isnull(I.BALANCE_DUE, 0)) as INVOICE_BALANCE_DUE
from forecast F
left join invoice_summary I
    on F.STORE_ID = I.STORE_ID
   and F.FORECAST_YEAR = I.INVOICE_YEAR
   and F.FORECAST_MONTH = I.INVOICE_MONTH
group by
    F.STORE_ID,
    F.FORECAST_YEAR,
    F.FORECAST_MONTH,
    F.FORECAST_DATE,
    F.FORECAST_TARGET
