with payments as (
    select distinct
        payment_method,
        paperless_billing
    from {{ ref('stg_customers') }}
)

select
    row_number() over (order by payment_method, paperless_billing) as payment_key,
    payment_method,
    paperless_billing
from payments
