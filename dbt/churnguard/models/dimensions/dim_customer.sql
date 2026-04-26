with customers as (
    select distinct
        customer_id,
        gender,
        senior_citizen,
        partner,
        dependents
    from {{ ref('stg_customers') }}
)

select
    row_number() over (order by customer_id) as customer_key,
    customer_id,
    gender,
    senior_citizen,
    partner,
    dependents
from customers
