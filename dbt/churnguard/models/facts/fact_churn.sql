with stg as (
    select * from {{ ref('stg_customers') }}
),

dim_cust as (
    select * from {{ ref('dim_customer') }}
),

dim_cont as (
    select * from {{ ref('dim_contract') }}
),

dim_pay as (
    select * from {{ ref('dim_payment') }}
)

select
    s.customer_id,
    dc.customer_key,
    dcon.contract_key,
    dp.payment_key,
    s.churn_flag,
    s.monthly_charges,
    s.revenue_lost,
    s.tenure_bucket
from stg s
left join dim_cust dc
    on s.customer_id = dc.customer_id
left join dim_cont dcon
    on s.contract = dcon.contract
    and s.tenure_bucket = dcon.tenure_bucket
left join dim_pay dp
    on s.payment_method = dp.payment_method
    and s.paperless_billing = dp.paperless_billing
