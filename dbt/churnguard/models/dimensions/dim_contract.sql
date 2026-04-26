with contracts as (
    select distinct
        contract,
        tenure_bucket
    from {{ ref('stg_customers') }}
)

select
    row_number() over (order by contract, tenure_bucket) as contract_key,
    contract,
    tenure_bucket
from contracts
