-- ChurnGuard Analytics Queries
-- Run against the DuckDB file: dbt/churnguard/churnguard.duckdb
-- All queries read from the fact_churn + dimension tables built by dbt.

-- ── 1. Overall Churn Rate ─────────────────────────────────────────────────────

select
    count(*)                                             as total_customers,
    sum(churn_flag)                                      as churned_customers,
    round(sum(churn_flag) * 100.0 / count(*), 2)        as churn_rate_pct
from main.fact_churn;


-- ── 2. Total Revenue at Risk (monthly) ───────────────────────────────────────

select
    round(sum(revenue_lost), 2)                         as total_revenue_lost,
    round(avg(monthly_charges), 2)                      as avg_monthly_charge,
    round(sum(revenue_lost) * 12, 2)                    as annualized_revenue_lost
from main.fact_churn;


-- ── 3. Churn Rate by Contract Type ───────────────────────────────────────────

select
    dc.contract,
    count(*)                                             as total_customers,
    sum(f.churn_flag)                                    as churned,
    round(sum(f.churn_flag) * 100.0 / count(*), 2)      as churn_rate_pct
from main.fact_churn f
join main.dim_contract dc on f.contract_key = dc.contract_key
group by dc.contract
order by churn_rate_pct desc;


-- ── 4. Churn Rate by Tenure Bucket ───────────────────────────────────────────

select
    dc.tenure_bucket,
    count(*)                                             as total_customers,
    sum(f.churn_flag)                                    as churned,
    round(sum(f.churn_flag) * 100.0 / count(*), 2)      as churn_rate_pct
from main.fact_churn f
join main.dim_contract dc on f.contract_key = dc.contract_key
group by dc.tenure_bucket
order by churn_rate_pct desc;


-- ── 5. Churn Rate by Payment Method ──────────────────────────────────────────

select
    dp.payment_method,
    count(*)                                             as total_customers,
    sum(f.churn_flag)                                    as churned,
    round(sum(f.churn_flag) * 100.0 / count(*), 2)      as churn_rate_pct
from main.fact_churn f
join main.dim_payment dp on f.payment_key = dp.payment_key
group by dp.payment_method
order by churn_rate_pct desc;


-- ── 6. Revenue Lost by Contract Type ─────────────────────────────────────────

select
    dc.contract,
    round(sum(f.revenue_lost), 2)                       as monthly_revenue_lost,
    round(sum(f.revenue_lost) * 12, 2)                  as annual_revenue_lost
from main.fact_churn f
join main.dim_contract dc on f.contract_key = dc.contract_key
group by dc.contract
order by monthly_revenue_lost desc;
