# ChurnGuard — Customer Retention Analytics Platform

An end-to-end data engineering project that ingests raw customer data, engineers churn features through a medallion architecture, and surfaces revenue-at-risk insights using a modern, fully free stack.

---

## Architecture

```
Kaggle CSV  (Telco Customer Churn — 7,043 rows)
      |
      v
Databricks Free Edition  (PySpark + Unity Catalog)
  ├── Bronze  →  raw ingestion, Delta table, no transformations
  ├── Silver  →  TotalCharges cast, SeniorCitizen standardized, nulls dropped → 7,032 rows
  └── Gold    →  tenure_bucket, churn_flag, revenue_lost → export Parquet
                          |
                          v
               dbt Core  +  DuckDB  (local, free forever)
                 ├── stg_customers     (staging view)
                 ├── dim_customer      (demographics)
                 ├── dim_contract      (contract type + tenure)
                 ├── dim_payment       (payment method)
                 └── fact_churn        (churn metrics + FK joins)
                          |
                          v
                  analytics/queries.sql
            (churn rate, revenue at risk, segmentation)
```

---

## Tech Stack

| Layer | Tool | Why |
|-------|------|-----|
| Distributed processing | Databricks Free Edition (PySpark) | Serverless Spark, Unity Catalog volumes |
| Transformation | dbt Core | Modular SQL, built-in testing, free |
| Local warehouse | DuckDB | Reads Parquet natively, zero setup, free forever |
| Cloud warehouse (reference) | Snowflake | DDL + profiles documented for production path |

> DuckDB is the default dbt target so anyone can clone and run `dbt run` with zero account signups.
> Snowflake DDL and profile config are committed as reference artifacts in `snowflake/`.

---

## Star Schema

```
              dim_customer
             (demographics)
                   |
dim_contract ── fact_churn ── dim_payment
(contract type,    |         (payment method,
 tenure bucket)    |          paperless billing)
                   |
            churn_flag
            monthly_charges
            revenue_lost
```

---

## Project Structure

```
ChurnGuard/
├── data/
│   ├── raw/                         # telco_churn.csv (gitignored)
│   └── gold/                        # Parquet exported from Databricks (gitignored)
├── databricks/
│   ├── 01_bronze_ingestion.py       # CSV → Bronze Delta table
│   ├── 02_silver_cleaning.py        # cleaning + snake_case rename
│   └── 03_gold_features.py          # feature engineering + Parquet export
├── dbt/churnguard/
│   ├── dbt_project.yml
│   ├── profiles.yml.example         # Snowflake + DuckDB config template
│   └── models/
│       ├── staging/
│       │   ├── stg_customers.sql
│       │   └── schema.yml
│       ├── dimensions/
│       │   ├── dim_customer.sql
│       │   ├── dim_contract.sql
│       │   ├── dim_payment.sql
│       │   └── schema.yml
│       └── facts/
│           ├── fact_churn.sql
│           └── schema.yml
├── snowflake/
│   └── setup.sql                    # reference DDL (BRONZE/SILVER/GOLD/MARTS)
└── analytics/
    └── queries.sql                  # churn rate, revenue at risk, segmentation
```

---

## How to Run

### Prerequisites

```bash
git clone https://github.com/Ayushhh26/ChurnGuard.git
cd ChurnGuard
python3 -m venv .venv && source .venv/bin/activate
pip install dbt-duckdb
```

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/blastchar/telco-customer-churn) and place it at `data/raw/telco_churn.csv`.

Run the three Databricks notebooks in order to produce the Gold Parquet, then download the output to `data/gold/`.

### Run dbt (DuckDB — default)

```bash
cd dbt/churnguard
cp profiles.yml.example profiles.yml   # no edits needed for DuckDB
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Run dbt (Snowflake — production path)

```bash
cp dbt/churnguard/profiles.yml.example dbt/churnguard/profiles.yml
# fill in Snowflake credentials under the prod target
# load data using snowflake/setup.sql
dbt run --target prod --profiles-dir .
```

---

## Key Insights

| Metric | Value |
|--------|-------|
| Total customers | 7,032 |
| Overall churn rate | **26.6%** |
| Monthly revenue lost to churn | calculated per run |
| Highest-risk segment | Month-to-month contracts (**42.7% churn**) |
| Lowest-risk segment | Two-year contracts (**2.9% churn**) |
| New customer churn (0–12 mo) | highest across tenure buckets |

---

## dbt Test Coverage

| Model | Tests |
|-------|-------|
| stg_customers | unique, not_null, accepted_values |
| dim_customer | unique + not_null on surrogate and natural keys |
| dim_contract | unique + not_null on surrogate key |
| dim_payment | unique + not_null on surrogate key |
| fact_churn | not_null + FK relationships to all 3 dimensions |
| **Total** | **22 tests — all passing** |
