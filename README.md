# ChurnGuard — Customer Retention Analytics Platform

An end-to-end data engineering project that identifies customer churn, quantifies revenue loss, and surfaces high-risk segments using a modern data stack.

---

## Architecture

```
Kaggle CSV (Telco Churn Dataset)
        |
        v
Databricks Community Edition (PySpark)
  ├── Bronze  →  raw ingestion
  ├── Silver  →  cleaning & standardization
  └── Gold    →  feature engineering → export Parquet
                          |
              +-----------+-----------+
              |                       |
   dbt + DuckDB (default)    dbt + Snowflake (production)
   local, free forever        cloud warehouse, fully documented
              |                       |
              +-----------+-----------+
                          |
                   Analytics Layer
                  (SQL queries + Snowsight)
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Processing | Databricks Community Edition (PySpark) |
| Transformation | dbt Core |
| Local Warehouse | DuckDB |
| Cloud Warehouse | Snowflake (trial) |
| Analytics / BI | Snowflake Snowsight |

---

## Project Structure

```
ChurnGuard/
├── data/
│   ├── raw/               # download dataset here (see below)
│   └── gold/              # exported Parquet from Databricks
├── databricks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_cleaning.ipynb
│   └── 03_gold_features.ipynb
├── snowflake/
│   ├── setup.sql          # DDL for all schemas and tables
│   └── screenshots/       # Snowflake pipeline proof
├── dbt/
│   └── churnguard/
│       ├── models/
│       │   ├── staging/
│       │   ├── dimensions/
│       │   └── facts/
│       └── schema.yml     # dbt tests
├── analytics/
│   └── queries.sql        # churn rate, revenue at risk, segmentation
└── README.md
```

---

## Dataset

**Telco Customer Churn** — IBM / Kaggle

Download from: https://www.kaggle.com/datasets/blastchar/telco-customer-churn

Place the CSV at: `data/raw/telco_churn.csv`

---

## How to Run Locally (DuckDB — no account needed)

```bash
# 1. Install dependencies
pip install dbt-duckdb

# 2. Export Gold Parquet from Databricks into data/gold/

# 3. Run dbt (uses DuckDB by default)
cd dbt/churnguard
dbt run
dbt test
```

---

## How to Run with Snowflake (Production)

```bash
# 1. Install dependencies
pip install dbt-snowflake

# 2. Configure credentials (never committed)
cp dbt/churnguard/profiles.yml.example dbt/churnguard/profiles.yml
# edit profiles.yml with your Snowflake credentials

# 3. Load data into Snowflake using snowflake/setup.sql

# 4. Run dbt against Snowflake
cd dbt/churnguard
dbt run --target prod
dbt test --target prod
```

---

## Key Insights (from Telco dataset)

- Overall churn rate: ~26%
- Month-to-month contract customers churn at the highest rate
- New customers (0–12 months tenure) are highest risk
- Electronic check payment method correlates with higher churn

---

## Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Repo setup & project structure | ✅ Done |
| 2 | Snowflake DDL | 🔜 |
| 3 | Databricks Bronze layer | 🔜 |
| 4 | Databricks Silver layer | 🔜 |
| 5 | Databricks Gold layer + export | 🔜 |
| 6 | Snowflake data loading | 🔜 |
| 7 | dbt project init + profiles | 🔜 |
| 8 | dbt staging model | 🔜 |
| 9 | dbt dimension models | 🔜 |
| 10 | dbt fact model | 🔜 |
| 11 | dbt tests | 🔜 |
| 12 | Analytics queries | 🔜 |
| 13 | Documentation | 🔜 |
