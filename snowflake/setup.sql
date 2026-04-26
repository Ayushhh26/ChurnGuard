-- ============================================================
-- ChurnGuard — Snowflake Setup
-- Run these statements in order in a Snowflake worksheet
-- ============================================================


-- ── 1. DATABASE & SCHEMAS ────────────────────────────────────

CREATE DATABASE IF NOT EXISTS CHURNGUARD_DB;

USE DATABASE CHURNGUARD_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;   -- raw ingested data
CREATE SCHEMA IF NOT EXISTS SILVER;   -- cleaned data
CREATE SCHEMA IF NOT EXISTS GOLD;     -- feature engineered data
CREATE SCHEMA IF NOT EXISTS MARTS;    -- dbt output (facts + dims)


-- ── 2. WAREHOUSE ─────────────────────────────────────────────

CREATE WAREHOUSE IF NOT EXISTS CHURNGUARD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'ChurnGuard compute warehouse';

USE WAREHOUSE CHURNGUARD_WH;


-- ── 3. BRONZE TABLE — raw ingestion ──────────────────────────

USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE raw_customers (
    customerID        STRING,
    gender            STRING,
    SeniorCitizen     STRING,
    Partner           STRING,
    Dependents        STRING,
    tenure            STRING,
    PhoneService      STRING,
    MultipleLines     STRING,
    InternetService   STRING,
    OnlineSecurity    STRING,
    OnlineBackup      STRING,
    DeviceProtection  STRING,
    TechSupport       STRING,
    StreamingTV       STRING,
    StreamingMovies   STRING,
    Contract          STRING,
    PaperlessBilling  STRING,
    PaymentMethod     STRING,
    MonthlyCharges    STRING,
    TotalCharges      STRING,
    Churn             STRING
);


-- ── 4. SILVER TABLE — cleaned data ───────────────────────────

USE SCHEMA SILVER;

CREATE OR REPLACE TABLE cleaned_customers (
    customer_id         STRING        NOT NULL,
    gender              STRING,
    senior_citizen      STRING,
    partner             STRING,
    dependents          STRING,
    tenure              INT,
    phone_service       STRING,
    multiple_lines      STRING,
    internet_service    STRING,
    online_security     STRING,
    online_backup       STRING,
    device_protection   STRING,
    tech_support        STRING,
    streaming_tv        STRING,
    streaming_movies    STRING,
    contract            STRING,
    paperless_billing   STRING,
    payment_method      STRING,
    monthly_charges     FLOAT,
    total_charges       FLOAT,
    churn               STRING
);


-- ── 5. GOLD TABLE — feature engineered ───────────────────────

USE SCHEMA GOLD;

CREATE OR REPLACE TABLE customers_features (
    customer_id         STRING        NOT NULL,
    gender              STRING,
    senior_citizen      STRING,
    partner             STRING,
    dependents          STRING,
    tenure              INT,
    tenure_bucket       STRING,       -- New / Growing / Loyal
    phone_service       STRING,
    multiple_lines      STRING,
    internet_service    STRING,
    online_security     STRING,
    online_backup       STRING,
    device_protection   STRING,
    tech_support        STRING,
    streaming_tv        STRING,
    streaming_movies    STRING,
    contract            STRING,
    paperless_billing   STRING,
    payment_method      STRING,
    monthly_charges     FLOAT,
    total_charges       FLOAT,
    churn               STRING,
    churn_flag          INT,          -- 0 or 1
    revenue_lost        FLOAT         -- monthly_charges if churned, else 0
);


-- ── 6. LOAD DATA INTO GOLD ────────────────────────────────────
-- Run after exporting Gold Parquet from Databricks

-- Step 1: create a named stage
CREATE OR REPLACE STAGE churnguard_stage
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Step 2: upload the Parquet file via SnowSQL CLI
--   PUT file:///path/to/data/gold/customers_features.parquet @churnguard_stage;

-- Step 3: copy into table
COPY INTO GOLD.customers_features
FROM @churnguard_stage/customers_features.parquet
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


-- ── 7. MARTS TABLES — dbt manages these ──────────────────────
-- dbt will CREATE OR REPLACE these automatically when you run:
--   dbt run --target prod
--
-- Expected tables created by dbt in MARTS schema:
--   MARTS.stg_customers
--   MARTS.dim_customer
--   MARTS.dim_contract
--   MARTS.dim_payment
--   MARTS.fact_churn


-- ── 8. QUICK VALIDATION QUERIES ──────────────────────────────

-- Row counts per layer
SELECT 'BRONZE' AS layer, COUNT(*) AS rows FROM BRONZE.raw_customers
UNION ALL
SELECT 'SILVER',          COUNT(*)          FROM SILVER.cleaned_customers
UNION ALL
SELECT 'GOLD',            COUNT(*)          FROM GOLD.customers_features;

-- Churn distribution in Gold
SELECT churn, COUNT(*) AS customers
FROM GOLD.customers_features
GROUP BY churn;
