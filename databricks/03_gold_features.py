# Databricks notebook source
# Gold Layer — Feature Engineering
# Reads Silver Delta table, creates derived features, exports as Parquet

# COMMAND ----------

# Read from Silver table

df_silver = spark.read.table("workspace.default.silver_cleaned_customers")

print(f"Silver row count: {df_silver.count()}")

# COMMAND ----------

# Create derived features

from pyspark.sql import functions as F

df_gold = df_silver \
    .withColumn("tenure_bucket",
        F.when(F.col("tenure").between(0, 12),  "New")
         .when(F.col("tenure").between(13, 24), "Growing")
         .otherwise("Loyal")
    ) \
    .withColumn("churn_flag",
        F.when(F.col("churn") == "Yes", 1).otherwise(0)
    ) \
    .withColumn("revenue_lost",
        F.when(F.col("churn") == "Yes", F.col("monthly_charges")).otherwise(0)
    )

print("Features created: tenure_bucket, churn_flag, revenue_lost")

# COMMAND ----------

# Verify features

df_gold.select("customer_id", "tenure", "tenure_bucket", "churn", "churn_flag", "revenue_lost").show(10)

# COMMAND ----------

# Save as Gold Delta table

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_customers_features")

print("Gold table created: workspace.default.gold_customers_features")

# COMMAND ----------

# Export as Parquet to volume (for dbt/DuckDB local pipeline)

df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/raw_data/gold/")

print("Parquet exported to: /Volumes/workspace/default/raw_data/gold/")
