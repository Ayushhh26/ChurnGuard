# Databricks notebook source
# Bronze Layer — Raw Ingestion
# Reads raw CSV from Unity Catalog volume into a Delta table with no transformations.

# COMMAND ----------

# Set file path and read CSV

FILE_PATH = "/Volumes/workspace/default/raw_data/telco_churn.csv"

df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(FILE_PATH)

print(f"Row count: {df_bronze.count()}")
print(f"Columns: {len(df_bronze.columns)}")

# COMMAND ----------

# Preview schema

df_bronze.printSchema()

# COMMAND ----------

# Show sample rows

df_bronze.show(5, truncate=False)

# COMMAND ----------

# Save as Delta table (Bronze layer)

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_raw_customers")

print("Bronze table created: workspace.default.bronze_raw_customers")
