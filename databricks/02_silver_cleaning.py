# Databricks notebook source
# Silver Layer — Cleaning & Standardization
# Reads Bronze Delta table, fixes data quality issues, writes to Silver

# COMMAND ----------

# Read from Bronze table

df_bronze = spark.read.table("workspace.default.bronze_raw_customers")

print(f"Bronze row count: {df_bronze.count()}")

# COMMAND ----------

# Fix TotalCharges (string → float, blank → null) and SeniorCitizen (0/1 → No/Yes)

from pyspark.sql import functions as F

df_silver = df_bronze \
    .withColumn("TotalCharges",
        F.when(F.trim(F.col("TotalCharges")) == "", None)
         .otherwise(F.col("TotalCharges").cast("float"))
    ) \
    .withColumn("SeniorCitizen",
        F.when(F.col("SeniorCitizen") == 1, "Yes").otherwise("No")
    )

print("Fixes applied: TotalCharges → float, SeniorCitizen → Yes/No")

# COMMAND ----------

# Drop rows where TotalCharges is null and rename columns to snake_case

df_silver = df_silver.dropna(subset=["TotalCharges"])

df_silver = df_silver \
    .withColumnRenamed("customerID",       "customer_id") \
    .withColumnRenamed("SeniorCitizen",    "senior_citizen") \
    .withColumnRenamed("Partner",          "partner") \
    .withColumnRenamed("Dependents",       "dependents") \
    .withColumnRenamed("PhoneService",     "phone_service") \
    .withColumnRenamed("MultipleLines",    "multiple_lines") \
    .withColumnRenamed("InternetService",  "internet_service") \
    .withColumnRenamed("OnlineSecurity",   "online_security") \
    .withColumnRenamed("OnlineBackup",     "online_backup") \
    .withColumnRenamed("DeviceProtection", "device_protection") \
    .withColumnRenamed("TechSupport",      "tech_support") \
    .withColumnRenamed("StreamingTV",      "streaming_tv") \
    .withColumnRenamed("StreamingMovies",  "streaming_movies") \
    .withColumnRenamed("PaperlessBilling", "paperless_billing") \
    .withColumnRenamed("PaymentMethod",    "payment_method") \
    .withColumnRenamed("MonthlyCharges",   "monthly_charges") \
    .withColumnRenamed("TotalCharges",     "total_charges") \
    .withColumnRenamed("Contract",         "contract") \
    .withColumnRenamed("tenure",           "tenure") \
    .withColumnRenamed("gender",           "gender") \
    .withColumnRenamed("Churn",            "churn")

print(f"Silver row count after cleaning: {df_silver.count()}")

# COMMAND ----------

# Verify schema

df_silver.printSchema()

# COMMAND ----------

# Show sample rows

df_silver.show(5, truncate=False)

# COMMAND ----------

# Save as Silver Delta table

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_cleaned_customers")

print("Silver table created: workspace.default.silver_cleaned_customers")
