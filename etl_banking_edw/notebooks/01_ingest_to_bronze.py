# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest Source Files to Bronze Layer
# MAGIC **Banking EDW Pipeline — Step 1**
# MAGIC
# MAGIC This notebook reads raw files from the landing zone (S3/ADLS/local)
# MAGIC and writes them as-is into the Bronze layer with audit columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime
import uuid

# Databricks widgets for parameterization
# dbutils.widgets.text("batch_id", "")
# dbutils.widgets.text("landing_zone", "s3://banking-edw/landing/")

batch_id = str(uuid.uuid4())[:8]
landing_zone = "data/landing_zone"  # local path; change to S3/ADLS for prod
bronze_path  = "data/warehouse/bronze"

print(f"Batch ID     : {batch_id}")
print(f"Landing Zone : {landing_zone}")
print(f"Bronze Path  : {bronze_path}")
print(f"Timestamp    : {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customers (CSV → Bronze)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

df_customers = (
    spark.read
    .option("header", "true")
    .csv(f"{landing_zone}/customers.csv")
)

# Add audit columns
df_customers = (
    df_customers
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("customers.csv"))
    .withColumn("_batch_id", F.lit(batch_id))
)

df_customers.write.mode("overwrite").parquet(f"{bronze_path}/bronze_customers")
print(f"bronze_customers: {df_customers.count()} rows")
display(df_customers.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Accounts (CSV → Bronze)

# COMMAND ----------

df_accounts = (
    spark.read
    .option("header", "true")
    .csv(f"{landing_zone}/accounts.csv")
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("accounts.csv"))
    .withColumn("_batch_id", F.lit(batch_id))
)

df_accounts.write.mode("overwrite").parquet(f"{bronze_path}/bronze_accounts")
print(f"bronze_accounts: {df_accounts.count()} rows")
display(df_accounts.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Transactions (CSV → Bronze)

# COMMAND ----------

df_transactions = (
    spark.read
    .option("header", "true")
    .csv(f"{landing_zone}/transactions.csv")
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("transactions.csv"))
    .withColumn("_batch_id", F.lit(batch_id))
)

df_transactions.write.mode("overwrite").parquet(f"{bronze_path}/bronze_transactions")
print(f"bronze_transactions: {df_transactions.count()} rows")
display(df_transactions.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Loans (CSV → Bronze)

# COMMAND ----------

df_loans = (
    spark.read
    .option("header", "true")
    .csv(f"{landing_zone}/loans.csv")
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("loans.csv"))
    .withColumn("_batch_id", F.lit(batch_id))
)

df_loans.write.mode("overwrite").parquet(f"{bronze_path}/bronze_loans")
print(f"bronze_loans: {df_loans.count()} rows")
display(df_loans.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Branches (JSON → Bronze)

# COMMAND ----------

df_branches = (
    spark.read
    .option("multiLine", "true")
    .json(f"{landing_zone}/branches.json")
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("branches.json"))
    .withColumn("_batch_id", F.lit(batch_id))
)

df_branches.write.mode("overwrite").parquet(f"{bronze_path}/bronze_branches")
print(f"bronze_branches: {df_branches.count()} rows")
display(df_branches.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Product Types (CSV → Bronze)

# COMMAND ----------

df_products = (
    spark.read
    .option("header", "true")
    .csv("data/reference/product_types.csv")
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("product_types.csv"))
    .withColumn("_batch_id", F.lit(batch_id))
)

df_products.write.mode("overwrite").parquet(f"{bronze_path}/bronze_product_types")
print(f"bronze_product_types: {df_products.count()} rows")
display(df_products.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 50)
print("  Source → Bronze Ingestion Complete")
print("=" * 50)
for table in ["bronze_customers", "bronze_accounts", "bronze_transactions",
              "bronze_loans", "bronze_branches", "bronze_product_types"]:
    count = spark.read.parquet(f"{bronze_path}/{table}").count()
    print(f"  {table:30s} : {count:>8,} rows")
