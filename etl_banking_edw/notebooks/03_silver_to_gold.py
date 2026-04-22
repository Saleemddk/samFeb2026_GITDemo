# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver to Gold Aggregation
# MAGIC **Banking EDW Pipeline — Step 3**
# MAGIC
# MAGIC Builds business-ready Gold layer tables from the Silver dimensional model.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

silver_path = "data/warehouse/silver"
gold_path   = "data/warehouse/gold"

print(f"Silver Path : {silver_path}")
print(f"Gold Path   : {gold_path}")
print(f"Timestamp   : {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_customer_360

# COMMAND ----------

dim_cust = spark.read.parquet(f"{silver_path}/dim_customer").filter(F.col("is_current") == "Y")
dim_acc  = spark.read.parquet(f"{silver_path}/dim_account")
fact_txn = spark.read.parquet(f"{silver_path}/fact_transaction")
fact_loan = spark.read.parquet(f"{silver_path}/fact_loan")

acc_agg = dim_acc.groupBy("customer_id").agg(
    F.count("account_id").alias("total_accounts"),
    F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_accounts"),
)

txn_agg = (
    fact_txn
    .join(dim_acc.select("account_sk", "customer_id"), "account_sk", "left")
    .groupBy("customer_id")
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum(F.when(F.col("transaction_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("total_credit_amount"),
        F.sum(F.when(F.col("transaction_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("total_debit_amount"),
        F.max("transaction_date").alias("last_transaction_date"),
    )
)

loan_agg = (
    fact_loan
    .join(dim_cust.select("customer_sk", "customer_id"), "customer_sk", "left")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_loans"),
        F.sum("outstanding_amount").alias("total_loan_outstanding"),
    )
)

gold_customer_360 = (
    dim_cust.select("customer_id", "full_name", "city", "state", "customer_since")
    .join(acc_agg, "customer_id", "left")
    .join(txn_agg, "customer_id", "left")
    .join(loan_agg, "customer_id", "left")
    .fillna(0)
    .withColumn("customer_segment",
        F.when(F.col("total_credit_amount") >= 1000000, "PLATINUM")
         .when(F.col("total_credit_amount") >= 500000, "GOLD")
         .when(F.col("total_credit_amount") >= 100000, "SILVER")
         .otherwise("BASIC"))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
)

gold_customer_360.write.mode("overwrite").parquet(f"{gold_path}/gold_customer_360")
print(f"gold_customer_360: {gold_customer_360.count()} rows")
display(gold_customer_360.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_daily_txn_summary

# COMMAND ----------

gold_daily = (
    fact_txn
    .groupBy("transaction_date", "channel", "transaction_type")
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount"),
    )
    .withColumn("_etl_loaded_ts", F.current_timestamp())
)

gold_daily.write.mode("overwrite").parquet(f"{gold_path}/gold_daily_txn_summary")
print(f"gold_daily_txn_summary: {gold_daily.count()} rows")
display(gold_daily.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_loan_portfolio

# COMMAND ----------

gold_loan = (
    fact_loan
    .withColumn("report_month", F.trunc("disburse_date", "month"))
    .groupBy("loan_type", "report_month")
    .agg(
        F.count("*").alias("total_loans"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_loans"),
        F.sum(F.when(F.col("status") == "DEFAULT", 1).otherwise(0)).alias("defaulted_loans"),
        F.sum("principal_amount").alias("total_principal"),
        F.sum("outstanding_amount").alias("total_outstanding"),
        F.avg("interest_rate").alias("avg_interest_rate"),
        F.avg("tenure_months").alias("avg_tenure_months"),
    )
    .withColumn("npa_ratio", F.round(F.col("defaulted_loans") / F.col("total_loans") * 100, 2))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
)

gold_loan.write.mode("overwrite").parquet(f"{gold_path}/gold_loan_portfolio")
print(f"gold_loan_portfolio: {gold_loan.count()} rows")
display(gold_loan.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 50)
print("  Silver → Gold Aggregation Complete")
print("=" * 50)
for table in ["gold_customer_360", "gold_daily_txn_summary", "gold_loan_portfolio"]:
    count = spark.read.parquet(f"{gold_path}/{table}").count()
    print(f"  {table:30s} : {count:>8,} rows")
