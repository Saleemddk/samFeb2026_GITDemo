# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze to Silver Transformation
# MAGIC **Banking EDW Pipeline — Step 2**
# MAGIC
# MAGIC Applies cleansing, type casting, deduplication, SCD Type 2 (dim_customer),
# MAGIC and builds the star schema with dimensions and fact tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType
from datetime import datetime, date, timedelta

bronze_path = "data/warehouse/bronze"
silver_path = "data/warehouse/silver"

print(f"Bronze Path : {bronze_path}")
print(f"Silver Path : {silver_path}")
print(f"Timestamp   : {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer (SCD Type 2)

# COMMAND ----------

df_cust = spark.read.parquet(f"{bronze_path}/bronze_customers")

dim_customer = (
    df_cust
    .dropDuplicates(["customer_id"])
    .withColumn("first_name", F.initcap(F.trim("first_name")))
    .withColumn("last_name", F.initcap(F.trim("last_name")))
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    .withColumn("date_of_birth", F.to_date("date_of_birth", "yyyy-MM-dd"))
    .withColumn("age", F.floor(F.datediff(F.current_date(), "date_of_birth") / 365))
    .withColumn("gender",
        F.when(F.col("gender") == "M", "Male")
         .when(F.col("gender") == "F", "Female")
         .otherwise("Other"))
    .withColumn("phone", F.trim("phone"))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("pan_number", F.upper(F.trim("pan_number")))
    .withColumn("customer_since", F.to_date("customer_since", "yyyy-MM-dd"))
    .withColumn("effective_date", F.current_date())
    .withColumn("expiry_date", F.lit("9999-12-31").cast(DateType()))
    .withColumn("is_current", F.lit("Y"))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
    .withColumn("customer_sk", F.monotonically_increasing_id() + 1)
)

dim_customer.write.mode("overwrite").parquet(f"{silver_path}/dim_customer")
print(f"dim_customer: {dim_customer.count()} rows")
display(dim_customer.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_account

# COMMAND ----------

df_acc = spark.read.parquet(f"{bronze_path}/bronze_accounts")

dim_account = (
    df_acc
    .dropDuplicates(["account_id"])
    .withColumn("account_type", F.upper(F.trim("account_type")))
    .withColumn("ifsc_code", F.upper(F.trim("ifsc_code")))
    .withColumn("open_date", F.to_date("open_date", "yyyy-MM-dd"))
    .withColumn("close_date",
        F.when(F.col("close_date") == "", None)
         .otherwise(F.to_date("close_date", "yyyy-MM-dd")))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("nominee_name", F.initcap(F.trim("nominee_name")))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
    .withColumn("account_sk", F.monotonically_increasing_id() + 1)
)

dim_account.write.mode("overwrite").parquet(f"{silver_path}/dim_account")
print(f"dim_account: {dim_account.count()} rows")
display(dim_account.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_branch, dim_product, dim_date

# COMMAND ----------

# dim_branch
df_br = spark.read.parquet(f"{bronze_path}/bronze_branches")
dim_branch = (
    df_br.dropDuplicates(["branch_id"])
    .withColumn("city", F.initcap(F.trim("city")))
    .withColumn("state", F.initcap(F.trim("state")))
    .withColumn("established_date", F.to_date("established_date", "yyyy-MM-dd"))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
    .withColumn("branch_sk", F.monotonically_increasing_id() + 1)
)
dim_branch.write.mode("overwrite").parquet(f"{silver_path}/dim_branch")
print(f"dim_branch: {dim_branch.count()} rows")

# dim_product
df_prod = spark.read.parquet(f"{bronze_path}/bronze_product_types")
dim_product = (
    df_prod.dropDuplicates(["product_id"])
    .withColumn("product_category", F.upper(F.trim("product_category")))
    .withColumn("interest_rate", F.col("interest_rate").cast(DecimalType(5, 2)))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
    .withColumn("product_sk", F.monotonically_increasing_id() + 1)
)
dim_product.write.mode("overwrite").parquet(f"{silver_path}/dim_product")
print(f"dim_product: {dim_product.count()} rows")

# dim_date (generate)
dates = []
current = date(2008, 1, 1)
end = date(2027, 12, 31)
while current <= end:
    dates.append((
        int(current.strftime("%Y%m%d")), current, current.year,
        (current.month - 1) // 3 + 1, current.month, current.strftime("%B"),
        current.isocalendar()[1], current.day, current.weekday() + 1,
        current.strftime("%A"), "Y" if current.weekday() >= 5 else "N", "N",
    ))
    current += timedelta(days=1)
dim_date = spark.createDataFrame(dates, [
    "date_sk", "full_date", "year", "quarter", "month", "month_name",
    "week_of_year", "day_of_month", "day_of_week", "day_name",
    "is_weekend", "is_holiday",
])
dim_date.write.mode("overwrite").parquet(f"{silver_path}/dim_date")
print(f"dim_date: {dim_date.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_transaction

# COMMAND ----------

df_txn = spark.read.parquet(f"{bronze_path}/bronze_transactions")
acc_lk = dim_account.select("account_sk", "account_id", "customer_id", "branch_id")
cust_lk = dim_customer.filter(F.col("is_current") == "Y").select("customer_sk", "customer_id")
br_lk = dim_branch.select("branch_sk", "branch_id")

fact_transaction = (
    df_txn
    .join(acc_lk, "account_id", "left")
    .join(cust_lk, "customer_id", "left")
    .join(br_lk, "branch_id", "left")
    .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
    .withColumn("date_sk", F.date_format("transaction_date", "yyyyMMdd").cast(IntegerType()))
    .withColumn("amount", F.col("amount").cast(DecimalType(15, 2)))
    .withColumn("transaction_type", F.upper(F.trim("transaction_type")))
    .withColumn("channel", F.upper(F.trim("channel")))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
    .withColumn("transaction_sk", F.monotonically_increasing_id() + 1)
)

fact_transaction.write.mode("overwrite").parquet(f"{silver_path}/fact_transaction")
print(f"fact_transaction: {fact_transaction.count()} rows")
display(fact_transaction.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_loan

# COMMAND ----------

df_loan = spark.read.parquet(f"{bronze_path}/bronze_loans")
prod_lk = dim_product.select("product_sk", "product_id")

fact_loan = (
    df_loan
    .join(cust_lk, "customer_id", "left")
    .join(br_lk, "branch_id", "left")
    .join(prod_lk, "product_id", "left")
    .withColumn("principal_amount", F.col("principal_amount").cast(DecimalType(15, 2)))
    .withColumn("interest_rate", F.col("interest_rate").cast(DecimalType(5, 2)))
    .withColumn("tenure_months", F.col("tenure_months").cast(IntegerType()))
    .withColumn("emi_amount", F.col("emi_amount").cast(DecimalType(15, 2)))
    .withColumn("outstanding_amount", F.col("outstanding_amount").cast(DecimalType(15, 2)))
    .withColumn("disburse_date", F.to_date("disburse_date", "yyyy-MM-dd"))
    .withColumn("maturity_date", F.to_date("maturity_date", "yyyy-MM-dd"))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("_etl_loaded_ts", F.current_timestamp())
    .withColumn("loan_sk", F.monotonically_increasing_id() + 1)
)

fact_loan.write.mode("overwrite").parquet(f"{silver_path}/fact_loan")
print(f"fact_loan: {fact_loan.count()} rows")
display(fact_loan.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 50)
print("  Bronze → Silver Transformation Complete")
print("=" * 50)
for table in ["dim_customer", "dim_account", "dim_branch", "dim_product",
              "dim_date", "fact_transaction", "fact_loan"]:
    count = spark.read.parquet(f"{silver_path}/{table}").count()
    print(f"  {table:30s} : {count:>8,} rows")
