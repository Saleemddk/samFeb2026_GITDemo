"""
bronze_to_silver.py
Transforms Bronze raw data into Silver cleansed dimensional model.
- Type casting, trimming, deduplication
- SCD Type 2 for dim_customer
- Surrogate key generation
- Fact table assembly with FK lookups
"""

from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType

from config.database_config import BRONZE_PATH, SILVER_PATH
from config.pipeline_config import BRONZE_TABLES, SILVER_TABLES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.parquet(f"{BRONZE_PATH}/{table}")


def write_silver(df: DataFrame, table: str, mode: str = "overwrite"):
    path = f"{SILVER_PATH}/{table}"
    df.write.mode(mode).parquet(path)
    print(f"  [{table}] Written {df.count()} rows → {path}")


# ---------------------------------------------------------------------------
# Dimension Transforms
# ---------------------------------------------------------------------------
def transform_dim_customer(spark: SparkSession) -> DataFrame:
    """Bronze customers → dim_customer (SCD Type 2 ready)."""
    df = read_bronze(spark, BRONZE_TABLES["customers"])

    dim = (
        df
        .dropDuplicates(["customer_id"])
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("age",
            F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365)
        )
        .withColumn("gender",
            F.when(F.col("gender") == "M", "Male")
             .when(F.col("gender") == "F", "Female")
             .otherwise("Other")
        )
        .withColumn("phone", F.trim(F.col("phone")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("pan_number", F.upper(F.trim(F.col("pan_number"))))
        .withColumn("aadhar_number", F.trim(F.col("aadhar_number")))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("state", F.initcap(F.trim(F.col("state"))))
        .withColumn("pin_code", F.trim(F.col("pin_code")))
        .withColumn("customer_since", F.to_date(F.col("customer_since"), "yyyy-MM-dd"))
        .withColumn("effective_date", F.current_date())
        .withColumn("expiry_date", F.lit("9999-12-31").cast(DateType()))
        .withColumn("is_current", F.lit("Y"))
        .withColumn("_etl_loaded_ts", F.current_timestamp())
        .withColumn("customer_sk", F.monotonically_increasing_id() + 1)
        .select(
            "customer_sk", "customer_id", "first_name", "last_name", "full_name",
            "date_of_birth", "age", "gender", "phone", "email", "pan_number",
            "aadhar_number", "city", "state", "pin_code", "customer_since",
            "is_active", "effective_date", "expiry_date", "is_current",
            "_etl_loaded_ts",
        )
    )
    return dim


def transform_dim_account(spark: SparkSession) -> DataFrame:
    """Bronze accounts → dim_account."""
    df = read_bronze(spark, BRONZE_TABLES["accounts"])

    dim = (
        df
        .dropDuplicates(["account_id"])
        .withColumn("account_type", F.upper(F.trim(F.col("account_type"))))
        .withColumn("ifsc_code", F.upper(F.trim(F.col("ifsc_code"))))
        .withColumn("open_date", F.to_date(F.col("open_date"), "yyyy-MM-dd"))
        .withColumn("close_date",
            F.when(F.col("close_date") == "", None)
             .otherwise(F.to_date(F.col("close_date"), "yyyy-MM-dd"))
        )
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("nominee_name", F.initcap(F.trim(F.col("nominee_name"))))
        .withColumn("_etl_loaded_ts", F.current_timestamp())
        .withColumn("account_sk", F.monotonically_increasing_id() + 1)
        .select(
            "account_sk", "account_id", "customer_id", "account_number",
            "account_type", "product_id", "branch_id", "ifsc_code",
            "currency", "open_date", "close_date", "status", "nominee_name",
            "_etl_loaded_ts",
        )
    )
    return dim


def transform_dim_branch(spark: SparkSession) -> DataFrame:
    """Bronze branches → dim_branch."""
    df = read_bronze(spark, BRONZE_TABLES["branches"])

    dim = (
        df
        .dropDuplicates(["branch_id"])
        .withColumn("ifsc_code", F.upper(F.trim(F.col("ifsc_code"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("state", F.initcap(F.trim(F.col("state"))))
        .withColumn("manager_name", F.initcap(F.trim(F.col("manager_name"))))
        .withColumn("established_date", F.to_date(F.col("established_date"), "yyyy-MM-dd"))
        .withColumn("_etl_loaded_ts", F.current_timestamp())
        .withColumn("branch_sk", F.monotonically_increasing_id() + 1)
        .select(
            "branch_sk", "branch_id", "branch_name", "branch_code", "ifsc_code",
            "city", "state", "pin_code", "phone", "manager_name",
            "established_date", "is_active", "_etl_loaded_ts",
        )
    )
    return dim


def transform_dim_product(spark: SparkSession) -> DataFrame:
    """Bronze product_types → dim_product."""
    df = read_bronze(spark, BRONZE_TABLES["product_types"])

    dim = (
        df
        .dropDuplicates(["product_id"])
        .withColumn("product_category", F.upper(F.trim(F.col("product_category"))))
        .withColumn("interest_rate", F.col("interest_rate").cast(DecimalType(5, 2)))
        .withColumn("_etl_loaded_ts", F.current_timestamp())
        .withColumn("product_sk", F.monotonically_increasing_id() + 1)
        .select(
            "product_sk", "product_id", "product_name", "product_category",
            "interest_rate", "_etl_loaded_ts",
        )
    )
    return dim


def generate_dim_date(spark: SparkSession, start_year: int = 2008, end_year: int = 2027) -> DataFrame:
    """Generate a date dimension table."""
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    dates = []
    current = start
    while current <= end:
        dates.append((
            int(current.strftime("%Y%m%d")),  # date_sk
            current,
            current.year,
            (current.month - 1) // 3 + 1,
            current.month,
            current.strftime("%B"),
            current.isocalendar()[1],
            current.day,
            current.weekday() + 1,  # 1=Mon
            current.strftime("%A"),
            "Y" if current.weekday() >= 5 else "N",
            "N",
            current.year if current.month >= 4 else current.year - 1,  # Indian FY
            (current.month - 4) % 12 // 3 + 1 if current.month >= 4 else (current.month + 8) // 3,
        ))
        current += timedelta(days=1)

    columns = [
        "date_sk", "full_date", "year", "quarter", "month", "month_name",
        "week_of_year", "day_of_month", "day_of_week", "day_name",
        "is_weekend", "is_holiday", "fiscal_year", "fiscal_quarter",
    ]
    return spark.createDataFrame(dates, columns)


# ---------------------------------------------------------------------------
# Fact Transforms
# ---------------------------------------------------------------------------
def transform_fact_transaction(spark: SparkSession, dim_account: DataFrame, dim_customer: DataFrame, dim_branch: DataFrame) -> DataFrame:
    """Bronze transactions → fact_transaction with FK lookups."""
    txn = read_bronze(spark, BRONZE_TABLES["transactions"])

    # Lookup account_sk
    acc_lookup = dim_account.select("account_sk", "account_id", "customer_id", "branch_id")
    # Lookup customer_sk
    cust_lookup = dim_customer.filter(F.col("is_current") == "Y").select("customer_sk", "customer_id")
    # Lookup branch_sk
    branch_lookup = dim_branch.select("branch_sk", "branch_id")

    fact = (
        txn
        .join(acc_lookup, "account_id", "left")
        .join(cust_lookup, "customer_id", "left")
        .join(branch_lookup, "branch_id", "left")
        .withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
        .withColumn("date_sk", F.date_format(F.col("transaction_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("amount", F.col("amount").cast(DecimalType(15, 2)))
        .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type"))))
        .withColumn("channel", F.upper(F.trim(F.col("channel"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("_etl_loaded_ts", F.current_timestamp())
        .withColumn("transaction_sk", F.monotonically_increasing_id() + 1)
        .select(
            "transaction_sk", "transaction_id", "account_sk", "customer_sk",
            "branch_sk", "date_sk", "transaction_date", "transaction_time",
            "transaction_type", "amount", "currency", "channel", "status",
            "description", "beneficiary_acc", "reference_number",
            "_etl_loaded_ts",
        )
    )
    return fact


def transform_fact_loan(spark: SparkSession, dim_customer: DataFrame, dim_branch: DataFrame, dim_product: DataFrame) -> DataFrame:
    """Bronze loans → fact_loan with FK lookups."""
    loans = read_bronze(spark, BRONZE_TABLES["loans"])

    cust_lookup = dim_customer.filter(F.col("is_current") == "Y").select("customer_sk", "customer_id")
    branch_lookup = dim_branch.select("branch_sk", "branch_id")
    product_lookup = dim_product.select("product_sk", "product_id")

    fact = (
        loans
        .join(cust_lookup, "customer_id", "left")
        .join(branch_lookup, "branch_id", "left")
        .join(product_lookup, "product_id", "left")
        .withColumn("loan_type", F.upper(F.trim(F.col("loan_type"))))
        .withColumn("principal_amount", F.col("principal_amount").cast(DecimalType(15, 2)))
        .withColumn("interest_rate", F.col("interest_rate").cast(DecimalType(5, 2)))
        .withColumn("tenure_months", F.col("tenure_months").cast(IntegerType()))
        .withColumn("emi_amount", F.col("emi_amount").cast(DecimalType(15, 2)))
        .withColumn("disburse_date", F.to_date(F.col("disburse_date"), "yyyy-MM-dd"))
        .withColumn("maturity_date", F.to_date(F.col("maturity_date"), "yyyy-MM-dd"))
        .withColumn("outstanding_amount", F.col("outstanding_amount").cast(DecimalType(15, 2)))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("collateral_type", F.upper(F.trim(F.col("collateral_type"))))
        .withColumn("_etl_loaded_ts", F.current_timestamp())
        .withColumn("loan_sk", F.monotonically_increasing_id() + 1)
        .select(
            "loan_sk", "loan_id", "customer_sk", "branch_sk", "product_sk",
            "loan_type", "principal_amount", "interest_rate", "tenure_months",
            "emi_amount", "disburse_date", "maturity_date", "outstanding_amount",
            "status", "collateral_type", "_etl_loaded_ts",
        )
    )
    return fact


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_bronze_to_silver(spark: SparkSession):
    """Execute full Bronze → Silver pipeline."""
    from datetime import datetime
    print("=" * 60)
    print(f"  Pipeline: Bronze → Silver")
    print(f"  Timestamp: {datetime.now()}")
    print("=" * 60)

    # Dimensions
    print("\n  Building dim_customer (SCD2)...")
    dim_cust = transform_dim_customer(spark)
    write_silver(dim_cust, SILVER_TABLES["dim_customer"])

    print("\n  Building dim_account...")
    dim_acc = transform_dim_account(spark)
    write_silver(dim_acc, SILVER_TABLES["dim_account"])

    print("\n  Building dim_branch...")
    dim_branch = transform_dim_branch(spark)
    write_silver(dim_branch, SILVER_TABLES["dim_branch"])

    print("\n  Building dim_product...")
    dim_prod = transform_dim_product(spark)
    write_silver(dim_prod, SILVER_TABLES["dim_product"])

    print("\n  Building dim_date...")
    dim_date = generate_dim_date(spark)
    write_silver(dim_date, SILVER_TABLES["dim_date"])

    # Facts
    print("\n  Building fact_transaction...")
    fact_txn = transform_fact_transaction(spark, dim_acc, dim_cust, dim_branch)
    write_silver(fact_txn, SILVER_TABLES["fact_transaction"])

    print("\n  Building fact_loan...")
    fact_loan = transform_fact_loan(spark, dim_cust, dim_branch, dim_prod)
    write_silver(fact_loan, SILVER_TABLES["fact_loan"])

    print("\n  Bronze → Silver pipeline complete.")


if __name__ == "__main__":
    from utils.spark_session import get_spark_session
    spark = get_spark_session("BronzeToSilver")
    run_bronze_to_silver(spark)
    spark.stop()
