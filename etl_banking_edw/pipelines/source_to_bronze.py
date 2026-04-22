"""
source_to_bronze.py
Ingests raw files from landing zone into Bronze layer (Delta / Parquet).
1:1 copy with added audit columns (_ingestion_ts, _source_file, _batch_id).
"""

import uuid
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)

from config.database_config import LANDING_ZONE, REFERENCE_ZONE, BRONZE_PATH
from config.pipeline_config import SOURCE_FILES, BRONZE_TABLES


# ---------------------------------------------------------------------------
# Schema definitions (all StringType for raw bronze)
# ---------------------------------------------------------------------------
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("date_of_birth", StringType()),
    StructField("gender", StringType()),
    StructField("phone", StringType()),
    StructField("email", StringType()),
    StructField("pan_number", StringType()),
    StructField("aadhar_number", StringType()),
    StructField("address", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("pin_code", StringType()),
    StructField("customer_since", StringType()),
    StructField("is_active", StringType()),
])

ACCOUNT_SCHEMA = StructType([
    StructField("account_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("account_number", StringType()),
    StructField("account_type", StringType()),
    StructField("product_id", StringType()),
    StructField("branch_id", StringType()),
    StructField("ifsc_code", StringType()),
    StructField("balance", StringType()),
    StructField("currency", StringType()),
    StructField("open_date", StringType()),
    StructField("close_date", StringType()),
    StructField("status", StringType()),
    StructField("nominee_name", StringType()),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("transaction_date", StringType()),
    StructField("transaction_time", StringType()),
    StructField("transaction_type", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
    StructField("description", StringType()),
    StructField("beneficiary_acc", StringType()),
    StructField("reference_number", StringType()),
])

LOAN_SCHEMA = StructType([
    StructField("loan_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("loan_type", StringType()),
    StructField("product_id", StringType()),
    StructField("principal_amount", StringType()),
    StructField("interest_rate", StringType()),
    StructField("tenure_months", StringType()),
    StructField("emi_amount", StringType()),
    StructField("disburse_date", StringType()),
    StructField("maturity_date", StringType()),
    StructField("outstanding_amount", StringType()),
    StructField("status", StringType()),
    StructField("branch_id", StringType()),
    StructField("collateral_type", StringType()),
])

SCHEMAS = {
    "customers":     CUSTOMER_SCHEMA,
    "accounts":      ACCOUNT_SCHEMA,
    "transactions":  TRANSACTION_SCHEMA,
    "loans":         LOAN_SCHEMA,
}


def add_audit_columns(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """Add standard bronze audit columns."""
    return (
        df
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def ingest_csv(spark: SparkSession, entity: str, batch_id: str) -> DataFrame:
    """Read a CSV file from the landing zone into a DataFrame."""
    cfg = SOURCE_FILES[entity]
    path = f"{LANDING_ZONE}/{cfg['file']}"
    schema = SCHEMAS.get(entity)

    reader = spark.read.option("header", str(cfg["header"])).option("delimiter", cfg["delimiter"])
    if schema:
        reader = reader.schema(schema)
    df = reader.csv(path)
    return add_audit_columns(df, cfg["file"], batch_id)


def ingest_json(spark: SparkSession, entity: str, batch_id: str) -> DataFrame:
    """Read a JSON file from the landing zone into a DataFrame."""
    cfg = SOURCE_FILES[entity]
    path = f"{LANDING_ZONE}/{cfg['file']}"
    df = spark.read.option("multiLine", cfg.get("multiLine", False)).json(path)
    # Cast all columns to StringType for bronze consistency
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
    return add_audit_columns(df, cfg["file"], batch_id)


def write_bronze(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Write DataFrame to bronze layer as Parquet (Delta on Databricks)."""
    output_path = f"{BRONZE_PATH}/{table_name}"
    df.write.mode(mode).parquet(output_path)
    print(f"  [{table_name}] Written {df.count()} rows → {output_path}")


def run_source_to_bronze(spark: SparkSession, mode: str = "overwrite"):
    """Execute full Source → Bronze pipeline.
    
    Args:
        mode: 'overwrite' for full load, 'append' for incremental
    """
    batch_id = str(uuid.uuid4())[:8]
    print("=" * 60)
    print(f"  Pipeline: Source → Bronze  |  batch_id = {batch_id}  |  mode = {mode}")
    print(f"  Timestamp: {datetime.now()}")
    print("=" * 60)

    # CSV sources
    for entity in ["customers", "accounts", "transactions", "loans"]:
        print(f"\n  Ingesting {entity}...")
        df = ingest_csv(spark, entity, batch_id)
        write_bronze(df, BRONZE_TABLES[entity], mode=mode)

    # JSON source
    print(f"\n  Ingesting branches...")
    df = ingest_json(spark, "branches", batch_id)
    write_bronze(df, BRONZE_TABLES["branches"], mode=mode)

    # Reference data
    print(f"\n  Ingesting product_types...")
    cfg = SOURCE_FILES["product_types"]
    path = f"{REFERENCE_ZONE}/{cfg['file']}"
    df = spark.read.option("header", "true").csv(path)
    for col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
    df = add_audit_columns(df, cfg["file"], batch_id)
    write_bronze(df, BRONZE_TABLES["product_types"], mode=mode)

    print("\n  Source → Bronze pipeline complete.")
    return batch_id


if __name__ == "__main__":
    from utils.spark_session import get_spark_session
    spark = get_spark_session("SourceToBronze")
    run_source_to_bronze(spark)
    spark.stop()
