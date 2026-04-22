"""
conftest.py — Shared pytest fixtures for the entire ETL Banking EDW test suite.
Provides SparkSession, data paths, and pre-loaded DataFrames.
"""

import sys
import os
import pytest

# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

from config.database_config import (
    LANDING_ZONE, REFERENCE_ZONE,
    BRONZE_PATH, SILVER_PATH, GOLD_PATH,
)
from config.pipeline_config import BRONZE_TABLES, SILVER_TABLES, GOLD_TABLES


# ---------------------------------------------------------------------------
# SparkSession fixture (session-scoped for performance)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("BankingEDW_Tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Path fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def landing_zone_path():
    return LANDING_ZONE

@pytest.fixture(scope="session")
def bronze_path():
    return BRONZE_PATH

@pytest.fixture(scope="session")
def silver_path():
    return SILVER_PATH

@pytest.fixture(scope="session")
def gold_path():
    return GOLD_PATH


# ---------------------------------------------------------------------------
# Source data fixtures (CSV / JSON)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def source_customers(spark):
    return spark.read.option("header", "true").csv(f"{LANDING_ZONE}/customers.csv")

@pytest.fixture(scope="session")
def source_accounts(spark):
    return spark.read.option("header", "true").csv(f"{LANDING_ZONE}/accounts.csv")

@pytest.fixture(scope="session")
def source_transactions(spark):
    return spark.read.option("header", "true").csv(f"{LANDING_ZONE}/transactions.csv")

@pytest.fixture(scope="session")
def source_loans(spark):
    return spark.read.option("header", "true").csv(f"{LANDING_ZONE}/loans.csv")

@pytest.fixture(scope="session")
def source_branches(spark):
    return spark.read.option("multiLine", "true").json(f"{LANDING_ZONE}/branches.json")


# ---------------------------------------------------------------------------
# Bronze layer fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def bronze_customers(spark):
    return spark.read.parquet(f"{BRONZE_PATH}/{BRONZE_TABLES['customers']}")

@pytest.fixture(scope="session")
def bronze_accounts(spark):
    return spark.read.parquet(f"{BRONZE_PATH}/{BRONZE_TABLES['accounts']}")

@pytest.fixture(scope="session")
def bronze_transactions(spark):
    return spark.read.parquet(f"{BRONZE_PATH}/{BRONZE_TABLES['transactions']}")

@pytest.fixture(scope="session")
def bronze_loans(spark):
    return spark.read.parquet(f"{BRONZE_PATH}/{BRONZE_TABLES['loans']}")

@pytest.fixture(scope="session")
def bronze_branches(spark):
    return spark.read.parquet(f"{BRONZE_PATH}/{BRONZE_TABLES['branches']}")


# ---------------------------------------------------------------------------
# Silver layer fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def dim_customer(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['dim_customer']}")

@pytest.fixture(scope="session")
def dim_account(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['dim_account']}")

@pytest.fixture(scope="session")
def dim_branch(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['dim_branch']}")

@pytest.fixture(scope="session")
def dim_product(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['dim_product']}")

@pytest.fixture(scope="session")
def dim_date(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['dim_date']}")

@pytest.fixture(scope="session")
def fact_transaction(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['fact_transaction']}")

@pytest.fixture(scope="session")
def fact_loan(spark):
    return spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['fact_loan']}")


# ---------------------------------------------------------------------------
# Gold layer fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def gold_customer_360(spark):
    return spark.read.parquet(f"{GOLD_PATH}/{GOLD_TABLES['customer_360']}")

@pytest.fixture(scope="session")
def gold_daily_txn_summary(spark):
    return spark.read.parquet(f"{GOLD_PATH}/{GOLD_TABLES['daily_txn_summary']}")

@pytest.fixture(scope="session")
def gold_loan_portfolio(spark):
    return spark.read.parquet(f"{GOLD_PATH}/{GOLD_TABLES['loan_portfolio']}")
