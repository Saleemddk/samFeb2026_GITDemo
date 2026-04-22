"""
test_completeness.py — Traditional ETL Test: Data Completeness
Ensures all expected entities, date ranges, and reference data are present.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.silver
class TestEntityCompleteness:
    """All source entities must be represented in downstream layers."""

    def test_all_branches_in_dim(self, bronze_branches, dim_branch):
        src = set(r["branch_id"] for r in bronze_branches.select("branch_id").collect())
        tgt = set(r["branch_id"] for r in dim_branch.select("branch_id").collect())
        missing = src - tgt
        assert len(missing) == 0, f"Branches missing from dim_branch: {missing}"

    def test_all_products_in_dim(self, spark):
        from config.database_config import BRONZE_PATH, SILVER_PATH
        from config.pipeline_config import BRONZE_TABLES, SILVER_TABLES
        brz = spark.read.parquet(f"{BRONZE_PATH}/{BRONZE_TABLES['product_types']}")
        dim = spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['dim_product']}")
        src = set(r["product_id"] for r in brz.select("product_id").collect())
        tgt = set(r["product_id"] for r in dim.select("product_id").collect())
        missing = src - tgt
        assert len(missing) == 0, f"Products missing from dim_product: {missing}"

    def test_all_customers_in_gold_360(self, dim_customer, gold_customer_360):
        current = set(
            r["customer_id"]
            for r in dim_customer.filter(F.col("is_current") == "Y").select("customer_id").collect()
        )
        gold = set(r["customer_id"] for r in gold_customer_360.select("customer_id").collect())
        missing = current - gold
        assert len(missing) == 0, f"{len(missing)} customers missing from gold_customer_360"


@pytest.mark.silver
class TestDateCompleteness:
    """Validate dim_date covers required range and has no gaps."""

    def test_dim_date_covers_2008_to_2027(self, dim_date):
        min_date = dim_date.agg(F.min("full_date")).collect()[0][0]
        max_date = dim_date.agg(F.max("full_date")).collect()[0][0]
        assert min_date.year <= 2008, f"dim_date starts at {min_date}, expected <= 2008"
        assert max_date.year >= 2027, f"dim_date ends at {max_date}, expected >= 2027"

    def test_dim_date_no_gaps(self, dim_date):
        """Number of rows should equal date range span + 1."""
        min_date = dim_date.agg(F.min("full_date")).collect()[0][0]
        max_date = dim_date.agg(F.max("full_date")).collect()[0][0]
        expected = (max_date - min_date).days + 1
        actual = dim_date.count()
        assert actual == expected, f"dim_date has {actual} rows, expected {expected} (gap detected)"

    def test_all_transaction_dates_covered(self, fact_transaction, dim_date):
        """Every transaction date_sk should exist in dim_date."""
        txn_dates = fact_transaction.select("date_sk").distinct()
        dim_dates = dim_date.select("date_sk").distinct()
        missing = txn_dates.subtract(dim_dates).count()
        assert missing == 0, f"{missing} transaction date_sk values missing from dim_date"


@pytest.mark.silver
class TestReferenceDataCompleteness:
    """All reference / lookup values in facts should exist in dimensions."""

    def test_transaction_channels_expected(self, fact_transaction):
        expected = {"NEFT", "RTGS", "IMPS", "UPI", "ATM", "BRANCH", "NET_BANKING", "MOBILE_BANKING"}
        actual = set(
            r["channel"]
            for r in fact_transaction.select("channel").distinct().collect()
        )
        unexpected = actual - expected
        assert len(unexpected) == 0, f"Unexpected channels: {unexpected}"

    def test_loan_types_expected(self, fact_loan):
        expected = {"HOME", "PERSONAL", "CAR", "EDUCATION", "GOLD", "BUSINESS"}
        actual = set(
            r["loan_type"]
            for r in fact_loan.select("loan_type").distinct().collect()
        )
        unexpected = actual - expected
        assert len(unexpected) == 0, f"Unexpected loan types: {unexpected}"

    def test_account_types_expected(self, dim_account):
        expected = {"SAVINGS", "CURRENT", "SALARY", "NRI", "FD", "RD"}
        actual = set(
            r["account_type"]
            for r in dim_account.select("account_type").distinct().collect()
        )
        unexpected = actual - expected
        assert len(unexpected) == 0, f"Unexpected account types: {unexpected}"
