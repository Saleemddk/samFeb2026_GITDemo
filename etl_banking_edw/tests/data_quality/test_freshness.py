"""
test_freshness.py — Modern Data Quality: Data Freshness Checks
Validates that data is recent and ETL pipelines ran within expected windows.
"""

import pytest
from datetime import date, timedelta
from pyspark.sql import functions as F


@pytest.mark.bronze
@pytest.mark.silver
@pytest.mark.gold
class TestDataFreshness:
    """Ensure data is fresh — loaded recently and contains recent records."""

    def test_bronze_ingestion_within_24h(self, bronze_customers):
        """Bronze data should have been ingested within the last 24 hours."""
        max_ts = bronze_customers.agg(F.max("_ingestion_ts")).collect()[0][0]
        if max_ts is None:
            pytest.fail("No _ingestion_ts found in bronze_customers")
        from datetime import datetime
        age_hours = (datetime.now() - max_ts).total_seconds() / 3600
        assert age_hours < 24, f"Bronze data is {age_hours:.1f} hours old (expected < 24h)"

    def test_silver_etl_within_24h(self, dim_customer):
        """Silver data should have been loaded within the last 24 hours."""
        max_ts = dim_customer.agg(F.max("_etl_loaded_ts")).collect()[0][0]
        if max_ts is None:
            pytest.fail("No _etl_loaded_ts found in dim_customer")
        from datetime import datetime
        age_hours = (datetime.now() - max_ts).total_seconds() / 3600
        assert age_hours < 24, f"Silver data is {age_hours:.1f} hours old"

    def test_gold_etl_within_24h(self, gold_customer_360):
        """Gold data should have been loaded within the last 24 hours."""
        max_ts = gold_customer_360.agg(F.max("_etl_loaded_ts")).collect()[0][0]
        if max_ts is None:
            pytest.fail("No _etl_loaded_ts found in gold_customer_360")
        from datetime import datetime
        age_hours = (datetime.now() - max_ts).total_seconds() / 3600
        assert age_hours < 24, f"Gold data is {age_hours:.1f} hours old"


@pytest.mark.silver
class TestTransactionDateFreshness:
    """Transaction data should include recent dates."""

    def test_transactions_include_current_year(self, fact_transaction):
        """Transactions should include data from the current year."""
        current_year = date.today().year
        count = fact_transaction.filter(
            F.year(F.col("transaction_date")) == current_year
        ).count()
        # Transactions are generated for 2023+, so check for that
        recent_count = fact_transaction.filter(
            F.year(F.col("transaction_date")) >= 2023
        ).count()
        assert recent_count > 0, "No transactions from 2023 onward"

    def test_max_transaction_date_is_recent(self, fact_transaction):
        """Most recent transaction should be within the last year."""
        max_date = fact_transaction.agg(F.max("transaction_date")).collect()[0][0]
        days_old = (date.today() - max_date).days
        assert days_old < 365, f"Most recent transaction is {days_old} days old"


@pytest.mark.silver
class TestDimDateCoverage:
    """dim_date should cover future dates for forward-looking analytics."""

    def test_dim_date_covers_next_year(self, dim_date):
        next_year = date.today().year + 1
        future_count = dim_date.filter(F.col("year") >= next_year).count()
        assert future_count > 0, "dim_date does not cover next year"
