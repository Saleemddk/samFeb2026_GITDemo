"""
test_row_counts.py — Traditional ETL Test: Row Count Validation
Validates that row counts match across layers (source → bronze → silver → gold).
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.bronze
class TestRowCountSourceToBronze:
    """Verify no data loss during Source → Bronze ingestion."""

    def test_customer_row_count(self, source_customers, bronze_customers):
        src_count = source_customers.count()
        brz_count = bronze_customers.count()
        assert brz_count == src_count, (
            f"Row count mismatch: source={src_count}, bronze={brz_count}"
        )

    def test_account_row_count(self, source_accounts, bronze_accounts):
        src_count = source_accounts.count()
        brz_count = bronze_accounts.count()
        assert brz_count == src_count, (
            f"Row count mismatch: source={src_count}, bronze={brz_count}"
        )

    def test_transaction_row_count(self, source_transactions, bronze_transactions):
        src_count = source_transactions.count()
        brz_count = bronze_transactions.count()
        assert brz_count == src_count, (
            f"Row count mismatch: source={src_count}, bronze={brz_count}"
        )

    def test_loan_row_count(self, source_loans, bronze_loans):
        src_count = source_loans.count()
        brz_count = bronze_loans.count()
        assert brz_count == src_count, (
            f"Row count mismatch: source={src_count}, bronze={brz_count}"
        )

    def test_branch_row_count(self, source_branches, bronze_branches):
        src_count = source_branches.count()
        brz_count = bronze_branches.count()
        assert brz_count == src_count, (
            f"Row count mismatch: source={src_count}, bronze={brz_count}"
        )


@pytest.mark.silver
class TestRowCountBronzeToSilver:
    """Verify expected row counts after Bronze → Silver transformation."""

    def test_dim_customer_count_after_dedup(self, bronze_customers, dim_customer):
        """dim_customer should have unique customer_ids (deduplicated)."""
        expected = bronze_customers.select("customer_id").distinct().count()
        actual = dim_customer.count()
        assert actual == expected, (
            f"dim_customer count mismatch: expected={expected}, actual={actual}"
        )

    def test_dim_account_count(self, bronze_accounts, dim_account):
        expected = bronze_accounts.select("account_id").distinct().count()
        actual = dim_account.count()
        assert actual == expected, (
            f"dim_account count mismatch: expected={expected}, actual={actual}"
        )

    def test_dim_branch_count(self, bronze_branches, dim_branch):
        expected = bronze_branches.select("branch_id").distinct().count()
        actual = dim_branch.count()
        assert actual == expected, (
            f"dim_branch count mismatch: expected={expected}, actual={actual}"
        )

    def test_fact_transaction_count(self, bronze_transactions, fact_transaction):
        """fact_transaction should have all bronze transactions (no dedup on txn)."""
        expected = bronze_transactions.count()
        actual = fact_transaction.count()
        assert actual == expected, (
            f"fact_transaction count mismatch: expected={expected}, actual={actual}"
        )

    def test_fact_loan_count(self, bronze_loans, fact_loan):
        expected = bronze_loans.count()
        actual = fact_loan.count()
        assert actual == expected, (
            f"fact_loan count mismatch: expected={expected}, actual={actual}"
        )

    def test_dim_date_has_enough_rows(self, dim_date):
        """dim_date should cover at least 2008-2027 (~7300+ days)."""
        actual = dim_date.count()
        assert actual >= 7300, f"dim_date too few rows: {actual}"


@pytest.mark.gold
class TestRowCountSilverToGold:
    """Verify gold aggregation row counts are reasonable."""

    def test_customer_360_count(self, dim_customer, gold_customer_360):
        """gold_customer_360 should have one row per unique current customer."""
        expected = dim_customer.filter(F.col("is_current") == "Y").count()
        actual = gold_customer_360.count()
        assert actual == expected, (
            f"gold_customer_360 count mismatch: expected={expected}, actual={actual}"
        )

    def test_daily_txn_summary_not_empty(self, gold_daily_txn_summary):
        assert gold_daily_txn_summary.count() > 0, "gold_daily_txn_summary is empty"

    def test_loan_portfolio_not_empty(self, gold_loan_portfolio):
        assert gold_loan_portfolio.count() > 0, "gold_loan_portfolio is empty"


@pytest.mark.bronze
class TestRowCountMinimums:
    """Sanity checks — tables must have minimum expected data."""

    @pytest.mark.parametrize("fixture_name,min_count", [
        ("bronze_customers", 400),
        ("bronze_accounts", 500),
        ("bronze_transactions", 4000),
        ("bronze_loans", 200),
        ("bronze_branches", 40),
    ])
    def test_bronze_minimum_rows(self, fixture_name, min_count, request):
        df = request.getfixturevalue(fixture_name)
        actual = df.count()
        assert actual >= min_count, (
            f"{fixture_name} has only {actual} rows, expected >= {min_count}"
        )
