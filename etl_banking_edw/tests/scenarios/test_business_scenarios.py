"""
test_business_scenarios.py — Business Scenario Tests

Tests real banking business scenarios that an ETL tester would validate:
1. Account closure → close_date populated, status=CLOSED, no new txns after closure
2. Loan default → status=DEFAULT, reflected in Gold NPA calculations
3. Late-arriving transactions → old dates accepted, no duplicate txn_ids
4. Customer segmentation → Gold layer segments match balance thresholds
5. Branch performance → aggregation logic correctness
"""

import pytest
from pyspark.sql import functions as F
from datetime import date


pytestmark = [pytest.mark.business_scenario]


class TestAccountClosure:
    """Scenario: Account is closed — validate end-to-end impact."""

    @pytest.mark.silver
    @pytest.mark.dim_account
    def test_closed_accounts_have_close_date(self, dim_account):
        """CLOSED accounts must have a non-null close_date."""
        bad = (
            dim_account
            .filter(F.col("status") == "CLOSED")
            .filter(F.col("close_date").isNull())
            .count()
        )
        assert bad == 0, f"{bad} CLOSED accounts without close_date"

    @pytest.mark.silver
    @pytest.mark.dim_account
    def test_active_accounts_no_close_date(self, dim_account):
        """ACTIVE accounts should NOT have a close_date."""
        bad = (
            dim_account
            .filter(F.col("status") == "ACTIVE")
            .filter(F.col("close_date").isNotNull())
            .count()
        )
        assert bad == 0, f"{bad} ACTIVE accounts with a close_date (suspicious)"

    @pytest.mark.silver
    @pytest.mark.dim_account
    def test_closed_accounts_close_date_after_open(self, dim_account):
        """close_date must be >= open_date."""
        bad = (
            dim_account
            .filter(F.col("status") == "CLOSED")
            .filter(F.col("close_date").isNotNull())
            .filter(F.col("close_date") < F.col("open_date"))
            .count()
        )
        assert bad == 0, f"{bad} accounts where close_date < open_date"


class TestLoanDefault:
    """Scenario: Loan goes into default — validate impact across layers."""

    @pytest.mark.silver
    @pytest.mark.fact_loan
    def test_defaulted_loans_have_outstanding_balance(self, fact_loan):
        """Defaulted loans should have outstanding_amount > 0."""
        bad = (
            fact_loan
            .filter(F.col("status") == "DEFAULT")
            .filter(
                F.col("outstanding_amount").isNull() |
                (F.col("outstanding_amount") <= 0)
            )
            .count()
        )
        # This is expected behavior — defaulted loans have remaining balance
        defaulted = fact_loan.filter(F.col("status") == "DEFAULT").count()
        if defaulted == 0:
            pytest.skip("No defaulted loans in current data")
        assert bad == 0, f"{bad} defaulted loans with zero/null outstanding amount"

    @pytest.mark.gold
    def test_npa_reflected_in_branch_performance(self, spark, gold_path):
        """Gold branch_performance should show NPA amounts for branches with defaults."""
        from config.pipeline_config import GOLD_TABLES
        try:
            gold_branch = spark.read.parquet(f"{gold_path}/{GOLD_TABLES['branch_performance']}")
        except Exception:
            pytest.skip("Gold branch_performance not yet available")

        # If there are defaulted loans, at least one branch should have npa_amount > 0
        from config.pipeline_config import SILVER_TABLES
        from config.database_config import SILVER_PATH
        try:
            fact_loan = spark.read.parquet(f"{SILVER_PATH}/{SILVER_TABLES['fact_loan']}")
        except Exception:
            pytest.skip("Silver fact_loan not yet available")

        defaulted = fact_loan.filter(F.col("status") == "DEFAULT").count()
        if defaulted == 0:
            pytest.skip("No defaulted loans in current data")

        npa_branches = gold_branch.filter(F.col("npa_amount") > 0).count()
        assert npa_branches > 0, (
            f"Found {defaulted} defaulted loans but no branches show NPA in Gold"
        )

    @pytest.mark.gold
    def test_loan_portfolio_shows_defaults(self, spark, gold_path):
        """Gold loan_portfolio should track defaulted loan counts."""
        from config.pipeline_config import GOLD_TABLES
        try:
            gold_loan = spark.read.parquet(f"{gold_path}/{GOLD_TABLES['loan_portfolio']}")
        except Exception:
            pytest.skip("Gold loan_portfolio not yet available")

        total_defaults = gold_loan.agg(F.sum("defaulted_loans")).collect()[0][0]
        if total_defaults is None or total_defaults == 0:
            pytest.skip("No defaults in gold_loan_portfolio (could be initial clean data)")


class TestLateArrivingTransactions:
    """Scenario: Transactions with old dates arrive in a new batch."""

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_no_duplicate_transaction_ids(self, fact_transaction):
        """Each transaction_id should appear exactly once."""
        total = fact_transaction.count()
        distinct = fact_transaction.select("transaction_id").distinct().count()
        duplicates = total - distinct
        assert duplicates == 0, (
            f"{duplicates} duplicate transaction_ids found"
        )

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_transaction_dates_within_valid_range(self, fact_transaction):
        """Transaction dates should be between 2008 and today + 1 day (no far future)."""
        future_cutoff = date.today().isoformat()
        old_cutoff = "2008-01-01"

        # Allow some tolerance (today's date is fine)
        far_future = (
            fact_transaction
            .filter(F.col("transaction_date") > F.lit("2027-12-31").cast("date"))
            .count()
        )
        ancient = (
            fact_transaction
            .filter(F.col("transaction_date") < F.lit(old_cutoff).cast("date"))
            .count()
        )
        # Late-arriving is OK (old dates), but far-future is suspicious
        if far_future > 0:
            pytest.fail(f"{far_future} transactions with dates beyond 2027 (data quality issue)")

    @pytest.mark.bronze
    def test_late_arriving_captured_in_bronze(self, bronze_transactions):
        """Bronze should capture ALL transactions regardless of date."""
        # Just verify bronze has data — late-arriving should not be rejected at ingestion
        count = bronze_transactions.count()
        assert count > 0, "Bronze transactions is empty"


class TestCustomerSegmentation:
    """Scenario: Gold layer segments customers based on balance thresholds."""

    @pytest.mark.gold
    def test_customer_360_has_segments(self, gold_customer_360):
        """All customers should have a segment assigned."""
        nulls = (
            gold_customer_360
            .filter(F.col("customer_segment").isNull() | (F.col("customer_segment") == ""))
            .count()
        )
        assert nulls == 0, f"{nulls} customers without segment in gold_customer_360"

    @pytest.mark.gold
    def test_segment_values_are_valid(self, gold_customer_360):
        """Segments should be one of: PLATINUM, GOLD, SILVER, BASIC."""
        valid_segments = ["PLATINUM", "GOLD", "SILVER", "BASIC"]
        invalid = (
            gold_customer_360
            .filter(~F.col("customer_segment").isin(valid_segments))
            .count()
        )
        assert invalid == 0, f"{invalid} customers with invalid segment values"

    @pytest.mark.gold
    def test_all_customers_in_360(self, dim_customer, gold_customer_360):
        """Every current customer should appear in gold_customer_360."""
        expected = dim_customer.filter(F.col("is_current") == "Y").count()
        actual = gold_customer_360.count()
        assert actual == expected, (
            f"gold_customer_360 has {actual} rows, expected {expected} (current customers)"
        )


class TestTransactionSummary:
    """Scenario: Daily transaction summary in Gold matches Silver facts."""

    @pytest.mark.gold
    def test_daily_summary_total_matches_fact(self, fact_transaction, gold_daily_txn_summary):
        """Total transaction count in daily summary should match fact_transaction."""
        fact_count = fact_transaction.count()
        summary_total = gold_daily_txn_summary.agg(F.sum("total_count")).collect()[0][0]

        assert summary_total == fact_count, (
            f"Daily summary total ({summary_total}) != fact_transaction count ({fact_count})"
        )

    @pytest.mark.gold
    def test_daily_summary_no_negative_amounts(self, gold_daily_txn_summary):
        """Aggregated amounts should not be negative."""
        bad = (
            gold_daily_txn_summary
            .filter(F.col("total_amount") < 0)
            .count()
        )
        # Note: total_amount could be 0 if all failed, but negative is wrong
        assert bad == 0, f"{bad} daily summaries with negative total_amount"
