"""
test_volume_anomaly.py — Modern Data Quality: Volume Anomaly Detection
Detects unusual row counts that may indicate pipeline failures or data issues.
"""

import pytest
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Expected volume ranges (based on data generation parameters)
# ---------------------------------------------------------------------------
EXPECTED_VOLUMES = {
    "bronze_customers":     (400, 600),     # generated ~500
    "bronze_accounts":      (500, 2000),    # 1-3 per customer
    "bronze_transactions":  (4000, 6000),   # generated ~5000
    "bronze_loans":         (200, 400),     # generated ~300
    "bronze_branches":      (40, 60),       # generated ~50
}


@pytest.mark.bronze
@pytest.mark.silver
@pytest.mark.gold
class TestVolumeRanges:
    """Verify table row counts fall within expected ranges."""

    @pytest.mark.parametrize("fixture_name,expected_range", [
        ("bronze_customers",    (400, 600)),
        ("bronze_accounts",     (500, 2000)),
        ("bronze_transactions", (4000, 6000)),
        ("bronze_loans",        (200, 400)),
        ("bronze_branches",     (40, 60)),
    ])
    def test_bronze_volume_in_range(self, fixture_name, expected_range, request):
        df = request.getfixturevalue(fixture_name)
        actual = df.count()
        lo, hi = expected_range
        assert lo <= actual <= hi, (
            f"{fixture_name}: {actual} rows outside expected range [{lo}, {hi}]"
        )

    def test_dim_customer_volume(self, dim_customer):
        count = dim_customer.count()
        assert 400 <= count <= 600, f"dim_customer volume anomaly: {count}"

    def test_fact_transaction_volume(self, fact_transaction):
        count = fact_transaction.count()
        assert 4000 <= count <= 6000, f"fact_transaction volume anomaly: {count}"

    def test_gold_customer_360_volume(self, gold_customer_360, dim_customer):
        gold_count = gold_customer_360.count()
        dim_count = dim_customer.filter(F.col("is_current") == "Y").count()
        # Gold should match silver
        assert gold_count == dim_count, (
            f"gold_customer_360 volume ({gold_count}) != dim_customer current ({dim_count})"
        )


@pytest.mark.bronze
class TestVolumeRatios:
    """Check ratios between related tables for anomalies."""

    def test_account_to_customer_ratio(self, bronze_accounts, bronze_customers):
        """Average accounts per customer should be reasonable (1-5)."""
        custs = bronze_customers.count()
        accs = bronze_accounts.count()
        ratio = accs / custs if custs > 0 else 0
        assert 1.0 <= ratio <= 5.0, (
            f"Account/Customer ratio is {ratio:.2f}, expected 1.0-5.0"
        )

    def test_transaction_to_account_ratio(self, bronze_transactions, bronze_accounts):
        """Average transactions per account should be reasonable."""
        accs = bronze_accounts.count()
        txns = bronze_transactions.count()
        ratio = txns / accs if accs > 0 else 0
        assert 1.0 <= ratio <= 50.0, (
            f"Transaction/Account ratio is {ratio:.2f}, expected 1.0-50.0"
        )

    def test_loan_to_customer_ratio(self, bronze_loans, bronze_customers):
        """Not every customer has a loan; ratio should be < 1."""
        custs = bronze_customers.count()
        loans = bronze_loans.count()
        ratio = loans / custs if custs > 0 else 0
        assert 0.1 <= ratio <= 2.0, (
            f"Loan/Customer ratio is {ratio:.2f}, expected 0.1-2.0"
        )


@pytest.mark.gold
class TestEmptyPartitions:
    """Detect partitions/segments with zero records."""

    def test_no_empty_transaction_dates(self, gold_daily_txn_summary):
        """Each date in summary should have at least 1 transaction."""
        empty = gold_daily_txn_summary.filter(F.col("total_count") == 0).count()
        assert empty == 0, f"{empty} date/channel combos with zero transactions"

    def test_no_empty_loan_portfolio_months(self, gold_loan_portfolio):
        """Each loan_type/month combo should have at least 1 loan."""
        empty = gold_loan_portfolio.filter(F.col("total_loans") == 0).count()
        assert empty == 0, f"{empty} loan portfolio entries with zero loans"
