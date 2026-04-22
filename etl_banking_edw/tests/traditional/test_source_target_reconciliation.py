"""
test_source_target_reconciliation.py — Traditional ETL Test: Source-to-Target Reconciliation
Compares aggregated values between source/bronze and target/silver to detect data corruption.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.bronze
class TestSourceBronzeReconciliation:
    """Reconcile Source files against Bronze tables."""

    def test_customer_ids_match(self, source_customers, bronze_customers):
        """All customer_ids in source must appear in bronze."""
        src_ids = set(
            row["customer_id"]
            for row in source_customers.select("customer_id").collect()
        )
        brz_ids = set(
            row["customer_id"]
            for row in bronze_customers.select("customer_id").collect()
        )
        missing = src_ids - brz_ids
        assert len(missing) == 0, f"{len(missing)} customer_ids missing in bronze: {list(missing)[:5]}"

    def test_transaction_ids_match(self, source_transactions, bronze_transactions):
        src_ids = set(
            row["transaction_id"]
            for row in source_transactions.select("transaction_id").collect()
        )
        brz_ids = set(
            row["transaction_id"]
            for row in bronze_transactions.select("transaction_id").collect()
        )
        missing = src_ids - brz_ids
        assert len(missing) == 0, f"{len(missing)} transaction_ids missing in bronze"

    def test_loan_amount_reconciliation(self, source_loans, bronze_loans):
        """Total principal amounts should match between source and bronze."""
        src_total = (
            source_loans
            .withColumn("amt", F.col("principal_amount").cast("double"))
            .agg(F.sum("amt"))
            .collect()[0][0]
        )
        brz_total = (
            bronze_loans
            .withColumn("amt", F.col("principal_amount").cast("double"))
            .agg(F.sum("amt"))
            .collect()[0][0]
        )
        assert abs(src_total - brz_total) < 0.01, (
            f"Loan amount mismatch: source={src_total}, bronze={brz_total}"
        )


@pytest.mark.silver
class TestBronzeSilverReconciliation:
    """Reconcile Bronze against Silver to detect transformation data loss."""

    def test_transaction_amount_sum(self, bronze_transactions, fact_transaction):
        """Total transaction amount should be preserved."""
        brz_total = (
            bronze_transactions
            .withColumn("amt", F.col("amount").cast("double"))
            .agg(F.sum("amt"))
            .collect()[0][0]
        )
        silver_total = (
            fact_transaction
            .agg(F.sum("amount").cast("double"))
            .collect()[0][0]
        )
        tolerance = brz_total * 0.001  # 0.1% tolerance for rounding
        assert abs(brz_total - silver_total) < tolerance, (
            f"Transaction amount mismatch: bronze={brz_total}, silver={silver_total}"
        )

    def test_loan_count_preserved(self, bronze_loans, fact_loan):
        brz = bronze_loans.count()
        silver = fact_loan.count()
        assert brz == silver, f"Loan count mismatch: bronze={brz}, silver={silver}"

    def test_customer_count_preserved(self, bronze_customers, dim_customer):
        brz = bronze_customers.select("customer_id").distinct().count()
        silver = dim_customer.select("customer_id").distinct().count()
        assert brz == silver, f"Customer count mismatch: bronze={brz}, silver={silver}"


@pytest.mark.gold
class TestSilverGoldReconciliation:
    """Reconcile Silver facts against Gold aggregations."""

    def test_total_transactions_matches_summary(self, fact_transaction, gold_daily_txn_summary):
        """Sum of total_count in daily summary should equal total fact rows."""
        fact_total = fact_transaction.count()
        gold_total = gold_daily_txn_summary.agg(F.sum("total_count")).collect()[0][0]
        assert fact_total == gold_total, (
            f"Transaction count mismatch: silver fact={fact_total}, gold summary={gold_total}"
        )

    def test_loan_count_matches_portfolio(self, fact_loan, gold_loan_portfolio):
        """Sum of total_loans in portfolio should equal fact_loan count."""
        fact_total = fact_loan.count()
        gold_total = gold_loan_portfolio.agg(F.sum("total_loans")).collect()[0][0]
        assert fact_total == gold_total, (
            f"Loan count mismatch: silver={fact_total}, gold={gold_total}"
        )

    def test_customer_360_count_matches_dim(self, dim_customer, gold_customer_360):
        current_custs = dim_customer.filter(F.col("is_current") == "Y").count()
        gold_custs = gold_customer_360.count()
        assert current_custs == gold_custs, (
            f"Customer count mismatch: dim={current_custs}, gold_360={gold_custs}"
        )
