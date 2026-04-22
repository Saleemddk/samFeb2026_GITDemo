"""
test_statistical_profiling.py — Modern Data Quality: Statistical Data Profiling
Validates distribution, outliers, and statistical properties of key columns.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.silver
class TestBalanceDistribution:
    """Validate balance / amount distributions are reasonable."""

    def test_transaction_amount_stats(self, fact_transaction):
        """Transaction amounts should have reasonable min/max/avg."""
        stats = fact_transaction.agg(
            F.min("amount").alias("min_amt"),
            F.max("amount").alias("max_amt"),
            F.avg("amount").alias("avg_amt"),
            F.stddev("amount").alias("std_amt"),
        ).collect()[0]

        assert float(stats["min_amt"]) >= 0, "Negative transaction amount found"
        assert float(stats["max_amt"]) <= 1000000, f"Max amount too high: {stats['max_amt']}"
        assert float(stats["avg_amt"]) > 100, f"Average amount suspiciously low: {stats['avg_amt']}"

    def test_loan_principal_stats(self, fact_loan):
        """Loan principal amounts should be within expected range."""
        stats = fact_loan.agg(
            F.min("principal_amount").alias("min_p"),
            F.max("principal_amount").alias("max_p"),
            F.avg("principal_amount").alias("avg_p"),
        ).collect()[0]

        assert float(stats["min_p"]) >= 10000, f"Min principal too low: {stats['min_p']}"
        assert float(stats["max_p"]) <= 20000000, f"Max principal too high: {stats['max_p']}"

    def test_interest_rate_stats(self, fact_loan):
        """Interest rates should be between 5% and 20%."""
        stats = fact_loan.agg(
            F.min("interest_rate").alias("min_r"),
            F.max("interest_rate").alias("max_r"),
            F.avg("interest_rate").alias("avg_r"),
        ).collect()[0]

        assert float(stats["min_r"]) >= 5.0, f"Min rate too low: {stats['min_r']}"
        assert float(stats["max_r"]) <= 20.0, f"Max rate too high: {stats['max_r']}"
        assert 8.0 <= float(stats["avg_r"]) <= 14.0, f"Avg rate unusual: {stats['avg_r']}"


@pytest.mark.silver
class TestDistributionBalance:
    """Check that data isn't skewed to a single value."""

    def test_transaction_type_distribution(self, fact_transaction):
        """CREDIT and DEBIT should each be at least 30% of total."""
        total = fact_transaction.count()
        credit = fact_transaction.filter(F.col("transaction_type") == "CREDIT").count()
        debit = fact_transaction.filter(F.col("transaction_type") == "DEBIT").count()
        credit_pct = credit / total * 100
        debit_pct = debit / total * 100
        assert credit_pct >= 30, f"Credit transactions only {credit_pct:.1f}%"
        assert debit_pct >= 30, f"Debit transactions only {debit_pct:.1f}%"

    def test_transaction_status_distribution(self, fact_transaction):
        """SUCCESS should be the dominant status (>70%)."""
        total = fact_transaction.count()
        success = fact_transaction.filter(F.col("status") == "SUCCESS").count()
        success_pct = success / total * 100
        assert success_pct >= 60, f"Success rate only {success_pct:.1f}% (expected >= 60%)"

    def test_gender_distribution(self, dim_customer):
        """Male and Female should each be at least 20%."""
        total = dim_customer.count()
        for gender in ["Male", "Female"]:
            count = dim_customer.filter(F.col("gender") == gender).count()
            pct = count / total * 100
            assert pct >= 20, f"{gender} is only {pct:.1f}% of customers"

    def test_city_distribution_not_single(self, dim_customer):
        """Data should span multiple cities (at least 10)."""
        city_count = dim_customer.select("city").distinct().count()
        assert city_count >= 10, f"Only {city_count} distinct cities (expected >= 10)"

    def test_loan_type_coverage(self, fact_loan):
        """All expected loan types should be represented."""
        types = set(r["loan_type"] for r in fact_loan.select("loan_type").distinct().collect())
        expected_min = 4  # At least 4 out of 6 loan types
        assert len(types) >= expected_min, f"Only {len(types)} loan types, expected >= {expected_min}"


@pytest.mark.silver
class TestOutlierDetection:
    """Flag statistical outliers using IQR method."""

    def test_transaction_amount_outliers(self, fact_transaction):
        """Flag if >5% of transactions are outliers (IQR method)."""
        quantiles = fact_transaction.approxQuantile("amount", [0.25, 0.75], 0.01)
        q1, q3 = float(quantiles[0]), float(quantiles[1])
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        total = fact_transaction.count()
        outliers = fact_transaction.filter(
            (F.col("amount") < lower) | (F.col("amount") > upper)
        ).count()

        outlier_pct = outliers / total * 100
        assert outlier_pct <= 15, (
            f"Transaction amount outliers: {outlier_pct:.1f}% (threshold 15%)"
        )

    def test_emi_amount_vs_principal(self, fact_loan):
        """EMI should not exceed principal (sanity check)."""
        bad = fact_loan.filter(F.col("emi_amount") > F.col("principal_amount")).count()
        assert bad == 0, f"{bad} loans where EMI > principal amount"
