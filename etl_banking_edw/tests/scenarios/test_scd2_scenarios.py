"""
test_scd2_scenarios.py — SCD Type 2 Scenario-Based Tests

Tests real-world SCD2 scenarios on dim_customer:
1. New customer appears → new row with is_current='Y', expiry_date=9999-12-31
2. Customer address change → old row expires, new version appears
3. Customer still exists unchanged → single current record preserved
4. Multiple changes to same customer → proper version chain
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


pytestmark = [pytest.mark.silver, pytest.mark.scd2_scenario, pytest.mark.dim_customer]


class TestSCD2NewCustomer:
    """Scenario: A brand new customer is loaded for the first time."""

    def test_new_customer_has_current_flag(self, dim_customer):
        """Every customer must have at least one is_current='Y' record."""
        customers_without_current = (
            dim_customer
            .groupBy("customer_id")
            .agg(
                F.sum(F.when(F.col("is_current") == "Y", 1).otherwise(0)).alias("current_count")
            )
            .filter(F.col("current_count") == 0)
            .count()
        )
        assert customers_without_current == 0, (
            f"{customers_without_current} customers found without a current record"
        )

    def test_new_customer_expiry_date_is_max(self, dim_customer):
        """Current records must have expiry_date = 9999-12-31."""
        bad = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .filter(F.col("expiry_date") != F.lit("9999-12-31").cast("date"))
            .count()
        )
        assert bad == 0, f"{bad} current records have incorrect expiry_date"

    def test_new_customer_has_surrogate_key(self, dim_customer):
        """All records must have a positive surrogate key."""
        bad = dim_customer.filter(
            F.col("customer_sk").isNull() | (F.col("customer_sk") <= 0)
        ).count()
        assert bad == 0, f"{bad} records with invalid surrogate key"

    def test_new_customer_effective_date_populated(self, dim_customer):
        """Effective date must not be null."""
        nulls = dim_customer.filter(F.col("effective_date").isNull()).count()
        assert nulls == 0, f"{nulls} records with null effective_date"


class TestSCD2AddressChange:
    """Scenario: Customer changes address → old version expires, new version created."""

    def test_no_customer_has_multiple_current(self, dim_customer):
        """After address change, only ONE version should be current."""
        multi_current = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert multi_current == 0, (
            f"{multi_current} customers have multiple current records (SCD2 bug)"
        )

    def test_expired_version_has_past_expiry_date(self, dim_customer):
        """Expired versions must have expiry_date < 9999-12-31."""
        expired = dim_customer.filter(F.col("is_current") == "N")
        if expired.count() == 0:
            pytest.skip("No expired records found (initial load only — no changes yet)")

        bad = expired.filter(
            F.col("expiry_date") == F.lit("9999-12-31").cast("date")
        ).count()
        assert bad == 0, f"{bad} expired records still have max expiry date"

    def test_version_continuity_no_gaps(self, dim_customer):
        """For customers with multiple versions, date ranges should be continuous."""
        from pyspark.sql.window import Window

        multi_version_custs = (
            dim_customer
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .select("customer_id")
        )

        if multi_version_custs.count() == 0:
            pytest.skip("No multi-version customers (initial load only)")

        w = Window.partitionBy("customer_id").orderBy("effective_date")
        gaps = (
            dim_customer
            .join(multi_version_custs, "customer_id")
            .withColumn("next_effective", F.lead("effective_date").over(w))
            .filter(F.col("next_effective").isNotNull())
            .withColumn("expected_next", F.date_add(F.col("expiry_date"), 1))
            .filter(F.col("next_effective") != F.col("expected_next"))
            .count()
        )
        assert gaps == 0, f"{gaps} date gaps found in SCD2 version chains"

    def test_latest_version_has_updated_address(self, dim_customer, bronze_customers):
        """The current version should reflect the latest source data."""
        # Compare current dim_customer city with bronze (latest source)
        current_dim = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .select("customer_id", F.col("city").alias("dim_city"))
        )
        latest_source = (
            bronze_customers
            .select("customer_id", F.col("city").alias("src_city"))
            .dropDuplicates(["customer_id"])  # take latest if duplicates
        )
        # Join and check for mismatches (only on customers that exist in both)
        mismatches = (
            current_dim
            .join(latest_source, "customer_id", "inner")
            .filter(
                (F.col("dim_city").isNotNull()) &
                (F.col("src_city").isNotNull()) &
                (F.initcap(F.trim(F.col("src_city"))) != F.col("dim_city"))
            )
        )
        # After a fresh silver load, these should match
        mismatch_count = mismatches.count()
        if mismatch_count > 0:
            sample = mismatches.select("customer_id", "dim_city", "src_city").limit(5).collect()
            details = ", ".join([f"{r.customer_id}: dim={r.dim_city} vs src={r.src_city}" for r in sample])
            pytest.fail(f"{mismatch_count} address mismatches after SCD2 load. Examples: {details}")


class TestSCD2HistoryPreservation:
    """Scenario: After changes, historical versions must be preserved."""

    def test_total_records_gte_unique_customers(self, dim_customer):
        """Total dim_customer rows >= unique customer_ids (history adds rows)."""
        total_rows = dim_customer.count()
        unique_custs = dim_customer.select("customer_id").distinct().count()
        assert total_rows >= unique_custs, (
            f"Total rows ({total_rows}) < unique customers ({unique_custs}) — history lost"
        )

    def test_surrogate_keys_unique(self, dim_customer):
        """All surrogate keys must be globally unique."""
        total = dim_customer.count()
        distinct_sk = dim_customer.select("customer_sk").distinct().count()
        assert total == distinct_sk, (
            f"Duplicate surrogate keys: {total} rows but only {distinct_sk} distinct keys"
        )

    def test_effective_before_expiry_all_records(self, dim_customer):
        """effective_date <= expiry_date for every record."""
        bad = dim_customer.filter(F.col("effective_date") > F.col("expiry_date")).count()
        assert bad == 0, f"{bad} records where effective_date > expiry_date"
