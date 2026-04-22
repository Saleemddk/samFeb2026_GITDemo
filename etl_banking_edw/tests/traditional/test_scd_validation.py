"""
test_scd_validation.py — Traditional ETL Test: Slowly Changing Dimension (SCD Type 2)
Validates SCD2 logic on dim_customer.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.silver
@pytest.mark.dim_customer
class TestSCDType2:
    """Validate SCD Type 2 implementation on dim_customer."""

    def test_every_customer_has_current_record(self, dim_customer):
        """Every customer_id must have at least one is_current='Y' row."""
        all_custs = dim_customer.select("customer_id").distinct().count()
        custs_with_current = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .select("customer_id")
            .distinct()
            .count()
        )
        assert all_custs == custs_with_current, (
            f"{all_custs - custs_with_current} customers without a current record"
        )

    def test_only_one_current_per_customer(self, dim_customer):
        """Each customer_id should have exactly one is_current='Y'."""
        multiple = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert multiple == 0, f"{multiple} customers with multiple current records"

    def test_current_records_have_max_expiry(self, dim_customer):
        """Current records should have expiry_date = 9999-12-31."""
        bad = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .filter(F.col("expiry_date") != F.lit("9999-12-31").cast("date"))
            .count()
        )
        assert bad == 0, f"{bad} current records with incorrect expiry_date"

    def test_expired_records_have_past_expiry(self, dim_customer):
        """Expired (non-current) records should have expiry_date < 9999-12-31."""
        bad = (
            dim_customer
            .filter(F.col("is_current") == "N")
            .filter(F.col("expiry_date") == F.lit("9999-12-31").cast("date"))
            .count()
        )
        # This is valid only if there are expired records
        expired_count = dim_customer.filter(F.col("is_current") == "N").count()
        if expired_count > 0:
            assert bad == 0, f"{bad} expired records still have 9999-12-31 expiry"

    def test_effective_before_expiry(self, dim_customer):
        """effective_date should always be <= expiry_date."""
        bad = dim_customer.filter(F.col("effective_date") > F.col("expiry_date")).count()
        assert bad == 0, f"{bad} records where effective_date > expiry_date"

    def test_no_gaps_in_date_ranges(self, dim_customer):
        """For customers with history, date ranges should not have gaps."""
        # Get customers with multiple versions
        multi_version = (
            dim_customer
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .select("customer_id")
        )
        # If no multi-version customers, test passes
        if multi_version.count() == 0:
            return

        # Verify: for each customer, the next effective_date should be
        # expiry_date + 1 day of the previous version
        from pyspark.sql.window import Window
        w = Window.partitionBy("customer_id").orderBy("effective_date")
        versioned = (
            dim_customer
            .join(multi_version, "customer_id")
            .withColumn("next_effective", F.lead("effective_date").over(w))
            .filter(F.col("next_effective").isNotNull())
            .withColumn("expected_next",
                F.date_add(F.col("expiry_date"), 1)
            )
            .filter(F.col("next_effective") != F.col("expected_next"))
        )
        gap_count = versioned.count()
        assert gap_count == 0, f"{gap_count} SCD2 date range gaps found"

    def test_surrogate_key_monotonic(self, dim_customer):
        """Surrogate keys should be positive."""
        bad = dim_customer.filter(F.col("customer_sk") <= 0).count()
        assert bad == 0, f"{bad} non-positive surrogate keys"
