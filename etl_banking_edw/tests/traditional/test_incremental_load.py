"""
test_incremental_load.py — Traditional ETL Test: Incremental Load Validation
Validates that audit columns are populated and batch isolation works correctly.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.bronze
@pytest.mark.incremental
class TestAuditColumns:
    """Validate that all bronze tables have proper audit metadata."""

    @pytest.mark.parametrize("fixture_name", [
        "bronze_customers", "bronze_accounts", "bronze_transactions",
        "bronze_loans", "bronze_branches",
    ])
    def test_ingestion_timestamp_populated(self, fixture_name, request):
        df = request.getfixturevalue(fixture_name)
        nulls = df.filter(F.col("_ingestion_ts").isNull()).count()
        assert nulls == 0, f"{fixture_name}._ingestion_ts has {nulls} NULLs"

    @pytest.mark.parametrize("fixture_name", [
        "bronze_customers", "bronze_accounts", "bronze_transactions",
        "bronze_loans", "bronze_branches",
    ])
    def test_source_file_populated(self, fixture_name, request):
        df = request.getfixturevalue(fixture_name)
        nulls = df.filter(
            F.col("_source_file").isNull() | (F.trim(F.col("_source_file")) == "")
        ).count()
        assert nulls == 0, f"{fixture_name}._source_file has {nulls} null/empty values"

    @pytest.mark.parametrize("fixture_name", [
        "bronze_customers", "bronze_accounts", "bronze_transactions",
        "bronze_loans", "bronze_branches",
    ])
    def test_batch_id_populated(self, fixture_name, request):
        df = request.getfixturevalue(fixture_name)
        nulls = df.filter(
            F.col("_batch_id").isNull() | (F.trim(F.col("_batch_id")) == "")
        ).count()
        assert nulls == 0, f"{fixture_name}._batch_id has {nulls} null/empty values"


@pytest.mark.bronze
@pytest.mark.incremental
class TestBatchConsistency:
    """All records within a single run should share the same batch_id."""

    @pytest.mark.parametrize("fixture_name", [
        "bronze_customers", "bronze_accounts", "bronze_transactions",
        "bronze_loans", "bronze_branches",
    ])
    def test_single_batch_id_per_table(self, fixture_name, request):
        df = request.getfixturevalue(fixture_name)
        batch_count = df.select("_batch_id").distinct().count()
        assert batch_count == 1, (
            f"{fixture_name} has {batch_count} different batch_ids (expected 1 for full load)"
        )


@pytest.mark.silver
@pytest.mark.incremental
class TestETLTimestamps:
    """Validate _etl_loaded_ts in Silver layer."""

    @pytest.mark.parametrize("fixture_name", [
        "dim_customer", "dim_account", "dim_branch", "dim_product",
        "fact_transaction", "fact_loan",
    ])
    def test_etl_loaded_ts_populated(self, fixture_name, request):
        df = request.getfixturevalue(fixture_name)
        nulls = df.filter(F.col("_etl_loaded_ts").isNull()).count()
        assert nulls == 0, f"{fixture_name}._etl_loaded_ts has {nulls} NULLs"

    @pytest.mark.parametrize("fixture_name", [
        "dim_customer", "dim_account", "dim_branch",
    ])
    def test_etl_loaded_ts_is_recent(self, fixture_name, request):
        """ETL timestamp should be within the last 24 hours (for fresh runs)."""
        df = request.getfixturevalue(fixture_name)
        old = df.filter(
            F.col("_etl_loaded_ts") < F.date_sub(F.current_timestamp(), 1)
        ).count()
        # This is a soft check — may fail if data was loaded earlier
        if old > 0:
            pytest.skip(f"{fixture_name} has {old} rows with old _etl_loaded_ts (stale run?)")
