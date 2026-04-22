"""
test_pipeline_e2e.py — Integration Test: End-to-End Pipeline Validation
Runs the full pipeline (Source → Bronze → Silver → Gold) and validates the output.
"""

import pytest
from pyspark.sql import functions as F


class TestEndToEndPipeline:
    """Full pipeline smoke tests — validate that all layers are populated
    and consistent after a complete run."""

    # ------------------------------------------------------------------
    # Layer existence checks
    # ------------------------------------------------------------------
    def test_bronze_layer_populated(self, bronze_customers, bronze_accounts,
                                    bronze_transactions, bronze_loans,
                                    bronze_branches):
        """All Bronze tables should have data."""
        assert bronze_customers.count() > 0, "bronze_customers is empty"
        assert bronze_accounts.count() > 0, "bronze_accounts is empty"
        assert bronze_transactions.count() > 0, "bronze_transactions is empty"
        assert bronze_loans.count() > 0, "bronze_loans is empty"
        assert bronze_branches.count() > 0, "bronze_branches is empty"

    def test_silver_layer_populated(self, dim_customer, dim_account,
                                    dim_branch, dim_product, dim_date,
                                    fact_transaction, fact_loan):
        """All Silver tables should have data."""
        assert dim_customer.count() > 0, "dim_customer is empty"
        assert dim_account.count() > 0, "dim_account is empty"
        assert dim_branch.count() > 0, "dim_branch is empty"
        assert dim_product.count() > 0, "dim_product is empty"
        assert dim_date.count() > 0, "dim_date is empty"
        assert fact_transaction.count() > 0, "fact_transaction is empty"
        assert fact_loan.count() > 0, "fact_loan is empty"

    def test_gold_layer_populated(self, gold_customer_360,
                                  gold_daily_txn_summary, gold_loan_portfolio):
        """All Gold tables should have data."""
        assert gold_customer_360.count() > 0, "gold_customer_360 is empty"
        assert gold_daily_txn_summary.count() > 0, "gold_daily_txn_summary is empty"
        assert gold_loan_portfolio.count() > 0, "gold_loan_portfolio is empty"

    # ------------------------------------------------------------------
    # Cross-layer row count flow
    # ------------------------------------------------------------------
    def test_customer_flow_source_to_gold(self, source_customers, bronze_customers,
                                          dim_customer, gold_customer_360):
        """Customer count should flow consistently: source → bronze → silver → gold."""
        src = source_customers.count()
        brz = bronze_customers.count()
        silver = dim_customer.filter(F.col("is_current") == "Y").count()
        gold = gold_customer_360.count()

        assert src == brz, f"Source ({src}) != Bronze ({brz})"
        # Silver dedups, so count of distinct customer_ids
        brz_distinct = bronze_customers.select("customer_id").distinct().count()
        assert brz_distinct == silver, f"Bronze distinct ({brz_distinct}) != Silver ({silver})"
        assert silver == gold, f"Silver ({silver}) != Gold ({gold})"

    def test_transaction_flow_source_to_gold(self, source_transactions,
                                             bronze_transactions,
                                             fact_transaction,
                                             gold_daily_txn_summary):
        """Transaction count should flow: source → bronze → silver ≈ gold summary."""
        src = source_transactions.count()
        brz = bronze_transactions.count()
        silver = fact_transaction.count()
        gold_total = gold_daily_txn_summary.agg(F.sum("total_count")).collect()[0][0]

        assert src == brz, f"Source ({src}) != Bronze ({brz})"
        assert brz == silver, f"Bronze ({brz}) != Silver ({silver})"
        assert silver == gold_total, f"Silver ({silver}) != Gold summary ({gold_total})"

    # ------------------------------------------------------------------
    # Data integrity across pipeline
    # ------------------------------------------------------------------
    def test_no_orphan_transactions(self, fact_transaction, dim_account):
        """All transactions should reference a valid account."""
        orphans = (
            fact_transaction
            .filter(F.col("account_sk").isNotNull())
            .join(dim_account, "account_sk", "left_anti")
            .count()
        )
        assert orphans == 0, f"{orphans} orphan transactions found"

    def test_gold_amounts_are_positive(self, gold_daily_txn_summary):
        """Aggregated amounts in gold should be >= 0."""
        neg = gold_daily_txn_summary.filter(F.col("total_amount") < 0).count()
        assert neg == 0, f"{neg} negative total_amount in daily summary"

    def test_all_layers_have_same_batch_window(self, bronze_customers):
        """All bronze tables should have the same ingestion window."""
        min_ts = bronze_customers.agg(F.min("_ingestion_ts")).collect()[0][0]
        max_ts = bronze_customers.agg(F.max("_ingestion_ts")).collect()[0][0]
        if min_ts and max_ts:
            diff_seconds = (max_ts - min_ts).total_seconds()
            assert diff_seconds < 300, (
                f"Bronze ingestion window is {diff_seconds}s (>5 min)"
            )
