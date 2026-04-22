"""
test_cross_layer_consistency.py — Modern Data Quality: Cross-Layer Consistency
Validates data consistency across Bronze → Silver → Gold layers.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.bronze
@pytest.mark.silver
class TestBronzeSilverConsistency:
    """Verify data consistency between Bronze and Silver layers."""

    def test_customer_ids_consistent(self, bronze_customers, dim_customer):
        """All bronze customer_ids should be in dim_customer."""
        brz = set(r["customer_id"] for r in bronze_customers.select("customer_id").distinct().collect())
        dim = set(r["customer_id"] for r in dim_customer.select("customer_id").distinct().collect())
        missing = brz - dim
        extra = dim - brz
        assert len(missing) == 0, f"{len(missing)} customers missing from silver"
        assert len(extra) == 0, f"{len(extra)} extra customers in silver"

    def test_account_ids_consistent(self, bronze_accounts, dim_account):
        brz = set(r["account_id"] for r in bronze_accounts.select("account_id").distinct().collect())
        dim = set(r["account_id"] for r in dim_account.select("account_id").distinct().collect())
        missing = brz - dim
        assert len(missing) == 0, f"{len(missing)} accounts missing from silver"

    def test_transaction_ids_consistent(self, bronze_transactions, fact_transaction):
        brz_count = bronze_transactions.select("transaction_id").distinct().count()
        fact_count = fact_transaction.select("transaction_id").distinct().count()
        assert brz_count == fact_count, (
            f"Transaction ID count mismatch: bronze={brz_count}, silver={fact_count}"
        )

    def test_branch_ids_consistent(self, bronze_branches, dim_branch):
        brz = set(r["branch_id"] for r in bronze_branches.select("branch_id").distinct().collect())
        dim = set(r["branch_id"] for r in dim_branch.select("branch_id").distinct().collect())
        assert brz == dim, f"Branch ID mismatch between bronze and silver"


@pytest.mark.silver
@pytest.mark.gold
class TestSilverGoldConsistency:
    """Verify data consistency between Silver and Gold layers."""

    def test_customer_count_matches(self, dim_customer, gold_customer_360):
        silver = dim_customer.filter(F.col("is_current") == "Y").count()
        gold = gold_customer_360.count()
        assert silver == gold, f"Customer count: silver={silver}, gold={gold}"

    def test_customer_ids_match(self, dim_customer, gold_customer_360):
        silver_ids = set(
            r["customer_id"]
            for r in dim_customer.filter(F.col("is_current") == "Y")
            .select("customer_id").collect()
        )
        gold_ids = set(
            r["customer_id"]
            for r in gold_customer_360.select("customer_id").collect()
        )
        assert silver_ids == gold_ids, "Customer ID sets differ between silver and gold"

    def test_transaction_total_amount_consistent(self, fact_transaction, gold_daily_txn_summary):
        """Total amount in fact should match sum across daily summary."""
        silver_total = fact_transaction.agg(F.sum("amount")).collect()[0][0]
        gold_total = gold_daily_txn_summary.agg(F.sum("total_amount")).collect()[0][0]
        if silver_total and gold_total:
            tolerance = float(silver_total) * 0.001
            assert abs(float(silver_total) - float(gold_total)) < tolerance, (
                f"Amount mismatch: silver={silver_total}, gold={gold_total}"
            )

    def test_loan_count_consistent(self, fact_loan, gold_loan_portfolio):
        silver = fact_loan.count()
        gold = gold_loan_portfolio.agg(F.sum("total_loans")).collect()[0][0]
        assert silver == gold, f"Loan count: silver={silver}, gold={gold}"


@pytest.mark.bronze
@pytest.mark.silver
class TestCrossLayerDataLineage:
    """Verify end-to-end data lineage from source through all layers."""

    def test_customer_name_lineage(self, source_customers, bronze_customers, dim_customer):
        """Pick a sample customer and verify name is preserved across layers."""
        sample = source_customers.limit(1).collect()[0]
        cust_id = sample["customer_id"]
        src_name = sample["first_name"].strip()

        brz = bronze_customers.filter(F.col("customer_id") == cust_id).collect()
        assert len(brz) > 0, f"Customer {cust_id} missing from bronze"
        assert brz[0]["first_name"].strip() == src_name, "Name changed in bronze"

        dim = dim_customer.filter(F.col("customer_id") == cust_id).collect()
        assert len(dim) > 0, f"Customer {cust_id} missing from silver"
        # Silver applies InitCap
        assert dim[0]["first_name"] == src_name.title(), "Name not properly transformed in silver"
