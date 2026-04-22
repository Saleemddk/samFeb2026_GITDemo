"""
test_referential_integrity.py — Traditional ETL Test: FK / Referential Integrity
Validates that foreign keys in fact tables reference valid dimension records.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.silver
@pytest.mark.fact_transaction
class TestFactTransactionRI:
    """Referential integrity checks for fact_transaction."""

    def test_account_sk_references_dim_account(self, fact_transaction, dim_account):
        """Every non-null account_sk in fact_transaction must exist in dim_account."""
        fact_keys = (
            fact_transaction
            .filter(F.col("account_sk").isNotNull())
            .select("account_sk")
            .distinct()
        )
        dim_keys = dim_account.select("account_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan account_sk values in fact_transaction"

    def test_customer_sk_references_dim_customer(self, fact_transaction, dim_customer):
        fact_keys = (
            fact_transaction
            .filter(F.col("customer_sk").isNotNull())
            .select("customer_sk")
            .distinct()
        )
        dim_keys = dim_customer.select("customer_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan customer_sk values in fact_transaction"

    def test_branch_sk_references_dim_branch(self, fact_transaction, dim_branch):
        fact_keys = (
            fact_transaction
            .filter(F.col("branch_sk").isNotNull())
            .select("branch_sk")
            .distinct()
        )
        dim_keys = dim_branch.select("branch_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan branch_sk values in fact_transaction"

    def test_date_sk_references_dim_date(self, fact_transaction, dim_date):
        fact_keys = (
            fact_transaction
            .filter(F.col("date_sk").isNotNull())
            .select("date_sk")
            .distinct()
        )
        dim_keys = dim_date.select("date_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan date_sk values in fact_transaction"


@pytest.mark.silver
@pytest.mark.fact_loan
class TestFactLoanRI:
    """Referential integrity checks for fact_loan."""

    def test_customer_sk_references_dim_customer(self, fact_loan, dim_customer):
        fact_keys = (
            fact_loan
            .filter(F.col("customer_sk").isNotNull())
            .select("customer_sk")
            .distinct()
        )
        dim_keys = dim_customer.select("customer_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan customer_sk in fact_loan"

    def test_branch_sk_references_dim_branch(self, fact_loan, dim_branch):
        fact_keys = (
            fact_loan
            .filter(F.col("branch_sk").isNotNull())
            .select("branch_sk")
            .distinct()
        )
        dim_keys = dim_branch.select("branch_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan branch_sk in fact_loan"

    def test_product_sk_references_dim_product(self, fact_loan, dim_product):
        fact_keys = (
            fact_loan
            .filter(F.col("product_sk").isNotNull())
            .select("product_sk")
            .distinct()
        )
        dim_keys = dim_product.select("product_sk").distinct()
        orphans = fact_keys.subtract(dim_keys).count()
        assert orphans == 0, f"{orphans} orphan product_sk in fact_loan"


@pytest.mark.silver
@pytest.mark.dim_account
class TestDimAccountRI:
    """Referential integrity for dimension cross-references."""

    def test_customer_id_references_dim_customer(self, dim_account, dim_customer):
        acc_custs = dim_account.select("customer_id").distinct()
        dim_custs = dim_customer.select("customer_id").distinct()
        orphans = acc_custs.subtract(dim_custs).count()
        assert orphans == 0, f"{orphans} orphan customer_id in dim_account"

    def test_branch_id_references_dim_branch(self, dim_account, dim_branch):
        acc_branches = dim_account.select("branch_id").distinct()
        dim_branches = dim_branch.select("branch_id").distinct()
        orphans = acc_branches.subtract(dim_branches).count()
        assert orphans == 0, f"{orphans} orphan branch_id in dim_account"
