"""
test_duplicates.py — Traditional ETL Test: Duplicate Detection
Ensures primary keys and unique columns have no duplicate values.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.bronze
class TestDuplicatesBronze:
    """Check for unexpected duplicates in Bronze layer."""

    def test_bronze_customer_id_unique(self, bronze_customers):
        total = bronze_customers.count()
        distinct = bronze_customers.select("customer_id").distinct().count()
        assert total == distinct, (
            f"Duplicate customer_id found: total={total}, distinct={distinct}"
        )

    def test_bronze_transaction_id_unique(self, bronze_transactions):
        total = bronze_transactions.count()
        distinct = bronze_transactions.select("transaction_id").distinct().count()
        assert total == distinct, (
            f"Duplicate transaction_id found: total={total}, distinct={distinct}"
        )

    def test_bronze_loan_id_unique(self, bronze_loans):
        total = bronze_loans.count()
        distinct = bronze_loans.select("loan_id").distinct().count()
        assert total == distinct, (
            f"Duplicate loan_id found: total={total}, distinct={distinct}"
        )

    def test_bronze_branch_id_unique(self, bronze_branches):
        total = bronze_branches.count()
        distinct = bronze_branches.select("branch_id").distinct().count()
        assert total == distinct, (
            f"Duplicate branch_id found: total={total}, distinct={distinct}"
        )


@pytest.mark.silver
class TestDuplicatesSilver:
    """Check for duplicate surrogate & business keys in Silver layer."""

    def test_dim_customer_sk_unique(self, dim_customer):
        total = dim_customer.count()
        distinct = dim_customer.select("customer_sk").distinct().count()
        assert total == distinct, "Duplicate customer_sk in dim_customer"

    def test_dim_customer_business_key_unique_per_version(self, dim_customer):
        """For SCD2: customer_id + effective_date should be unique."""
        total = dim_customer.count()
        distinct = dim_customer.select("customer_id", "effective_date").distinct().count()
        assert total == distinct, "Duplicate customer_id + effective_date in dim_customer"

    def test_dim_account_sk_unique(self, dim_account):
        total = dim_account.count()
        distinct = dim_account.select("account_sk").distinct().count()
        assert total == distinct, "Duplicate account_sk in dim_account"

    def test_dim_account_id_unique(self, dim_account):
        total = dim_account.count()
        distinct = dim_account.select("account_id").distinct().count()
        assert total == distinct, "Duplicate account_id in dim_account"

    def test_dim_branch_sk_unique(self, dim_branch):
        total = dim_branch.count()
        distinct = dim_branch.select("branch_sk").distinct().count()
        assert total == distinct, "Duplicate branch_sk in dim_branch"

    def test_fact_transaction_sk_unique(self, fact_transaction):
        total = fact_transaction.count()
        distinct = fact_transaction.select("transaction_sk").distinct().count()
        assert total == distinct, "Duplicate transaction_sk in fact_transaction"

    def test_fact_transaction_id_unique(self, fact_transaction):
        total = fact_transaction.count()
        distinct = fact_transaction.select("transaction_id").distinct().count()
        assert total == distinct, "Duplicate transaction_id in fact_transaction"

    def test_fact_loan_sk_unique(self, fact_loan):
        total = fact_loan.count()
        distinct = fact_loan.select("loan_sk").distinct().count()
        assert total == distinct, "Duplicate loan_sk in fact_loan"

    def test_dim_date_sk_unique(self, dim_date):
        total = dim_date.count()
        distinct = dim_date.select("date_sk").distinct().count()
        assert total == distinct, "Duplicate date_sk in dim_date"


@pytest.mark.gold
class TestDuplicatesGold:
    """Check for duplicate keys in Gold layer."""

    def test_gold_customer_360_unique(self, gold_customer_360):
        total = gold_customer_360.count()
        distinct = gold_customer_360.select("customer_id").distinct().count()
        assert total == distinct, "Duplicate customer_id in gold_customer_360"

    def test_gold_daily_txn_summary_unique(self, gold_daily_txn_summary):
        """Grain: transaction_date + channel + transaction_type."""
        total = gold_daily_txn_summary.count()
        distinct = (
            gold_daily_txn_summary
            .select("transaction_date", "channel", "transaction_type")
            .distinct()
            .count()
        )
        assert total == distinct, "Duplicate grain key in gold_daily_txn_summary"

    def test_gold_loan_portfolio_unique(self, gold_loan_portfolio):
        """Grain: loan_type + report_month."""
        total = gold_loan_portfolio.count()
        distinct = (
            gold_loan_portfolio
            .select("loan_type", "report_month")
            .distinct()
            .count()
        )
        assert total == distinct, "Duplicate grain key in gold_loan_portfolio"
