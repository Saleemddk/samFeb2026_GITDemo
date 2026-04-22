"""
test_null_checks.py — Traditional ETL Test: Null / Missing Value Validation
Ensures critical columns do not contain NULLs or empty strings.
"""

import pytest
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def count_nulls_or_empty(df, column):
    """Return count of NULL or empty-string values in a column."""
    return df.filter(
        F.col(column).isNull() | (F.trim(F.col(column)) == "")
    ).count()


# ---------------------------------------------------------------------------
# Bronze Null Checks
# ---------------------------------------------------------------------------
@pytest.mark.bronze
class TestNullChecksBronze:

    @pytest.mark.parametrize("column", [
        "customer_id", "first_name", "last_name", "email", "phone",
    ])
    def test_bronze_customers_not_null(self, bronze_customers, column):
        nulls = count_nulls_or_empty(bronze_customers, column)
        assert nulls == 0, f"bronze_customers.{column} has {nulls} null/empty values"

    @pytest.mark.parametrize("column", [
        "account_id", "customer_id", "account_number", "account_type", "balance",
    ])
    def test_bronze_accounts_not_null(self, bronze_accounts, column):
        nulls = count_nulls_or_empty(bronze_accounts, column)
        assert nulls == 0, f"bronze_accounts.{column} has {nulls} null/empty values"

    @pytest.mark.parametrize("column", [
        "transaction_id", "account_id", "transaction_date", "amount", "status",
    ])
    def test_bronze_transactions_not_null(self, bronze_transactions, column):
        nulls = count_nulls_or_empty(bronze_transactions, column)
        assert nulls == 0, f"bronze_transactions.{column} has {nulls} null/empty values"

    @pytest.mark.parametrize("column", [
        "loan_id", "customer_id", "principal_amount", "interest_rate", "status",
    ])
    def test_bronze_loans_not_null(self, bronze_loans, column):
        nulls = count_nulls_or_empty(bronze_loans, column)
        assert nulls == 0, f"bronze_loans.{column} has {nulls} null/empty values"

    @pytest.mark.parametrize("column", [
        "branch_id", "branch_name", "city", "state",
    ])
    def test_bronze_branches_not_null(self, bronze_branches, column):
        nulls = count_nulls_or_empty(bronze_branches, column)
        assert nulls == 0, f"bronze_branches.{column} has {nulls} null/empty values"


# ---------------------------------------------------------------------------
# Silver Null Checks
# ---------------------------------------------------------------------------
@pytest.mark.silver
class TestNullChecksSilver:

    @pytest.mark.parametrize("column", [
        "customer_sk", "customer_id", "first_name", "last_name", "full_name",
        "email", "effective_date", "is_current",
    ])
    def test_dim_customer_not_null(self, dim_customer, column):
        null_count = dim_customer.filter(F.col(column).isNull()).count()
        assert null_count == 0, f"dim_customer.{column} has {null_count} NULLs"

    @pytest.mark.parametrize("column", [
        "account_sk", "account_id", "customer_id", "account_type", "status",
    ])
    def test_dim_account_not_null(self, dim_account, column):
        null_count = dim_account.filter(F.col(column).isNull()).count()
        assert null_count == 0, f"dim_account.{column} has {null_count} NULLs"

    @pytest.mark.parametrize("column", [
        "transaction_sk", "transaction_id", "transaction_date", "amount", "status",
    ])
    def test_fact_transaction_not_null(self, fact_transaction, column):
        null_count = fact_transaction.filter(F.col(column).isNull()).count()
        assert null_count == 0, f"fact_transaction.{column} has {null_count} NULLs"

    @pytest.mark.parametrize("column", [
        "loan_sk", "loan_id", "principal_amount", "status",
    ])
    def test_fact_loan_not_null(self, fact_loan, column):
        null_count = fact_loan.filter(F.col(column).isNull()).count()
        assert null_count == 0, f"fact_loan.{column} has {null_count} NULLs"

    def test_dim_date_no_nulls(self, dim_date):
        for col_name in ["date_sk", "full_date", "year", "month", "day_of_month"]:
            null_count = dim_date.filter(F.col(col_name).isNull()).count()
            assert null_count == 0, f"dim_date.{col_name} has {null_count} NULLs"


# ---------------------------------------------------------------------------
# Gold Null Checks
# ---------------------------------------------------------------------------
class TestNullChecksGold:

    @pytest.mark.parametrize("column", [
        "customer_id", "full_name", "customer_segment",
    ])
    def test_gold_customer_360_not_null(self, gold_customer_360, column):
        null_count = gold_customer_360.filter(F.col(column).isNull()).count()
        assert null_count == 0, f"gold_customer_360.{column} has {null_count} NULLs"

    def test_gold_daily_txn_not_null_keys(self, gold_daily_txn_summary):
        for col_name in ["transaction_date", "channel", "transaction_type"]:
            null_count = gold_daily_txn_summary.filter(F.col(col_name).isNull()).count()
            assert null_count == 0, f"gold_daily_txn_summary.{col_name} has NULLs"
