"""
test_data_lineage.py — Integration Test: Data Lineage Tracing
Traces individual records end-to-end across all layers to verify data lineage.
"""

import pytest
from pyspark.sql import functions as F


class TestCustomerLineage:
    """Trace a customer record from source through all layers."""

    def test_single_customer_lineage(self, source_customers, bronze_customers,
                                     dim_customer, gold_customer_360):
        """Pick the first customer and trace through all layers."""
        # Source
        src_row = source_customers.limit(1).collect()[0]
        cust_id = src_row["customer_id"]
        src_first = src_row["first_name"].strip()
        src_last  = src_row["last_name"].strip()
        src_email = src_row["email"].strip()

        # Bronze — should be identical to source
        brz = bronze_customers.filter(F.col("customer_id") == cust_id).collect()
        assert len(brz) >= 1, f"Customer {cust_id} missing from bronze"
        assert brz[0]["first_name"] == src_row["first_name"], "first_name changed in bronze"
        assert brz[0]["email"] == src_row["email"], "email changed in bronze"

        # Silver — should have transformations applied
        dim = dim_customer.filter(F.col("customer_id") == cust_id).collect()
        assert len(dim) >= 1, f"Customer {cust_id} missing from silver"
        assert dim[0]["first_name"] == src_first.title(), "first_name not InitCap in silver"
        assert dim[0]["email"] == src_email.lower(), "email not lowercased in silver"
        assert dim[0]["full_name"] == f"{src_first.title()} {src_last.title()}", "full_name wrong"

        # Gold — should be present
        gold = gold_customer_360.filter(F.col("customer_id") == cust_id).collect()
        assert len(gold) == 1, f"Customer {cust_id} missing from gold_customer_360"
        assert gold[0]["full_name"] == f"{src_first.title()} {src_last.title()}", "Name mismatch in gold"


class TestTransactionLineage:
    """Trace a transaction from source through to gold summary."""

    def test_single_transaction_lineage(self, source_transactions, bronze_transactions,
                                        fact_transaction):
        """Pick a transaction and verify it exists in each layer."""
        src_row = source_transactions.limit(1).collect()[0]
        txn_id = src_row["transaction_id"]
        src_amount = float(src_row["amount"])

        # Bronze
        brz = bronze_transactions.filter(F.col("transaction_id") == txn_id).collect()
        assert len(brz) == 1, f"Transaction {txn_id} missing from bronze"
        assert float(brz[0]["amount"]) == src_amount, "amount changed in bronze"

        # Silver
        fact = fact_transaction.filter(F.col("transaction_id") == txn_id).collect()
        assert len(fact) == 1, f"Transaction {txn_id} missing from silver"
        assert abs(float(fact[0]["amount"]) - src_amount) < 0.01, "amount changed in silver"

        # Verify FK lookups populated
        assert fact[0]["account_sk"] is not None, "account_sk not resolved"
        assert fact[0]["date_sk"] is not None, "date_sk not resolved"

    def test_transaction_type_preserved(self, source_transactions, fact_transaction):
        """Transaction type should be uppercase version of source."""
        sample = source_transactions.limit(5).collect()
        for row in sample:
            txn_id = row["transaction_id"]
            src_type = row["transaction_type"].strip().upper()
            fact = fact_transaction.filter(F.col("transaction_id") == txn_id).collect()
            if fact:
                assert fact[0]["transaction_type"] == src_type, (
                    f"Type mismatch for {txn_id}: source={src_type}, silver={fact[0]['transaction_type']}"
                )


class TestLoanLineage:
    """Trace a loan from source through all layers."""

    def test_single_loan_lineage(self, source_loans, bronze_loans, fact_loan):
        src_row = source_loans.limit(1).collect()[0]
        loan_id = src_row["loan_id"]

        # Bronze
        brz = bronze_loans.filter(F.col("loan_id") == loan_id).collect()
        assert len(brz) == 1, f"Loan {loan_id} missing from bronze"

        # Silver
        fact = fact_loan.filter(F.col("loan_id") == loan_id).collect()
        assert len(fact) == 1, f"Loan {loan_id} missing from silver"
        assert fact[0]["customer_sk"] is not None, "customer_sk not resolved for loan"
        assert fact[0]["product_sk"] is not None, "product_sk not resolved for loan"


class TestBranchLineage:
    """Trace branch data across layers."""

    def test_branch_lineage(self, source_branches, bronze_branches, dim_branch):
        src_row = source_branches.limit(1).collect()[0]
        branch_id = src_row["branch_id"]

        brz = bronze_branches.filter(F.col("branch_id") == branch_id).collect()
        assert len(brz) == 1, f"Branch {branch_id} missing from bronze"

        dim = dim_branch.filter(F.col("branch_id") == branch_id).collect()
        assert len(dim) == 1, f"Branch {branch_id} missing from silver"
        # City should be InitCap in silver
        src_city = src_row["city"].strip()
        assert dim[0]["city"] == src_city.title(), f"City not InitCap: {dim[0]['city']}"
