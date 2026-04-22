"""
test_schema_validation.py — Modern Data Quality: Schema Validation
Validates that each table has the expected columns, types, and no schema drift.
"""

import pytest
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DecimalType, DateType, TimestampType,
)


# ---------------------------------------------------------------------------
# Expected schema definitions
# ---------------------------------------------------------------------------
EXPECTED_BRONZE_CUSTOMERS_COLS = [
    "customer_id", "first_name", "last_name", "date_of_birth", "gender",
    "phone", "email", "pan_number", "aadhar_number", "address", "city",
    "state", "pin_code", "customer_since", "is_active",
    "_ingestion_ts", "_source_file", "_batch_id",
]

EXPECTED_DIM_CUSTOMER_COLS = [
    "customer_sk", "customer_id", "first_name", "last_name", "full_name",
    "date_of_birth", "age", "gender", "phone", "email", "pan_number",
    "aadhar_number", "city", "state", "pin_code", "customer_since",
    "is_active", "effective_date", "expiry_date", "is_current",
    "_etl_loaded_ts",
]

EXPECTED_FACT_TRANSACTION_COLS = [
    "transaction_sk", "transaction_id", "account_sk", "customer_sk",
    "branch_sk", "date_sk", "transaction_date", "transaction_time",
    "transaction_type", "amount", "currency", "channel", "status",
    "description", "beneficiary_acc", "reference_number", "_etl_loaded_ts",
]

EXPECTED_FACT_LOAN_COLS = [
    "loan_sk", "loan_id", "customer_sk", "branch_sk", "product_sk",
    "loan_type", "principal_amount", "interest_rate", "tenure_months",
    "emi_amount", "disburse_date", "maturity_date", "outstanding_amount",
    "status", "collateral_type", "_etl_loaded_ts",
]

EXPECTED_GOLD_CUSTOMER_360_COLS = [
    "customer_id", "full_name", "city", "state", "customer_since",
    "total_accounts", "active_accounts", "total_transactions",
    "customer_segment",
]


@pytest.mark.bronze
@pytest.mark.silver
@pytest.mark.gold
class TestSchemaColumns:
    """Validate expected columns exist (no missing columns)."""

    def test_bronze_customers_columns(self, bronze_customers):
        actual = set(bronze_customers.columns)
        expected = set(EXPECTED_BRONZE_CUSTOMERS_COLS)
        missing = expected - actual
        assert len(missing) == 0, f"Missing columns in bronze_customers: {missing}"

    def test_dim_customer_columns(self, dim_customer):
        actual = set(dim_customer.columns)
        expected = set(EXPECTED_DIM_CUSTOMER_COLS)
        missing = expected - actual
        assert len(missing) == 0, f"Missing columns in dim_customer: {missing}"

    def test_fact_transaction_columns(self, fact_transaction):
        actual = set(fact_transaction.columns)
        expected = set(EXPECTED_FACT_TRANSACTION_COLS)
        missing = expected - actual
        assert len(missing) == 0, f"Missing columns in fact_transaction: {missing}"

    def test_fact_loan_columns(self, fact_loan):
        actual = set(fact_loan.columns)
        expected = set(EXPECTED_FACT_LOAN_COLS)
        missing = expected - actual
        assert len(missing) == 0, f"Missing columns in fact_loan: {missing}"

    def test_gold_customer_360_columns(self, gold_customer_360):
        actual = set(gold_customer_360.columns)
        expected = set(EXPECTED_GOLD_CUSTOMER_360_COLS)
        missing = expected - actual
        assert len(missing) == 0, f"Missing columns in gold_customer_360: {missing}"


@pytest.mark.bronze
@pytest.mark.silver
class TestSchemaDrift:
    """Detect unexpected new columns (schema drift)."""

    def test_bronze_customers_no_extra_columns(self, bronze_customers):
        actual = set(bronze_customers.columns)
        expected = set(EXPECTED_BRONZE_CUSTOMERS_COLS)
        extra = actual - expected
        assert len(extra) == 0, f"Unexpected columns in bronze_customers: {extra}"

    def test_dim_customer_no_extra_columns(self, dim_customer):
        actual = set(dim_customer.columns)
        expected = set(EXPECTED_DIM_CUSTOMER_COLS)
        extra = actual - expected
        assert len(extra) == 0, f"Unexpected columns in dim_customer: {extra}"

    def test_fact_transaction_no_extra_columns(self, fact_transaction):
        actual = set(fact_transaction.columns)
        expected = set(EXPECTED_FACT_TRANSACTION_COLS)
        extra = actual - expected
        assert len(extra) == 0, f"Unexpected columns in fact_transaction: {extra}"


@pytest.mark.silver
class TestSchemaTypes:
    """Validate critical column types have not changed."""

    def test_dim_customer_key_types(self, dim_customer):
        schema = {f.name: type(f.dataType) for f in dim_customer.schema.fields}
        assert schema["customer_sk"] in (IntegerType, LongType), "customer_sk should be numeric"
        assert schema["date_of_birth"] == DateType, "date_of_birth should be DateType"
        assert schema["_etl_loaded_ts"] == TimestampType, "_etl_loaded_ts should be TimestampType"

    def test_fact_transaction_key_types(self, fact_transaction):
        schema = {f.name: type(f.dataType) for f in fact_transaction.schema.fields}
        assert schema["amount"] == DecimalType, "amount should be DecimalType"
        assert schema["date_sk"] in (IntegerType, LongType), "date_sk should be numeric"
        assert schema["transaction_date"] == DateType, "transaction_date should be DateType"
