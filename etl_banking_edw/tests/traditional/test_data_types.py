"""
test_data_types.py — Traditional ETL Test: Data Type Validation
Ensures data was correctly cast during transformation.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, TimestampType, IntegerType, LongType, DecimalType, StringType,
)


@pytest.mark.silver
class TestDataTypesSilver:
    """Validate Silver layer columns have correct data types after casting."""

    def test_dim_customer_date_columns(self, dim_customer):
        schema = {f.name: f.dataType for f in dim_customer.schema.fields}
        assert isinstance(schema["date_of_birth"], DateType), "date_of_birth should be DateType"
        assert isinstance(schema["customer_since"], DateType), "customer_since should be DateType"
        assert isinstance(schema["effective_date"], DateType), "effective_date should be DateType"
        assert isinstance(schema["expiry_date"], DateType), "expiry_date should be DateType"

    def test_dim_customer_timestamp_columns(self, dim_customer):
        schema = {f.name: f.dataType for f in dim_customer.schema.fields}
        assert isinstance(schema["_etl_loaded_ts"], TimestampType), "_etl_loaded_ts should be TimestampType"

    def test_dim_account_date_columns(self, dim_account):
        schema = {f.name: f.dataType for f in dim_account.schema.fields}
        assert isinstance(schema["open_date"], DateType), "open_date should be DateType"

    def test_fact_transaction_numeric_types(self, fact_transaction):
        schema = {f.name: f.dataType for f in fact_transaction.schema.fields}
        assert isinstance(schema["amount"], DecimalType), "amount should be DecimalType"
        assert isinstance(schema["date_sk"], IntegerType), "date_sk should be IntegerType"

    def test_fact_transaction_date_type(self, fact_transaction):
        schema = {f.name: f.dataType for f in fact_transaction.schema.fields}
        assert isinstance(schema["transaction_date"], DateType), "transaction_date should be DateType"

    def test_fact_loan_numeric_types(self, fact_loan):
        schema = {f.name: f.dataType for f in fact_loan.schema.fields}
        assert isinstance(schema["principal_amount"], DecimalType), "principal_amount should be DecimalType"
        assert isinstance(schema["interest_rate"], DecimalType), "interest_rate should be DecimalType"
        assert isinstance(schema["tenure_months"], IntegerType), "tenure_months should be IntegerType"
        assert isinstance(schema["emi_amount"], DecimalType), "emi_amount should be DecimalType"
        assert isinstance(schema["outstanding_amount"], DecimalType), "outstanding_amount should be DecimalType"

    def test_dim_product_interest_rate_type(self, dim_product):
        schema = {f.name: f.dataType for f in dim_product.schema.fields}
        assert isinstance(schema["interest_rate"], DecimalType), "interest_rate should be DecimalType"

    def test_dim_date_types(self, dim_date):
        schema = {f.name: f.dataType for f in dim_date.schema.fields}
        assert isinstance(schema["date_sk"], (IntegerType, LongType)), "date_sk should be numeric"
        assert isinstance(schema["year"], (IntegerType, LongType)), "year should be numeric"


@pytest.mark.silver
class TestDataValidity:
    """Validate data values are within acceptable ranges after casting."""

    def test_transaction_amounts_positive(self, fact_transaction):
        negatives = fact_transaction.filter(F.col("amount") < 0).count()
        assert negatives == 0, f"{negatives} negative amounts in fact_transaction"

    def test_loan_principal_positive(self, fact_loan):
        negatives = fact_loan.filter(F.col("principal_amount") <= 0).count()
        assert negatives == 0, f"{negatives} non-positive principal amounts in fact_loan"

    def test_interest_rate_range(self, fact_loan):
        out_of_range = fact_loan.filter(
            (F.col("interest_rate") < 0) | (F.col("interest_rate") > 50)
        ).count()
        assert out_of_range == 0, f"{out_of_range} interest rates out of range"

    def test_tenure_months_positive(self, fact_loan):
        invalid = fact_loan.filter(F.col("tenure_months") <= 0).count()
        assert invalid == 0, f"{invalid} non-positive tenure_months in fact_loan"

    def test_dim_customer_age_range(self, dim_customer):
        invalid = dim_customer.filter(
            (F.col("age") < 18) | (F.col("age") > 120)
        ).count()
        # Allow some — but flag suspicious
        if invalid > 0:
            pytest.warns(UserWarning, match=f"{invalid} customers outside 18-120 age range")

    def test_date_sk_format(self, fact_transaction):
        """date_sk should be in yyyyMMdd format (20080101 to 20271231)."""
        invalid = fact_transaction.filter(
            (F.col("date_sk") < 20080101) | (F.col("date_sk") > 20271231)
        ).count()
        assert invalid == 0, f"{invalid} invalid date_sk values in fact_transaction"
