"""
test_transformation_accuracy.py — Traditional ETL Test: Transformation Logic Validation
Verifies that business rules and transformations were applied correctly.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.silver
@pytest.mark.dim_customer
class TestCustomerTransformations:
    """Validate dim_customer transformation rules."""

    def test_full_name_is_first_plus_last(self, dim_customer):
        """full_name should be first_name + ' ' + last_name."""
        mismatches = (
            dim_customer
            .filter(
                F.col("full_name") != F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
            )
            .count()
        )
        assert mismatches == 0, f"{mismatches} rows with incorrect full_name"

    def test_names_are_initcap(self, dim_customer):
        """first_name and last_name should be InitCap."""
        bad_first = dim_customer.filter(
            F.col("first_name") != F.initcap(F.col("first_name"))
        ).count()
        bad_last = dim_customer.filter(
            F.col("last_name") != F.initcap(F.col("last_name"))
        ).count()
        assert bad_first == 0, f"{bad_first} rows with non-InitCap first_name"
        assert bad_last == 0, f"{bad_last} rows with non-InitCap last_name"

    def test_email_is_lowercase(self, dim_customer):
        bad = dim_customer.filter(F.col("email") != F.lower(F.col("email"))).count()
        assert bad == 0, f"{bad} rows with non-lowercase email"

    def test_pan_is_uppercase(self, dim_customer):
        bad = dim_customer.filter(F.col("pan_number") != F.upper(F.col("pan_number"))).count()
        assert bad == 0, f"{bad} rows with non-uppercase pan_number"

    def test_gender_mapping(self, dim_customer):
        """Gender should be Male, Female, or Other."""
        invalid = dim_customer.filter(
            ~F.col("gender").isin("Male", "Female", "Other")
        ).count()
        assert invalid == 0, f"{invalid} rows with invalid gender mapping"

    def test_scd2_current_flag(self, dim_customer):
        """Each customer_id should have exactly one is_current='Y' record."""
        multiple_current = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .groupBy("customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert multiple_current == 0, f"{multiple_current} customers with multiple current records"

    def test_scd2_expiry_date_for_current(self, dim_customer):
        """Current records should have expiry_date = 9999-12-31."""
        bad = (
            dim_customer
            .filter(F.col("is_current") == "Y")
            .filter(F.col("expiry_date") != F.lit("9999-12-31").cast("date"))
            .count()
        )
        assert bad == 0, f"{bad} current records with wrong expiry_date"


@pytest.mark.silver
@pytest.mark.dim_account
class TestAccountTransformations:
    """Validate dim_account transformation rules."""

    def test_account_type_uppercase(self, dim_account):
        bad = dim_account.filter(
            F.col("account_type") != F.upper(F.col("account_type"))
        ).count()
        assert bad == 0, f"{bad} rows with non-uppercase account_type"

    def test_status_uppercase(self, dim_account):
        bad = dim_account.filter(
            F.col("status") != F.upper(F.col("status"))
        ).count()
        assert bad == 0, f"{bad} rows with non-uppercase status"

    def test_valid_status_values(self, dim_account):
        valid = {"ACTIVE", "DORMANT", "CLOSED"}
        invalid = dim_account.filter(~F.col("status").isin(valid)).count()
        assert invalid == 0, f"{invalid} rows with invalid status"


@pytest.mark.silver
@pytest.mark.fact_transaction
class TestTransactionTransformations:
    """Validate fact_transaction transformation rules."""

    def test_transaction_type_uppercase(self, fact_transaction):
        bad = fact_transaction.filter(
            F.col("transaction_type") != F.upper(F.col("transaction_type"))
        ).count()
        assert bad == 0, f"{bad} rows with non-uppercase transaction_type"

    def test_valid_transaction_types(self, fact_transaction):
        valid = {"CREDIT", "DEBIT"}
        invalid = fact_transaction.filter(~F.col("transaction_type").isin(valid)).count()
        assert invalid == 0, f"{invalid} rows with invalid transaction_type"

    def test_valid_channels(self, fact_transaction):
        valid = {"NEFT", "RTGS", "IMPS", "UPI", "ATM", "BRANCH", "NET_BANKING", "MOBILE_BANKING"}
        invalid = fact_transaction.filter(~F.col("channel").isin(valid)).count()
        assert invalid == 0, f"{invalid} rows with invalid channel"

    def test_valid_status(self, fact_transaction):
        valid = {"SUCCESS", "FAILED", "PENDING", "REVERSED"}
        invalid = fact_transaction.filter(~F.col("status").isin(valid)).count()
        assert invalid == 0, f"{invalid} rows with invalid status"


@pytest.mark.gold
class TestGoldTransformations:
    """Validate Gold layer derived columns."""

    def test_customer_segment_values(self, gold_customer_360):
        valid = {"PLATINUM", "GOLD", "SILVER", "BASIC"}
        invalid = gold_customer_360.filter(~F.col("customer_segment").isin(valid)).count()
        assert invalid == 0, f"{invalid} rows with invalid customer_segment"

    def test_daily_summary_success_plus_failed_lte_total(self, gold_daily_txn_summary):
        """success_count + failed_count should be <= total_count."""
        bad = (
            gold_daily_txn_summary
            .filter(
                (F.col("success_count") + F.col("failed_count")) > F.col("total_count")
            )
            .count()
        )
        assert bad == 0, f"{bad} rows where success+failed > total"

    def test_npa_ratio_range(self, gold_loan_portfolio):
        """NPA ratio should be between 0 and 100."""
        bad = gold_loan_portfolio.filter(
            (F.col("npa_ratio") < 0) | (F.col("npa_ratio") > 100)
        ).count()
        assert bad == 0, f"{bad} rows with NPA ratio out of 0-100 range"
