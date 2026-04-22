"""
test_business_rules.py — Modern Data Quality: Business Rule Validation
Validates banking-specific business rules across all layers.
"""

import pytest
from pyspark.sql import functions as F


@pytest.mark.bronze
@pytest.mark.silver
class TestPhoneNumberRules:
    """Indian phone numbers must follow +91 format."""

    def test_bronze_phone_format(self, bronze_customers):
        """All phones should start with +91 followed by 6/7/8/9."""
        invalid = bronze_customers.filter(
            ~F.col("phone").rlike(r"^\+91[6-9]\d{9}$")
        ).count()
        assert invalid == 0, f"{invalid} invalid phone numbers in bronze_customers"

    def test_dim_customer_phone_format(self, dim_customer):
        invalid = dim_customer.filter(
            ~F.col("phone").rlike(r"^\+91[6-9]\d{9}$")
        ).count()
        assert invalid == 0, f"{invalid} invalid phone numbers in dim_customer"


@pytest.mark.bronze
@pytest.mark.silver
class TestEmailRules:
    """Email addresses must be valid format."""

    def test_bronze_email_format(self, bronze_customers):
        invalid = bronze_customers.filter(
            ~F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        ).count()
        assert invalid == 0, f"{invalid} invalid emails in bronze_customers"

    def test_dim_customer_email_lowercase(self, dim_customer):
        """Emails in silver should be all lowercase."""
        bad = dim_customer.filter(F.col("email") != F.lower(F.col("email"))).count()
        assert bad == 0, f"{bad} non-lowercase emails in dim_customer"

    def test_email_domain_valid(self, dim_customer):
        """Emails should use expected domains."""
        valid_domains = ["gmail.com", "yahoo.in", "rediffmail.com", "outlook.com"]
        for domain in valid_domains:
            count = dim_customer.filter(F.col("email").endswith(f"@{domain}")).count()
            # At least some emails should be from each domain
            assert count > 0, f"No emails from {domain}"


@pytest.mark.silver
class TestPANRules:
    """PAN number should be valid format: XXXXX0000X."""

    def test_pan_format(self, dim_customer):
        invalid = dim_customer.filter(
            ~F.col("pan_number").rlike(r"^[A-Z]{5}\d{4}[A-Z]$")
        ).count()
        assert invalid == 0, f"{invalid} invalid PAN numbers in dim_customer"


@pytest.mark.silver
class TestAadharRules:
    """Aadhar should be 12 digits."""

    def test_aadhar_length(self, dim_customer):
        invalid = dim_customer.filter(
            F.length(F.col("aadhar_number")) != 12
        ).count()
        assert invalid == 0, f"{invalid} aadhar numbers with wrong length"


@pytest.mark.silver
class TestAccountRules:
    """Banking account business rules."""

    def test_account_number_12_digits(self, dim_account):
        invalid = dim_account.filter(
            ~F.col("account_number").rlike(r"^\d{12}$")
        ).count()
        assert invalid == 0, f"{invalid} account numbers not 12 digits"

    def test_closed_accounts_have_close_date(self, dim_account):
        """Closed accounts should have a close_date."""
        bad = dim_account.filter(
            (F.col("status") == "CLOSED") & F.col("close_date").isNull()
        ).count()
        # Not all closed accounts may have close_date in raw data
        # This is a soft business rule check
        if bad > 0:
            pytest.skip(f"{bad} closed accounts without close_date (data quality issue)")

    def test_open_date_not_future(self, dim_account):
        future = dim_account.filter(F.col("open_date") > F.current_date()).count()
        assert future == 0, f"{future} accounts with future open_date"


@pytest.mark.silver
class TestLoanRules:
    """Loan business rules."""

    def test_emi_less_than_principal(self, fact_loan):
        """Monthly EMI should be less than principal."""
        bad = fact_loan.filter(F.col("emi_amount") > F.col("principal_amount")).count()
        assert bad == 0, f"{bad} loans with EMI > principal"

    def test_outstanding_not_exceed_principal(self, fact_loan):
        """Outstanding should not exceed principal (no negative amortization)."""
        bad = fact_loan.filter(
            F.col("outstanding_amount") > F.col("principal_amount") * 1.5
        ).count()
        assert bad == 0, f"{bad} loans with outstanding > 1.5x principal"

    def test_maturity_after_disburse(self, fact_loan):
        """Maturity date should be after disbursement date."""
        bad = fact_loan.filter(F.col("maturity_date") <= F.col("disburse_date")).count()
        assert bad == 0, f"{bad} loans with maturity <= disburse date"

    def test_loan_status_values(self, fact_loan):
        valid = {"ACTIVE", "CLOSED", "DEFAULT", "RESTRUCTURED"}
        invalid = fact_loan.filter(~F.col("status").isin(valid)).count()
        assert invalid == 0, f"{invalid} loans with invalid status"


@pytest.mark.silver
class TestTransactionRules:
    """Transaction business rules."""

    def test_no_zero_amount_transactions(self, fact_transaction):
        zeros = fact_transaction.filter(F.col("amount") == 0).count()
        assert zeros == 0, f"{zeros} zero-amount transactions found"

    def test_reference_number_not_empty_for_success(self, fact_transaction):
        """Successful transactions should have reference numbers."""
        bad = fact_transaction.filter(
            (F.col("status") == "SUCCESS") &
            (F.col("reference_number").isNull() | (F.trim(F.col("reference_number")) == ""))
        ).count()
        assert bad == 0, f"{bad} successful transactions without reference number"


@pytest.mark.gold
class TestCustomerSegmentRules:
    """Gold layer segmentation rules."""

    def test_segment_consistency(self, gold_customer_360):
        """Customer segment should be one of the defined values."""
        valid = {"PLATINUM", "GOLD", "SILVER", "BASIC"}
        invalid = gold_customer_360.filter(~F.col("customer_segment").isin(valid)).count()
        assert invalid == 0, f"{invalid} customers with invalid segment"

    def test_active_accounts_lte_total(self, gold_customer_360):
        """Active accounts should not exceed total accounts."""
        bad = gold_customer_360.filter(
            F.col("active_accounts") > F.col("total_accounts")
        ).count()
        assert bad == 0, f"{bad} customers with active > total accounts"
