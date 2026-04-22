"""
test_negative_testing.py — Negative/Edge Case Tests

Tests how the pipeline handles bad, malformed, or unexpected data:
1. Negative amounts — should be captured but flagged
2. Future dates — should be identified
3. Null/empty required fields — should not break pipeline
4. Invalid date formats — should become null after casting
5. Zero-amount transactions — should be allowed but flagged
6. Orphan records — FK references to non-existent parents
"""

import pytest
from pyspark.sql import functions as F
from datetime import date


pytestmark = [pytest.mark.negative]


class TestNegativeAmounts:
    """Bad data: Negative transaction amounts."""

    @pytest.mark.bronze
    def test_bronze_accepts_negative_amounts(self, bronze_transactions):
        """Bronze should accept ALL data as-is (no rejection at ingestion)."""
        # Bronze stores raw strings — negative amounts are just strings
        neg = bronze_transactions.filter(F.col("amount").startswith("-")).count()
        # This is informational — bronze accepts everything
        print(f"  INFO: {neg} records with negative amounts in Bronze (accepted as raw)")

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_silver_captures_negative_amounts(self, fact_transaction):
        """Silver should cast amounts — negative values preserved as-is for now."""
        neg = fact_transaction.filter(F.col("amount") < 0).count()
        # Informational: in a real system, these would be flagged for review
        print(f"  INFO: {neg} transactions with negative amounts in Silver")

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_silver_amounts_are_numeric(self, fact_transaction):
        """All amount values in Silver must be properly cast to decimal (not null from bad cast)."""
        # If casting fails, value becomes null — check for unexpected nulls
        total = fact_transaction.count()
        nulls = fact_transaction.filter(F.col("amount").isNull()).count()
        null_pct = (nulls / total * 100) if total > 0 else 0
        # Allow small % of nulls (from genuinely bad data) but not majority
        assert null_pct < 5, (
            f"{null_pct:.1f}% of transactions have null amount (casting failures?)"
        )


class TestFutureDates:
    """Bad data: Transaction dates in the far future."""

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_identify_future_dated_transactions(self, fact_transaction):
        """Identify transactions with dates beyond today (potential data issue)."""
        today = date.today().isoformat()
        future = (
            fact_transaction
            .filter(F.col("transaction_date") > F.lit(today).cast("date"))
            .count()
        )
        # In strict mode this would fail; here we just report
        if future > 0:
            print(f"  WARNING: {future} transactions with future dates detected")
        # Allow up to a few (could be timezone differences) but not many
        assert future < 50, (
            f"{future} future-dated transactions — likely bad source data"
        )

    @pytest.mark.silver
    @pytest.mark.fact_loan
    def test_loan_maturity_in_future_is_valid(self, fact_loan):
        """Active loan maturity dates SHOULD be in the future (this is correct)."""
        active_loans = fact_loan.filter(F.col("status") == "ACTIVE")
        if active_loans.count() == 0:
            pytest.skip("No active loans")

        past_maturity = (
            active_loans
            .filter(F.col("maturity_date") < F.current_date())
            .count()
        )
        # Active loans with past maturity = suspicious (should have been closed)
        active_total = active_loans.count()
        if past_maturity > 0:
            pct = past_maturity / active_total * 100
            print(f"  WARNING: {past_maturity} active loans ({pct:.1f}%) have past maturity dates")


class TestNullRequiredFields:
    """Bad data: NULL/empty values in fields that should be mandatory."""

    @pytest.mark.silver
    @pytest.mark.dim_customer
    def test_customer_id_never_null(self, dim_customer):
        """customer_id must never be null."""
        nulls = dim_customer.filter(
            F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")
        ).count()
        assert nulls == 0, f"{nulls} dim_customer records with null customer_id"

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_transaction_id_never_null(self, fact_transaction):
        """transaction_id must never be null."""
        nulls = fact_transaction.filter(
            F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == "")
        ).count()
        assert nulls == 0, f"{nulls} fact_transaction records with null transaction_id"

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_account_sk_orphans(self, fact_transaction):
        """Transactions with null account_sk = FK lookup failed (orphan records)."""
        orphans = fact_transaction.filter(F.col("account_sk").isNull()).count()
        total = fact_transaction.count()
        orphan_pct = (orphans / total * 100) if total > 0 else 0
        # Small % acceptable (from bad account_ids in source)
        print(f"  INFO: {orphans} transactions ({orphan_pct:.1f}%) with null account_sk (orphans)")
        assert orphan_pct < 10, (
            f"{orphan_pct:.1f}% orphan transactions — too many FK lookup failures"
        )

    @pytest.mark.silver
    @pytest.mark.dim_account
    def test_account_id_never_null(self, dim_account):
        """account_id must never be null."""
        nulls = dim_account.filter(
            F.col("account_id").isNull() | (F.trim(F.col("account_id")) == "")
        ).count()
        assert nulls == 0, f"{nulls} dim_account records with null account_id"

    @pytest.mark.silver
    @pytest.mark.fact_loan
    def test_loan_customer_sk_populated(self, fact_loan):
        """Loans should have customer_sk populated (FK to dim_customer)."""
        orphans = fact_loan.filter(F.col("customer_sk").isNull()).count()
        total = fact_loan.count()
        orphan_pct = (orphans / total * 100) if total > 0 else 0
        assert orphan_pct < 5, (
            f"{orphan_pct:.1f}% loans without customer_sk — FK lookup failures"
        )


class TestInvalidDateFormats:
    """Bad data: Dates in wrong format (dd-mm-yyyy instead of yyyy-mm-dd)."""

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_null_dates_from_bad_format(self, fact_transaction):
        """Transactions with invalid date formats will have null transaction_date after cast."""
        null_dates = fact_transaction.filter(F.col("transaction_date").isNull()).count()
        total = fact_transaction.count()
        null_pct = (null_dates / total * 100) if total > 0 else 0
        # Report how many dates failed to parse
        if null_dates > 0:
            print(f"  INFO: {null_dates} transactions ({null_pct:.1f}%) have null dates (parse failure)")
        # Allow small % but not catastrophic failure
        assert null_pct < 5, (
            f"{null_pct:.1f}% of transactions have null dates — too many format failures"
        )

    @pytest.mark.silver
    @pytest.mark.dim_customer
    def test_customer_dob_reasonable(self, dim_customer):
        """date_of_birth should be between 1920 and 2010 (no absurd values)."""
        bad = (
            dim_customer
            .filter(F.col("date_of_birth").isNotNull())
            .filter(
                (F.col("date_of_birth") < F.lit("1920-01-01").cast("date")) |
                (F.col("date_of_birth") > F.lit("2010-12-31").cast("date"))
            )
            .count()
        )
        assert bad == 0, f"{bad} customers with unreasonable date_of_birth"


class TestOrphanRecords:
    """Bad data: Records referencing non-existent parent entities."""

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_transactions_reference_valid_accounts(self, fact_transaction, dim_account):
        """Transactions should reference accounts that exist in dim_account."""
        valid_accounts = dim_account.select("account_sk").distinct()
        txn_accounts = fact_transaction.filter(
            F.col("account_sk").isNotNull()
        ).select("account_sk").distinct()

        orphans = txn_accounts.subtract(valid_accounts).count()
        assert orphans == 0, (
            f"{orphans} unique account_sk values in transactions not found in dim_account"
        )

    @pytest.mark.silver
    @pytest.mark.fact_loan
    def test_loans_reference_valid_branches(self, fact_loan, dim_branch):
        """Loans should reference branches that exist in dim_branch."""
        valid_branches = dim_branch.select("branch_sk").distinct()
        loan_branches = fact_loan.filter(
            F.col("branch_sk").isNotNull()
        ).select("branch_sk").distinct()

        orphans = loan_branches.subtract(valid_branches).count()
        assert orphans == 0, (
            f"{orphans} unique branch_sk values in loans not found in dim_branch"
        )

    @pytest.mark.silver
    @pytest.mark.fact_loan
    def test_loans_reference_valid_customers(self, fact_loan, dim_customer):
        """Loans should reference customers that exist in dim_customer."""
        valid_customers = dim_customer.select("customer_sk").distinct()
        loan_customers = fact_loan.filter(
            F.col("customer_sk").isNotNull()
        ).select("customer_sk").distinct()

        orphans = loan_customers.subtract(valid_customers).count()
        assert orphans == 0, (
            f"{orphans} unique customer_sk values in loans not found in dim_customer"
        )


class TestZeroAmountTransactions:
    """Edge case: Zero-amount transactions."""

    @pytest.mark.silver
    @pytest.mark.fact_transaction
    def test_zero_amount_transactions_exist(self, fact_transaction):
        """Identify zero-amount transactions (valid but worth monitoring)."""
        zeros = fact_transaction.filter(F.col("amount") == 0).count()
        if zeros > 0:
            print(f"  INFO: {zeros} zero-amount transactions found (review needed)")
        # Zero is technically valid (e.g., fee waiver) — just flag it
        total = fact_transaction.count()
        zero_pct = (zeros / total * 100) if total > 0 else 0
        assert zero_pct < 5, (
            f"{zero_pct:.1f}% zero-amount transactions — suspiciously high"
        )
