"""
Pipeline configuration — table names, file paths, and processing settings.
"""

# ---------------------------------------------------------------------------
# Source file definitions (simulating S3 landing zone)
# ---------------------------------------------------------------------------
SOURCE_FILES = {
    "customers":     {"file": "customers.csv",      "format": "csv",  "header": True, "delimiter": ","},
    "accounts":      {"file": "accounts.csv",       "format": "csv",  "header": True, "delimiter": ","},
    "transactions":  {"file": "transactions.csv",   "format": "csv",  "header": True, "delimiter": ","},
    "loans":         {"file": "loans.csv",          "format": "csv",  "header": True, "delimiter": ","},
    "branches":      {"file": "branches.json",      "format": "json", "multiLine": True},
    "product_types": {"file": "product_types.csv",  "format": "csv",  "header": True, "delimiter": ","},
}

# ---------------------------------------------------------------------------
# Bronze layer table names
# ---------------------------------------------------------------------------
BRONZE_TABLES = {
    "customers":     "bronze_customers",
    "accounts":      "bronze_accounts",
    "transactions":  "bronze_transactions",
    "loans":         "bronze_loans",
    "branches":      "bronze_branches",
    "product_types": "bronze_product_types",
}

# ---------------------------------------------------------------------------
# Silver layer table names
# ---------------------------------------------------------------------------
SILVER_TABLES = {
    "dim_customer":    "dim_customer",      # SCD Type 2
    "dim_account":     "dim_account",
    "dim_branch":      "dim_branch",
    "dim_date":        "dim_date",
    "dim_product":     "dim_product",
    "fact_transaction": "fact_transaction",
    "fact_loan":       "fact_loan",
}

# ---------------------------------------------------------------------------
# Gold layer table names
# ---------------------------------------------------------------------------
GOLD_TABLES = {
    "customer_360":         "gold_customer_360",
    "branch_performance":   "gold_branch_performance",
    "daily_txn_summary":    "gold_daily_txn_summary",
    "loan_portfolio":       "gold_loan_portfolio",
}

# ---------------------------------------------------------------------------
# Processing settings
# ---------------------------------------------------------------------------
BATCH_SIZE        = 10000
PARTITION_COLUMNS = {
    "bronze_transactions": "transaction_date",
    "fact_transaction":    "transaction_date",
}
