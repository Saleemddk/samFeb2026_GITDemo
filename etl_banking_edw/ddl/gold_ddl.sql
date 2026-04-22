-- ============================================================
-- DDL: Gold Layer Tables (Aggregated business views)
-- ============================================================

-- Customer 360 — single view of the customer
CREATE TABLE IF NOT EXISTS gold_customer_360 (
    customer_id         VARCHAR(20) PRIMARY KEY,
    full_name           VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(100),
    customer_since      DATE,
    total_accounts      INT,
    active_accounts     INT,
    total_balance       DECIMAL(18,2),
    avg_balance         DECIMAL(18,2),
    total_transactions  INT,
    total_credit_amount DECIMAL(18,2),
    total_debit_amount  DECIMAL(18,2),
    active_loans        INT,
    total_loan_outstanding DECIMAL(18,2),
    last_transaction_date  DATE,
    customer_segment    VARCHAR(30),        -- PLATINUM / GOLD / SILVER / BASIC
    risk_score          DECIMAL(5,2),
    _etl_loaded_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Branch Performance Summary
CREATE TABLE IF NOT EXISTS gold_branch_performance (
    branch_id           VARCHAR(20),
    branch_name         VARCHAR(200),
    city                VARCHAR(100),
    report_month        DATE,
    total_customers     INT,
    total_accounts      INT,
    total_deposits      DECIMAL(18,2),
    total_withdrawals   DECIMAL(18,2),
    net_flow            DECIMAL(18,2),
    total_loans_disbursed DECIMAL(18,2),
    active_loans        INT,
    npa_amount          DECIMAL(18,2),
    _etl_loaded_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (branch_id, report_month)
);

-- Daily Transaction Summary
CREATE TABLE IF NOT EXISTS gold_daily_txn_summary (
    transaction_date    DATE,
    channel             VARCHAR(30),
    transaction_type    VARCHAR(10),
    total_count         INT,
    success_count       INT,
    failed_count        INT,
    total_amount        DECIMAL(18,2),
    avg_amount          DECIMAL(18,2),
    max_amount          DECIMAL(18,2),
    min_amount          DECIMAL(18,2),
    _etl_loaded_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_date, channel, transaction_type)
);

-- Loan Portfolio Summary
CREATE TABLE IF NOT EXISTS gold_loan_portfolio (
    loan_type           VARCHAR(30),
    report_month        DATE,
    total_loans         INT,
    active_loans        INT,
    defaulted_loans     INT,
    total_principal     DECIMAL(18,2),
    total_outstanding   DECIMAL(18,2),
    avg_interest_rate   DECIMAL(5,2),
    avg_tenure_months   DECIMAL(5,1),
    npa_ratio           DECIMAL(5,2),
    _etl_loaded_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (loan_type, report_month)
);
