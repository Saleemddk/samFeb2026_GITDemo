-- ============================================================
-- DDL: Bronze Layer Tables (Raw ingestion — 1:1 from source)
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze_customers (
    customer_id       VARCHAR(20),
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    date_of_birth     VARCHAR(20),
    gender            VARCHAR(5),
    phone             VARCHAR(15),
    email             VARCHAR(255),
    pan_number        VARCHAR(20),
    aadhar_number     VARCHAR(20),
    address           VARCHAR(500),
    city              VARCHAR(100),
    state             VARCHAR(100),
    pin_code          VARCHAR(10),
    customer_since    VARCHAR(20),
    is_active         VARCHAR(5),
    _ingestion_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR(500),
    _batch_id         VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze_accounts (
    account_id        VARCHAR(20),
    customer_id       VARCHAR(20),
    account_number    VARCHAR(20),
    account_type      VARCHAR(30),
    product_id        VARCHAR(20),
    branch_id         VARCHAR(20),
    ifsc_code         VARCHAR(20),
    balance           VARCHAR(30),
    currency          VARCHAR(5),
    open_date         VARCHAR(20),
    close_date        VARCHAR(20),
    status            VARCHAR(20),
    nominee_name      VARCHAR(200),
    _ingestion_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR(500),
    _batch_id         VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze_transactions (
    transaction_id    VARCHAR(30),
    account_id        VARCHAR(20),
    transaction_date  VARCHAR(20),
    transaction_time  VARCHAR(20),
    transaction_type  VARCHAR(10),
    amount            VARCHAR(30),
    currency          VARCHAR(5),
    channel           VARCHAR(30),
    status            VARCHAR(20),
    description       VARCHAR(500),
    beneficiary_acc   VARCHAR(20),
    reference_number  VARCHAR(30),
    _ingestion_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR(500),
    _batch_id         VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze_loans (
    loan_id             VARCHAR(20),
    customer_id         VARCHAR(20),
    loan_type           VARCHAR(30),
    product_id          VARCHAR(20),
    principal_amount    VARCHAR(30),
    interest_rate       VARCHAR(10),
    tenure_months       VARCHAR(10),
    emi_amount          VARCHAR(30),
    disburse_date       VARCHAR(20),
    maturity_date       VARCHAR(20),
    outstanding_amount  VARCHAR(30),
    status              VARCHAR(20),
    branch_id           VARCHAR(20),
    collateral_type     VARCHAR(30),
    _ingestion_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file        VARCHAR(500),
    _batch_id           VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze_branches (
    branch_id         VARCHAR(20),
    branch_name       VARCHAR(200),
    branch_code       VARCHAR(20),
    ifsc_code         VARCHAR(20),
    address           VARCHAR(500),
    city              VARCHAR(100),
    state             VARCHAR(100),
    pin_code          VARCHAR(10),
    phone             VARCHAR(15),
    manager_name      VARCHAR(200),
    established_date  VARCHAR(20),
    is_active         VARCHAR(5),
    _ingestion_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file      VARCHAR(500),
    _batch_id         VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS bronze_product_types (
    product_id         VARCHAR(20),
    product_name       VARCHAR(100),
    product_category   VARCHAR(30),
    interest_rate      VARCHAR(10),
    _ingestion_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file       VARCHAR(500),
    _batch_id          VARCHAR(50)
);
