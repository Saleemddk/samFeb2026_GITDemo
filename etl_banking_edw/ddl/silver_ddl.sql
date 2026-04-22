-- ============================================================
-- DDL: Silver Layer Tables (Cleansed — Dimensions & Facts)
-- ============================================================

-- SCD Type 2 Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk        BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id        VARCHAR(20) NOT NULL,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    full_name          VARCHAR(200),
    date_of_birth      DATE,
    age                INT,
    gender             VARCHAR(10),
    phone              VARCHAR(15),
    email              VARCHAR(255),
    pan_number         VARCHAR(20),
    aadhar_number      VARCHAR(20),
    city               VARCHAR(100),
    state              VARCHAR(100),
    pin_code           VARCHAR(10),
    customer_since     DATE,
    is_active          VARCHAR(5),
    -- SCD2 columns
    effective_date     DATE NOT NULL,
    expiry_date        DATE DEFAULT '9999-12-31',
    is_current         CHAR(1) DEFAULT 'Y',
    _etl_loaded_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Account Dimension
CREATE TABLE IF NOT EXISTS dim_account (
    account_sk         BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id         VARCHAR(20) NOT NULL,
    customer_id        VARCHAR(20),
    account_number     VARCHAR(20),
    account_type       VARCHAR(30),
    product_id         VARCHAR(20),
    branch_id          VARCHAR(20),
    ifsc_code          VARCHAR(20),
    currency           VARCHAR(5),
    open_date          DATE,
    close_date         DATE,
    status             VARCHAR(20),
    nominee_name       VARCHAR(200),
    _etl_loaded_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Branch Dimension
CREATE TABLE IF NOT EXISTS dim_branch (
    branch_sk          BIGINT AUTO_INCREMENT PRIMARY KEY,
    branch_id          VARCHAR(20) NOT NULL,
    branch_name        VARCHAR(200),
    branch_code        VARCHAR(20),
    ifsc_code          VARCHAR(20),
    city               VARCHAR(100),
    state              VARCHAR(100),
    pin_code           VARCHAR(10),
    phone              VARCHAR(15),
    manager_name       VARCHAR(200),
    established_date   DATE,
    is_active          VARCHAR(5),
    _etl_loaded_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date Dimension (auto-generated)
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk            INT PRIMARY KEY,
    full_date          DATE NOT NULL,
    year               INT,
    quarter            INT,
    month              INT,
    month_name         VARCHAR(20),
    week_of_year       INT,
    day_of_month       INT,
    day_of_week        INT,
    day_name           VARCHAR(20),
    is_weekend         CHAR(1),
    is_holiday         CHAR(1) DEFAULT 'N',
    fiscal_year        INT,
    fiscal_quarter     INT
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk         BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_id         VARCHAR(20) NOT NULL,
    product_name       VARCHAR(100),
    product_category   VARCHAR(30),
    interest_rate      DECIMAL(5,2),
    _etl_loaded_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transaction Fact
CREATE TABLE IF NOT EXISTS fact_transaction (
    transaction_sk     BIGINT AUTO_INCREMENT PRIMARY KEY,
    transaction_id     VARCHAR(30) NOT NULL,
    account_sk         BIGINT,
    customer_sk        BIGINT,
    branch_sk          BIGINT,
    date_sk            INT,
    transaction_date   DATE,
    transaction_time   TIME,
    transaction_type   VARCHAR(10),
    amount             DECIMAL(15,2),
    currency           VARCHAR(5),
    channel            VARCHAR(30),
    status             VARCHAR(20),
    description        VARCHAR(500),
    beneficiary_acc    VARCHAR(20),
    reference_number   VARCHAR(30),
    _etl_loaded_ts     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Loan Fact
CREATE TABLE IF NOT EXISTS fact_loan (
    loan_sk             BIGINT AUTO_INCREMENT PRIMARY KEY,
    loan_id             VARCHAR(20) NOT NULL,
    customer_sk         BIGINT,
    branch_sk           BIGINT,
    product_sk          BIGINT,
    loan_type           VARCHAR(30),
    principal_amount    DECIMAL(15,2),
    interest_rate       DECIMAL(5,2),
    tenure_months       INT,
    emi_amount          DECIMAL(15,2),
    disburse_date       DATE,
    maturity_date       DATE,
    outstanding_amount  DECIMAL(15,2),
    status              VARCHAR(20),
    collateral_type     VARCHAR(30),
    _etl_loaded_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
