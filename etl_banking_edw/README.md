# Banking EDW — ETL Testing Automation Repository

A production-grade **Indian Retail Banking** EDW simulation with full **Medallion Architecture** (Bronze → Silver → Gold), built on **Databricks / PySpark**, with comprehensive **pytest-based ETL testing**.

---

## Architecture

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  SOURCE (OLTP)   │───→│  BRONZE (Raw)    │───→│  SILVER (Clean)  │───→│  GOLD (Business) │
│                  │    │                  │    │                  │    │                  │
│ customers.csv    │    │ bronze_customers │    │ dim_customer     │    │ gold_customer_360│
│ accounts.csv     │    │ bronze_accounts  │    │ dim_account      │    │ gold_branch_perf │
│ transactions.csv │    │ bronze_txns      │    │ dim_branch       │    │ gold_daily_txn   │
│ loans.csv        │    │ bronze_loans     │    │ dim_product      │    │ gold_loan_portf  │
│ branches.json    │    │ bronze_branches  │    │ dim_date         │    │                  │
│ products.csv     │    │ bronze_products  │    │ fact_transaction │    │                  │
│                  │    │                  │    │ fact_loan        │    │                  │
└──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
    S3 / Files           1:1 Raw + Audit       Type-cast, SCD2, FK      Aggregated KPIs
```

## Project Structure

```
etl_banking_edw/
├── config/                         # Database & pipeline configuration
│   ├── database_config.py
│   └── pipeline_config.py
├── data/                           # Generated data files
│   ├── landing_zone/               # Source files (simulates S3)
│   ├── reference/                  # Reference/lookup data
│   └── warehouse/                  # Bronze/Silver/Gold Parquet files
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── ddl/                            # SQL DDL for all layers
│   ├── bronze_ddl.sql
│   ├── silver_ddl.sql
│   └── gold_ddl.sql
├── source_to_target/               # Source-to-Target mapping YAML
│   ├── stm_bronze.yaml
│   ├── stm_silver.yaml
│   └── stm_gold.yaml
├── pipelines/                      # PySpark ETL pipeline code
│   ├── source_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── notebooks/                      # Databricks-compatible notebooks
│   ├── 01_ingest_to_bronze.py
│   ├── 02_bronze_to_silver.py
│   └── 03_silver_to_gold.py
├── utils/                          # Shared utilities
│   ├── spark_session.py
│   ├── db_connector.py
│   └── data_generator.py
├── tests/                          # ★ Comprehensive pytest test suite
│   ├── traditional/                # Traditional ETL testing
│   │   ├── test_row_counts.py
│   │   ├── test_duplicates.py
│   │   ├── test_null_checks.py
│   │   ├── test_referential_integrity.py
│   │   ├── test_data_types.py
│   │   ├── test_source_target_reconciliation.py
│   │   ├── test_transformation_accuracy.py
│   │   ├── test_scd_validation.py
│   │   ├── test_completeness.py
│   │   └── test_incremental_load.py
│   ├── data_quality/               # Modern data quality testing
│   │   ├── test_schema_validation.py
│   │   ├── test_freshness.py
│   │   ├── test_volume_anomaly.py
│   │   ├── test_statistical_profiling.py
│   │   ├── test_cross_layer_consistency.py
│   │   └── test_business_rules.py
│   └── integration/                # End-to-end integration tests
│       ├── test_pipeline_e2e.py
│       └── test_data_lineage.py
├── conftest.py                     # Shared pytest fixtures
├── run_pipeline.py                 # Master orchestrator
├── pytest.ini                      # Pytest configuration
├── requirements.txt
└── README.md
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Source Data + Run Full Pipeline
```bash
cd etl_banking_edw
python run_pipeline.py
```

### 3. Run Tests
```bash
# All tests
pytest tests/ -v

# Traditional ETL tests only
pytest tests/traditional/ -v

# Modern data quality tests only
pytest tests/data_quality/ -v

# Integration / E2E tests
pytest tests/integration/ -v

# Run pipeline + tests together
python run_pipeline.py --test
```

### 4. Generate Source Data Only
```bash
python run_pipeline.py --generate-only
```

## Data Domain: Indian Retail Banking

| Entity | Volume | Description |
|--------|--------|-------------|
| Customers | 500 | Indian names, PAN, Aadhar, phone (+91), email |
| Accounts | ~900 | Savings, Current, Salary, NRI, FD, RD |
| Transactions | 5,000 | NEFT, RTGS, IMPS, UPI, ATM, Branch |
| Loans | 300 | Home, Personal, Car, Education, Gold, Business |
| Branches | 50 | Across 37 Indian cities |
| Products | 12 | Account & loan product catalog |

## Test Coverage (100+ test cases)

### Traditional ETL Testing
| Test Category | Count | What It Tests |
|---------------|-------|---------------|
| Row Counts | 16 | Source↔Bronze↔Silver↔Gold count matching |
| Duplicates | 14 | PK/UK uniqueness across all layers |
| Null Checks | 25+ | Critical column NOT NULL validation |
| Referential Integrity | 8 | FK relationships in fact tables |
| Data Types | 12 | Type casting correctness |
| Source-Target Reconciliation | 9 | Amount/count matching across layers |
| Transformation Accuracy | 16 | InitCap, UPPER, gender mapping, etc. |
| SCD Validation | 7 | SCD2 current flag, date ranges, gaps |
| Completeness | 9 | Entity coverage, date range, reference data |
| Incremental Load | 12 | Audit columns, batch consistency |

### Modern Data Quality
| Test Category | Count | What It Tests |
|---------------|-------|---------------|
| Schema Validation | 10 | Column presence, drift detection, types |
| Freshness | 6 | Data age, recency of transactions |
| Volume Anomaly | 10 | Expected ranges, ratios, empty partitions |
| Statistical Profiling | 10 | Distribution, outliers (IQR), skew |
| Cross-Layer Consistency | 9 | Data matching across Bronze/Silver/Gold |
| Business Rules | 20+ | Phone format, PAN, Aadhar, EMI < principal |

### Integration Tests
| Test Category | Count | What It Tests |
|---------------|-------|---------------|
| End-to-End Pipeline | 8 | Full flow validation, no orphans |
| Data Lineage | 7 | Record tracing across all layers |

## Key Transformations

| Layer | Transformation | Example |
|-------|---------------|---------|
| Bronze | Raw + audit columns | `_ingestion_ts`, `_source_file`, `_batch_id` |
| Silver | InitCap names | `aarav` → `Aarav` |
| Silver | Lowercase email | `AARAV@GMAIL.COM` → `aarav@gmail.com` |
| Silver | Gender mapping | `M` → `Male`, `F` → `Female` |
| Silver | SCD Type 2 | `effective_date`, `expiry_date`, `is_current` |
| Silver | FK resolution | `account_id` → `account_sk` (surrogate key) |
| Silver | Date dimension | `date_sk` = `yyyyMMdd` integer |
| Gold | Segmentation | Balance-based: PLATINUM/GOLD/SILVER/BASIC |
| Gold | NPA ratio | `defaulted_loans / total_loans * 100` |

## Databricks Deployment

The `notebooks/` folder contains Databricks-compatible Python notebooks:
1. `01_ingest_to_bronze.py` — Import to Databricks, attach to cluster, run
2. `02_bronze_to_silver.py` — Transforms raw to dimensional model
3. `03_silver_to_gold.py` — Builds business aggregation layer

Change file paths from `data/...` to `s3://your-bucket/...` or `dbfs:/...` for production.
