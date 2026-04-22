"""
Database configuration for all layers of the Banking EDW.
Reads from environment variables with sensible defaults for local dev.
"""

import os


# ---------------------------------------------------------------------------
# Source OLTP Database (MySQL)
# ---------------------------------------------------------------------------
SOURCE_DB = {
    "host":     os.getenv("SRC_DB_HOST",     "localhost"),
    "port":     int(os.getenv("SRC_DB_PORT", "3306")),
    "user":     os.getenv("SRC_DB_USER",     "root"),
    "password": os.getenv("SRC_DB_PASSWORD", "Admin123"),
    "database": os.getenv("SRC_DB_NAME",     "retail_banking"),
}

# ---------------------------------------------------------------------------
# Target / Warehouse Database (Oracle or Databricks catalog)
# ---------------------------------------------------------------------------
TARGET_DB = {
    "host":     os.getenv("TGT_DB_HOST",     "localhost"),
    "port":     os.getenv("TGT_DB_PORT",     "1521"),
    "user":     os.getenv("TGT_DB_USER",     "TEST_USER"),
    "password": os.getenv("TGT_DB_PASSWORD", "Admin123"),
    "service":  os.getenv("TGT_DB_SERVICE",  "XEPDB1"),
}

# ---------------------------------------------------------------------------
# Databricks / Spark Catalog Settings
# ---------------------------------------------------------------------------
CATALOG   = os.getenv("DATABRICKS_CATALOG",   "banking_edw")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

# ---------------------------------------------------------------------------
# Landing Zone Paths (simulate S3)
# ---------------------------------------------------------------------------
BASE_DATA_DIR   = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
LANDING_ZONE    = os.path.join(BASE_DATA_DIR, "landing_zone")
REFERENCE_ZONE  = os.path.join(BASE_DATA_DIR, "reference")
CHECKPOINT_DIR  = os.path.join(BASE_DATA_DIR, "checkpoints")

# ---------------------------------------------------------------------------
# Delta Lake / Warehouse Paths (local simulation)
# ---------------------------------------------------------------------------
WAREHOUSE_DIR   = os.path.join(BASE_DATA_DIR, "warehouse")
BRONZE_PATH     = os.path.join(WAREHOUSE_DIR, "bronze")
SILVER_PATH     = os.path.join(WAREHOUSE_DIR, "silver")
GOLD_PATH       = os.path.join(WAREHOUSE_DIR, "gold")
