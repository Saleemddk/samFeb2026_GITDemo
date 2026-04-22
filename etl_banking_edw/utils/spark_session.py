"""
Spark session factory — works locally and on Databricks.
"""

from pyspark.sql import SparkSession
from config.database_config import WAREHOUSE_DIR


def get_spark_session(app_name: str = "BankingEDW") -> SparkSession:
    """
    Return a SparkSession configured for Delta Lake.
    On Databricks this simply returns the existing session.
    Locally it creates one with Delta extensions.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.driver.memory", "2g")
    )
    return builder.getOrCreate()
