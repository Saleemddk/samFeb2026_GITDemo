"""
run_pipeline.py — Master orchestrator
Generates source data, runs all ETL pipelines (Source → Bronze → Silver → Gold),
and optionally runs the test suite.

Usage:
    python run_pipeline.py                          # Run full pipeline (all layers)
    python run_pipeline.py --layer source           # Generate source data only
    python run_pipeline.py --layer bronze           # Run Source → Bronze only
    python run_pipeline.py --layer silver           # Run Bronze → Silver only
    python run_pipeline.py --layer gold             # Run Silver → Gold only
    python run_pipeline.py --layer bronze --test    # Run bronze + test bronze
    python run_pipeline.py --layer silver --test    # Run silver + test silver
    python run_pipeline.py --test                   # Run full pipeline + all tests
    python run_pipeline.py --incremental            # Run incremental load (Day 2+)
    python run_pipeline.py --layer bronze --incremental  # Incremental bronze only
"""

import sys
import os
import argparse

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

LAYERS = ["source", "bronze", "silver", "gold"]


def run_tests_for_layer(layer):
    """Run pytest filtered by layer marker."""
    import pytest as pt
    marker = f"-m {layer}"
    print(f"\n  Running tests for layer: {layer}")
    print("-" * 50)
    exit_code = pt.main([
        "tests/",
        "-v",
        "--tb=short",
        marker,
    ])
    return exit_code


def main():
    parser = argparse.ArgumentParser(description="Banking EDW Pipeline Orchestrator")
    parser.add_argument("--layer", choices=LAYERS, help="Run a specific layer only")
    parser.add_argument("--test", action="store_true", help="Run pytest after pipeline")
    parser.add_argument("--incremental", action="store_true", help="Run incremental load (append mode)")
    parser.add_argument("--generate-only", action="store_true", help="Only generate source data (deprecated, use --layer source)")
    args = parser.parse_args()

    # Handle deprecated flag
    if args.generate_only:
        args.layer = "source"

    print("=" * 70)
    print("  Banking EDW — Full Pipeline Orchestrator")
    mode_str = f"Layer: {args.layer or 'ALL'} | Mode: {'INCREMENTAL' if args.incremental else 'FULL'}"
    print(f"  {mode_str}")
    print("=" * 70)

    # Determine which layers to run
    if args.layer:
        layers_to_run = [args.layer]
    else:
        layers_to_run = LAYERS

    spark = None

    # ------- SOURCE -------
    if "source" in layers_to_run:
        print("\n" + "=" * 70)
        print("  STEP 1: Generate Source Data")
        print("=" * 70)
        if args.incremental:
            from utils.incremental_generator import generate_incremental
            generate_incremental()
        else:
            from utils.data_generator import generate_all
            generate_all()

        if args.test and args.layer == "source":
            run_tests_for_layer("source")

        if args.layer == "source":
            print("\n  Source data generated. Done.")
            return

    # ------- BRONZE -------
    if "bronze" in layers_to_run:
        print("\n" + "=" * 70)
        print("  STEP 2: Source → Bronze Ingestion")
        print("=" * 70)
        from utils.spark_session import get_spark_session
        if not spark:
            spark = get_spark_session("BankingEDW_Pipeline")

        from pipelines.source_to_bronze import run_source_to_bronze
        write_mode = "append" if args.incremental else "overwrite"
        run_source_to_bronze(spark, mode=write_mode)

        if args.test and args.layer == "bronze":
            spark.stop()
            spark = None
            run_tests_for_layer("bronze")
            return

    # ------- SILVER -------
    if "silver" in layers_to_run:
        print("\n" + "=" * 70)
        print("  STEP 3: Bronze → Silver Transformation")
        print("=" * 70)
        from utils.spark_session import get_spark_session
        if not spark:
            spark = get_spark_session("BankingEDW_Pipeline")

        from pipelines.bronze_to_silver import run_bronze_to_silver
        run_bronze_to_silver(spark)

        if args.test and args.layer == "silver":
            spark.stop()
            spark = None
            run_tests_for_layer("silver")
            return

    # ------- GOLD -------
    if "gold" in layers_to_run:
        print("\n" + "=" * 70)
        print("  STEP 4: Silver → Gold Aggregation")
        print("=" * 70)
        from utils.spark_session import get_spark_session
        if not spark:
            spark = get_spark_session("BankingEDW_Pipeline")

        from pipelines.silver_to_gold import run_silver_to_gold
        run_silver_to_gold(spark)

        if args.test and args.layer == "gold":
            spark.stop()
            spark = None
            run_tests_for_layer("gold")
            return

    if spark:
        spark.stop()

    print("\n" + "=" * 70)
    print("  Pipeline Complete!")
    print("=" * 70)

    # Run all tests if --test without specific layer
    if args.test and not args.layer:
        print("\n" + "=" * 70)
        print("  STEP 5: Running ALL tests")
        print("=" * 70)
        import pytest as pt
        exit_code = pt.main([
            "tests/",
            "-v",
            "--tb=short",
        ])
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
