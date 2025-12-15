import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

#!/usr/bin/env python3
"""
HelloPyspark.py - read a parquet file with PySpark

Usage:
    python HelloPyspark.py --path /path/to/file.parquet
"""


def main():
        parser = argparse.ArgumentParser(description="Read a parquet file with PySpark")
        parser.add_argument("--path", "-p", required=True, help="Parquet file or directory path")
        parser.add_argument("--show", "-s", type=int, default=20, help="Number of rows to show")
        args = parser.parse_args()

        spark = SparkSession.builder.appName("HelloPySpark").master("local[*]").getOrCreate()

        try:
                df = spark.read.parquet("\parquet\nested_transactions.parquet")
        except AnalysisException as e:
                print(f"Failed to read parquet: {e}", file=sys.stderr)
                spark.stop()
                sys.exit(2)
        except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                spark.stop()
                sys.exit(1)

        print("Schema:")
        df.printSchema()
        print(f"\nShowing up to {args.show} rows:")
        df.show(args.show, truncate=False)

        try:
                print(f"\nTotal rows: {df.count()}")
        except Exception:
                pass

        spark.stop()

if __name__ == "__main__":
        main()