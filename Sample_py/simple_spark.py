#!/usr/bin/env python3
"""
Simple PySpark example: word count using the DataFrame API.
Reads 'sample_data.txt' from the same directory and prints top word counts.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col


def main():
    print("Starting Spark word count example...")
    # Create a local Spark session
    #spark = SparkSession.builder.appName("SimpleSparkExample").master("local[*]").config("spark.hadoop.security.authentication", "simple").config("spark.ui.showConsoleProgress", "false").getOrCreate()
    
    spark = SparkSession.builder \
    .appName("SimpleSparkExample") \
    .master("local[*]") \
    .config("spark.hadoop.security.authentication", "simple") \
    .config("spark.hadoop.security.authorization", "false") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    #print("Spark started successfully:", spark.version) 

    """
    # Read the sample text file (one sentence per line)
    df = spark.read.text("sample_data.txt").withColumnRenamed("value", "line")

    # Split lines into words, normalize to lower-case, and explode into rows
    words = df.select(explode(split(lower(col("line")), r"\\s+")).alias("word"))

    # Filter out empty tokens
    words = words.filter(col("word") != "")

    # Count occurrences of each word and show the top results
    counts = words.groupBy("word").count().orderBy(col("count").desc())
    print("✅ Word counts computed successfully! Here are the results:")
    counts.show(truncate=False)
    """
    #spark.stop()


if __name__ == "__main__":
    main()
