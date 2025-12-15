

from pyspark.sql import SparkSession
import pandas as pd

# Create Spark session
spark = SparkSession.builder \
    .appName("JSON to Parquet") \
    .getOrCreate()

# Read JSON file
df = pd.read_json(r"D:\Malay\Study\1_Programming\Spark_Learning\Files\nested_trades.json")
print("DataFrame created successfully.")  
# Write to Parquet
#df.write.mode("overwrite").parquet(r"D:\Malay\Study\1_Programming\Spark_Learning\Parquet\nested_trades")
df.to_parquet(r"D:\Malay\Study\1_Programming\Spark_Learning\Parquet\nested_trades.parquet", engine="pyarrow", index=False)
df = spark.read.parquet(r"D:\Malay\Study\1_Programming\Spark_Learning\Parquet\nested_trades.parquet")

# Stop Spark session

spark.stop()        
