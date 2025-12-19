from pyspark.sql import SparkSession
from Config.spark_utils import *
from Config.spark_utils import flatten_df

# Initialize Spark Session
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

# Read JSON file
df = spark.read.option("multiline", True).json("Files/nested_trades.json")
df = flatten_df(df,  True)
# Display in tabular format
print(type(df))
df.show()

# Stop Spark Session
spark.stop()