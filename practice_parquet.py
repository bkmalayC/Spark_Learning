from Config.spark_utils import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col


def main(): 
    spark = init_spark()
    df = spark.read.parquet(r"D:\Malay\Study\1_Programming\Spark_Learning\Parquet\nested_trades.parquet")
    # spark queries practice
    #df.select("trade_id", col("product.product_details").getItem(1)).show()
    # SQL expression string — note the quotes around 'T003'
    #df.filter("trade_id = 'T003'").select("product.product_details").show(truncate=False)

    # or using Column API
    df.filter(col("trade_id") == "T003").select(col("trade_id").alias("Trade") , size("parties").alias("Party size") , col("product.product_type").alias("Product Type)")) \
    .show(truncate=False)
    df.filter(col("trade_id") == "T003").select("trade_id" , typeof("parties") , "product.product_type") \
    .show(truncate=False)
    

    #df.filter(expr("exists(parties, xx -> xx.name = 'Diana')")) \
    #.select("trade_id", "parties.name").show(truncate=False)
              

    shut_spark(spark)
    print("Spark job completed successfully!")
   
if __name__ == "__main__":
    main()