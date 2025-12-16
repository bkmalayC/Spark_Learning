from Config.spark_utils import *

#!/usr/bin/env python3
# HelloSpark.py
# Read a CSV file with PySpark and display results on the screen.
# Usage: python HelloSpark.py [path/to/file.csv]

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else r".\Files\flattened_output.xlsx"
    #path = "d:/Malay/Study/1_Programming/Spark_Learning/Files/cust.xlsx"
    if not os.path.exists(path):
        print(f"File not found: {path}")
        print("Provide a excel path as an argument, e.g. python HelloSpark.py /path/to/file.xlsx")
        print("Exiting...")
        return
    
    spark = init_spark()
    
    try:
        df = spark.read.format("com.crealytics.spark.excel")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(path)
        #print("Schema:")
        #df.printSchema()
        #print("First 20 rows:")
        #print(df.show(20, truncate=False))
        #print(f"Total rows: {df.count()}")

        # create a temporary view and query it with Spark SQL
        view_name = "excel_view"
        df.createOrReplaceTempView(view_name)
        # read the data back using Spark SQL
        result = spark.sql(f"SELECT * FROM {view_name}")

        # show some rows and basic info
        result.show(20, truncate=False)
        print("Rows counted via SQL:", result.count())
        print(type(result))
        
    except Exception as e:
        print("Error excel:", e)
    finally:
        print("Stopping Spark session...")
        shut_spark(spark)

if __name__ == "__main__":
    main()