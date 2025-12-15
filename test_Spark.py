from Config.spark_utils import *

def main(): 
    try :
        print("Starting Spark session...")
        spark = init_spark()
    except Exception as e:
        print("Error starting Spark session:", e)
        exit(1)

    print("Spark version:", spark.version)

    try :
        print("Creating DataFrame...")
        df = spark.read.parquet(r"D:\Malay\Study\1_Programming\Spark_Learning\Parquet\nested_trades.parquet")
        print("DataFrame created successfully.")
        
    except Exception as e:
        print("Error starting Spark session:", e)
        exit(1)

    df.printSchema()
    print("Flattening DataFrame...")
    flat_df = flatten_df(df, explode_arrays=True)

    pandas_df = flat_df.toPandas()
    output_path = r"D:\Malay\Study\1_Programming\Spark_Learning\Files\flattened_output.xlsx"
    print(f"Number of records in DataFrame: {flat_df.count()}")

    # Check if file exists and remove it before writing (pandas.to_excel will overwrite, but this makes intent explicit)
    if os.path.exists(output_path):
        print(f"Output file exists at {output_path}. Overwriting.")
        try:
            os.remove(output_path)
        except Exception as e:
            print("Warning: could not remove existing file:", e)
    else:
        print(f"Output file does not exist. Creating {output_path}.")

    pandas_df.to_excel(output_path, index=False)
    
       
    shut_spark(spark)
    print("Spark job completed successfully!")
   

if __name__ == "__main__":
    main()

