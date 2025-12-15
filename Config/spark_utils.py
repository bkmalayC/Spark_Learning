
from Config.Spark_config import *

# create a spark object

def init_spark() :
    
 # Create SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

    return spark

def shut_spark(spark) :
    # Stop SparkSession and SparkContext
    spark.stop()
    if spark.sparkContext._jsc:
        spark.sparkContext.stop()

    

def _flatten_once(df: DataFrame, explode_arrays: bool):
    """
    Perform a single pass flatten:
    - expand top-level StructType columns into separate columns with underscore names
    - either explode the first-level ArrayType columns (if explode_arrays=True)
      or convert arrays/maps to JSON strings
    Returns (new_df, saw_struct, saw_array)
    """
    cols = []
    saw_struct = False
    saw_array = False

    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        if isinstance(dtype, StructType):
            saw_struct = True
            # expand struct fields to top-level with underscore separator
            for sub in dtype.fields:
                cols.append(F.col(f"{name}.{sub.name}").alias(f"{name}_{sub.name}"))
        elif isinstance(dtype, ArrayType):
            saw_array = True
            if explode_arrays:
                # explode arrays to rows (preserve nulls/empties)
                cols.append(F.explode_outer(F.col(name)).alias(name))
            else:
                # preserve array as JSON string so single-row output remains
                cols.append(F.to_json(F.col(name)).alias(name))
        elif isinstance(dtype, MapType):
            # convert maps to JSON string
            cols.append(F.to_json(F.col(name)).alias(name))
        else:   
            cols.append(F.col(name))

    return df.select(*cols), saw_struct, saw_array


def flatten_df(df: DataFrame, explode_arrays: bool = False, max_iters: int = 20) -> DataFrame:
    """
    Recursively flatten a nested DataFrame schema:
    - StructType columns are expanded into separate top-level columns named parent_child
    - ArrayType columns are either exploded (if explode_arrays=True) or converted to JSON strings
    - MapType columns are converted to JSON strings
    The loop continues until there are no top-level StructType columns left, or until no more
    ArrayType columns (when not exploding) / until max_iters is reached to avoid infinite loops.

    Returns a new flattened DataFrame.
    """
    cur = df
    for i in range(max_iters):
        cur, saw_struct, saw_array = _flatten_once(cur, explode_arrays=explode_arrays)
        # continue while there are structs to expand
        if not saw_struct and (not saw_array or not explode_arrays):
            break
    return cur


def flatten_parquet(spark, input_path: str, output_path: str = None, explode_arrays: bool = False) -> DataFrame:
    """
    Read a Parquet file (or folder), flatten nested columns and optionally write result.

    Parameters:
    - spark: SparkSession
    - input_path: path to parquet file or directory
    - output_path: if provided, write flattened parquet to this path (overwrite)
    - explode_arrays: if True, arrays are exploded to multiple rows; if False, arrays are kept as JSON strings

    Returns:
    - flattened DataFrame
    """
    df = spark.read.parquet(input_path)
    flat = flatten_df(df, explode_arrays=explode_arrays)
    if output_path:
        flat.write.mode("overwrite").parquet(output_path)
    return flat




