
from pyspark.sql import SparkSession
import findspark, os , shutil  
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType
import findspark
import pandas as pd
import pyarrow as pa    
import pyarrow.parquet as pq
import sys


app_name="PySparkApp"
master="local[2]"
java_home= r"C:\PROGRA~1\EclipseAdoptium\jdk-11.0.29.7-hotspot"         
spark_home=r"D:\Malay\Study\99_Software_Installation\Spark\spark-3.5.7-bin-hadoop3"
hadoop_home=r"D:\Malay\Study\99_Software_Installation\Hadoop"   