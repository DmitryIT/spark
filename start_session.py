from pyspark.sql import SparkSession
from time import sleep
import os

os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"

spark = SparkSession.builder.master('local[*]').appName('my session').getOrCreate()
print(spark.version)

spark.stop()