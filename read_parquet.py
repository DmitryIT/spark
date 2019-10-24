from pyspark.sql import SparkSession

parquet_files = 'landings.parquet/*/*.parquet'

spark = SparkSession.builder.master('local[*]').getOrCreate()

file = spark.read.parquet(parquet_files)
num_rows = file.count()
file.describe().show()
msg = 'file {} has {} rows'
print(msg.format(parquet_files, num_rows))

spark.stop()