from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('my session').getOrCreate()
print(spark.version)
#spark.stop()