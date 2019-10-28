from pyspark.sql import SparkSession, DataFrameWriter

spark = SparkSession.builder.master("local[*]").appName("Hello_App").getOrCreate()
spark.conf.set("spark.sql.shuffle.partition", "5")
spark.conf.set("parquet.summary.metadata", "false")
landing_data = spark.read.csv("46421-0011_00_mod.csv", sep=";", inferSchema=True)
landing_data.sort("_c1")
landing_data.printSchema()
landing_data.show(n=2, truncate=False)
print('DataFrame has {} rows'.format( landing_data.count()) )
landing_data.write.parquet('landing.parquet', 
                                            compression='gzip', mode='overwrite')
spark.stop()