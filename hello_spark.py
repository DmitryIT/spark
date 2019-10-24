from pyspark.sql import SparkSession, DataFrameWriter

log_file = "46421-0011_00_mod.csv"
spark = SparkSession.builder.master("local[*]").appName("Hello_App").getOrCreate()

log_data = spark.read.csv(path=log_file, sep=";")
num_rows = log_data.count()
log_data.summary().show()
log_data.printSchema()

msg = "log file {} has {} rows"
print(msg.format(log_file, num_rows))

log_data.write.parquet(path="landings.parquet", mode="overwrite", compression="gzip")
spark.stop()