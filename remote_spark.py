from pyspark.sql import SparkSession

import sys
import os
from random import random
from operator import add

os.environ["PYSPARK_PYTHON"]="python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="python3"

spark = SparkSession.builder.master(
    'local[*]').appName('Hello Spark').getOrCreate()

partitions = 2
n = 100000 * partitions


def f(_):
    y = random() * 2 - 1
    x = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print("Pi is roughly %f" % (4.0 * count / n))


spark.stop()
