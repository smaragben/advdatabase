
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

from pyspark.sql.functions import *
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder.getOrCreate()
print("spark session created")
data_df =spark.read.parquet("lab/")
start = time.time()
data_df = data_df.filter(date_format(data_df.tpep_pickup_datetime, 'yyyy').cast("int") == 2022)
data_df = data_df.filter(date_format(data_df.tpep_pickup_datetime, 'MM').cast("int") < 8)
data_df = data_df.select(date_format(data_df.tpep_pickup_datetime,'MM').alias("month"), date_format(data_df.tpep_pickup_datetime, 'dd').alias("day"), (data_df.tip_amount/data_df.fare_amount).alias("tip_percentage"))
q5_df = data_df.groupBy(data_df.month, data_df.day).agg(avg(data_df.tip_percentage).alias("mean_tip_percentage"))
window = Window.partitionBy(q5_df.month).orderBy(q5_df.mean_tip_percentage.desc())
q5 = q5_df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 5)
q5.collect()
end = time.time()
print("Q5 finished in {} seconds".format(end-start))
q5.sort(q5.month).show()

q5.write.csv("output/q5.csv", mode="overwrite")




