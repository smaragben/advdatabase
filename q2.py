from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, date_format
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

stocks = "hdfs://namenode:9000/stocks.txt"

spark = SparkSession.builder.getOrCreate()
print("spark session created")
#val sparkConf = new SparkConf().setAppName("Gathering Data")            
#val sc = new SparkContext(sparkConf)

data_df =spark.read.parquet("lab/")

start = time.time()
data_df = data_df.select("*", date_format(data_df.tpep_pickup_datetime, 'MM').cast("int").alias("month"))
data_df = data_df.filter(data_df.month >0)
data_df = data_df.filter(data_df.month < 7)
data_df = data_df.filter(data_df.tolls_amount > 0)
q2 = data_df.join(data_df.groupBy(data_df.month).agg(max("tolls_amount").alias("tolls_amount")) , on = ['month', 'tolls_amount'] , how="right")
q2.collect()
end = time.time()
print("Query 2 took {} sec".format(end-start))
q2.show()
q2.write.csv("output/q2.csv", mode="overwrite")



