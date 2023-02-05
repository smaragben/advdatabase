from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
print("spark session created")
data_df =spark.read.parquet("lab/")
schema ="LocationID INT, Borough STRING, Zone STRING, service_zone STRING"
location_conv_df = spark.read.option("header", "false").csv("files/", schema=schema)
start = time.time()
q1_df = data_df.join(location_conv_df, data_df.PULocationID == location_conv_df.LocationID , how="left")
q1_df = q1_df.filter(month("tpep_pickup_datetime") == 2)
q1_df = q1_df.filter(q1_df.Zone == "Battery Park")
q1_df = q1_df.join(q1_df.agg(max('tip_amount').alias('tip_amount')),on='tip_amount',how='right').select(data_df["*"])
q1_df.collect()
end = time.time()
print("Query 1 took {} sec".format(start-end))

q1_df.show()
q1_df.write.csv("output/q1.csv", mode="overwrite")

