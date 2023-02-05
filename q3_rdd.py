from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, date_format
import os
import sys
import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()
print("spark session created")
data_df =spark.read.parquet("lab/")
def conv_day(date) :
	if date.day < 16 :
     		return 0
	else :
		return 1
def conv_month(date):
	return date.month

start = time.time()
rdd1 = data_df.rdd
rdd1 = rdd1.filter(lambda x : x["tpep_pickup_datetime"].year== 2022)
rdd1 = rdd1.filter(lambda x : conv_month(x["tpep_pickup_datetime"])< 7)
rdd1 = rdd1.filter(lambda x : conv_month(x["tpep_pickup_datetime"])< 7)
rdd1 = rdd1.filter(lambda x : x["PULocationID"] != x["DOLocationID"])
rdd_amount = rdd1.map(lambda x : ( str(conv_day(x["tpep_pickup_datetime"]))+"/"+str(conv_month(x["tpep_pickup_datetime"])),  x["total_amount"]))
rdd_amount = rdd_amount.aggregateByKey((0,0), lambda a,b: (a[0] + b,    a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
rdd_amount = rdd_amount.mapValues(lambda x: ( x[0]/x[1],))
df_amount = rdd_amount.toDF()
rdd_dist = rdd1.map(lambda x : ( str(conv_day(x["tpep_pickup_datetime"]))+"/"+str(conv_month(x["tpep_pickup_datetime"])),  x["trip_distance"]))
rdd_dist = rdd_dist.aggregateByKey((0,0), lambda a,b: (a[0] + b,    a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
rdd_dist = rdd_dist.mapValues(lambda x: ( x[0]/x[1],))
df_dist = rdd_dist.toDF()
df_dist.collect() 
df_amount.collect()
end = time.time()
print("Q3 with RDD took {} seconds".format(end-start))
df_dist.show()
df_amount.show()
