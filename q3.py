
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

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
data_df = data_df.filter(data_df.PULocationID != data_df.DOLocationID)
data_df = data_df.filter(date_format(data_df.tpep_pickup_datetime, 'yyyy').cast("int") == 2022)
data_df = data_df.filter(date_format(data_df.tpep_pickup_datetime, 'MM').cast("int") < 7)
data_df = data_df.select(date_format(data_df.tpep_pickup_datetime, 'MM').cast("int").alias("month"), date_format(data_df.tpep_pickup_datetime, 'dd').cast("int").alias("day"), data_df.trip_distance, data_df.total_amount)

@udf(returnType=IntegerType())
def conv(num) :
	if num is not None:
        	if num > 15 : 
                	return 1
        	else : 
                	return 0
data_df = data_df.select( data_df.month, data_df.trip_distance, data_df.total_amount,conv(data_df.day).alias("period"))
q3_df = data_df.groupBy(data_df.month, data_df.period).agg(avg(data_df.trip_distance).alias("avg_dist"), avg(data_df.total_amount).alias("avg_amount")) 
q3 = q3_df.collect()
end = time.time()
print("Q3 took {}".format(end-start))
q3 = q3_df.sort(q3_df.month, q3_df.period)
q3.show()
q3.write.csv("output/q3.csv", mode="overwrite")





