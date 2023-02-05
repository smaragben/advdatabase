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
@udf(returnType=StringType())
def to_weekday(date) :
	return date.strftime('%A')
 

data_df =spark.read.parquet("lab/")
start = time.time()
data_df = data_df.filter(date_format(data_df.tpep_pickup_datetime, 'yyyy').cast("int") == 2022)
data_df = data_df.filter(date_format(data_df.tpep_pickup_datetime, 'MM').cast("int") < 8)
data_df = data_df.select(to_weekday(data_df.tpep_pickup_datetime).alias("weekday"), data_df.passenger_count, date_format(data_df.tpep_pickup_datetime, 'HH').alias("hour"))
q4_df = data_df.groupBy(data_df.weekday, data_df.hour).agg(avg(data_df.passenger_count).alias("avg_passengers"))
window = Window.partitionBy(q4_df.weekday).orderBy(q4_df.avg_passengers.desc())
q4 = q4_df.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3)
q4.collect()
end = time.time()
print("Q4 finished in {} seconds".format(end-start))
q4.show(21)
q4.write.csv("output/q4.csv", mode="overwrite")
