from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
"""
# Dataset Info:
# https://www.kaggle.com/atulanandjha/temperature-readings-iot-devices?select=IOT-temp.csv

Temperature readings of an entreprise building room ( admin), both iniside and outside. This was recorded at random intervals. The recording speed was per second.

id
unique IDs for each reading

room_id/id
the room id in which device was installed (inside and/or outside).

noted_date
date and time of reading

temp
temperature readings

out/in
whether reading was taken from device installed inside or outside of room?


############   get data  ###########################
import requests, zipfile
from io import BytesIO
import os
zip_file_url = 'https://github.com/erkansirin78/datasets/raw/master/IOT-temp.csv.zip'

r = requests.get(zip_file_url, stream=True)
z = zipfile.ZipFile(BytesIO(r.content))
z.extractall("/home/train/datasets")

print(os.listdir("/home/train/datasets"))
mkdir /tmp/iot-temp-input

############   data generator  ###########################
rm -rf /tmp/iot-temp-input/*

python dataframe_to_log.py \
-i ~/datasets/IOT-temp.csv \
-o /tmp/iot-temp-input
"""
spark = SparkSession.builder \
    .appName("Simple Streaming IOT-temp") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('ERROR')
iot_temp_schema = "id string, room_id_id string, noted_date string, temp int, out_in string, gentime_time timestamp"
# File Source
dStream = spark.readStream.format("csv") \
    .schema(iot_temp_schema) \
    .load("file:///tmp/iot-temp-input")


# Operation

dStream2 = dStream \
    .withColumn("noted_date_ts", F.to_timestamp(F.col("noted_date"), "dd-MM-yyyy hh:mm")) \
    .withColumn("event_year", F.year("noted_date_ts")) \
    .withColumn("event_month", F.month("noted_date_ts")) \
    .withColumn("event_day", F.dayofweek("noted_date_ts")).drop("value")

# Console Sink
checkpointLocation = "file:///tmp/streaming/iot-temp"

# dStream2.writeStream.format("console") \
#     .outputMode("append") \
#     .trigger(processingTime='4 seconds') \
#     .option("checkpointLocation", checkpointLocation) \
#     .option("sep", "|") \
#     .option("header", False) \
#     .start("file:///tmp/iot-temp-output") \
#     .awaitTermination()

dStream2.writeStream.format("parquet") \
    .outputMode("append") \
    .trigger(processingTime='4 seconds') \
    .option("checkpointLocation", checkpointLocation) \
    .option("sep", "|") \
    .option("header", False) \
    .start("file:///tmp/iot-temp-output") \
    .awaitTermination()