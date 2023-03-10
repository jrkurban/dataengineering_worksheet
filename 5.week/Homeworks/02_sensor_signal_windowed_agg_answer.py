import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("IOT").master("local[2]") \
    .config("spark.driver.memory","3g") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')
# 230,1594512482.573491,00:0f:00:70:91:0a,0.00284008860710157,75.19999694824219,False,0.005114383400977071,False,0.013274836704851536,19.700000762939453,2022-01-20 21:35:43.178241
iot_schema = "row_id int, event_ts double, device string, co float, humidity float, light boolean, " \
              "lpg float, motion boolean, smoke float, temp float, generate_ts timestamp"
# Before run
# rm -rf ~/data-generator/output/*
# rm -rf ~/iotCountCheckpoint/
# (venvspark) [train@localhost data-generator]$ python dataframe_to_log.py -i ~/datasets/iot_telemetry_data.csv.zip -idx True


lines = (spark
         .readStream.format("csv")
         .schema(iot_schema)
         .option("header", False)
         .option("maxFilesPerTrigger", 1)
         .load("file:///home/train/data-generator/output"))



lines2 = (lines.withColumn("ts_long", F.col("event_ts").cast("long")) \
                .withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("ts_long"))))
                .drop("ts_long"))

ten_min_avg = lines2.groupBy(
        F.window(F.col("event_ts"), "10 minutes"), F.col("device")
        ).agg( F.count("*"), F.mean("co"), F.mean("humidity"))

checkpointDir = "file:///home/train/iotCountCheckpoint"

streamingQuery = (ten_min_avg
                  .writeStream
                  .format("console")
                  .outputMode("complete")
                  .trigger(processingTime="1 second")
                  .option("numRows", 10)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .start())

#
streamingQuery.awaitTermination()