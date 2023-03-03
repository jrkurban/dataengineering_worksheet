from utility.my_helpers_answer import MyHelpers
import findspark
findspark.init("/opt/manual/spark")
from pyspark.sql import SparkSession, functions as F

my_helper_obj = MyHelpers()

## get spark session
session_params_dict = {
    "master": "yarn",
    "appName": "Clean Transactions",
    "hive_support": True,
    "executor_memory": "1500M",
    "executor_cores": "1",
    "executor_instances": "2"
}
spark = my_helper_obj.get_spark_session(session_params_dict)



# read data
read_params_dict = {
    "format": "csv",
    "header": True,
    "sep": ",",
    "inferSchema": True,
    "path": "/user/train/datasets/all_anonymized_2015_11_2017_03.csv"
}

df = my_helper_obj.get_data(spark_session=spark, read_params=read_params_dict)
df.show(n=5, truncate=False)
df.printSchema()

## format dates
df1 = my_helper_obj.format_dates(date_cols=["date_created", "date_last_seen"], input_df=df)
print("format dates")
df1.show(n=5, truncate=False)
df1.printSchema()

## make_nulls_to_unknown
df2 = my_helper_obj.make_nulls_to_unknown(input_df=df1)
print("make_nulls_to_unknown")
df2.show(5)
df2.printSchema()

## trim_str_cols
df3 = my_helper_obj.trim_str_cols(input_df=df2)
print("trim_str_cols")
df3.printSchema()
df3.show(5)

## write_hive
print("write_hive")
my_helper_obj.write_to_hive(df3)

## Check hive
df4 = my_helper_obj.get_spark_session(session_params_dict).sql("select * from test1.cars_cleaned")
print("check_hive")
df4.show(5)
spark.stop()