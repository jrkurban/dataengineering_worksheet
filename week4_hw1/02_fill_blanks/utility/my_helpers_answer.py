from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame


class MyHelpers:

    def get_spark_session(self, session_params: dict) -> SparkSession:
        ## Put your code here.
        if session_params['hive_support']:
            spark = (SparkSession.builder
                     .master(session_params["master"])
                     .appName(session_params["appName"])
                     .enableHiveSupport()
                     .config("spark.executor.memory", session_params["executor_memory"])
                     .config("spark.executor.cores", session_params["executor_cores"])
                     .config("spark.executor.instances", session_params["executor_instances"])
                     .getOrCreate())
            return spark
        else:
            spark = (SparkSession.builder
                     .master(session_params["master"])
                     .appName(session_params["appName"])
                     .config("spark.executor.memory", session_params["executor_memory"])
                     .config("spark.executor.cores", session_params["executor_cores"])
                     .config("spark.executor.instances", session_params["executor_instances"])
                     .getOrCreate())
            return spark

    def get_data(self, spark_session: SparkSession, read_params: dict) -> DataFrame:
        ## Put your code here
        df = spark_session.read \
            .format(read_params["format"]) \
            .option("header", read_params["header"]) \
            .option("sep", read_params["sep"]) \
            .option("inferSchema", read_params["inferSchema"]) \
            .load(read_params["path"])

        return df

    def format_dates(self, date_cols: list, input_df: DataFrame) -> DataFrame:
        ## Make timestamp of string dates/ts
        output_df = input_df
        for date_col in date_cols:
            """output_df = output_df.withColumn(date_col, F.col(date_col).cast("timestamp"))"""
            output_df = output_df.withColumn(date_col, F.to_timestamp(date_col, "yyyy-MM-dd HH:mm:ss.SSSSSSx")) \
                .withColumn(date_col, F.date_format(date_col, "yyyy-MM-dd HH:mm:ss")) \
                .withColumn(date_col, F.to_timestamp(date_col, "yyyy-MM-dd HH:mm:ss"))

        return output_df

    def make_nulls_to_unknown(self, input_df: DataFrame) -> DataFrame:
        ## Replace Null/None/Empty values with Unknown in string columns
        str_cols = [col[0] for col in input_df.dtypes if col[1] == "string"]
        output_df = input_df
        for my_col in str_cols:
            output_df = output_df.withColumn(my_col, F.when(
                (F.col(my_col).isNull() | (F.col(my_col) == "None")),
                "Unknown").otherwise(F.col(my_col)))

        return output_df

    def trim_str_cols(self, input_df) -> DataFrame:
        ## trim all string cols
        str_cols = [col[0] for col in input_df.dtypes if col[1] == "string"]
        output_df = input_df
        for my_col in str_cols:
            output_df = output_df.withColumn(my_col, F.trim(my_col))
        return output_df

    def write_to_hive(self, input_df: DataFrame):
        # write to hive test1 database cars_cleaned table in orc format
        input_df.write.format("orc") \
            .mode("overwrite") \
            .saveAsTable("test1.cars_cleaned")
