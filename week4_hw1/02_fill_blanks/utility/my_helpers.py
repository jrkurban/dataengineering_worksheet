from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame


class MyHelpers:

    def get_spark_session(self, session_params: dict) -> SparkSession:
        ## Create spark session and return it.

        return

    def get_data(self, spark_session: SparkSession, read_params: dict) -> DataFrame:
        ## Put your code here

        return

    def format_dates(self, date_cols: list, input_df: DataFrame) -> DataFrame:
        ## Make timestamp of string dates/ts

        return

    def make_nulls_to_unknown(self, input_df: DataFrame) -> DataFrame:
        ## Replace Null/None/Empty values with Unknown in string columns

        return

    def trim_str_cols(self, input_df) -> DataFrame:
        ## trim all string cols

        return

    def write_to_hive(self, input_df: DataFrame):
        # write to hive test1 database cars_cleaned table in avro format
        pass
