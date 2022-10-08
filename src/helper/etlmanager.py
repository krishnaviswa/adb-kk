from pyspark.sql import SparkSession

# import os
import pkgutil
# import sys
# import datetime
# import time
import json


def get_config():
    """

    :return: json in dict format
    """
    config_file = pkgutil.get_data(__package__, 'config.json')
    return json.loads(config_file.decode())


class EtlManager(object):
    """
    Class to set spark commonly used method
    """

    def set_spark_session(self, app_name):
        global spark
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()
        return spark

    def extract_parquet_data(self, src_path):
        """Load data from Parquet file format.
        :param  src_path
        :return: Spark DataFrame.
        """
        df_pqt = spark.read.parquet(src_path)
        return df_pqt

    def write_parquet_data(df_pqt, write_method, tgt_path, col1, col2, col3):
        """Collect data locally and write to parquet.
        :param  write_method,tgt_path,col1,col2,col3
        :return: None
        """
        df_pqt.write.mode(write_method).partitionBy(col1, col2, col3).parquet(tgt_path)
        return None
