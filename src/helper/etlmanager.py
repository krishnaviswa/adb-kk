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

    # def extract_csv_data(self, src_path, _schema, **sp_options):
    #     """Load data from csv file format.
    #     :param spark: Spark, sp_options and src_path
    #     :return: Spark DataFrame.
    #     """
    #     print(type(spark))
    #     df_csv = spark.read.options(**sp_options).schema(_schema).csv(str(src_path))
    #     return df_csv

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
    #
    # def create_test_data(spark, config):
    #     """Create test data.
    #     This function creates both both pre- and post- transformation data
    #     saved as Parquet files in tests/test_data. This will be used for
    #     unit tests as well as to load as part of the example ETL job.
    #     :return: None
    #     """
    #     # create example data from scratch
    #     local_records = [
    #         Row(id=1, first_name='Dan', second_name='Germain', floor=1),
    #         Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
    #         Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
    #         Row(id=4, first_name='Ken', second_name='Lai', floor=2),
    #         Row(id=5, first_name='Stu', second_name='White', floor=3),
    #         Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
    #         Row(id=7, first_name='Phil', second_name='Bird', floor=4),
    #         Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    #     ]
    #
    #     df = spark.createDataFrame(local_records)
    #
    #     # write to Parquet file format
    #     (df
    #      .coalesce(1)
    #      .write
    #      .parquet('tests/test_data/employees', mode='overwrite'))
    #
    #     # create transformed version of data
    #     df_tf = transform_data(df, config['steps_per_floor'])
    #
    #     # write transformed version of data to Parquet
    #     (df_tf
    #      .coalesce(1)
    #      .write
    #      .parquet('tests/test_data/employees_report', mode='overwrite'))
    #
    #     return None
