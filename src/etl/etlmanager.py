from pyspark.sql import SparkSession

import os
# import pkgutil
# import sys
# import datetime
# import time
import boto3
import json


def get_aws_account_id():
    """
    returns AWS account ID
    :return: long
    """
    return boto3.client('sts').get_caller_identity().get('Account')


def get_aws_region():
    """
    returns AWS region
    :return: string
    """
    return boto3.client('s3').meta.region_name


def get_config():
    """

    :return: json in dict format
    """
    with open(os.path.abspath(os.path.join(os.path.dirname(__file__), 'config.json'))) as config_file:
        return json.load(config_file)


class EtlManager(object):
    """
    Class to set spark commonly used method
    """

    def get_spark_session(self, app_name):
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .getOrCreate()
        return spark

    def extract_csv_data(self, spark, src_path, sp_options):
        """Load data from csv file format.
        :param spark: Spark, sp_options and src_path
        :return: Spark DataFrame.
        """
        df_csv = spark.read.options(**sp_options).csv(src_path)

        return df_csv

    def extract_parquet_data(self, spark, src_path, sp_options):
        """Load data from Parquet file format.
        :param spark: Spark, csp_options and src_path
        :return: Spark DataFrame.
        """
        df_pqt = spark.read.options(**sp_options).parquet(src_path)

        return df_pqt

    # def transform_data(df, stf):
    #     """Transform original dataset.
    #     :param df: Input DataFrame.
    #     :param stf: The number of steps per-floor at 43 Tanner
    #         Street.
    #     :return: Transformed DataFrame.
    #     """
    #     df_transformed = (
    #         df
    #             .select(
    #             col('id'),
    #             concat_ws(
    #                 ' ',
    #                 col('first_name'),
    #                 col('second_name')).alias('name'),
    #             (col('floor') * lit(stf)).alias('steps_to_desk')))
    #
    #     return df_transformed
    #
    # def load_parquet_data(df):
    #     """Collect data locally and write to parquet.
    #     :param df: DataFrame to print.
    #     :return: None
    #     """
    #     (df
    #      .write
    #      .csv('loaded_data', mode='overwrite', header=True))
    #     return None
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
