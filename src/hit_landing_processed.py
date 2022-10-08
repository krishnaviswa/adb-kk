import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

from helper.etlmanager import EtlManager, get_config


def main(src_name, src_path, tgt_path):
    _schema = StructType(
        [StructField('hit_time_gmt', IntegerType(), nullable=True),
         StructField('date_time', TimestampType(), nullable=True),
         StructField('user_agent', StringType(), nullable=True),
         StructField('ip', StringType(), nullable=True),
         StructField('event_list', StringType(), nullable=True),
         StructField('geo_city', StringType(), nullable=True),
         StructField('geo_region', StringType(), nullable=True),
         StructField('geo_country', StringType(), nullable=True),
         StructField('pagename', StringType(), nullable=True),
         StructField('page_url', StringType(), nullable=True),
         StructField('product_list', StringType(), nullable=True),
         StructField('referrer', StringType(), nullable=True)]
    )

    sp_options = {"header": True,
                  "sep": "\t",
                  "quote": "\"",
                  "escape": "\"",
                  "ignoreTrailingWhiteSpace": True
                  }

    hit_process = EtlManager()
    spark = hit_process.set_spark_session(src_name)

    df_src = spark.read.options(**sp_options).schema(_schema).csv(src_path)
    # df_src = EtlManager.extract_csv_data( src_path,_schema, sp_options)
    print(df_src.printSchema())
    df_src = df_src.drop_duplicates(["hit_time_gmt", "date_time"])
    print(f"Count {df_src.count()}")
    df_src = df_src.withColumn('date', F.to_date(F.to_timestamp(df_src.date_time, "y-M-d H:mm:ss")))

    df_src.write.mode("overwrite").partitionBy("date").parquet(tgt_path)
    print(df_src.printSchema())


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit [source feed name ] ")
        print(sys.argv)
        sys.exit(1)

    src_name = sys.argv[1]
    src_path = get_config()[src_name]['s3_paths']['data_landing_bucket']
    tgt_path = get_config()[src_name]['s3_paths']['data_processing_bucket']
    main(src_name, src_path, tgt_path)
