import sys
# from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import datetime

from etl.etlmanager import EtlManager, get_config


def main(src_name, src_path, tgt_path):
    hit_process = EtlManager()
    spark = hit_process.set_spark_session(src_name)

    df_pqt = hit_process.extract_parquet_data(src_path)
    print(df_pqt.printSchema())
    df_pqt = df_pqt.drop_duplicates(["hit_time_gmt", "date_time"])
    print(f"Count {df_pqt.count()}")
    df_detailed = df_pqt.withColumn('product_list_delmtd', F.split(df_pqt["product_list"], ','))
    df_detailed = df_detailed.withColumn('product_list_exploded', F.explode_outer(df_detailed.product_list_delmtd))
    expanded = F.split(df_detailed.product_list_exploded, ';')
    df_detailed = df_detailed.withColumn('category', F.trim(expanded[0])).withColumn('product_name',
                                                                                     F.trim(expanded[1])).withColumn(
        'number_of_items', F.trim(expanded[2])).withColumn('total_revenue', F.trim(expanded[3]))
    df_detailed = df_detailed.drop(*['product_list_delmtd', 'product_list', 'product_list_exploded'])

    df_detailed.createOrReplaceTempView("df_detailed_vw")
    df_agg = spark.sql(""" with cte_qry as (select *,lag(ip) over(partition by ip order by hit_time_gmt) as lag_ip, 
                           ip, event_list,referrer, 
                           hit_time_gmt, parse_url(referrer,'HOST') as host, 
                           upper(split(parse_url(referrer,'HOST'),'[.]',3)[1]) as domain, total_revenue 
                           from df_detailed_vw order by ip,hit_time_gmt ), 
              cte_summary as (select distinct  max(case when nvl(lag_ip,'NULL')='NULL' then domain end) as domain, 
                       max(upper(case when domain= 'GOOGLE' then parse_url(referrer,'QUERY','q') 
                        when domain= 'BING' then parse_url(referrer,'QUERY','q')  
                        when domain= 'YAHOO' then parse_url(referrer,'QUERY','p')
                        else parse_url(referrer,'QUERY','p') end )) as Keyword, 
                       sum(total_revenue) as Revenue 
                   from cte_qry group by ip )  
             select domain,Keyword, sum(nvl(Revenue,'0')) Revenue from cte_summary group by domain,Keyword """)

    df_agg = df_agg.withColumnRenamed("domain", "Search Engine Domain").withColumnRenamed("Keyword", "Search Keyword")
    print(df_agg.show())
    # df_pqt.write.mode("overwrite").partitionBy("date").parquet(tgt_path)
    EtlManager.write_parquet_data(df_detailed, "overwrite", tgt_path + "/hit_detail/", "geo_country", "geo_region",
                                  "date")

    df_agg.coalesce(1).write.mode("overwrite").options(**{"header": "true", "sep": "\t"}).csv(
        tgt_path + "/hit_agg/" + datetime.date.today().strftime("%Y-%m-%d") + "_SearchKeywordPerformance.tab")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit [source feed name ] ")
        print(sys.argv)
        sys.exit(1)

    src_name = sys.argv[1]
    src_path = get_config()[src_name]['s3_paths']['data_processing_bucket']
    tgt_path = get_config()[src_name]['s3_paths']['data_curated_bucket']
    main(src_name, src_path, tgt_path)
