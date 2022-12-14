Build Steps carried out:

1. AWS Transfer Family (for SFTP Push of data files) , created in AWS Console.
2. Few initial roles created in AWS Console for AWS Cloud Formation, Transfer Family  
3. Set up scripts (using AWS cli2 ),install libraries for new env and trigger cloud formation using templates. 
4. VPC Cloud formation (CFN) stack for MWAA Networking setup. (AWS default template)
5. Cloud formation (CFN) stack for creating S3buckets, EMR, Glue Crawler and MWAAirflow setup. Note: However MWAA UI via CFN didn't work. Hence I used Console to create MWAA env, which then spin up EMR cluster and submit jobs Automatically from S3.
6. AWS cli2 to move the pyspark scripts, dags and helper.zip file into S3 buckets
7. Once the dag completes. It complete the etl with pyspark jobs in emr cluster and run the Glue crawlers.
8. Run the Athena to check the data counts across buckets

Components:
1. Github Repo as code repository. I assume CloudShell as CICD server to clone github and push artifacts to AWS account.
2. set_env.sh, a bash script for install and move code scripts/artifacts to designated S3 buckets.
3. S3 buckets and policies. Data Lake approach with 3 buckets
   1. Buckets are AES256 SSE_S3 encrypted
   2. Lifecycle policies to Storage IA and Glaciers ar e set by Console.. (short of time to do in CD)
   3. Source files are extracted as Spark DF and deduplicated for the hit_time_gmt and datetime
   4. Then, df write into Processed bucket in Parquet format with partitioned by date
   5. Then, from Processed bucket, data is transformed/aggregated and wrote it into csv format with partitioned by geo country,geo region and date in Curated bucket
   6. Curated bucket will also have data agg with result of this assessment
   7. SQS/SNS are not built (short of time) after etl dag completion   
4. Cloud formation templates -2 one for VPC ( AWS provided and one created for me for ETL resources)
5. Pyspark scripts for raw to landing and further to curated
6. Helper python module 
7. Config files for bucket and Crawler names
8. MWAA Dags and requirements.txt for file AWS libraries 
9. Glue jobs (but dropped intermittently considering the transformation complexities). Not checked in. Can show on Code Review.

 Steps to run: Clone Github Repo (https://github.com/krishnaviswa/adb-kk/) and start set_env.sh as onetime setup to create/provision cloud resources.

Exceptions/Challenges:

1. CFN is not so friendly in terms of redeploying existing objects/resources. Need to explore how to retain existing resource. I used terraform in the past which was better.
2. EMR scaling is not enabled in CFN stack, from console it was set.
3. Bucket lifecycle policies are not set in CFN stack, from console it was set.
4. Though i have the Stack with airflow and EMR, MWAA ui is not connecting even after vpc peering/SG open for public. Need to explore.

Overall, I managed to setup ETL pipeline and to an extent completed this assessment in mid of my current project schedule.
