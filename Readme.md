Design Flow:
    
  


Build Steps carried out:

1. AWS Transfer Family (for SFTP Push of data files) in AWS Console.
2. Few Necessary in AWS Console for AWS Cloud Formation, Transfer Family  
3. Set up scripts, for setting up env and trigger cloud formation templates. 
4. VPC Cloud formation (CFN) stack for MWAA Networking setup. (AWS default template)
5. Cloud formation (CFN) stack for creating S3buckets, EMR, Glue Crawler and MWAAirflow setup. Note: However MWAA UI via CFN didn't work. Hence I used Console to create MWAA env, which then spin up EMR cluster and submit jobs Automatically from S3.
6. AWS cli2 to move the pyspark scripts, dags and helper.zip file into S3 buckets
7. Once the dag completes. It complete the etl with pyspark jobs in emr cluster and run the Glue crawlers.
8. Run the Athena to check the data counts across buckets

Components:
1. Github Repo with code repository. I assume CloudShell as CICD server to clone github and push artifacts to AWS account.
2. Setup_env bash script for install and move code scripts/artifacts to designated S3 buckets.
3. Cloud formation templates -2
4. Pyspark scripts -2
5. Helper python module -2 (OOPS style)
6. Cnfig files for bucket and Crawler names
7. MWAA Dags
8. Glue jobs (but dropped intermittently considering the transformation complexities)


Exceptions/Challenges:

a. CFN is not so friendly in terms of redeploying existing objects/resources. Need to explore how to retain existing resource. I used terraform in the past which was better.
b. EMR scaling is not enabled in CFN stack, from console it was set.
c. Bucket lifecycle policies are not set in CFN stack, from console it was set.
d. Though i have the Stack with airflow and EMR, MWAA ui is not connecting even after vpc peering/SG open for public. Need to explore.

Overall, I managed to setup ETL pipeline and completed this assessment in mid of my current project schedule.
