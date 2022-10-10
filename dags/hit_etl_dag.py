import os
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
# from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import boto3
from botocore.exceptions import ClientError

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['connectme.kk@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

JOB_FLOW_OVERRIDES = {
    "Name": "ETL CLuster",
    "ReleaseLabel": "emr-6.2.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "VisibleToAllUsers": True,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "Tags": [
        {
            "Key": "project",
            "Value": "adb"
        }
    ]
}

SPARK_STEPS = [
    {
        "Name": "etl landing to processed",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--py-files",
                "s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/helper.zip",
                "s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/hit_landing_processed.py",
                "hit_process"
            ],
        },
    },
    {
        "Name": "etl processed to curated",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--py-files",
                "s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/helper.zip",
                "s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/hit_curated.py",
                "hit_process"
            ],
        },
    }
]


# glue_crawler_raw = {
#     'Name': 'adb-cfn-raw',
#     'Role': 'arn:aws:iam::061553549694:role/EmrDemoCrawlerRole',
#     'DatabaseName': 'adb_db_cfn',
#     'Targets': {'S3Targets': [{'Path': f'adb-cfn-raw-061553549694-us-east-1/etl_landing/'}]},
# }
#
# glue_crawler_processed = {
#     'Name': 'adb-cfn-processed',
#     'Role': 'arn:aws:iam::061553549694:role/EmrDemoCrawlerRole',
#     'DatabaseName': 'adb_db_cfn',
#     'Targets': {'S3Targets': [{'Path': f'adb-cfn-processed-061553549694-us-east-1/etl_processing/'}]},
# }
#
# glue_crawler_curated = {
#     'Name': 'adb-cfn-curated',
#     'Role': 'arn:aws:iam::061553549694:role/EmrDemoCrawlerRole',
#     'DatabaseName': 'adb_db_cfn',
#     'Targets': {'S3Targets': [{'Path': f'adb-cfn-curated-061553549694-us-east-1/'}]},
# }


def start_a_crawler(**kwargs):
    print(kwargs['job_name'])
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        response = glue_client.start_crawler(Name=kwargs['job_name'])
        return response
    except ClientError as e:
        raise Exception("boto3 client error in start_a_crawler: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in start_a_crawler: " + e.__str__())


with DAG(
        dag_id=DAG_ID,
        description='Run built-in Spark app on Amazon EMR',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['emr'],
) as dag:
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    hit_etl = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    #   crawl_raw = GlueCrawlerOperator(
    #       task_id='crawl_raw',
    #       config=glue_crawler_raw,
    #   )
    #
    #   crawl_processs = GlueCrawlerOperator(
    #       task_id='crawl_processed',
    #       config=glue_crawler_processed,
    #   )
    #
    #   crawl_curated = GlueCrawlerOperator(
    #       task_id='crawl_curated',
    #       config=glue_crawler_curated,
    #   )

    # aws glue start-crawler --name my-crawler

    # crawl_raw_command = "aws glue start-crawler --name adb-cfn-raw"
    #
    # crawl_raw = BashOperator(
    #     task_id="crawl_raw",
    #     bash_command=crawl_raw_command,
    #     dag=dag,
    # )
    #
    # crawl_process_command = "aws glue start-crawler --name adb-cfn-processed"
    #
    # crawl_process = BashOperator(
    #     task_id="crawl_process",
    #     bash_command=crawl_process_command,
    #     dag=dag,
    # )
    #
    # crawl_curated_command = "aws glue start-crawler --name adb-cfn-curated"
    #
    # crawl_curated = BashOperator(
    #     task_id="crawl_curated",
    #     bash_command=crawl_curated_command,
    #     dag=dag,
    # )

    crawl_raw = PythonOperator(task_id='crawl_raw', python_callable=start_a_crawler,
                               op_kwargs={'job_name': 'adb-cfn-raw'}, dag=dag)

    crawl_process = PythonOperator(task_id='crawl_process', python_callable=start_a_crawler,
                                   op_kwargs={'job_name': 'adb-cfn-processed'}, dag=dag)

    crawl_curated = PythonOperator(task_id='crawl_curated', python_callable=start_a_crawler,
                                   op_kwargs={'job_name': 'adb-cfn-curated'}, dag=dag)

    end_data_pipeline = DummyOperator(task_id="end_data_pipeline")

    cluster_creator >> hit_etl >> step_checker >> crawl_raw >> crawl_process >> crawl_curated >> end_data_pipeline

    # cluster_creator >> hit_etl >> step_checker >> crawl_raw >> end_data_pipeline
