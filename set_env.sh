#!/bin/bash

# update and install some useful yum packages
sudo yum install -y jq

# set region for boto3
aws configure set region "us-east-1"

# Clone repo and install some useful python packages
#git clone https://github.com/krishnaviswa/adb-kk.git

cd ~/adb-kk
python3 -m pip install -r requirements.txt

cd ~/adb-kk/cicd

aws cloudformation create-stack --stack-name mwaa-environment --template-body file://cfn_vpc_mwaa_setup.yml
if [ $? -ne '0' ]; then
  echo "Error"
  exit 1
fi

aws cloudformation create-stack --stack-name cfn-env-setup --template-body file://cfn_etl_setup.yml --capabilities CAPABILITY_NAMED_IAM
if [ $? -ne '0' ]; then
  exit 1
  echo "Error"
fi

cd ~/adb-kk/src/
aws s3 cp hit_curated.py s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/
aws s3 cp hit_landing_processed.py s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/
aws s3 cp helper.zip s3://adb-cfn-script-061553549694-us-east-1/emr_scripts/

cd ~/adb-kk/dags/
aws s3 cp hit_etl_dag.py s3://adb-cfn-airflow-061553549694-us-east-1/dags/
aws s3 cp requirements.txt s3://adb-cfn-airflow-061553549694-us-east-1/



