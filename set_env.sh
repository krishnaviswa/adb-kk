#!/bin/bash

# update and install some useful yum packages
sudo yum install -y jq

# set region for boto3
aws configure set region "us-east-1"

# Clone repo and install some useful python packages
#git clone https://github.com/krishnaviswa/adb-kk.git
cd adb-kk
python3 -m pip install -r requirements.txt

python3 -m pip list

cd ~/adb-kk/cicd

aws cloudformation create-stack --stack-name mwaa-environment --template-body cfn_vpc_mwaa_setup.yml
if [ $? -ne '0' ]; then
  echo "Error"
  exit 1
fi
aws cloudformation create-stack --stack-name cfn_env_setup --template-body cfn_etl_setup.yml
if [ $? -ne '0' ]; then
  exit 1
  echo "Error"
fi