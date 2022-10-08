#!/bin/bash

# update and install some useful yum packages
sudo yum install -y jq

# set region for boto3
aws configure set region "us-east-1"

# Clone repo and install some useful python packages
git clone https://github.com/krishnaviswa/adb-kk.git
cd adb-kk
python3 -m pip install -r requirements.txt



