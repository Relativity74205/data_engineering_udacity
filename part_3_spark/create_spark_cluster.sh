#!/bin/bash

aws emr create-cluster \
--name spark1 \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark \
--ec2-attributes KeyName=spark_ssh,SubnetId=subnet-bd8e6ff1 \
--instance-type m5.xlarge

ssh -i .ssh/spark_ssh.pem hadoop@ec2-54-226-233-51.compute-1.amazonaws.com
ssh -i .ssh/spark_ssh.pem -N -D 8157 hadoop@ec2-54-226-233-51.compute-1.amazonaws.com
aws emr socks --cluster-id j-4RS5EBVTDT8O --key-pair-file ~/.ssh/spark_ssh.pem