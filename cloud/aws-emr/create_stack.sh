#!/bin/bash

aws --debug cloudformation create-stack --stack-name emr-test-stack \
--template-url https://s3-ap-southeast-2.amazonaws.com/au.csiro.pbdava.test/variant-spark/cf/template.yaml --parameters \
ParameterKey=clusterName,ParameterValue=vs-cluster \
ParameterKey=taskInstanceCount,ParameterValue=1 \
ParameterKey=coreInstanceType,ParameterValue=m4.large \
ParameterKey=taskInstanceType,ParameterValue=m4.large \
ParameterKey=emrVersion,ParameterValue=emr-5.7.0 \
ParameterKey=environmentType,ParameterValue=test \
ParameterKey=masterInstanceType,ParameterValue=m4.large \
ParameterKey=s3BucketBasePath,ParameterValue=s3://au.csiro.pbdava.test/variant-spark/logs/ \
ParameterKey=terminationProtected,ParameterValue=false \
ParameterKey=taskBidPrice,ParameterValue=0.055 --region ap-southeast-2
