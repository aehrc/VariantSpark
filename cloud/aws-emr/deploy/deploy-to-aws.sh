#!/bin/bash

set -e

EMR_ROOT=$(cd `dirname "$0"`/..; pwd)
VS_ROOT=$(cd `dirname "$0"`/../../..; pwd)


aws s3 cp ${EMR_ROOT}/bootstrap/install-variant-spark.sh s3://au.csiro.pbdava.test/variant-spark/bootstrap/

if [[ -f "${VS_ROOT}/target/dist/variant-spark_2.11-0.0.2-SNAPSHOT.tar.gz" ]]; then
	echo "Deploying distribution from: ${VS_ROOT}/target/dist/variant-spark_2.11-0.0.2-SNAPSHOT.tar.gz" 
	aws s3 cp ${VS_ROOT}/target/dist/variant-spark_2.11-0.0.2-SNAPSHOT.tar.gz s3://au.csiro.pbdava.test/variant-spark/dist/
fi