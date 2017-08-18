#!/bin/bash
set -x -e

# AWS EMR bootstrap script 
# Install variant spark

# check for master node
IS_MASTER=false
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

# error message
error_msg ()
{
  echo 1>&2 "Error: $1"
}

INST_DIR=${INST_VOL:-/mnt}

echo "Bootstraping variant-spark"

if [ "$IS_MASTER" = true ]; then
	echo "Installing variant-spark in: ${INST_DIR}"
	#download and install variant spark
 	cd ${INST_DIR}
 	aws s3 cp s3://au.csiro.pbdava.test/variant-spark/dist/variant-spark_2.11-0.0.2-SNAPSHOT.tar.gz .
 	tar -xzf variant-spark_2.11-0.0.2-SNAPSHOT.tar.gz
	rm variant-spark_2.11-0.0.2-SNAPSHOT.tar.gz
	ln -s variant-spark_2.11-0.0.2-SNAPSHOT variant-spark-0.0.2
	VARIANT_SPARK_HOME=${INST_DIR}/variant-spark-0.0.2
	cat << EOF | sudo tee /etc/profile.d/variant-spark.sh
export VARIANT_SPARK_HOME=${VARIANT_SPARK_HOME}
export PATH=\${PATH}:\${VARIANT_SPARK_HOME}
EOF
	echo "Installed variant-spark in: ${INST_DIR}"
fi

echo "Finished bootstraping variant-spark"
