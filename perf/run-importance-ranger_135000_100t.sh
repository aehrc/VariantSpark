#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}

DATA_ROOT=${VS_DATA_DIR:-.}
DATA_DIR=${DATA_ROOT}/variant-spark-data
DATA_INPUT_DIR=${DATA_DIR}/input

export VS_ECHO_CMDLINE=YES

variant-spark --spark --master yarn-client --num-executors 128 --executor-memory 2G --driver-memory 4G --conf spark.serializer=org.apache.spark.serializer.KryoSerializer -- importance -if "${DATA_INPUT_DIR}/ranger-wide_150000_10000.csv.bz2" -it csv -ff "${DATA_INPUT_DIR}/ranger-labels_10000.csv" -fc resp5 -v -sp 128 -rmt 135000 -rn 100 -rbs 50  "$@"
