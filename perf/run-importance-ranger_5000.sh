#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}

DATA_ROOT=${VS_DATA_DIR:-.}
DATA_DIR=${DATA_ROOT}/variant-spark-data
DATA_INPUT_DIR=${DATA_DIR}/input

export VS_ECHO_CMDLINE=YES

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G --conf spark.serializer=org.apache.spark.serializer.KryoSerializer -- importance -if "${DATA_INPUT_DIR}/ranger-wide_150000_10000.csv.bz2" -it csv -ff "${DATA_INPUT_DIR}/ranger-labels_100000.csv" -fc resp5 -v -sp 32 -rt 5000 -t 20 "$@"
