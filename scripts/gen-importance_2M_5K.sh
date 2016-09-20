#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}

export VS_ECHO_CMDLINE=YES

FLUSH_ROOT=${VS_FLUSH_DIR:-.}
FLUSH_DIR=${FLUSH_ROOT}/variant-spark-flush
GEN_DIR=${FLUSH_DIR}/gen

variant-spark --spark --master yarn-client --num-executors 128 --executor-memory 6G --driver-memory 8G \
 --conf spark.locality.wait=10s \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  --  \
 importance -if "${GEN_DIR}/synthetic_2M_5K_3.parquet" -it parquet -ff "${GEN_DIR}/synthetic_2M_5K_3-labels.csv" -fc resp -v -sp 256 -rn 200 -rmt 100000 -rbs 50  -sr 13

