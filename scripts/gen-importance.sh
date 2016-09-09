#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}

DATA_ROOT=${VS_DATA_DIR:-.}
DATA_DIR=${DATA_ROOT}/variant-spark-data
DATA_INPUT_DIR=${DATA_DIR}/input

export VS_ECHO_CMDLINE=YES

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- importance -if "/data/szu004/synthetic_10000_100_3.parquet" -it parquet -ff "/data/szu004/synthetic_10000_100_3-labels.csv" -fc resp -v -sp 32 -rn 1000 -rmt 20000 -rbs 50 

