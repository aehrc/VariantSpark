#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

FLUSH_ROOT=${VS_FLUSH_DIR:-.}
FLUSH_DIR=${FLUSH_ROOT}/variant-spark-flush
GEN_DIR=${FLUSH_DIR}/gen

hadoop fs -mkdir -p ${GEN_DIR}

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- \
 gen-features -of ${GEN_DIR}/synthetic_5M_10K_3.parquet -sp 256 -sr 13 -v -gv 5000000 -gs 10000 -gl 3 "$@"


