#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

FLUSH_ROOT=${VS_FLUSH_DIR:-.}
FLUSH_DIR=${FLUSH_ROOT}/variant-spark-flush
GEN_DIR=${FLUSH_DIR}/gen

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- \
 gen-labels -if ${GEN_DIR}/synthetic_5M_10K_3.parquet -sp 256 -sr 13 -v -ff ${GEN_DIR}/synthetic_5M_10K_3-labels.csv -fc resp -ge v_10:1.0 -ge v_100:1.0 -ge v_1000:1.0 -ge v_10000:1.0 -ge v_10000:1.0 "$@"


