#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

mkdir -p ${PWD}/tmp

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- \
 gen-features -of /data/szu004/synthetic_10000_100_3.parquet -sp 64 -sr 13 -v -gv 150000 -gs 10000 -gl 3 "$@"


