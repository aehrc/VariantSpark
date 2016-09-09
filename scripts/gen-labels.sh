#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES


variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- \
 gen-labels -if /data/szu004/synthetic_10000_100_3.parquet -sp 4 -sr 13 -v -ff /data/szu004/synthetic_10000_100_3-labels.csv -fc resp -ge 0:1.0 -ge 1:1.0 -ge 2:1.0 -ge 3:1.0 -ge 4:1.0 "$@"


