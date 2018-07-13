#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin

DATA_ROOT=${VS_DATA_DIR:-.}
DATA_DIR=${DATA_ROOT}/variant-spark-data
DATA_INPUT_DIR=${DATA_DIR}/input

export VS_ECHO_CMDLINE=YES

variant-spark --spark --master yarn-client --num-executors 64 --executor-memory 4G --driver-memory 4G -- importance -if "${DATA_INPUT_DIR}/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.bz2" -ff "${DATA_INPUT_DIR}/chr22-labels_release_v3.20101123.csv" -fc 22_16051249 -v -rn 500 -rbs 50 -sr 13 -sp 64 -rmt 5000 -ro
