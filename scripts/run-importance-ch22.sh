#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}

DATA_ROOT=${VS_DATA_DIR:-.}
DATA_DIR=${DATA_ROOT}/variant-spark-data
DATA_INPUT_DIR=${DATA_DIR}/input

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- importance -if "${DATA_INPUT_DIR}/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.bz2" -ff "${DATA_INPUT_DIR}/small-labels.csv" -fc 22_16051249 -v -t 20