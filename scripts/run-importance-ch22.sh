#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}

variant-spark --spark --master yarn-client --num-executors 32 --executor-memory 4G --driver-memory 4G -- importance -if /flush2/szu004/variant-spark/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf -v -t 20
