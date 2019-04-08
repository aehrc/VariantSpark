#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES
DATA_ROOT="${VS_DATA_ROOT:-$PWD}"


variant-spark --spark --master 'local[*]' --driver-memory 20G -- \
 importance -if ${DATA_ROOT}/data/hipsterIndex/hipster.vcf  -ff ${DATA_ROOT}/data/hipsterIndex/hipster_labels.txt -fc label -v -rn 5000 -rbs 200  -sr 13 \
 -on 0 \
 -of ${DATA_ROOT}/tmp/imp_hipster.csv \
 -ovn raw -oc 1 \
 "$@"

