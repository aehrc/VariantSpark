#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES
DATA_ROOT="${VS_DATA_ROOT:-$PWD}"

NO_TREES=2000
NO_PERM=100

variant-spark --spark --master 'local[*]' --driver-memory 20G -- \
 null-importance -if ${DATA_ROOT}/data/hipsterIndex/hipster.vcf  -ff ${DATA_ROOT}/data/hipsterIndex/hipster_labels.txt -fc label -v -rn $NO_TREES -rbs 200  -sr 13 \
 -of ${DATA_ROOT}/tmp/null_imp_hipster_${NO_TREES}_${NO_PERM}.csv \
 -pn $NO_PERM  -ovn raw -oc 1 \
 "$@"

