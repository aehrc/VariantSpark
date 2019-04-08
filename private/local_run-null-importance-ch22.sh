#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

variant-spark --spark --master 'local[*]' -- \
 null-importance -if ${PWD}/data/chr22_1000.vcf -ff ${PWD}/data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 100  -sr 13 \
 -of ${PWD}/tmp/null_imp_chr22.csv \
 -pn 50 -ivb -ovn raw -oc 1 \
 "$@"

