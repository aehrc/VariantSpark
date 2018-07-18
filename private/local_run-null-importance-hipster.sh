#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

variant-spark --spark --master 'local[*]' -- \
 null-importance -if ${PWD}/tmp/hipster.vcf  -ff ${PWD}/data/hipsterIndex/hipster_labels.txt -fc label -v -rn 1000 -rbs 100  -sr 13 \
 -of ${PWD}/tmp/null_imp_hipster.csv \
 -pn 50  -ovn raw -oc 1 \
 "$@"

