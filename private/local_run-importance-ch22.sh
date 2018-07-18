#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

variant-spark --spark --master 'local[*]' -- \
 importance -if ${PWD}/data/chr22_1000.vcf -ff ${PWD}/data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 20 -ro -sr 13 "$@"

