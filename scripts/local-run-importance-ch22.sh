#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

variant-spark --local  --\
 importance -if ${PWD}/data/chr22_1000.vcf -ff ${PWD}/data/chr22-labels.csv -fc 22_16051249 -v -rn 500 -rbs 20 -ro "$@"


