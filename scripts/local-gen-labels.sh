#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

mkdir -p ${PWD}/tmp

variant-spark --local  --\
 gen-labels -if ${PWD}/tmp/synthetic_10000_500_3.parquet -sp 4 -sr 13 -v -ff ${PWD}/tmp/synthetic_10000_500_3-labels.csv -fc resp -ge v_0:1.0 -ge v_1:1.0 -ge v_22:4.0 "$@"


