#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

variant-spark --local  --\
 importance -if ${PWD}/tmp/synthetic_10000_500_3.parquet -it parquet -ff ${PWD}/tmp/synthetic_10000_500_3-labels.csv -fc resp -v -rn 1000 -rmt 500 -rbs 20 -ro "$@"


