#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

mkdir -p ${PWD}/tmp

variant-spark --local  --\
 convert -if ${PWD}/tmp/synthetic_10000_500_3.parquet -sp 4 -of ${PWD}/tmp/synthetic_10000_500_3.csv  "$@"


