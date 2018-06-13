#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

mkdir -p ${PWD}/tmp

variant-spark --local  --\
 gen-features -of ${PWD}/tmp/synthetic_10000_500_3.parquet -sp 4 -sr 13 -v -gv 10000 -gs 500 -gl 3 "$@"


