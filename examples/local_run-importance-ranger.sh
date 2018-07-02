#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

variant-spark --spark --master 'local[*]'  --\
 importance -if ${PWD}/data/ranger-wide_1000_10000.csv.bz2 -it csv  -ff ${PWD}/data/ranger-labels_10000.csv -fc resp5 -v -rn 20 -ro

