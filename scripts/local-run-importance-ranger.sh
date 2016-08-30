#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

variant-spark --local  --\
 importance -if ${PWD}/data/ranger-wide.csv.bz2 -it csv  -ff ${PWD}/data/ranger-labels.csv -fc resp5 -v -t 20

