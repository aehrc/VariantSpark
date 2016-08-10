#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}
export VS_ECHO_CMDLINE=YES

variant-spark --local  --\
 importance -if ${PWD}/data/small.vcf -ff ${PWD}/data/small-labels.csv -fc 22_16051249 -v -t 20

