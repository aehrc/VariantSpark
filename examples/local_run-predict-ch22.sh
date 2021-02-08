#!/bin/bash

PWD=$(
  cd $(dirname "$0")/..
  pwd
)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

variant-spark --spark --master 'local[*]' -- \
  predict -if ${PWD}/data/chr22_1000.vcf -im ${PWD}/data/ch22-model.ser "$@"
