#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

#Construct the input speficifcation in JSON as here document (easier to read)
INPUT_JSON=$(cat << EOF 
[
   {"inputFile":"${PWD}/data/chr22_1000_pheno-wide.csv","type":"csv"},
   {"inputFile":"${PWD}/data/chr22_1000.vcf","type":"vcf"}
]
EOF
)

variant-spark --spark --master 'local[*]' -- \
 importance -ff ${PWD}/data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 20 -ro -sr 13 \
 -ij "${INPUT_JSON}"  \
  "$@"

