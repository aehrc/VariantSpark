#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

#Construct the input speficifcation in JSON as here document (easier to read)
INPUT_OPTIONS_JSON=$(cat << EOF 
{},
EOF
)

variant-spark --spark --master 'local[*]' -- \
 importance -ff ${PWD}/data/har_aal_labels.csv -fc label -v -rn 500 -rbs 50 -ro -sr 13 -rmt 23 \
 -if "${PWD}/data/har_aal.csv.bz2" -it "stdcsv" \
 -io "${INPUT_OPTIONS_JSON}"  \
  "$@"

