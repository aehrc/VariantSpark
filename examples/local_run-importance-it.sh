#!/bin/bash

PWD=$(cd `dirname "$0"`/..; pwd)
PATH=${PATH}:${PWD}/bin
export VS_ECHO_CMDLINE=YES

#Construct the input speficifcation in JSON as here document (easier to read)
INPUT_OPTIONS_JSON=$(cat << EOF 
{},
EOF
)

variant-spark --spark --master 'local' -- \
 importance -ff ${PWD}/src/test/data/data-labels.csv -fc label -v -rn 2000 -rbs 50 -ro -sr 13 -ovn raw \
 -on 0 \
 -if "${PWD}/src/test/data/data.csv" -it "stdcsv" \
 -io "${INPUT_OPTIONS_JSON}"  \
  "$@"

