#!/bin/bash
# Generate syntetic datasets for testing

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

OUTPUT_DIR="${FWDIR}/src/test/data/synth"
mkdir -p "${OUTPUT_DIR}"

"${FWDIR}/src/test/R/make_test_case.R" -p "${OUTPUT_DIR}/synth_2000_500_fact_10_0.0" -r 13 -v 2000 -s 500 -m 10 -f -t 0.0
"${FWDIR}/src/test/R/make_test_case.R" -p "${OUTPUT_DIR}/synth_2000_500_fact_10_0.995" -r 13 -v 2000 -s 500 -m 10 -f -t 0.995
"${FWDIR}/src/test/R/make_test_case.R" -p "${OUTPUT_DIR}/synth_2000_500_fact_3_0.0" -r 13 -v 2000 -s 500 -m 3 -f -t 0.0
"${FWDIR}/src/test/R/make_test_case.R" -p "${OUTPUT_DIR}/synth_2000_500_fact_3_0.995" -r 13 -v 2000 -s 500 -m 3 -f -t 0.995


