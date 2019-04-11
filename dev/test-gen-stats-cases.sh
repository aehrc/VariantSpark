#!/bin/bash
# Generate data for stats testing

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

OUTPUT_DIR="${FWDIR}/src/test/data/stats"
mkdir -p "${OUTPUT_DIR}"

"${FWDIR}/src/test/R/make_test_case.R" -p "${OUTPUT_DIR}/stats_100_1000_cont_0.0" -r 13 -v 100 -s 1000 -m 10  -t 0.0


