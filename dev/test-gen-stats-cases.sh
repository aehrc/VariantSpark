#!/bin/bash
# Generate data for stats testing

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

OUTPUT_DIR="${FWDIR}/src/test/data/stats"
mkdir -p "${OUTPUT_DIR}"

# Generate datasets
"${FWDIR}/src/test/R/make_test_case.R" -p "${OUTPUT_DIR}/stats_100_1000_cont_0.0" -r 13 -v 100 -s 1000 -m 10  -t 0.0

# Generate distribution of importances
for l in labels labels_null; do 
	"${FWDIR}/src/test/R/make_stats_case.R" -p "${OUTPUT_DIR}/stats_100_1000_cont_0.0" -l ${l} -s 13 -r 50 -t 2000
done 


