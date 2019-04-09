#!/bin/bash
# Generate syntetic datasets for testing

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

OUTPUT_DIR="${FWDIR}/src/test/data/regression"
mkdir -p "${OUTPUT_DIR}"




DATA_DIR="${FWDIR}/data"
#
#Make manual cases
#

# chr22-100
RESP="22_16050408"
PREFIX="chr22"
"${FWDIR}/bin/variant-spark" --spark --master local[2] -- importance -if "${DATA_DIR}/${PREFIX}_1000.vcf" -ff "${DATA_DIR}/${PREFIX}-labels.csv" \
         -fc "${RESP}" \
         -on 100 -of "${OUTPUT_DIR}/${PREFIX}-imp_${RESP}.csv" \
<<<<<<< HEAD
         -v -ro -rn 100 -rbs 50 -sp 4 -sr 17
=======
         -v -ro -rn 100 -rbs 50 -sp 4 -sr 17 -ivb
>>>>>>> origin/_fix_regression

# CNAE-9
RESP="category"
PREFIX="CNAE-9"
"${FWDIR}/bin/variant-spark" --spark --master local[2] -- importance -if "${DATA_DIR}/${PREFIX}-wide.csv" -ff "${DATA_DIR}/${PREFIX}-labels.csv" \
         -fc "${RESP}" \
         -on 100 -of "${OUTPUT_DIR}/${PREFIX}-imp_${RESP}.csv" \
         -ivo 10 \
         -it csv -v -ro -rn 100 -rbs 50 -sp 4 -sr  17

#
#Make Synthetic cases
#
DATA_DIR="${FWDIR}/src/test/data/synth"
for CASE in ${FWDIR}/src/test/data/synth/*-meta.txt; do 
	FILENAME=`basename $CASE`
	PREFIX=${FILENAME%-*}
	PREFIX_ARGS=(${PREFIX//_/ })
	IVO=${PREFIX_ARGS[4]}
	for l in 2 10; do
	RESP="cat${l}"
	"${FWDIR}/bin/variant-spark" --spark --master local[2] -- importance -if "${DATA_DIR}/${PREFIX}-wide.csv" -ff "${DATA_DIR}/${PREFIX}-labels.csv" \
	 -fc "${RESP}" \
	 -on 100 -of "${OUTPUT_DIR}/${PREFIX}-imp_${RESP}.csv" \
	 -ivo ${IVO} \
 	 -it csv -v -ro -rn 100 -rbs 50 -sp 4 -sr 17
	done
done
