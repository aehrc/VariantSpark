#!/bin/bash
# Generate syntetic datasets for testing

set -e
set -x

FWDIR="$(
  cd "$(dirname $0)"/..
  pwd
)"
cd "$FWDIR"

OUTPUT_DIR="${FWDIR}/src/test/data/hail"
mkdir -p "${OUTPUT_DIR}"

DATA_DIR="${FWDIR}/data"
WORK_DIR="${FWDIR}/target"

mkdir -p "${WORK_DIR}"
#
#Make manual cases
#

# also generate hail vds
python "${FWDIR}/src/test/python/extract_rf_vds.py"

# chr22-100
RESP="22_16050408"
PREFIX="chr22"

OUT_FILE="${WORK_DIR}/test-gen-hail-cases.$$.out"

"${FWDIR}/bin/variant-spark" --spark --master local[1] -- importance \
  -if "${DATA_DIR}/${PREFIX}_1000.vcf" -ff "${DATA_DIR}/${PREFIX}-labels.csv" \
  -fc "${RESP}" \
  -on 100 -of "${OUTPUT_DIR}/${PREFIX}_${RESP}-imp.csv" \
  --model-file-format json --model-file "${OUTPUT_DIR}/${PREFIX}_${RESP}-model.json" \
  --rf-mtry-fraction 0.1 --rf-max-depth 10 --rf-min-node-size 2 \
  -v -ro -rn 100 -rbs 25 -sp 1 -sr 17 | tee "${OUT_FILE}"

# Extract oob error
OOB_ERROR=`grep "Random forest oob accuracy:" "${OUT_FILE}" | awk -F ',' '{print $1}' | awk '{print $5}'`

cat > "${OUTPUT_DIR}/${PREFIX}_${RESP}-meta.yml" <<EOF
output:
  oob_error: ${OOB_ERROR}
EOF

#      --rf-sample-no-replacement --rf-subsample-fraction 0.7 \


