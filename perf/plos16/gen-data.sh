#!/bin/bash

DATA_DIR=${VS_FLUSH_DIR}/variant-spark-flush/plos16
mkdir -p ${DATA_DIR}
echo "Data dir: ${DATA_DIR}"

python run-search.py gen_data -v 150000 -v 500000 -v 2500000 -v 10000000 -v 50000000  -s 1000 -s 5000 -s 10000 --output-dir ${DATA_DIR}
ls -l ${DATA_DIR}
