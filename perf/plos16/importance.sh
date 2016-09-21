#!/bin/bash

DATA_DIR=${VS_FLUSH_DIR}/variant-spark-flush/plos16
mkdir -p ${DATA_DIR}
echo "Data dir: ${DATA_DIR}"

python run-search.py importance -v 2500000 -s 5000 -m 0.1 -m 0.25 -m 0.5 -m 0.8 -c 16 -c 32 -c 64 -c 128 --times 5 --data-dir ${DATA_DIR}
ls -l ${DATA_DIR}
