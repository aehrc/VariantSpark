#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# check code with pylint

pushd python
python -m unittest varspark.test.test_core
popd
