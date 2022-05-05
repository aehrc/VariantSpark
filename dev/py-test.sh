#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# check code with pylint

pushd python
pytest -s -m spark
pytest -s -m hail
popd
