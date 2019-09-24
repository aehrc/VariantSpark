#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# check code with pylint

pushd python

export PYTHONPATH=$PYTHONPATH:./deps/hail-py-0.1-src.zip
pylint --max-line-length=140 varspark
popd
