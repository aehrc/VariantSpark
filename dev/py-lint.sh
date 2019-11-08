#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# check code with pylint

pushd python
pylint --max-line-length=140 varspark
popd
