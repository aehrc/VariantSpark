#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

. ${FWDIR}/dev/travis-set-ver.sh

# build and package the project
mvn package -B

# make build info
env | tee "target/buildinfo" 

# build the python distribution

pushd  ${FWDIR}/python
python setup.py sdist --formats zip
popd 

