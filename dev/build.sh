#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

# run code checks

/bin/bash ${FWDIR}/dev/py-lint.sh

# build and package the project
mvn clean package -B

# make build info
env | tee "target/buildinfo" 
