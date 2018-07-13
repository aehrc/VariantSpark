#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

pushd python
mkdir -p dist
rm -f dist/varspark-src.zip
find varspark -name "*.py" | zip dist/varspark-src.zip -@ 
popd
