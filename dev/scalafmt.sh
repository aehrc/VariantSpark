#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

VERSION="${@:-2.11}"
mvn mvn-scalafmt_$VERSION:format -Dscalafmt.skip=false -Dscalafmt.validateOnly=false \
  -Dscalafmt.onlyChangedFiles=false
