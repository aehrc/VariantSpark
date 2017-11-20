#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

pip install -r ${FWDIR}/dev/dev-requirements.txt
