

#!/bin/bash
# build the python distribution

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

pushd  ${FWDIR}/python
python setup.py sdist
popd 

