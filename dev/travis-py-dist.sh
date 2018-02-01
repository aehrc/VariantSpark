

#!/bin/bash
# build the python distribution

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

. ${FWDIR}/dev/travis-set-ver.sh

pushd  ${FWDIR}/python
python egg_info -b "${CI_PYTHON_TAG}" setup.py sdist
popd 

