

#!/bin/bash
# build the python distribution

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

. ${FWDIR}/dev/travis-set-ver.sh

pushd  ${FWDIR}/python
python setup.py egg_info -b "${CI_PYTHON_TAG}" sdist
popd 

