#!/bin/bash
# upload distbution to PyPi (or PyPI test)

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

pushd  ${FWDIR}/python
twine upload --repository-url ${PYPI_URL} --username ${PYPI_USER} --password ${PYPI_PASSWORD} dist/*
popd 

