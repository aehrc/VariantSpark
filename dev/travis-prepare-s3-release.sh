#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version $@ 2>/dev/null | grep -v "INFO" | tail -n 1)

COMMIT_TAG=${TRAVIS_COMMIT:-unknown}

echo "Version: ${PROJECT_VERSION}-${COMMIT_TAG}"

rm -rf target/s3-release
mkdir -p target/s3-release
mkdir -p target/s3-release/lib

cp target/variant-spark_*.jar target/s3-release/lib
