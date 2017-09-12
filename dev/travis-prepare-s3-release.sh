#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

. ${FWDIR}/dev/travis-set-ver.sh


S3_VERSION="${CI_BASE_VERSION}-${CI_COMMIT_TAG_SHORT}"
echo "Release to S3 as: ${S3_VERSION}"


S3_VERSION_DIR="${CI_BASE_VERSION}"
S3_RELEASE_DIR="${S3_VERSION_DIR}/${S3_VERSION}"


S3_BUILD_DIR=target/s3-release
rm -rf "${S3_BUILD_DIR}" 

S3_BUILD_VERSION_DIR="${S3_BUILD_DIR}/${S3_VERSION_DIR}"
S3_BUILD_RELEASE_DIR="${S3_BUILD_DIR}/${S3_RELEASE_DIR}"

mkdir -p "${S3_BUILD_VERSION_DIR}"
echo "${S3_VERSION}" | tee  "${S3_BUILD_VERSION_DIR}/latest"

mkdir -p "${S3_BUILD_RELEASE_DIR}/lib"
cp target/buildinfo "${S3_BUILD_RELEASE_DIR}/buildinfo"
cp target/variant-spark_*.jar "${S3_BUILD_RELEASE_DIR}/lib"
