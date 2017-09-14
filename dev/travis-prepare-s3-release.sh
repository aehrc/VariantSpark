#!/bin/bash

set -e
set -x

FWDIR="$(cd "`dirname $0`"/..; pwd)"
cd "$FWDIR"

. ${FWDIR}/dev/travis-set-ver.sh

if [[ -n "${CI_TAG}" ]]; then
	echo "Tagged build for tag: ${CI_TAG}"
	#TODO: Add the check that the CI_VERSION is the same as TAGGED"
 	S3_VERSION="${CI_VERSION}"	
	S3_BASE_DIR=stable
else
        echo "Commit build for hash: ${CI_COMMIT_HASH_SHORT}"
	S3_VERSION="${CI_BASE_VERSION}-${CI_COMMIT_HASH_SHORT}"
        S3_BASE_DIR=unstable
fi

S3_VERSION_DIR="${S3_BASE_DIR}/${CI_BASE_VERSION}"
S3_RELEASE_DIR="${S3_VERSION_DIR}/${S3_VERSION}"

echo "Release to s3 as: ${S3_RELEASE_DIR}" 

S3_BUILD_DIR=target/s3-release
rm -rf "${S3_BUILD_DIR}" 

S3_BUILD_VERSION_DIR="${S3_BUILD_DIR}/${S3_VERSION_DIR}"
S3_BUILD_RELEASE_DIR="${S3_BUILD_DIR}/${S3_RELEASE_DIR}"

mkdir -p "${S3_BUILD_VERSION_DIR}"
echo "${S3_VERSION}" | tee  "${S3_BUILD_VERSION_DIR}/latest"

mkdir -p "${S3_BUILD_RELEASE_DIR}/lib"
cp target/buildinfo "${S3_BUILD_RELEASE_DIR}/buildinfo"
cp target/variant-spark_*.jar "${S3_BUILD_RELEASE_DIR}/lib"

# deploy boostrap scripts 
mkdir -p "${S3_BUILD_RELEASE_DIR}/bootstrap"
cp cloud/aws-emr/bootstrap/* "${S3_BUILD_RELEASE_DIR}/bootstrap" 

