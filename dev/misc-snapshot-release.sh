#!/bin/bash
set -e -x 

RELEASE_VERSION="${1:?Missing_release_number}"
RELEASE_NUMBER=${RELEASE_VERSION%%-*}
RELEASE_S3_URL="s3://variant-spark/unstable/${RELEASE_NUMBER}/${RELEASE_VERSION}"
SNAPSHOT_S3_URL="s3://variant-spark/snapshots/${RELEASE_VERSION}"

aws s3 cp --recursive --acl public-read  ${RELEASE_S3_URL} ${SNAPSHOT_S3_URL}
