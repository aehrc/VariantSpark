#!/bin/bash

PWD=$(cd `dirname "$0"`; pwd)

mvn clean install


DIST_JAR=$(echo target/variant-spark-*-all.jar)

if [[ "${DIST_JAR}" =~ .*variant-spark-(.*)-all.jar ]]; then
	DIST_VERSION="${BASH_REMATCH[1]}"
else
	echo "Cannot determine the version from the jar:  ${DIST_JAR}"
	exit 1
fi

DIST_NAME="variant-spark-${DIST_VERSION}"

echo "Builing distribution for version: ${DIST_VERSION}"

DIST_DIR="target/dist/${DIST_NAME}"

rm -rf target/dist
mkdir -p ${DIST_DIR}
mkdir -p ${DIST_DIR}/lib

cp LICENSE README.md THIRDPARTY ${DIST_DIR}

cp variant-spark ${DIST_DIR}/variant-spark
cp target/variant-spark-0.0.1-SNAPSHOT-all.jar  ${DIST_DIR}/lib
cp -r data/ ${DIST_DIR}/data/
cp -r scripts ${DIST_DIR}/scripts/
cp -r conf ${DIST_DIR}/conf/

tar -czf target/dist/${DIST_NAME}.tar.gz -C target/dist ${DIST_NAME}

