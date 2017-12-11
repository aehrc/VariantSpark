#!/bin/bash

PWD=$(cd `dirname "$0"`; pwd)

if [[ $# -gt 0 ]]; then
   ${PWD}/dev/build.sh 
fi

DIST_JAR=$(echo target/variant-spark_*-all.jar)

if [[ "${DIST_JAR}" =~ .*variant-spark_(.*)-all.jar ]]; then
	DIST_VERSION="${BASH_REMATCH[1]}"
else
	echo "Cannot determine the version from the jar:  ${DIST_JAR}"
	exit 1
fi

DIST_NAME="variant-spark_${DIST_VERSION}"

echo "Building distribution for version: ${DIST_VERSION}"
echo "Dist jar: ${DIST_JAR}"

DIST_DIR="target/dist/${DIST_NAME}"

rm -rf target/dist
mkdir -p ${DIST_DIR}
mkdir -p ${DIST_DIR}/lib
mkdir -p ${DIST_DIR}/python


cp LICENSE README.md THIRDPARTY ${DIST_DIR}

cp variant-spark ${DIST_DIR}/variant-spark
cp ${DIST_JAR}  ${DIST_DIR}/lib
cp -r data/ ${DIST_DIR}/data/
cp -r scripts ${DIST_DIR}/scripts/
cp -r conf ${DIST_DIR}/conf/
cp python/dist/variants-0.1.0-py2.7.egg ${DIST_DIR}/python/


tar -czf target/dist/${DIST_NAME}.tar.gz -C target/dist ${DIST_NAME}

