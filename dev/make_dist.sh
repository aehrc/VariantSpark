#!/bin/bash
set -e

PWD=$(cd "`dirname $0`"/..; pwd)

CLEAN=NO
#parse command line
while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--clean)
            CLEAN=YES
            shift # past argument
            ;;
            *)    # unknown option
            shift # past argument
            ;;
        esac
done

if [ "$CLEAN" == "YES" ]; then
	echo "Running a clean build"
	${PWD}/dev/build.sh 
fi

# Build python source distribution
${PWD}/dev/py-zip.sh

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


cd "${PWD}"

DIST_DIR="target/dist/${DIST_NAME}"

rm -rf target/dist
mkdir -p ${DIST_DIR}
mkdir -p ${DIST_DIR}/lib
mkdir -p ${DIST_DIR}/python


cp LICENSE README.md THIRDPARTY ${DIST_DIR}

cp -r bin ${DIST_DIR}/bin/
cp ${DIST_JAR}  ${DIST_DIR}/lib
cp -r data/ ${DIST_DIR}/data/
cp -r examples ${DIST_DIR}/examples/
cp -r conf ${DIST_DIR}/conf/
cp python/dist/varspark-src.zip ${DIST_DIR}/python/


tar -czf target/dist/${DIST_NAME}.tar.gz -C target/dist ${DIST_NAME}

