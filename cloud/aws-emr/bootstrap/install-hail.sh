#!/bin/bash
set -x -e

INPUT_PATH=""
HAIL_VERSION="0.1"
SPARK_VERSION="2.2.1"
IS_MASTER=false

if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

while [ $# -gt 0 ]; do
    case "$1" in
    --input-path)
      shift
      INPUT_PATH=$1
      ;;
    --hail-version)
      shift
      HAIL_VERSION=$1
      ;;
    --spark-version)
      shift
      SPARK_VERSION=$1
      ;;
    --path-prefix)
      # not used by this script
      shift
      ;;
    -*)
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
    esac
    shift
done

# copy hail to both master and workers
# as ther is not shared dir and the bgz codec is needed on classpath for both

aws s3 cp ${INPUT_PATH}/hail-python.zip ${HOME}
aws s3 cp ${INPUT_PATH}/hail-all-spark.jar ${HOME}
