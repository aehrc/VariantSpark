#!/bin/bash
set -x -e

# AWS EMR bootstrap script 
# Install variant spark

# check for master node
IS_MASTER=false
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

# error message
error_msg ()
{
  echo 1>&2 "Error: $1"
}

# error message
fatal_error_msg ()
{
  echo 1>&2 "Fatal error: $1"
  exit 1
}

VS_BUCKET="variant-spark"
RELEASE_DIR=

# get input parameters
while [ $# -gt 0 ]; do
    case "$1" in
    --release-url)
	    shift
      VS_RELEASE_URL="$1"
      ;;
    -*)
      # do not exit out, just note failure
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
    esac
    shift
done

if [[ -z "${VS_RELEASE_URL}" ]]; then
  fatal_error_msg "Parameter: --release-url is required"
fi

echo "Release location is: ${VS_RELEASE_URL}"

# peform some basic check on the location
VS_BUILD_INFO=$(aws s3 ls "${VS_RELEASE_URL}/buildinfo" || echo "")

if [ -z "${VS_BUILD_INFO}" ]; then
  fatal_error_msg "There is no variant-spark release in: ${VS_RELEASE_URL}. Please check the '--release-url' parameter value"
fi

INST_VOL="${INST_VOL:-/mnt}"
VS_INST_DIR="${INST_VOL}/variant-spark"

echo "Bootstraping variant-spark"

if [ "$IS_MASTER" = true ]; then
  echo "Installing variant-spark in: ${VS_INST_DIR}"
  mkdir -p "${VS_INST_DIR}"
  #download and install variant spark
  cd ${VS_INST_DIR}
  aws s3 cp --recursive "${VS_RELEASE_URL}" .
  VS_UNVERSIONED_JAR="lib/variant-spark_2.11-*-all.jar"
  VS_VERSIONED_JAR="$(echo ${VS_UNVERSIONED_JAR})"
  if [[ "${VS_UNVERSIONED_JAR}" == ${VS_VERSIONED_JAR} ]]; then
    fatal_error_msg "Could not find variant-spark assembly jar. Check if ${VS_RELEASE_URL} is a valid release url in S3"
  fi
  echo "Found variant-spark assembly jar: ${VS_VERSIONED_JAR}"
  #create symbolic link for libaries
  ln -s "${VS_VERSIONED_JAR}" variant-spark_2.11-all.jar
  echo "Installed variant-spark in: ${VS_INST_DIR}"
fi

echo "Finished bootstraping variant-spark"
