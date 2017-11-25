#!/bin/bash
set -x -e

# AWS EMR bootstrap script 
# Install hail

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
      HAIL_RELEASE_URL="$1"
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

if [[ -z "${HAIL_RELEASE_URL}" ]]; then
  fatal_error_msg "Parameter: --release-url is required"
fi

echo "Hail release location is: ${HAIL_RELEASE_URL}"

INST_VOL="${INST_VOL:-/mnt}"
HAIL_INST_DIR="${INST_VOL}/hail"

echo "Bootstraping hail"

echo "Installing hail in: ${HAIL_INST_DIR}"
mkdir -p "${HAIL_INST_DIR}"
#download and install variant spark
cd ${HAIL_INST_DIR}
aws s3 cp --recursive "${HAIL_RELEASE_URL}/" .
echo "Installed variant-spark in: ${HAIL_INST_DIR}"
echo "Finished bootstraping hail"
