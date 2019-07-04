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
    --notebookPath)
      shift
      NotebookPath=$1
      ;;
    --path-prefix)
      # Passed in by default, but not used here
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


if [ "$IS_MASTER" = true ]; then
    #Install miniconda
    wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh 
    sh Miniconda2-latest-Linux-x86_64.sh -b
    export PATH=~/miniconda2/bin:$PATH
    conda create -y -n jupyter python=2.7
    source activate jupyter
    #Install other packages
    #TODO: make these configurable
    pip install --upgrade matplotlib pandas click variant-spark
    ln -s "/home/hadoop/miniconda2/envs/jupyter/lib/python2.7/site-packages/varspark/jars/variant-spark"*.jar "/home/hadoop/miniconda2/envs/jupyter/lib/python2.7/site-packages/varspark/jars/varspark.jar"
    
fi
