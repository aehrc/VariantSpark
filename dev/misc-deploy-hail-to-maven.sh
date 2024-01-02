#!/bin/bash
set -e -x 

HAIL_ROOT="$1"

HAIL_JAR="${HAIL_ROOT}/hail/build/libs/hail.jar"
HAIL_JAR_ALL="${HAIL_ROOT}/hail/build/libs/hail-all-spark.jar"

mvn deploy:deploy-file \
  -Durl=https://oss.sonatype.org/content/repositories/snapshots \
  -DrepositoryId=ossrh \
  -Dfile=${HAIL_JAR} \
  -Dpackaging=jar \
  -DgeneratePom=true \
  -DgroupId=au.csiro.aehrc.third.hail-is \
  -DartifactId=hail_2.12_3.1 \
  -Dversion=0.2.74-SNAPSHOT \
  -DgeneratePom.description="Private deployment of hail to maven"


mvn deploy:deploy-file \
  -Durl=https://oss.sonatype.org/content/repositories/snapshots \
  -DrepositoryId=ossrh \
  -Dfile=${HAIL_JAR_ALL} \
  -Dpackaging=jar \
  -DgeneratePom=true \
  -DgroupId=au.csiro.aehrc.third.hail-is \
  -DartifactId=hail_2.12_3.1 \
  -Dversion=0.2.74-SNAPSHOT \
  -Dclassifier=all \
  -DgeneratePom.description="Private deployment of hail to maven"
