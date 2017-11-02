#!/bin/bash
set -e -x 

HAIL_JAR="$1"

mvn deploy:deploy-file \
  -Durl=https://oss.sonatype.org/content/repositories/snapshots \
  -DrepositoryId=ossrh \
  -Dfile=${HAIL_JAR} \
  -Dpackaging=jar \
  -DgeneratePom=true \
  -DgroupId=au.csiro.aehrc.third.hail \
  -DartifactId=hail \
  -Dversion=0.1-SNAPSHOT \
  -DgeneratePom.description="Private deployment of hail to maven"

