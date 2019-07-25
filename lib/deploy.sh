#!/bin/bash
mvn org.apache.maven.plugins:maven-deploy-plugin:3.0.0-M1:deploy-file -Durl=https://oss.sonatype.org/content/repositories/snapshots \
                                                                            -DrepositoryId=ossrh \
                                                                            -Dfile=hail_2.11-0.2.16.jar \
                                                                            -DgroupId=au.csiro.aehrc.third.hail-is \
                                                                            -DartifactId=hail_2.11_2.2 \
                                                                            -Dversion=0.2.16-SNAPSHOT \
                                                                            -Dpackaging=jar \
                                                                            -DgeneratePom=true
