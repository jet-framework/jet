#!/bin/bash

set -e
#sets up the projects it depends on
#needs git, sbt (0.11.3 launcher) and mvn3
mkdir deps
cd deps

git clone -b master https://github.com/mesos/spark
cd spark
git checkout tags/v0.5.0
sbt/sbt publish-local assembly
cp target/spark-core-assembly-*.jar ../../benchmarking/progs/spark-core-assembly.jar
cd -

#git clone https://github.com/cloudera/crunch
#cd crunch
#mvn3 install -DskipTests=true
#cd -

cd ..
