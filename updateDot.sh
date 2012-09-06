#!/bin/bash
set -e
for backend in spark scoobi crunch
do
DIR=$backend/src/main/scala/ch/epfl/distributed/datastruct/
mkdir -p $DIR
cp dsl/src/main/resources/*.scala $DIR
done
#pkill feh || true

for x in $(find | grep .dot$)
do
y=${x/.dot/}
echo "Updating png for $y.dot"
dot -Tpng $y.dot > $y.png
done;
