# For local testing
OUTPUT=./output
#OUTPUT="hdfs:///tmp/job6"
INPUTS="/home/stivo/master/testdata/pagecounts $OUTPUT"
INPUTS="/home/stivo/master/testdata/tpch/small 50000 1997-06-04"
INPUTS="kmeans_data.txt 2 0.001"
INPUTS="/home/stivo/master/testdata/kmeans/k5d10big.dat 5 0.0000001"
INPUTS="/home/stivo/master/testdata/kmlocal-1.7.2/bigkmeans.txt 49 0.0001"
INPUTS="/home/stivo/master/testdata/wiki/freebase-wex-2009-01-12-articles.tsv.head $OUTPUT"
INPUTS="/home/stivo/Downloads/wex/start.tsv.head "

#INPUTS="/home/stivo/master/testdata/currenttmp-5m $OUTPUT"
INPUTS="/home/stivo/master/testdata/tpch/ $OUTPUT 1995-01-01 TRUCK SHIP"
#INPUTS="hdfs:///inputs/tpch/ $OUTPUT 1995-01-01 TRUCK SHIP"
#INPUTS="hdfs:///tmp/wikilog $OUTPUT"
#INPUTS="input/pagecount $OUTPUT"

./compile.sh

#./warmup.sh
#./run.sh $INPUTS
#du -h output/
#rm -rf output

#./warmup.sh
rm -rf $OUTPUT
./runspark.sh $INPUTS
#du -h $OUTPUT
#rm -rf output
#./compare.py
