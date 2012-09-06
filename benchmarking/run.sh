#!/bin/bash

OUTPUT=output
#HADOOP_HOME=/home/stivo/hadooptemp/hadoop-0.20.2-cdh3u3/
HADOOPHOME=$HADOOP_HOME/bin/
HADOOPHOME=""
HDFS="hdfs://ec2-23-22-79-83.compute-1.amazonaws.com/user/stivo/"
A=""
JOB=11
for x in 4 #{0..4}
do
A="$A$x\n"
#A="$A${x}w\n"
#A="$A${x}k\n"
done
for RUN in 10 #{10..12}
do
for BACKEND in crunch #scoobi #scrunch crunch
do
	for version in $(echo -e $A | sort )
	do	
		JOB=$[JOB+2]
		#if [ "$BACKEND" = "crunch" ]; then
		#	version="${version}k"
		#fi
		DESC=${BACKEND:0:1}_v$version
		TIMEARGS="${BACKEND}_${version}\t%e\t$JOB\t%S\t%U\t%M\t%P"

		#### TPCH ####
		PROG=dcdsl.generated.v$version.TpchQueries
		TPCHDATA="/home/stivo/master/testdata/tpch/small/"
		#TPCHDATA="$HDFS/inputs/tpch/ "
		#PROG=dcdsl.generated.v$version.TpchQueriesMapper
		#OUTPUT="$HDFS/outputs/tpch/run$JOB/output_$DESC/"
		INPUTS="$TPCHDATA $OUTPUT 1995-01-01 TRUCK SHIP"

		#### WIKI ####
		#PROG=dcdsl.generated.v$version.WordCountApp
		#OUTPUT="$HDFS/outputs/wiki/s2/run$RUN/output_$DESC"
		#INPUTS="$HDFS/inputs/wiki/ $OUTPUT"
		#INPUTS="/home/stivo/master/testdata/wiki2009-articles-10k.tsv $OUTPUT"
		#echo "$BACKEND $DESC $OUTPUT $PROG"
		#-XX:MaxInlineSize=1000	
		/usr/bin/time -f $TIMEARGS -o /dev/stdout env HADOOP_HEAPSIZE="4096" hadoop jar progs/${BACKEND}-gen*.jar $PROG $INPUTS 2> ./${PROG}_${DESC}.txt 
	done
done
done
