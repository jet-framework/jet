#set -x
CLASSPATH=:../spark/lib/spark-core-assembly-0.4-SNAPSHOT.jar:progs/spark_2.9.1-0.1-SNAPSHOT.jar
PROG=spark.examples.PageCountApp
PROG=spark.examples.TpchQueries
PROG=spark.examples.WordCountApp
#PROG=spark.examples.KMeansApp
#PROG=spark.examples.SparkKMeans
#PROG=generated.Benchmark
#PROG=spark.examples.v0.BenchmarkGenerated
#PROG_BAK=generated.Benchmark
JAVA=/usr/lib/jvm/java-1.6.0-openjdk/bin/java
HEAP=4g
CORES=1

#TIME=/usr/bin/time -f $TIMEARGS


#set -x

for version in {0..6}
do
#./warmup.sh
PROG=spark.examples.v$version.WordCountApp
#PROG=spark.examples.v$version.TpchQueries
OUTPUT=output_v$version
#INPUTS="/home/stivo/master/testdata/tpch/mb200/ $OUTPUT 1995-01-01 TRUCK SHIP"
INPUTS="/home/stivo/Downloads/wex/start.tsv.head $OUTPUT"
cat /home/stivo/Downloads/wex/start.tsv.head > /dev/null
TIMEARGS="$version\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
/usr/bin/time -f $TIMEARGS -o /dev/stdout env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $INPUTS 2> ./spark_$PROG.txt 
done

#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG mesos://master@localhost:5050 $@ 2> ./spark_$PROG.txt
#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt 
#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG_BAK local[$CORES] $@ 2> ./spark_$PROG_BAK.txt 
#PROG=spark.examples.SparkKMeans
#time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG_Orig.txt > ./spark_$PROG_Orig.txt


#du -h output/


set +x

A="""

rm -rf output/
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/

rm -rf output/
PROG=${PROG}_Orig
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/

PROG=$PROG_BAK
CORES=2
rm -rf output/
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/

rm -rf output/
PROG=${PROG}_Orig
./warmup.sh
time env JAVA_OPTS="-Xmx$HEAP" scala -cp $CLASSPATH $PROG local[$CORES] $@ 2> ./spark_$PROG.txt
du -h output/
"""
