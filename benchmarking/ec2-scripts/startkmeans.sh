#set -x
#CLASSPATH=:../spark/lib/spark-core-assembly-0.4-SNAPSHOT.jar:progs/spark_2.9.1-0.1-SNAPSHOT.jar
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

JOB=6
MASTER=ec2-75-101-217-129.compute-1.amazonaws.com
for version in KMeansApp SparkKMeans
do
#./warmup.sh
PROG=spark.examples.$version
#PROG=spark.examples.v$version.TpchQueries
#INPUTS="hdfs://$MASTER:9000/user/root/input $OUTPUT 1995-01-01 TRUCK SHIP"
INPUTS="hdfs://$MASTER:9000/user/root/input/kmeansd10_2.txt 50 0.1"

#cat /home/stivo/Downloads/wex/start.tsv.head > /dev/null
TIMEARGS="$version\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
/usr/bin/time -f $TIMEARGS -o /dev/stdout ./run $PROG mesos://master@localhost:5050 $INPUTS 2> ./spark_${JOB}_$PROG.txt 
done


#du -h outp
