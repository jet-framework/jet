
JOB=3
MASTER=ec2-75-101-217-129.compute-1.amazonaws.com
for version in {0..5}
do
#./warmup.sh
PROG=spark.examples.v$version.TpchQueries
OUTPUT="hdfs://$MASTER:9000/outputs/job$JOB/output_v$version"
INPUTS="hdfs://$MASTER:9000/user/root/input $OUTPUT 1995-01-01 TRUCK SHIP"
TIMEARGS="$version\t%e\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
/usr/bin/time -f $TIMEARGS -o /dev/stdout ./run $PROG mesos://master@localhost:5050 $INPUTS 2> ./spark_${JOB}_$PROG.txt 
done


#du -h outp
