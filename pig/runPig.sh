#export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64
HDFS="hdfs://ec2-23-22-2-78.compute-1.amazonaws.com/user/stivo/"

rm output -rf

for x in {1..3}
do
JOB=$(python ../benchmarking/jobnumber.py)
export PIG_CLASSPATH=/etc/hadoop/conf.whirr/
export HADOOP_CONF_DIR=/etc/hadoop/conf.whirr/
TIMEARGS="pig\t%e\t$JOB\t%S\t%U\t%M\t%P"
#-XX:MaxInlineSize=1000
PROG=Wordcount2009.pig

TPCHDATA=/home/stivo/master/testdata/tpch/small/
#INPUT=/home/stivo/master/testdata/tpch/smaller/

TPCHDATA="$HDFS/inputs/tpch/s200/"
INPUT=$TPCHDATA
OUTPUT="$HDFS/job$JOB/"

#/usr/bin/time -f $TIMEARGS -o /dev/stdout pig10 -p input="$INPUT" -p shipmodes="TRUCK|SHIP" -p startdate=1995-01-01 -p enddate=1996-01-01 -p output="$OUTPUT" Q12.pig
#/usr/bin/time -f $TIMEARGS -o /dev/stdout pig10 -p input=$INPUT -p shipmodes="TRUCK|SHIP" -p startdate=1995-01-01 -p enddate=1996-01-01 -p output="$OUTPUT" Q12Mapper.pig

#INPUT=/home/stivo/master/testdata/wiki2009-articles-10k.tsv
INPUT="$HDFS/inputs/wiki/"
#OUTPUT="$HDFS/outputs/pig/job$JOB/"
/usr/bin/time -f $TIMEARGS -o timedata pig10 -p input=$INPUT -p output="$OUTPUT" Wordcount2009.pig
cat timedata
cat timedata >> allresults.txt

#/usr/bin/time -f $TIMEARGS -o /dev/stdout java -Xmx4096m -cp udfs.jar:udfs/pig-0.10.0.jar org.apache.pig.Main -p input=$INPUT -p output="output_pig_wc" -x local  Wordcount2009.pig


done
