export _JAVA_OPTIONS=-Xmx70G
hadoop_path='~/ephemeral-hdfs/bin'
hadoop_indir_prefix='Rwanda_In/RawMobilityFiles/'
hadoop_indir_suffix='-Mobility.sor.txt-DailyEveningCOG/part-*'

output_path='./'
output_suffix='_HourlyEveningCOG'

exec_obj_name='DailyCOGMain'

export SPARK_JAVA_OPTS+=" -verbose:gc -Xmx15g -Xms1g -XX:-PrintGCDetails -XX:+PrintGCTimeStamps "

#for month in   0703
for month in    0604
do
#        echo "Trying jar $jar file $month$file_suffix ";

#        spark-submit  --class $exec_obj_name --master yarn-client $jar $month$file_suffix --verbose;
       echo "Executing export hadoop fs -cat ~/ephemeral-hdfs/bin/hadoop fs -cat $hadoop_indir_prefix$month$hadoop_indir_suffix "

       #$hadoop_path/hadoop fs -cat $hadoop_indir_prefix$month$hadoop_indir_suffix>>$output_path$month$output_suffix;
       ~/ephemeral-hdfs/bin/hadoop fs -cat $hadoop_indir_prefix$month$hadoop_indir_suffix>>$output_path$month$output_suffix;
done

