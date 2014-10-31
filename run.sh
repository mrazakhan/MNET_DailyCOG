#!/bin/bash
export _JAVA_OPTIONS=-Xmx70G
jar='/export/home/mraza/Rwanda_DailyCOG/target/scala-2.10/dailycogmain_2.10-0.1.jar'
file_suffix='-Mobility.sor.txt'

input_path='Rwanda_In/RawMobilityFiles/'
output_suffix='_DailyCOG'

exec_obj_name='DailyCOGMain'

export SPARK_JAVA_OPTS+=" -verbose:gc -Xmx15g -Xms1g -XX:-PrintGCDetails -XX:+PrintGCTimeStamps "

for month in   0603 
#for month in   0801 0802    
#0807 0808 0809 0810 0811 0812 
do
   	echo "Trying jar $jar file $input_path$month$file_suffix ";
	
	spark-submit  --class $exec_obj_name --master yarn-client $jar $month$file_suffixi $input_path $output_path --verbose;


done
