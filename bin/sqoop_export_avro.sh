#!/bin/bash
##############################################
## Sqoop into Oracle
##
## Usage: sqoop_export_avro.sh -entity schema_name export_dir
##
## Where OPTIONS are:
##   
##
## Description: Load group's equivalent for Hadoop
## 
##############################################

export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_MAJOR_VERSION=2
export LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib:/lib:/usr/lib
usr=`whoami`
my_dir="$(dirname "$0")"

fp=$(readlink -f $0)
bp=$(dirname ${fp})
#DATE2=`date +"%F %T"`  #### for splunk log
DATE2=`date +"%FT%T"`
while getopts "e:" OPTION; do
    case ${OPTION} in
        e) entity_std=${OPTARG} ;;
        *) error "Unknown option \"${OPTION}\"";;
    esac
done
shift $((OPTIND-1))


if [[ $entity_std ]]; then
        entity_upp=$(echo "${entity_std}" | tr '[a-z]' '[A-Z]')
fi


schema=${1:?"Must specify an Oracle Schema"}
groupid=${2:?"Must specify a groupid"}
client_ds_id=${3:?"Must specify a client_ds_id"}
export_dir=${4:?"Must specify an export folder"}
file=$5

if [[ $entity_upp ]]; then
	file=$entity_upp
fi

if [[ -z $file ]]; then
	echo "Failure : No entity to load"
	exit
fi

schema=$(echo "${schema}" | tr '[a-z]' '[A-Z]')

if [ $usr = mercury ]; then
pwd_mercury=`cat /opt/mercury/etc/database/hadoop_mercury.properties|grep password|awk -F'=' '{print $2}'`
pwd_fdr=`cat /opt/mercury/etc/database/hadoop_fdr.properties|grep password|awk -F'=' '{print $2}'`
cdr_table=`hadoop fs -cat /optum/mercury/data/croswalk/table_cross_walk.txt|grep -w $file |awk -F'=' '{print$2}'`
elif [ $usr = mercury-stage ]; then
pwd_mercury=`cat /opt/mercury-stage/etc/database/hadoop_mercury.properties|grep password|awk -F'=' '{print $2}'`
pwd_fdr=`cat /opt/mercury-stage/etc/database/hadoop_fdr.properties|grep password|awk -F'=' '{print $2}'`
cdr_table=`hadoop fs -cat /optum/mercury-stage/data/croswalk/table_cross_walk.txt|grep -w $file |awk -F'=' '{print$2}'`
else 
pwd_mercury=`cat ${bp}/pw/hadoop_mercury`
pwd_fdr=`cat ${bp}/pw/hadoop_fdr`
cdr_table=`hadoop fs -cat /optum/mercury-stage/data/croswalk/table_cross_walk.txt|grep -w $file |awk -F'=' '{print$2}'`
fi



temp_table=`sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn  @${bp}/temp_table_name.sql ${schema} ${groupid} ${cdr_table} ${client_ds_id}`



table=$schema.$temp_table


dt1=`date +"%FT%T"`
echo "eventName:Start-sqoop_export_avro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};file:${file};table:${table};"


sqoop export -Dmapreduce.job.user.classpath.first=true -Djava.security.egd=file:/dev/../dev/urandom \
 -Dmapreduce.input.fileinputformat.split.minsize=2684354560 \
 -Doraoop.jdbc.url.verbatim=true \
 -Doraoop.oracle.rac.service.name=AUTOMTN \
 -Doracle.sessionTimeZone=GMT \
 -Doraoop.timestamp.string=true \
 --direct \
 --connect jdbc:oracle:thin:@som-racload03.humedica.net:1521/AUTOMTN \
 --username hadoop_mercury \
 --password ${pwd_mercury} \
 --table $table \
 --export-dir $export_dir/$file \
 --verbose \
 --num-mappers 8

dt1=`date +"%FT%T"`
echo "eventName:End-sqoop_export_avro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};file:${file};table:${table};"
