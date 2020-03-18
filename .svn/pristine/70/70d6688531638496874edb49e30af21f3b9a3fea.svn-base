#!/bin/bash
##############################################
## ParquetToAvro
##
## Usage: ParquetToAvro -e entity -o build_source ParquetLocation AvroLocation
##
## Where OPTIONS are:
##   -e                     Entity
##   -o			    Build Source	
##
## Description: Converting Parquet to Avro and Loading the entity into the Oracle table
## 
##############################################

#DATE=`date +%Y%m%d`
DATE=`date '+%Y%m%d%H%M%S'`
DATE2=`date +"%FT%T"`
export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_MAJOR_VERSION=2
export LD_LIBRARY_PATH=/usr/lib/oracle/11.2/client64/lib:/lib:/usr/lib

my_dir="$(dirname "$0")"
usr=`whoami`
fp=$(readlink -f $0)
bp=$(dirname ${fp})

while getopts "e:o:" OPTION; do
    case ${OPTION} in
        e) entity_std=${OPTARG} ;;
        o) build_source=${OPTARG} ;;
        *) error "Unknown option \"${OPTION}\"";;
    esac
done
shift $((OPTIND-1))

groupid=${1:?"Must specify a groupid"}
client_ds_id=${2:?"Must specify a client_ds_id"}
cdr_location=${3:?"Must specify a CDR directory"}
cdr_location_avro=${4:-"${cdr_location}/AVRO"}
LIB=${5:-/users/mercury/lib}
jar=${6:-mercury-etl-all-1.0-SNAPSHOT.jar}
config=${7:?"Must specify a config"}
JARS=$(echo "${LIB}"/*.jar | tr ' ' ',')

source ${bp}/${config}

if [ $usr = mercury ]; then
pwd_mercury=`cat /opt/mercury/etc/database/hadoop_mercury.properties|grep password|awk -F'=' '{print $2}'`
pwd_fdr=`cat /opt/mercury/etc/database/hadoop_fdr.properties|grep password|awk -F'=' '{print $2}'`
elif [ $usr = mercury-stage ]; then
pwd_mercury=`cat /opt/mercury-stage/etc/database/hadoop_mercury.properties|grep password|awk -F'=' '{print $2}'`
pwd_fdr=`cat /opt/mercury-stage/etc/database/hadoop_fdr.properties|grep password|awk -F'=' '{print $2}'`
else 
pwd_mercury=`cat ${bp}/pw/hadoop_mercury`
pwd_fdr=`cat ${bp}/pw/hadoop_fdr`
fi

sqoop_err_log=${log_dir}/${groupid}_${client_ds_id}_${build_source}_${DATE}_sqoop_error.log

if [[ $entity_std ]]; then
        entity_upp=$(echo "${entity_std}" | tr '[a-z]' '[A-Z]')
	entity_path="${cdr_location}/${entity_upp}"
	entity_outpath="${cdr_location_avro}/${entity_upp}"
	if $(hadoop fs -test -e "${entity_path}"); then
		if $(hadoop fs -test -e "${entity_outpath}"); then
                	hadoop fs -rm -r "${entity_outpath}";
        	fi
dt1=`date +"%FT%T"`
echo "eventName:Start-parquetToavro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};build_source:${build_source};"

	spark-submit --master yarn --jars "${LIB}"/spark-avro_2.11-4.0.0.jar --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --class com.humedica.mercury.etl.core.apps.ParquetToAvro "${LIB}"/${jar} $entity_path $entity_outpath
	rc=$?
	if [ $rc -eq 0 ] && [ $build_source ]; then
	#	if [ -e ${sqoop_err_log} ]; then
	#		rm -f ${sqoop_err_log}
	#	fi
		sqoop_log=${log_dir}/${groupid}_${client_ds_id}_${build_source}_${entity_upp}_${DATE}_sqoop.log
		${bp}/sqoop_export_avro.sh -e $entity_std $build_source $groupid $client_ds_id $cdr_location_avro  > ${sqoop_log}  2>&1
	
		err_cnt=`grep -i error ${sqoop_log} | grep -i "Export job failed" | wc -l`
                if [[ $err_cnt -gt 0 ]] ; then
                        echo "${entity_std} did not get sqooped - for more details, check the sqoop log ${sqoop_log}" >> ${sqoop_err_log}  2>&1
                fi

		if [ -f ${sqoop_err_log} ]; then
		{
	 	  sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn @${bp}/end_load.sql $build_source $groupid $client_ds_id FAILED
		  mailbody=`cat ${sqoop_err_log}`
		  echo -e "Failure. Sqoop did not work for ${entity_std} \n\n\n ${mailbody}" | mail -s "Sqoop CDR failure for Groupid - $groupid, client_ds_id - $client_ds_id, cdr schema ${build_source} for $entity" $mailto
		  dt1=`date +"%FT%T"`
		  echo "eventName:Failed-parquetToavro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};build_source:${build_source};"
		}
		else
		{
		  sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn @${bp}/end_load.sql $build_source $groupid $client_ds_id SUCCESS
		  dt1=`date +"%FT%T"`
		  echo "eventName:Success-parquetToavro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};build_source:${build_source};"
		  echo "CDR Built  for Groupid - $groupid and client_ds_id - $client_ds_id for $entity_std" | mail -s "Success. CDR Built for Groupid - $groupid, client_ds_id - $client_ds_id, cdr schema ${build_source} for $entity_std" $mailto
		}
		fi
	fi
	else
	echo "parquet to avro conversion did not run"
	fi
else
	if $(hadoop fs -test -e "${cdr_location_avro}"); then
                        hadoop fs -rm -r "${cdr_location_avro}";
        fi
	dt1=`date +"%FT%T"`
	echo "eventName:Started-parquetToavro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};build_source:${build_source};"

	for file in $(hadoop fs -ls "${cdr_location}" | awk  '{print $8}' | awk -F "/" '{print $NF}')
	do
	spark-submit --master yarn --jars "${LIB}"/spark-avro_2.11-4.0.0.jar --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --class com.humedica.mercury.etl.core.apps.ParquetToAvro "${LIB}"/${jar} $cdr_location/$file $cdr_location_avro/$file
	rc=$?
	if [ $rc -eq 0 ] && [ $build_source ]; then
	#	if [ -e ${sqoop_err_log} ]; then
	#		rm -f ${sqoop_err_log}
	#	fi
		sqoop_log=${log_dir}/${groupid}_${client_ds_id}_${build_source}_${file}_${DATE}_sqoop.log
		${bp}/sqoop_export_avro.sh $build_source $groupid $client_ds_id $cdr_location_avro $file  > ${sqoop_log}  2>&1
		err_cnt=`grep -i error ${sqoop_log} | grep -i "Export job failed" | wc -l`
		if [[ $err_cnt -gt 0 ]] ; then
			echo "${file} did not get sqooped - for more details, check the sqoop log ${sqoop_log}" >> ${sqoop_err_log}  2>&1
		fi
	fi
	done
	if [ -f ${sqoop_err_log} ]; then 
		sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn @${bp}/end_load.sql $build_source $groupid $client_ds_id FAILED
		mailbody=`cat ${sqoop_err_log}`
		echo -e "Failure. Sqoop did not work \n\n\n ${mailbody}" | mail -s "Sqoop CDR failure for Groupid - $groupid, client_ds_id - $client_ds_id, cdr schema ${build_source}" $mailto
		dt1=`date +"%FT%T"`
		echo "eventName:Failed-parquetToavro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};build_source:${build_source};"

	else
		sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn @${bp}/end_load.sql $build_source $groupid $client_ds_id SUCCESS
		echo "CDR Built  for Groupid - $groupid and client_ds_id - $client_ds_id" | mail -s "Success. CDR Built for Groupid - $groupid, client_ds_id - $client_ds_id, cdr schema ${build_source}" $mailto
		dt1=`date +"%FT%T"`
		echo "eventName:Success-parquetToavro.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};build_source:${build_source};"
	
	fi
fi

