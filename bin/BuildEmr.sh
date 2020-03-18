#!/bin/bash
##############################################
## BuildEmr
##
## Usage: BuildEmr 
##
## Where OPTIONS are:
##   -e                    -- Entity
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



while getopts "c:e:o:b:f:" OPTION; do
    case ${OPTION} in
	c) collection_set=${OPTARG} ;;
        e) entity_std=${OPTARG} ;;
	o) build_source=${OPTARG} ;;
	b) delta_start_date=${OPTARG} ;;
	f) delta_end_date=${OPTARG} ;;
        *) error "Unknown option \"${OPTION}\"";;
    esac
done
shift $((OPTIND-1))

DATE=`date '+%Y%m%d%H%M%S'`

if [ -z $collection_set ]; then
	emr=${1:?"Must specify an EMR"}
	emr=$(echo "${emr}" | tr '[A-Z]' '[a-z]')
	build_source1=${build_source:-TEST}
	groupid=${2:?"Must specify an H number"}
	client_ds_id=${3:?"Must specify a client_ds_id"}
	client_ds_name=${4:?"Must specify a client_ds_name"}
	stage_schema=${5:?"Must specify a Stage Schema"}
	run_mode=${6:?"Must specify a Run Mode"}
	run_mode=$(echo "${run_mode}" | tr '[A-Z]' '[a-z]')
	config=${7:?"Must specify a config name"}
	source ${bp}/$config
	collection=${collection_set:-${emr}}
	LIB=${lib_dir:-/opt/mercury/lib}
	jar=${jar:-mercury-etl-all-1.0-SNAPSHOT.jar}
	cdr_dir=$cdr_dir/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
	cdr_dir_avro=$cdr_dir_avro/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
	cache_dir=$cache_dir/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
elif [ $collection_set ]; then
	case "$collection_set" in *@*)
        	emr=${1:?"Must specify an EMR"}
        	emr=$(echo "${emr}" | tr '[A-Z]' '[a-z]')
		build_source1=${build_source:-TEST}
        	groupid=${2:?"Must specify an H number"}
        	client_ds_id=${3:?"Must specify a client_ds_id"}
        	client_ds_name=${4:?"Must specify a client_ds_name"}
        	stage_schema=${5:?"Must specify a Stage Schema"}
		run_mode=${6:?"Must specify a Run Mode"}
		run_mode=$(echo "${run_mode}" | tr '[A-Z]' '[a-z]')
        	config=${7:?"Must specify a config name"}
		source ${bp}/$config
		collection=$(echo "${collection_set}" | tr '[A-Z]' '[a-z]')
		LIB=${lib_dir:-/opt/mercury/lib}
		jar=${jar:-mercury-etl-all-1.0-SNAPSHOT.jar}
 		cdr_dir=$cdr_dir/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
        	cdr_dir_avro=$cdr_dir_avro/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
        	cache_dir=$cache_dir/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
	;;
	*)
 		emr=$(echo $collection_set | cut -f1 -d'@')
        	emr=$(echo "${emr}" | tr '[A-Z]' '[a-z]')
        	build_source1=${build_source:-TEST}
        	groupid=${1:?"Must specify an H number"}
        	client_ds_id=${2:?"Must specify a client_ds_id"}
        	client_ds_name=${3:?"Must specify a client_ds_name"}
        	stage_schema=${4:?"Must specify a Stage Schema"}
		run_mode=${5:?"Must specify a Run Mode"}
		run_mode=$(echo "${run_mode}" | tr '[A-Z]' '[a-z]')
        	config=${6:?"Must specify a config name"}
        	source ${bp}/$config
        	collection=${emr}
        	LIB=${lib_dir:-/opt/mercury/lib}
        	jar=${jar:-mercury-etl-all-1.0-SNAPSHOT.jar}
	        cdr_dir=$cdr_dir/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
                cdr_dir_avro=$cdr_dir_avro/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
                cache_dir=$cache_dir/$groupid/$client_ds_id/$client_ds_name/$build_source1/$DATE
	;;
	esac
fi



if [ -z $entity_std ]; then
	entity=''
	if ( hadoop fs -test -e "${cdr_dir}" ); then
	hadoop fs -rm -r ${cdr_dir};
	fi
else
	entity="--entity ${entity_std}"
	entity_upp=$(echo "${entity_std}" | tr '[a-z]' '[A-Z]')
	entity_path="${cdr_dir}/${entity_upp}"
	if $(hadoop fs -test -e "${entity_path}"); then
		hadoop fs -rm -r "${entity_path}";
	fi
fi

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

log_file=${log_dir}/${groupid}_${client_ds_id}_${build_source1}_${DATE}_buildemr.log

if [[ $build_source ]] && [[ -z $entity_std ]]; then
sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn @${bp}/start_load.sql $build_source $groupid $client_ds_id
rc=$?
if [ $rc -ne 0 ]; then
	echo "There are 0 PENDING jobs for $client_ds_id, expected 1" | mail -s "No Hadoop jobs to run for $client_ds_id for $groupid" $mailto
	exit $rc
fi
fi

date

if [ $groupid = H557454 ] || [ $groupid = H641171 ] || [ $groupid = H406239 ]; then
	maxExecutors=800
else
	maxExecutors=100
fi

if [ $run_mode = delta ] || [ $run_mode = fdr ]; then
        ds_name=$(echo "${client_ds_name}" | cut -f2- -d"_")
	case "$stage_schema" in *CUST*)
	for i in $(echo $stage_schema | sed "s/,/ /g")
	do
		case "$i" in *CUST*)
			stage_schema1=${i}
		;;
		*)
		output_location=${delta_cache}/DELTA/TRANSIENT/${DATE}
        	hadoop fs -mkdir -p ${delta_cache}/DELTA/TRANSIENT/${DATE}
        	spark-submit --master yarn --deploy-mode cluster --driver-memory 2g --deploy-mode=cluster \
        	--class com.humedica.mercury.etl.core.apps.HdfsCopy --conf "spark.driver.extraJavaOptions=-Ddcp.startDate=${delta_start_date} -Ddcp.endDate=${delta_end_date} -Ddcp.groupId=${groupid} -Ddcp.dsName=${ds_name} -Ddcp.input.location=${i} -Ddcp.output.location=${output_location}" ${LIB}/${jar} >> ${log_file} 2>&1
        	schema=$(echo ${i} | awk -F'/' '{print $NF}')
		stage_schema=${output_location}/${schema}
		;;
		esac
	done
	stage_schema=${stage_schema},${stage_schema1}
	echo $stage_schema
	;;
	*)
	output_location=${delta_cache}/DELTA/TRANSIENT/${DATE}
        hadoop fs -mkdir -p ${delta_cache}/DELTA/TRANSIENT/${DATE}
        spark-submit --master yarn --deploy-mode cluster --driver-memory 2g --deploy-mode=cluster \
        --class com.humedica.mercury.etl.core.apps.HdfsCopy --conf "spark.driver.extraJavaOptions=-Ddcp.startDate=${delta_start_date} -Ddcp.endDate=${delta_end_date} -Ddcp.groupId=${groupid} -Ddcp.dsName=${ds_name} -Ddcp.input.location=${stage_schema} -Ddcp.output.location=${output_location}" ${LIB}/${jar} >> ${log_file} 2>&1
        schema=$(echo ${stage_schema} | awk -F'/' '{print $NF}')
	stage_schema=${output_location}/${schema}
	esac
	rc=$?
	if [ $rc = 0 ]; then
        	echo "Filter Sucessful"
	else
        	echo "Failure: Delta Filter failed" | mail -s "Failure: Delta Filter failed" $mailto
	fi
fi

dt1=`date +"%FT%T"`
echo "eventName:Started-BuildEmr.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};client_ds_name:${client_ds_name};CDR Schema:${build_source1};emr:${emr};run_mode:${run_mode};user:${usr};maxExecutors=${maxExecutors};" >> ${log_file}


## As a workaround until we implement a Hive abstraction layer, we need to pause the loading of all streams into hadoop history before running this job
# determine all data streams for this client_ds_id from fasttrack (active_flag: 0 is paused, 1 active, 2 is deactivated)
data_stream_ids=$(sqlplus -L -S 2>&1 <<ENDSQL
hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn
set serveroutput on size unlimited linesize 32767 wrap off pagesize 0 heading off
set feedback off flush on trimout on sqlblanklines on time off timing off tab off
set colsep '|'
ALTER SESSION SET recyclebin = OFF;
WHENEVER SQLERROR EXIT
select st.data_stream_id
from metadata.vw_client_source_streams_id v
inner join fasttrack.staging_streams st on (v.client_data_stream_id = st.data_stream_id)
where client_data_src_id = ${client_ds_id}
and st.active_flag = 1;
quit
ENDSQL
)
rc=$?
echo "data_stream_ids: ${data_stream_ids}" >> ${log_file}
if [[ $rc != 0 ]]; then
    mail_body="Data stream lookup failed for Client DS ID: ${client_ds_id}"
else
    # pause loading for all streams
    pause_mins=$(echo "( 24 * 60 )" | bc) # pause in minutes, max is 24 hrs
    backlog_mins=180 #  optional time to wait for backlogged files to finish
    pause_dir=/opt/data_factory/prod/ingest/ingest-latest/bin
    pause_script=${pause_dir}/PauseUnpauseStream.sh
    cd ${pause_dir}
    for stream_id in ${data_stream_ids}; do
        echo "${pause_script} $stream_id \"pause\" $pause_mins $backlog_mins"
        ${pause_script} $stream_id "pause" $pause_mins $backlog_mins prod prod  >> ${log_file}
        rc=$?
        if [[ $rc != 0 ]]; then
            mail_body="Failed to pause data_stream_id: $stream_id  by running: \
                       ${pause_script} $stream_id \"pause\" $pause_mins $backlog_mins"
            break
        fi
    done
    cd $OLDPWD
fi

if [[ $rc == 0 ]]; then
    # execute the mercury etl package
    spark-submit --class com.humedica.mercury.etl.core.apps.BuildEMR --master yarn --executor-memory 14G --driver-memory 14G --deploy-mode cluster --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.executor.memoryOverhead=2048 --conf spark.shuffle.service.enabled=true --conf spark.network.timeout=1200 --conf spark.sql.broadcastTimeout=1200 --conf spark.sql.shuffle.partitions=400 --conf spark.default.parallelism=600 --conf spark.sql.parquet.mergeSchema=true ${LIB}/${jar} \
    --collection $collection ${entity} --EMR $emr --GROUP $groupid --CLIENT_DS_ID $client_ds_id --CLIENT_DS_NAME $client_ds_name --EMR_DATA_ROOT $stage_schema --OUT_DATA_ROOT $cdr_dir --CDR_DATA_ROOT $mapping_dir  --PACKAGE_ROOT com.humedica.mercury.etl \
    --cache ${cache_dir}/${groupid}/${client_ds_name}  --resetcache \
    --keytab /etc/security/keytabs/${usr}.keytab --principal ${usr} \
    >> ${log_file} 2>&1
    rc=$?
    sleep 10
    appid=`grep -i submitting ${log_file} | grep application_ | head -1 | awk '{print $7}'`
    mail_body=`echo -e "\n\n \t Application Id: ${appid} \n \t Groupid: ${groupid} \n \t Client DS ID: ${client_ds_id} \n \t CDR Schema: ${build_source1} \n \t Logfile: ${log_file} \n\n Please check the logfile ${log_file} for more details."`
fi
mail_sub=`echo "Groupid: ${groupid} Client DS ID: ${client_ds_id} in ${build_source1} Application Id: ${appid}"`

if [ $rc != 0 ]; then
	if [[ $build_source ]] && [[ -z $entity_std ]]; then
		sqlplus -s hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn @${bp}/end_load.sql $build_source $groupid $client_ds_id FAILED
	fi

	err_msg1=`yarn logs -applicationId ${appid} | grep -i "Error " | grep "RECEIVED SIGNAL TERM" | head -1`
	err_msg2=`yarn logs -applicationId ${appid} | grep -i "Error " | grep -v "RECEIVED SIGNAL TERM"`

	dt1=`date +"%FT%T"`
        echo "eventName:Failed-BuildEmr.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};client_ds_name:${client_ds_name};CDR Schema:${build_source1};emr:${emr};run_mode:${run_mode};user:${usr};maxExecutors=${maxExecutors};" >> ${log_file}


	echo -e "Hadoop build failed for: ${mail_body} \n\n\n -------------- Potential errors listed below. Please check the application logs for further details. -------------- \n\n ${err_msg1} \n\n\n ${err_msg2}" | mail -s "FAILED: Hadoop build for ${mail_sub}" ${mailto}

fi
date

if [[ $rc == 0 ]]; then
        # remove the pause for these streams.
        cd ${pause_dir}
        for stream_id in ${data_stream_ids}; do
            ${pause_script} $stream_id "unpause" $pause_mins $backlog_mins prod prod >> ${log_file}
            rc=$?
            if [[ $rc != 0 ]]; then
                mail_body="Failed to unpause data_stream_id: $stream_id  by running: \
                           ${pause_script} $stream_id \"unpause\" $pause_mins $backlog_mins"
                break
            fi
        done
        cd $OLDPWD

        if [ $build_source ]; then
                if [[ $entity_std ]]; then
                        ${bp}/parquetToavro.sh -e $entity_std -o $build_source $groupid $client_ds_id $cdr_dir $cdr_dir_avro $LIB $jar $config > ${log_dir}/${groupid}_${client_ds_id}_${build_source}_${entity_std}_${DATE}_parquet_to_avro.log 2>&1
                        rc=$?
                        exit $rc
                else
                        ${bp}/parquetToavro.sh -o $build_source $groupid $client_ds_id $cdr_dir $cdr_dir_avro $LIB $jar $config > ${log_dir}/${groupid}_${client_ds_id}_${build_source}_all_${DATE}_parquet_to_avro.log 2>&1
                        rc=$?
                        exit $rc
                fi
        fi
	if [ "$collection_set" = 'fdrhybrid' ]; then

		dt1=`date +"%FT%T"`
		fdr_log_file=${log_dir}/${groupid}_FDR_${DATE}.log
		echo "eventName:Started-BuildEmr.sh-FDR;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};client_ds_name:${client_ds_name};CDR Schema:${build_source1};emr:${emr};run_mode:${run_mode};user:${usr};maxExecutors=${maxExecutors};" >> ${fdr_log_file}
		spark-submit   --master yarn --deploy-mode cluster  --conf spark.dynamicAllocation.enabled=true   --conf spark.shuffle.service.enabled=true   --conf spark.dynamicAllocation.maxExecutors=16   --conf spark.dynamicAllocation.minExecutors=0   --conf spark.dynamicAllocation.initialExecutors=0   --conf spark.executor.memoryOverhead=2000   --driver-memory 14g --executor-cores 4 --executor-memory 14g   --class com.humedica.mercury.etl.core.apps.ParquetJdbcExport    --driver-java-options "-Djdbc.ids=pqe -Dpqe.user=hadoop_fdr[FDR_V1_H704847] -Dpqe.password=${pwd_fdr} -Dpqe.host=som-racload03.humedica.net -Dpqe.port=1521 -Dpqe.service=AUTOMTN -Dpqe.inputDir=${cdr_dir} -Dpqe.tableNameSuffix=_TMP" ${LIB}/${jar} --keytab /etc/security/keytabs/${usr}.keytab --principal ${usr} >> ${fdr_log_file} 2>&1

		dt=date +"%m/%d/%Y"
                fdr_appid=`grep -i submitting ${fdr_log_file} | grep application_ | head -1 | awk '{print $7}'`
                fdr_mail_sub=`echo "Groupid: ${groupid} Date:${dt} Application Id: ${fdr_appid}"`
                fdr_mail_body=`echo -e "\n\n \t Groupid: ${groupid} \n \t Application Id: ${fdr_appid} \n \t Logfile: ${fdr_log_file} \n\n Please check the logfile ${fdr_log_file} for more details."`

		rc=$?
		if [ $rc = 0 ];then

			dt1=`date +"%FT%T"`
                	echo "eventName:Success-BuildEmr.sh-FDR;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};client_ds_name:${client_ds_name};CDR Schema:${build_source1};emr:${emr};run_mode:${run_mode};user:${usr};maxExecutors=${maxExecutors};" >> ${fdr_log_file}

			echo "Success: FDR ran successfully for: ${fdr_mail_body}" | mail -s "FDR Success for: ${fdr_mail_sub}" $mailto
		else
			dt1=`date +"%FT%T"`
                        echo "eventName:Failed-BuildEmr.sh-FDR;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};client_ds_name:${client_ds_name};CDR Schema:${build_source1};emr:${emr};run_mode:${run_mode};user:${usr};maxExecutors=${maxExecutors};" >> ${fdr_log_file}

			echo "Failure: FDR failed for: ${fdr_mail_body}" | mail -s "Failure: FDR failed for: ${fdr_mail_sub}" $mailto
		fi
	else
		exit
	fi
else
	exit
fi
dt1=`date +"%FT%T"`
echo "eventName:End-BuildEmr.sh;eventTime:${dt1};client:${groupid};client_ds_id:${client_ds_id};client_ds_name:${client_ds_name};CDR Schema:${build_source1};emr:${emr};run_mode:${run_mode};user:${usr};maxExecutors=${maxExecutors};" >> ${log_file}

