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

my_dir="$(dirname "$0")"

usr=`whoami`
fp=$(readlink -f $0)
bp=$(dirname ${fp})

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


hadoop_jobs=$(sqlplus -L -S 2>&1 <<ENDSQL
hadoop_mercury/${pwd_mercury}@som-racload03:1521/automtn
set serveroutput on size unlimited linesize 32767 wrap off pagesize 0 heading off
set feedback off flush on trimout on sqlblanklines on time off timing off tab off
set colsep '|'
ALTER SESSION SET recyclebin = OFF;
select target_schema, groupid, client_ds_id, client_ds_name, emr_pkg_name, ext_stage_location, to_char(delta_start_date - 1, 'yyyymmdd'), to_char(delta_end_date + 1, 'yyyymmdd'), run_mode
from   cdr_common.external_data_job
where  platform = 'HADOOP'
and    status='PENDING'
and  (regexp_like(target_schema, '^CDR_20\d{4}$') or regexp_like(target_schema, '^CDR_DELTA_20\d{4}$'))
and rownum = 1;
quit
ENDSQL
)
rc=$?
hadoop_jobs=$(echo "${hadoop_jobs}" | tr -d " ")

for job in ${hadoop_jobs}
do
        echo "1|"$job
        set colsep '|'
        IFS='|' read -r target_schema groupid client_ds_id client_ds_name emr_pkg_name ext_stage_location delta_start_date delta_end_date run_mode<<< ${job}
        for x in $(IFS='/';echo $ext_stage_location);
        do
                if ! case $x in H[0-9]*) ;; *) false;; esac; then
		x=$(echo "${x}" | tr '[A-Z]' '[a-z]');
                fi
                ext_stage_location_fin+="$x/"
        done
        run_mode=$(echo "${run_mode}" | tr '[A-Z]' '[a-z]');
        case "$run_mode" in
        *delta*)
                delta_start_date="-b ${delta_start_date}"
                delta_end_date="-f ${delta_end_date}"
        ;;
        *fdr*)
                delta_start_date="-b ${delta_start_date}"
                delta_end_date="-f ${delta_end_date}"
        ;; *)
                delta_start_date=""
                delta_end_date=""
        esac
        case "$emr_pkg_name" in *@*)
                collection=$emr_pkg_name
                emr_pkg_name=$(echo $emr_pkg_name | cut -f1 -d'@')
                ${bp}/BuildEmr.sh -c $collection -o $target_schema  $delta_start_date $delta_end_date $emr_pkg_name  $groupid $client_ds_id  $client_ds_name /${ext_stage_location_fin} $run_mode ${target_schema}.cfg &
        ;; *)
                ${bp}/BuildEmr.sh  -o $target_schema  $delta_start_date $delta_end_date $emr_pkg_name  $groupid $client_ds_id  $client_ds_name /${ext_stage_location_fin} $run_mode ${target_schema}.cfg &
        ;;
        esac
done
       
