#!/usr/bin/env bash


#!/bin/bash
#export SQOOP_HOME=/usr/bin/sqoop/
#export PATH=$SQOOP_HOME/bin:$PATH
#export HADOOP_OPTS="-Djava.security.egd=file:/dev/../dev/urandom -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts=-Xmx2048m"
sudo -u mercury sqoop import -Dmapred.child.java.opts=-Xmx2g -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts=-Xmx2048m -Doraoop.timestamp.string=false -Doraoop.jdbc.url.verbatim=true -Djava.security.egd=file:/dev/../dev/urandom \
        --direct \
        --connect "$4" \
        --username "$5" \
        --password "$6" \
        --table "$2"."$1" \
        --split-by "$7" \
        --target-dir "/optum/crossix/data/rxorder/monthly/$3/$1" \
        --fetch-size 3000 \
        --as-parquetfile \
        --verbose \
        --num-mappers 16 \
        --columns $8

