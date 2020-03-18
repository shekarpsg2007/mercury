#!/usr/bin/env bash

export SPARK_HOME=/usr/hdp/current/spark2-client
export SPARK_MAJOR_VERSION=2

spark-submit --class com.humedica.mercury.etl.crossix.main.Main --master yarn --num-executors 40 --executor-cores 16 --executor-memory 8G --driver-memory 8G --jars /users/fdr/apps/launcher/lib/config-1.3.0.jar,/users/rbabu/lib/mercury-etl-core-1.0-SNAPSHOT.jar,/users/rbabu/lib/mercury-etl-crossix-1.0.jar /users/rbabu/lib/mercury-etl-crossix-1.0.jar
