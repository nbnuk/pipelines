#!/usr/bin/env bash

FS_PATH="hdfs:///"
DATA_PATH="pipelines-data"
MIGRATION_JAR="/usr/share/la-pipelines/migration.jar"

echo 'Setting up HDFS directories'    
hdfs dfs -mkdir -p /pipelines-data
hdfs dfs -chmod 777 /pipelines-data

echo 'Running spark uuid migration job'

/usr/bin/spark-submit \
--name "Migrate UUIDs" \
--conf spark.default.parallelism=48 \
--num-executors 8 \
--executor-cores 8 \
--executor-memory 18G \
--driver-memory 2G \
--class au.org.ala.pipelines.spark.MigrateUUIDPipeline \
--driver-java-options "-Dlog4j.configuration=file:/data/la-pipelines/config/log4j.properties" \
$MIGRATION_JAR \
--occUuidExportPath=$FS_PATH/migration/occ_uuid.csv.gz \
--occFirstLoadedExportPath=$FS_PATH/migration/occ_first_loaded_date.csv.gz \
--targetPath=$FS_PATH/$DATA_PATH \

echo 'List HDFS /$DATA_PATH'
hdfs dfs -ls /$DATA_PATH