#!/bin/bash

java -jar target/file-download-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --hdfs-url hdfs://192.168.54.10:8020 \
  --hdfs-user hdfs \
  --hive-table default.cross_operations \
  --hive-url 'jdbc:hive2://192.168.54.10:10000/;ssl=false' \
  --hive-user $1 \
  --hive-pwd $2 \
  --schema-file /data/schemas/cross-operations.avsc \
  --source-charset UTF-8 \
  --source-delim "," \
  --source-esc "\\" \
  --source-quote "\"" \
  --source-with-header \
  -d /data/datagouv/cross/operations/extract_operations.csv \
  -f csv \
  -s https://raw.githubusercontent.com/bbonnin/datasets/master/datagouv/cross/extract_operations.csv

