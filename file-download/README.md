# File download

This job for the the Saagie DataFabric allows the following steps:
* downloading a file from a source accessible by http
* vaidate the content of the file using an avro schema (optional)
* copy the source file into HDFS
* create a Hive table (optional)

Parameters:

```sh
 --hdfs-url VAL       : Set HDFS URL (default: hdfs://nn1:8020)
 --hdfs-user VAL      : Set HDFS user (default: hdfs)
 --hive-pwd VAL       : Set Hive password (default: )
 --hive-table VAL     : Set Hive table to create (database.table)
 --hive-url VAL       : Set Hive JDBC url (default: jdbc:hive2://nn1:10000/;ssl=
                        false)
 --hive-user VAL      : Set Hive user (default: )
 --schema-file VAL    : Set schema file name (in HDFS only)
 --source-charset VAL : Set source charset (default: UTF-8)
 --source-delim VAL   : Set source delimiter (csv) (default: ,)
 --source-esc VAL     : Set source escape char (csv) (default: \)
 --source-quote VAL   : Set source quote (csv) (default: ")
 --source-with-header : Set source with header (csv) (default: false)
 -d VAL               : Set destination file name (in HDFS only)
 -f [csv | json]      : Set source format
 -s VAL               : Set source url
```


Example:

```sh
java -jar {file} \
  --hdfs-url hdfs://192.168.54.10:8020 \
  --hdfs-user hdfs \
  --hive-table default.cross_operations \
  --hive-url 'jdbc:hive2://192.168.54.10:10000/;ssl=false' \
  --hive-user bobmorane \
  --hive-pwd levraiherosdetouslestemps \
  --schema-file /data/schemas/cross-operations.avsc \
  --source-charset UTF-8 \
  --source-delim "," \
  --source-esc "\\" \
  --source-quote "\"" \
  --source-with-header \
  -d /data/datagouv/cross/operations/extract_operations.csv \
  -f csv \
  -s https://raw.githubusercontent.com/bbonnin/datasets/master/datagouv/cross/extract_operations.csv
```

