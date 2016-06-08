#!/bin/bash

set -o xtrace

source ./server_list.sh

java -cp `echo lib/*.jar | tr ' ' ':'` \
-Dcom.sun.management.jmxremote=true \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.port=12345 \
\
-Dcom.kakao.adt.worker.type=MysqlBinlogReceiver \
-Dcom.kakao.adt.worker.configFilePath=msr_binlog_test_config.json \
\
-server -XX:+UseG1GC -XX:MaxHeapSize=8G -XX:NewSize=7G -XX:MaxTenuringThreshold=15 \
\
-Dcom.kakao.adt.handler.msr.testMode=false \
-Dcom.kakao.adt.handler.msr.shardCount=2 \
"-Dcom.kakao.adt.handler.msr.shard.0.url=jdbc:mysql://$HOST_DB_DEST1:3306/adt_test" \
"-Dcom.kakao.adt.handler.msr.shard.1.url=jdbc:mysql://$HOST_DB_DEST2:3306/adt_test" \
-Dcom.kakao.adt.handler.msr.shard.0.username=adt \
-Dcom.kakao.adt.handler.msr.shard.1.username=adt \
-Dcom.kakao.adt.handler.msr.shard.0.password=adt \
-Dcom.kakao.adt.handler.msr.shard.1.password=adt \
com.kakao.adt.WorkerMain
