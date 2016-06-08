#!/bin/bash

set -o xtrace

source ./server_list.sh

java -cp `echo lib/*.jar | tr ' ' ':'` \
-Dmsr.src.url=jdbc:mysql://$HOST_DB_SRC:3306/adt_test \
-Dmsr.src.username=adt \
-Dmsr.src.password=adt \
 \
-Dmsr.dest.count=2 \
 \
-Dmsr.dest.0.url=jdbc:mysql://$HOST_DB_DEST1:3306/adt_test \
-Dmsr.dest.0.username=adt \
-Dmsr.dest.0.password=adt \
 \
-Dmsr.dest.1.url=jdbc:mysql://$HOST_DB_DEST2:3306/adt_test \
-Dmsr.dest.1.username=adt \
-Dmsr.dest.1.password=adt \
 \
-Dmsr.tool.dmlQueryTool.threadCount=512 \
-Dmsr.tool.dmlQueryTool.dbcp2.maxTotal=1024 \
-Dmsr.tool.dmlQueryTool.dbcp2.maxIdle=1024 \
-Dmsr.tool.dmlQueryTool.dbcp2.minIdle=0 \
 \
-Dmsr.tool.dmlQueryTool.tableCount=2 \
 \
-Dmsr.tool.dmlQueryTool.table.0.maxNoValue=1000 \
-Dmsr.tool.dmlQueryTool.table.0.maxSeqValue=1000 \
-Dmsr.tool.dmlQueryTool.table.0.maxUkValue=1000 \
 \
-Dmsr.tool.dmlQueryTool.table.0.queryRatio.insert=1 \
-Dmsr.tool.dmlQueryTool.table.0.queryRatio.update=1 \
-Dmsr.tool.dmlQueryTool.table.0.queryRatio.delete=1 \
-Dmsr.tool.dmlQueryTool.table.0.queryRatio.replace=1 \
-Dmsr.tool.dmlQueryTool.table.0.queryRatio.insertIgnore=1 \
-Dmsr.tool.dmlQueryTool.table.0.queryRatio.insertOnDupKey=1 \
 \
-Dmsr.tool.dmlQueryTool.table.1.maxNoValue=100000 \
-Dmsr.tool.dmlQueryTool.table.1.maxSeqValue=100000 \
-Dmsr.tool.dmlQueryTool.table.1.maxUkValue=100000 \
 \
-Dmsr.tool.dmlQueryTool.table.1.queryRatio.insert=1 \
-Dmsr.tool.dmlQueryTool.table.1.queryRatio.update=1 \
-Dmsr.tool.dmlQueryTool.table.1.queryRatio.delete=1 \
-Dmsr.tool.dmlQueryTool.table.1.queryRatio.replace=1 \
-Dmsr.tool.dmlQueryTool.table.1.queryRatio.insertIgnore=1 \
-Dmsr.tool.dmlQueryTool.table.1.queryRatio.insertOnDupKey=1 \
 \
com.kakao.adt.test.tool.DataIntegrityTestTool
