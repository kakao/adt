#!/bin/bash

set -e
set -o xtrace

source `dirname $0`/server_list.sh

LOG_FILE_TIME=`date +%Y%m%d_%H%M%S`
cd `dirname $0`/../../
ADT_HOME=`pwd`
DEPLOY_DIR="~/adt_shard_rebalancer_test"

function deploy {

  DEPLOY_SERVER=$1

  ssh $DEPLOY_SERVER "mkdir -p $DEPLOY_DIR/lib"
  ssh $DEPLOY_SERVER "mkdir -p $DEPLOY_DIR/logs"
  ssh $DEPLOY_SERVER "rm -rf $DEPLOY_DIR/lib/*"

  rsync -avzr $ADT_HOME/adt-handler-parent/adt-handler-mysql-shard-rebalancer/target/jar/* $DEPLOY_SERVER:$DEPLOY_DIR/lib
  rsync -avzr $ADT_HOME/adt-test/target/jar/*.jar $DEPLOY_SERVER:$DEPLOY_DIR/lib
  rsync -avzr $ADT_HOME/adt-test/msr_test_script/* $DEPLOY_SERVER:$DEPLOY_DIR/

}

function startup {
  DEPLOY_SERVER=$1
  EXEC_CMD=$2
  ssh $DEPLOY_SERVER "cd $DEPLOY_DIR; $EXEC_CMD"
}

###################################################################################

cd $ADT_HOME
mvn -DskipTests=true clean package
cd ./adt-handler-parent/adt-handler-mysql-shard-rebalancer

cd $ADT_HOME/adt-test/msr_test_script
sed "s/\"host\".*$/\"host\": \"$HOST_DB_SRC\",/g" msr_binlog_test_config_template.json > msr_binlog_test_config.json

deploy $HOST_WORKER &
deploy $HOST_DML_TOOL1 &
deploy $HOST_DML_TOOL2 &
deploy $HOST_DML_TOOL3 &
deploy $HOST_DB_SRC &
deploy $HOST_DB_DEST1 &
deploy $HOST_DB_DEST2 &

wait

echo "SUCCESS"
