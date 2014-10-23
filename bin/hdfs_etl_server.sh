#!/bin/sh

export HADOOP_HOME=/usr/lib/hadoop

BIN_DIR=`dirname $0`
WORK_DIR=$BIN_DIR/..
LOG_DIR=/tmp/

JRUBY_OPTS="-J-server -J-Xmx2048m -J-Xms512m -J-XX:MinHeapFreeRatio=10 -J-XX:MaxHeapFreeRatio=50"
DAEMON_OPT="--daemon"
ETL_OPTS="$DAEMON_OPT --logfile $LOG_DIR/hdfs_etl.log --conf $BIN_DIR/../conf/config.rb"

export RUBYLIB=$WORK_DIR/lib

cd $WORK_DIR
bundle exec jruby $JRUBY_OPTS $BIN_DIR/hdfs_etl.rb $ETL_OPTS >$LOG_DIR/hdfs_etl.out 2>&1 &

