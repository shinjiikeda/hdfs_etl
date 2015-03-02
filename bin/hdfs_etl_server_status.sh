#!/bin/sh

BIN_DIR=`dirname $0`
HOME_DIR=$BIN_DIR/..
CONF_DIR=$HOME_DIR/conf

if [ "$LOG_DIR" == "" ]; then
  LOG_DIR=/tmp/
fi

if [ -f $CONF_DIR/setenv.sh ] ;then
  . $CONF_DIR/setenv.sh
fi

PID_FILE=$LOG_DIR/hdfs_etl.pid

if [ -f $PID_FILE ]; then
  if [ -f /proc/$(cat $PID_FILE)/status ] ;then
    echo hdfs_etl is running..
    exit 0
  fi
  echo hdfs_etl is no running..
  exit 1
fi


