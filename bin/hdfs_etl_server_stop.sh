#!/bin/sh

BIN_DIR=`dirname $0`
HOME_DIR=$BIN_DIR/..
CONF_DIR=$HOME_DIR/conf

if [ -f $CONF_DIR/setenv.sh ] ;then
  . $CONF_DIR/setenv.sh
fi

if [ "$LOG_DIR" == "" ]; then
  LOG_DIR=/tmp/
fi

PID_FILE=$LOG_DIR/hdfs_etl.pid

if [ -f $PID_FILE ]; then
  pid=`cat $PID_FILE`
  kill $pid
  
  rm -f $PID_FILE
else
  echo "not running.."
fi



