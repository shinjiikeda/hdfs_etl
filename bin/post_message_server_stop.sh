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

PID_FILE=$LOG_DIR/post_message.pid

if [ -f $PID_FILE ]; then
  pid=`cat $PID_FILE`
  kill $pid
  
  # wait 3min
  for i in {0..17}; do
    if [ ! -f /proc/$pid/status ];then
      break
    fi
    echo -n "."
    sleep 10
  done
  echo ""
   
  rm -f $PID_FILE
else
  echo "not running.."
fi



