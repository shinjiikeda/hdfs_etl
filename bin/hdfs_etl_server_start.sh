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

if [ ! -f $LOG_DIR ]; then
  mkdir -p $LOG_DIR
fi

PID_FILE=$LOG_DIR/hdfs_etl.pid

if [ -f $PID_FILE ]; then
  echo "$0 is already running.. pid: `cat $PID_FILE`, pid file: $PID_FILE"
  echo "pid: `cat $PID_FILE`, pid file: $PID_FILE"
  exit
fi

DAEMON_OPT="--daemon"
ETL_OPTS="$DAEMON_OPT --logfile $LOG_DIR/hdfs_etl.log --conf $BIN_DIR/../conf/config.rb"

export RUBYLIB=$HOME_DIR/lib

echo -n "start.."
cd $HOME_DIR

bundle exec jruby $JRUBY_OPTS $BIN_DIR/hdfs_etl.rb $ETL_OPTS >$LOG_DIR/hdfs_etl.out 2>&1 &
if [ $? == 0 ]; then
  echo $! > $PID_FILE
  echo "success"
else
  echo "failed"
  exit 1
fi

