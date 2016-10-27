#!/bin/sh

if [ $# -lt 1 ]
then
  echo "usage:bin/etl-flume.sh configFile"
  echo "ex:bin/etl-flume.sh ./conf/file2kafka-S1udns.conf"
  exit -1
fi

BASEDIR=`dirname "$0"`/..
cd $BASEDIR

#bin/flume-ng agent --conf ./conf/ -n etlAgent -f conf/file2kafka-S1udns.conf
bin/flume-ng agent --conf ./conf/ -n etlAgent -f $@