#!/bin/sh

if [ $# -lt 2 ]
then
  echo "usage:bin/etl-kafkastream.sh configFile streamAgentNo"
  exit -1
fi

BASEDIR=`dirname "$0"`/..
cd $BASEDIR
configFile=$1
no=$2
  
BIGDATA_CLASSPATH="$BASEDIR/conf:$BASEDIR/lib/"
for i in "$BASEDIR"/lib/*.jar
do
  BIGDATA_CLASSPATH="$BIGDATA_CLASSPATH:$i"
done

#RUN_CMD="\"$JAVA_HOME/bin/java\""
RUN_CMD="java "
RUN_CMD="$RUN_CMD -classpath \"$BIGDATA_CLASSPATH\""
RUN_CMD="$RUN_CMD -Xmx8G -Xms8G "
((port=19877+$no))
RUN_CMD="$RUN_CMD -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false "
RUN_CMD="$RUN_CMD -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=$port "

logConf="./conf/instance/log4j"$no".properties"
sed "s:LOG_FILE:/data/kafkastream/logs/stream${no}.log:g" ./conf/log4j.properties > $logConf
RUN_CMD="$RUN_CMD -Dlog4j.configuration=file:$logConf "
RUN_CMD="$RUN_CMD com.gsta.bigdata.stream.Application $configFile"
  
echo $RUN_CMD
eval $RUN_CMD
